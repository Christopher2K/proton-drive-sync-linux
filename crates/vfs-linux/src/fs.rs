//! FUSE filesystem presenting the remote tree with on-demand placeholders.
//!
//! Structure (lookup/getattr/readdir) is served from the engine's [`Store`]
//! snapshot — no Drive traffic, no local copies. Opening a placeholder
//! queues a hydrate request; the first `read()` blocks (bounded) until the
//! engine downloads the backing file, then serves bytes from it.
//!
//! Writes go through to Drive (M3.2): `create` makes an ephemeral backing
//! file, the final `write`/`setattr` queues an upload through the engine's
//! normal machinery (F7, retries, pause), `unlink`/`rmdir` queue remote
//! trashes, `mkdir` queues remote folder creation. Ephemeral (not yet
//! uploaded) files appear via a write-overlay the tree consults before the
//! snapshot. `.pds` is answered from the real `<root>/.pds` metadata so
//! the CLI control socket stays reachable through the mount.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use fuser::{
    Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo, KernelConfig,
    OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyXattr, Request,
};
use pds_core::store::Store;
use pds_core::to_abs;
use tracing::{debug, warn};

use crate::tree::{Tree, STATE_PROXY};

/// Upper bound a `read()` waits for one hydration. Generous: fetching a
/// large file on explicit user action beats an EIO, but nothing hangs
/// forever on a dead engine.
const HYDRATE_WAIT: Duration = Duration::from_secs(120);
/// Poll slice while waiting (the store is a plain mutex; no condvar).
const HYDRATE_POLL: Duration = Duration::from_millis(50);
/// Attribute/entry validity window. Short on purpose: hydrations and
/// remote events must become visible promptly.
const TTL: Duration = Duration::from_secs(1);
/// Write-overlay inodes start here (snapshot inos grow from 3; a session
/// would need millions of nodes to collide).
const OVERLAY_INO_BASE: u64 = 1 << 40;

pub struct PdsFs {
    tree: Tree,
    store: Store,
    /// Real `<root>/.pds` directory, answered at the `.pds` mount entry.
    state_dir: PathBuf,
    /// Backing dir root (`<root>/.pds-vfs-backing`) — where ephemeral
    /// write-through files are created before upload.
    backing_dir: PathBuf,
    /// Write-overlay entries created through the mount that have no
    /// snapshot entry yet. Files upload on close; dirs appear in the
    /// engine's next snapshot. The `ino → rel` side makes every overlay
    /// inode resolvable (lookup, getattr, release, unlink…).
    overlay: Mutex<Overlay>,
    /// Open write handles: fh → file + dirty flag.
    handles: Mutex<HashMap<u64, WriteHandle>>,
    next_fh: std::sync::atomic::AtomicU64,
    uid: u32,
    gid: u32,
}

/// A file created through the mount, not yet in any snapshot.
#[derive(Debug, Clone)]
struct WriteEntry {
    ino: INodeNo,
    size: u64,
    mtime: i64,
    is_dir: bool,
}

/// All write-overlay entries: rel → entry. Files stay until their engine
/// snapshot row replaces them; dirs until their snapshot row appears.
/// `ino → rel` is kept in sync so any op on an overlay inode resolves.
#[derive(Debug, Default)]
struct Overlay {
    by_rel: HashMap<String, WriteEntry>,
    by_ino: HashMap<u64, String>,
}

impl Overlay {
    fn insert(&mut self, rel: &str, entry: WriteEntry) {
        self.by_ino.insert(entry.ino.0, rel.to_string());
        self.by_rel.insert(rel.to_string(), entry);
    }
    /// Gone entirely (unlinked before upload).
    fn remove(&mut self, rel: &str) {
        if let Some(e) = self.by_rel.remove(rel) {
            self.by_ino.remove(&e.ino.0);
        }
    }
    /// Snapshot now covers `rel`: drop it from listings but KEEP the
    /// ino→rel alias — the kernel may still hold a dentry with the
    /// overlay inode (TTL window), and ops on it must resolve.
    fn retire(&mut self, rel: &str) {
        self.by_rel.remove(rel);
    }
}

/// Per-open state of a write handle.
#[derive(Debug)]
struct WriteHandle {
    file: File,
    dirty: bool,
}

impl PdsFs {
    pub fn new(store: Store, state_dir: PathBuf, backing_dir: PathBuf) -> Self {
        // nolibc shims: fuser depends on libc anyway.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Self {
            tree: Tree::new(),
            store,
            state_dir,
            backing_dir,
            overlay: Mutex::new(Overlay::default()),
            handles: Mutex::new(HashMap::new()),
            next_fh: std::sync::atomic::AtomicU64::new(1),
            uid,
            gid,
        }
    }

    fn attr_for(&self, ino: INodeNo, is_dir: bool, size: u64, mtime: i64) -> FileAttr {
        let when = unix_time(mtime);
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: when,
            mtime: when,
            ctime: when,
            crtime: when,
            kind: if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: if is_dir { 0o755 } else { 0o644 },
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Block until the backing file for `rel` exists or the deadline passes.
    fn wait_for_backing(&self, rel: &str) -> Option<File> {
        // Ephemeral write files are their own backing content.
        if self.overlay_entry(rel).is_some() && let Some(h) = self.open_backing(rel) {
            return Some(h);
        }
        self.store.begin_hydrate(rel);
        let deadline = std::time::Instant::now() + HYDRATE_WAIT;
        loop {
            if let Some(path) = self.store.backing_path(rel)
                && let Ok(file) = File::open(&path)
            {
                return Some(file);
            }
            if std::time::Instant::now() >= deadline {
                warn!(path = %rel, "hydration wait expired");
                return None;
            }
            std::thread::sleep(HYDRATE_POLL);
        }
    }

    /// Open a backing file read-only (write-created files only: hydrated
    /// content goes through the store's registered path).
    fn open_backing(&self, rel: &str) -> Option<File> {
        File::open(to_abs(&self.backing_dir, rel)).ok()
    }

    /// Now as unix seconds (write timestamps).
    fn now_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Metadata for an overlay entry (file or dir), if `rel` is one.
    fn overlay_entry(&self, rel: &str) -> Option<WriteEntry> {
        self.overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .get(rel)
            .cloned()
    }

    /// Inode → path, consulting the write overlay first (overlay inos are
    /// not in the tree), then the snapshot tree. `""` is the root.
    fn rel_of_any(&self, ino: INodeNo) -> Option<String> {
        if ino >= INodeNo(OVERLAY_INO_BASE) {
            return self
                .overlay
                .lock()
                .expect("overlay mutex")
                .by_ino
                .get(&ino.0)
                .cloned();
        }
        self.tree.rel_of(ino)
    }

    /// Drop overlay entries the snapshot now covers (uploaded file, mirrored
    /// mkdir): the tree's row is authoritative and serving both would list
    /// the path twice. The ino→rel alias stays so kernel dentries still
    /// holding the overlay inode keep resolving during the TTL window.
    /// Cheap: overlay holds only this session's writes.
    fn reconcile_overlay(&self) {
        let mut overlay = self.overlay.lock().expect("overlay mutex");
        let covered: Vec<String> = overlay
            .by_rel
            .keys()
            .filter(|rel| self.tree.node(rel).is_some())
            .cloned()
            .collect();
        for rel in covered {
            overlay.retire(&rel);
        }
    }

    /// Tree lookup that also consults the write overlay. Returns
    /// (is_dir, size, mtime, content_ready) for `rel`, or None.
    fn node_meta(&self, rel: &str) -> Option<(bool, u64, i64, bool)> {
        if let Some(e) = self.overlay_entry(rel) {
            return Some((e.is_dir, e.size, e.mtime, true));
        }
        if rel == STATE_PROXY {
            return Some((true, 0, 0, true));
        }
        if rel.is_empty() {
            return Some((true, 0, 0, true));
        }
        self.tree
            .node(rel)
            .map(|n| (n.is_dir, n.size, n.mtime, n.content_ready))
    }

    /// A finished write (release with dirty data, or fsync): fix up the
    /// overlay size and hand the file to the engine for upload. The rel
    /// comes from [`Self::rel_of_any`] — overlay inos resolve here too.
    fn finalize_write(&self, rel: &str, size: u64) {
        if rel.is_empty() {
            return;
        }
        if let Some(entry) = self
            .overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .get_mut(rel)
        {
            entry.size = size;
            entry.mtime = self.now_secs();
        }
        self.store.record_write(rel);
    }
}

fn unix_time(secs: i64) -> std::time::SystemTime {
    if secs > 0 {
        std::time::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        std::time::UNIX_EPOCH
    }
}

impl fuser::Filesystem for PdsFs {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        // Prime the tree so the mount is browsable immediately.
        self.tree.refresh(&self.store.snapshot());
        Ok(())
    }

    fn destroy(&mut self) {
        debug!("vfs: unmounted");
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        // Refresh + overlay reconciliation: both cheap (generation check /
        // tiny map) and keep uploaded files from being listed twice.
        self.tree.refresh(&self.store.snapshot());
        self.reconcile_overlay();
        // Overlay children (write-created, not yet in the snapshot) are not
        // tree entries: build the rel manually when the name matches.
        let rel = match self.tree.child_rel(parent, name) {
            Some(rel) => rel,
            None => {
                let name = name.to_str().unwrap_or("");
                if name.is_empty() || name.contains('/') {
                    return reply.error(Errno::ENOENT);
                }
                let parent_rel = self.rel_of_any(parent).unwrap_or_default();
                if parent_rel.is_empty() {
                    name.to_string()
                } else {
                    format!("{parent_rel}/{name}")
                }
            }
        };
        let Some((is_dir, size, mtime, _ready)) = self.node_meta(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        let ino = if rel == STATE_PROXY {
            let path = self.state_dir.join(name);
            let Ok(meta) = std::fs::metadata(&path) else {
                return reply.error(Errno::ENOENT);
            };
            let _ = meta;
            self.tree.ino_of(&rel)
        } else if let Some(e) = self.overlay_entry(&rel) {
            e.ino
        } else {
            self.tree.ino_of(&rel)
        };
        reply.entry(&TTL, &self.attr_for(ino, is_dir, size, mtime), Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        self.tree.refresh(&self.store.snapshot());
        self.reconcile_overlay();
        let Some(rel) = self.rel_of_any(ino) else {
            return reply.error(Errno::ENOENT);
        };
        if rel == STATE_PROXY {
            let Ok(meta) = std::fs::metadata(&self.state_dir) else {
                return reply.error(Errno::ENOENT);
            };
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return reply.attr(&TTL, &self.attr_for(ino, true, 0, mtime));
        }
        match self.node_meta(&rel) {
            Some((is_dir, size, mtime, _ready)) => {
                reply.attr(&TTL, &self.attr_for(ino, is_dir, size, mtime))
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        reply.ok(); // we report 0644/0755 owned by the user; let it pass
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        self.tree.refresh(&self.store.snapshot());
        self.reconcile_overlay();
        let Some(children) = self.tree.children(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let parent_ino = self.tree.parent_ino(ino);
        let mut entries: Vec<(INodeNo, FileType, String)> = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for child in children {
            let Some(node) = self.tree.node(&child) else {
                continue;
            };
            let name = child.rsplit('/').next().unwrap_or(&child).to_string();
            let kind = if node.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push((node.ino, kind, name));
        }
        // Write-overlay entries created in this directory (not yet in the
        // snapshot). Prefix check keeps this O(small).
        let overlay = self.overlay.lock().expect("overlay mutex");
        let dir_prefix = self
            .rel_of_any(ino)
            .map(|r| if r.is_empty() { String::new() } else { format!("{r}/") });
        if let Some(prefix) = dir_prefix {
            for (rel, entry) in overlay.by_rel.iter() {
                let Some(name) = rel.strip_prefix(&prefix) else {
                    continue;
                };
                if name.contains('/') {
                    continue;
                }
                let kind = if entry.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };
                entries.push((entry.ino, kind, name.to_string()));
            }
        }
        drop(overlay);
        for (index, (child_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*child_ino, (index + 1) as u64, *kind, name) {
                break; // buffer full; kernel re-asks from the given offset
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.tree.refresh(&self.store.snapshot());
        self.reconcile_overlay();
        let Some(rel) = self.rel_of_any(ino) else {
            return reply.error(Errno::ENOENT);
        };
        if rel == STATE_PROXY {
            return reply.error(Errno::EISDIR);
        }
        let Some((is_dir, _size, _mtime, content_ready)) = self.node_meta(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if is_dir {
            return reply.opened(FileHandle(0), FopenFlags::empty());
        }
        use fuser::OpenAccMode;
        if flags.acc_mode() != OpenAccMode::O_RDONLY && self.overlay_entry(&rel).is_none() {
            // Existing hydrated/snapshot files stay read-only (M3.3 adds
            // in-place revision writes; ephemeral write files are RW).
            return reply.error(Errno::EROFS);
        }
        // Kick the engine early; the bounded wait happens in read().
        if !content_ready && self.overlay_entry(&rel).is_none() {
            self.store.begin_hydrate(&rel);
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let Some(rel) = self.rel_of_any(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let Some((is_dir, node_size, _mtime, _ready)) = self.node_meta(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if is_dir {
            return reply.error(Errno::EISDIR);
        }
        let Some(mut file) = self.wait_for_backing(&rel) else {
            return reply.error(Errno::EIO);
        };
        if offset >= node_size {
            return reply.data(&[]);
        }
        if file.seek(SeekFrom::Start(offset)).is_err() {
            return reply.error(Errno::EIO);
        }
        let mut buf = vec![0u8; size as usize];
        match file.read(&mut buf) {
            Ok(n) => reply.data(&buf[..n]),
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _flags: fuser::WriteFlags,
        _open_flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let handles = self.handles.lock().expect("handles mutex");
        let Some(handle) = handles.get(&fh.0) else {
            return reply.error(Errno::EIO);
        };
        let mut file = handle.file.try_clone().expect("file clone");
        drop(handles);
        if file.seek(SeekFrom::Start(offset)).is_err() {
            return reply.error(Errno::EIO);
        }
        match file.write_all(data) {
            Ok(()) => {
                if let Some(h) = self.handles.lock().expect("handles mutex").get_mut(&fh.0) {
                    h.dirty = true;
                }
                // Keep the overlay size current for getattr while open.
                if let Ok(meta) = file.metadata()
                    && let Some(rel) = self.rel_of_any(ino)
                    && let Some(entry) = self
                        .overlay
                        .lock()
                        .expect("overlay mutex")
                        .by_rel
                        .get_mut(&rel)
                {
                    entry.size = entry.size.max(meta.len());
                }
                reply.written(data.len() as u32)
            }
            Err(e) => {
                warn!(error = %e, "vfs write failed");
                reply.error(Errno::EIO)
            }
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        // Sync to disk; the upload happens on release (final close) or
        // fsync. Flush fires before every close — uploading here would
        // make `echo x > f` upload twice.
        if let Some(handle) = self.handles.lock().expect("handles mutex").get(&fh.0) {
            handle.file.sync_all().ok();
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!(fh = fh.0, "vfs: release");
        let handle = self.handles.lock().expect("handles mutex").remove(&fh.0);
        if let Some(h) = handle
            && h.dirty
        {
            let size = h.file.metadata().map(|m| m.len()).unwrap_or(0);
            h.file.sync_all().ok();
            drop(h);
            if let Some(rel) = self.rel_of_any(ino) {
                self.finalize_write(&rel, size);
            }
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let upload = self
            .handles
            .lock()
            .expect("handles mutex")
            .get(&fh.0)
            .map(|h| (h.dirty, h.file.metadata().map(|m| m.len()).unwrap_or(0)));
        if let Some((dirty, size)) = upload {
            if let Some(handle) = self.handles.lock().expect("handles mutex").get(&fh.0) {
                handle.file.sync_all().ok();
            }
            if dirty {
                if let Some(h) = self.handles.lock().expect("handles mutex").get_mut(&fh.0) {
                    h.dirty = false;
                }
                if let Some(rel) = self.rel_of_any(ino) {
                    self.finalize_write(&rel, size);
                }
            }
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Placeholder sizes make free-space math meaningless; report an
        // effectively unlimited filesystem so tools don't refuse work.
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 4096);
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!(parent = parent.0, name = ?name, flags = flags, "vfs: create");
        let Some(parent_rel) = self.rel_of_any(parent) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(name) = name.to_str() else {
            return reply.error(Errno::EINVAL);
        };
        if name.contains('/') || name == "." || name == ".." || name.starts_with(".pdsw-") {
            return reply.error(Errno::EINVAL);
        }
        let rel = if parent_rel.is_empty() {
            name.to_string()
        } else {
            format!("{parent_rel}/{name}")
        };
        if self.node_meta(&rel).is_some() {
            return reply.error(Errno::EEXIST);
        }
        // Ephemeral backing file; the engine uploads it on release.
        let abs = to_abs(&self.backing_dir, &rel);
        if let Some(dir) = abs.parent()
            && let Err(e) = std::fs::create_dir_all(dir)
        {
            warn!(error = %e, path = %rel, "backing mkdir failed");
            return reply.error(Errno::EIO);
        }
        let file = match std::fs::File::create(&abs) {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, path = %rel, "backing create failed");
                return reply.error(Errno::EIO);
            }
        };
        let fh = self
            .next_fh
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mtime = self.now_secs();
        // Inode numbering beyond the tree's range: overlay inos start high
        // to avoid colliding with snapshot-assigned ones for this session.
        let ino = INodeNo(OVERLAY_INO_BASE + fh);
        self.overlay.lock().expect("overlay mutex").insert(
            &rel,
            WriteEntry {
                ino,
                size: 0,
                mtime,
                is_dir: false,
            },
        );
        self.handles
            .lock()
            .expect("handles mutex")
            .insert(fh, WriteHandle { file, dirty: false });
        let fopen = FopenFlags::empty();
        reply.created(
            &TTL,
            &self.attr_for(ino, false, 0, mtime),
            Generation(0),
            FileHandle(fh),
            fopen,
        );
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(parent_rel) = self.rel_of_any(parent) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(name) = name.to_str() else {
            return reply.error(Errno::EINVAL);
        };
        if name.contains('/') || name == "." || name == ".." {
            return reply.error(Errno::EINVAL);
        }
        let rel = if parent_rel.is_empty() {
            name.to_string()
        } else {
            format!("{parent_rel}/{name}")
        };
        if self.node_meta(&rel).is_some() {
            return reply.error(Errno::EEXIST);
        }
        // Local backing dir entry (so nested creates have somewhere to
        // land) + queue the remote folder for the engine. The folder
        // becomes visible via the engine's next snapshot (which includes
        // the DB row the engine creates); until then overlay it so the
        // user sees their mkdir immediately.
        let abs = to_abs(&self.backing_dir, &rel);
        if let Err(e) = std::fs::create_dir_all(&abs) {
            warn!(error = %e, path = %rel, "backing mkdir failed");
            return reply.error(Errno::EIO);
        }
        self.store.record_mkdir(&rel);
        let ino = {
            let mut overlay = self.overlay.lock().expect("overlay mutex");
            let max_ino = overlay
                .by_ino
                .keys()
                .filter(|k| **k >= OVERLAY_INO_BASE)
                .max()
                .copied()
                .unwrap_or(OVERLAY_INO_BASE);
            let ino = INodeNo(max_ino + 1);
            overlay.insert(
                &rel,
                WriteEntry {
                    ino,
                    size: 0,
                    mtime: self.now_secs(),
                    is_dir: true,
                },
            );
            ino
        };
        let mtime = self.now_secs();
        reply.entry(&TTL, &self.attr_for(ino, true, 0, mtime), Generation(0));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        debug!(parent = parent.0, name = ?name, "vfs: unlink");
        let Some(parent_rel) = self.rel_of_any(parent) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(name) = name.to_str() else {
            return reply.error(Errno::EINVAL);
        };
        let rel = if parent_rel.is_empty() {
            name.to_string()
        } else {
            format!("{parent_rel}/{name}")
        };
        if self.node_meta(&rel).is_none() {
            return reply.error(Errno::ENOENT);
        }
        // Drop local state; the engine mirrors the delete remotely (harmless
        // no-op for unknown paths — handle_local_delete ignores them).
        self.overlay.lock().expect("overlay mutex").remove(&rel);
        std::fs::remove_file(to_abs(&self.backing_dir, &rel)).ok();
        self.store.forget(&rel);
        self.store.record_delete(&rel);
        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(parent_rel) = self.rel_of_any(parent) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(name) = name.to_str() else {
            return reply.error(Errno::EINVAL);
        };
        let rel = if parent_rel.is_empty() {
            name.to_string()
        } else {
            format!("{parent_rel}/{name}")
        };
        let Some((is_dir, _size, _mtime, _ready)) = self.node_meta(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        // POSIX: only empty directories are removable. Check snapshot
        // children + overlay children.
        let prefix = format!("{rel}/");
        let snapshot_children = self
            .tree
            .children(self.tree.ino_of(&rel))
            .map(|c| !c.is_empty())
            .unwrap_or(false);
        let overlay_child = self
            .overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .keys()
            .any(|child| child.starts_with(&prefix));
        if snapshot_children || overlay_child {
            return reply.error(Errno::ENOTEMPTY);
        }
        self.overlay.lock().expect("overlay mutex").remove(&rel);
        std::fs::remove_dir(to_abs(&self.backing_dir, &rel)).ok();
        self.store.forget_tree(&rel);
        self.store.record_delete(&rel);
        reply.ok();
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let Some(rel) = self.rel_of_any(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let Some((is_dir, mut cur_size, mut cur_mtime, _ready)) = self.node_meta(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        // truncate(2) on a write-created file: resize the backing file.
        if let Some(new_size) = size {
            if is_dir {
                return reply.error(Errno::EISDIR);
            }
            if self.overlay_entry(&rel).is_none() {
                // Snapshot files are read-only in M3.2.
                return reply.error(Errno::EROFS);
            }
            let abs = to_abs(&self.backing_dir, &rel);
            if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&abs)
                && let Err(e) = f.set_len(new_size)
            {
                warn!(error = %e, path = %rel, "truncate failed");
                return reply.error(Errno::EIO);
            }
            cur_size = new_size;
        }
        if let Some(t) = mtime {
            cur_mtime = match t {
                fuser::TimeOrNow::SpecificTime(st) => st
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                fuser::TimeOrNow::Now => self.now_secs(),
            };
        }
        if let Some(entry) = self
            .overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .get_mut(&rel)
        {
            entry.size = cur_size;
            entry.mtime = cur_mtime;
        }
        reply.attr(&TTL, &self.attr_for(ino, is_dir, cur_size, cur_mtime));
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::ENODATA);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.size(0);
    }
}
