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
/// Chunk size for `copy_file_range`: bounds one buffer while a large
/// server-side copy streams; typical CAD files fit in a single chunk.
const COPY_CHUNK: u64 = 1 << 20;

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

/// Per-open state of a write handle (fh ≥ 1). `overlay` marks files
/// created through the mount (no snapshot row yet); revision handles on
/// snapshot-backed files are LRU/open-tracked like read handles.
#[derive(Debug)]
struct WriteHandle {
    rel: String,
    file: File,
    dirty: bool,
    overlay: bool,
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

    /// Open `rel`'s backing file read-write, waiting (bounded) for the
    /// engine to hydrate it first. Creates the parent dirs so an unlinked-
    /// then-recreated path still works.
    fn open_backing_rw(&self, rel: &str) -> Option<File> {
        let abs = to_abs(&self.backing_dir, rel);
        let deadline = std::time::Instant::now() + HYDRATE_WAIT;
        loop {
            if let Some(dir) = abs.parent() {
                std::fs::create_dir_all(dir).ok();
            }
            if let Ok(f) = std::fs::OpenOptions::new().read(true).write(true).open(&abs) {
                return Some(f);
            }
            if std::time::Instant::now() >= deadline {
                warn!(path = %rel, "revision open: hydration wait expired");
                return None;
            }
            std::thread::sleep(HYDRATE_POLL);
        }
    }

    /// Now as unix seconds (write timestamps).
    fn now_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Snapshot children + overlay children check (rmdir and the dir
    /// overwrite arm of rename share it). A folder still only in the
    /// overlay has no snapshot children by definition.
    fn dir_is_empty(&self, rel: &str) -> bool {
        let prefix = format!("{rel}/");
        // Children with a queued delete are gone on disk; the snapshot
        // keeps serving them only until the engine drains the tombstone
        // and republishes. Counting them would hand an `rm -rf` an
        // ENOTEMPTY milliseconds after its last unlink.
        let pending_delete =
            |c: &String| -> bool { self.store.has_pending_delete(c) };
        let snapshot_child = match self.tree.ino_of_opt(rel) {
            Some(ino) => match self.tree.children(ino) {
                Some(children) => children.iter().any(|c| !pending_delete(c)),
                None => false,
            },
            None => false,
        };
        if snapshot_child {
            return false;
        }
        !self
            .overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .keys()
            .any(|c| c.starts_with(&prefix) && !pending_delete(c))
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

/// `parent` ("" for root) + `name` → rel path.
fn join_rel(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
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
            // The proxy stands for `state_dir` itself (same target as in
            // `getattr`), not for a child of it. A missing state dir must
            // ENOENT here too — otherwise root readdir advertises `.pds`
            // while every lookup of it fails (phantom entry in `ls`).
            if !self.state_dir.exists() {
                return reply.error(Errno::ENOENT);
            }
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
        let writable = flags.acc_mode() != OpenAccMode::O_RDONLY;
        let overlay = self.overlay_entry(&rel);
        if writable && overlay.is_some() {
            // Writable open of an overlay (created-this-session, not yet
            // snapshotted) file: e.g. `cp src dst` re-opened dst with
            // O_WRONLY|O_TRUNC before the upload landed. Falling through
            // to the read path returns fh 0 and no handle is recorded,
            // so every write(2) fails EIO. Serve a real write handle on
            // the backing file instead.
            let truncate = flags.0 & libc::O_TRUNC != 0;
            let abs = to_abs(&self.backing_dir, &rel);
            let file = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(truncate)
                .open(&abs)
            {
                Ok(f) => f,
                Err(e) => {
                    warn!(error = %e, path = %rel, "overlay rewrite open failed");
                    return reply.error(Errno::EIO);
                }
            };
            let fh = self
                .next_fh
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if truncate
                && let Some(entry) = self
                    .overlay
                    .lock()
                    .expect("overlay mutex")
                    .by_rel
                    .get_mut(&rel)
            {
                entry.size = 0;
            }
            self.handles.lock().expect("handles mutex").insert(
                fh,
                WriteHandle {
                    rel: rel.clone(),
                    file,
                    // O_TRUNC already destroyed the old content; even a
                    // close with no writes must re-upload (or the mount
                    // would serve the stale uploaded head forever).
                    dirty: truncate,
                    overlay: true,
                },
            );
            return reply.opened(FileHandle(fh), FopenFlags::empty());
        }
        if writable && overlay.is_none() {
            // Revision write (M3.3): the file must be hydrated before the
            // handle can serve bytes — same bounded wait as read().
            if !content_ready {
                self.store.begin_hydrate(&rel);
            }
            // Open the backing file RW now; if hydration is still pending
            // the fd points at nothing yet — wait here (bounded) so writes
            // land on real content.
            let Some(file) = self.open_backing_rw(&rel) else {
                warn!(path = %rel, "open: backing file unavailable for writable handle");
                return reply.error(Errno::EIO);
            };
            let fh = self
                .next_fh
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.handles.lock().expect("handles mutex").insert(
                fh,
                WriteHandle {
                    rel: rel.clone(),
                    file,
                    dirty: false,
                    overlay: false,
                },
            );
            self.store.record_access(&rel);
            // Revision handles keep the file evict-protected too.
            self.store.add_open_handle(&rel);
            return reply.opened(FileHandle(fh), FopenFlags::empty());
        }
        // Read open (or overlay file, which needs no hydration).
        if !content_ready && self.overlay_entry(&rel).is_none() {
            self.store.begin_hydrate(&rel);
        } else {
            // LRU touch (M3.2 eviction). Open-for-read of hydrated content.
            self.store.record_access(&rel);
        }
        // Open files are never evicted; overlay (write-created) files are
        // not tracked (they are never in the hydrated map).
        if self.overlay_entry(&rel).is_none() {
            self.store.add_open_handle(&rel);
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
            warn!(ino = ino.0, "read: unknown inode");
            return reply.error(Errno::ENOENT);
        };
        // LRU touch on every read pass (cheap; open() covers the cold case).
        if self.overlay_entry(&rel).is_none() {
            self.store.record_access(&rel);
        }
        let Some((is_dir, node_size, _mtime, _ready)) = self.node_meta(&rel) else {
            warn!(ino = ino.0, path = %rel, "read: path vanished from the served tree");
            return reply.error(Errno::ENOENT);
        };
        if is_dir {
            return reply.error(Errno::EISDIR);
        }
        let Some(mut file) = self.wait_for_backing(&rel) else {
            warn!(path = %rel, offset, "read: backing unavailable (hydration failed)");
            return reply.error(Errno::EIO);
        };
        if offset >= node_size {
            return reply.data(&[]);
        }
        if file.seek(SeekFrom::Start(offset)).is_err() {
            warn!(path = %rel, offset, "read: backing seek failed");
            return reply.error(Errno::EIO);
        }
        let mut buf = vec![0u8; size as usize];
        match file.read(&mut buf) {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => {
                warn!(error = %e, path = %rel, offset, "read: backing read failed");
                reply.error(Errno::EIO)
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
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
            warn!(fh = fh.0, "write: unknown file handle");
            return reply.error(Errno::EIO);
        };
        let handle_rel = handle.rel.clone();
        let mut file = handle.file.try_clone().expect("file clone");
        drop(handles);
        if file.seek(SeekFrom::Start(offset)).is_err() {
            warn!(path = %handle_rel, offset, "write: backing seek failed");
            return reply.error(Errno::EIO);
        }
        match file.write_all(data) {
            Ok(()) => {
                if let Some(h) = self.handles.lock().expect("handles mutex").get_mut(&fh.0) {
                    h.dirty = true;
                }
                // Keep the overlay size current for getattr while open
                // (overlay files only; revision files keep snapshot size
                // until the engine's post-upload snapshot).
                if let Ok(meta) = file.metadata()
                    && let Some(entry) = self
                        .overlay
                        .lock()
                        .expect("overlay mutex")
                        .by_rel
                        .get_mut(&handle_rel)
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

    /// Server-side copy for same-mount ranges (`fs.copyFile`, `cp` with
    /// `copy_file_range(2)`). fuser's default replies ENOSYS, which makes
    /// the kernel fall back to a splice copy that reads from and writes to
    /// this daemon — workable, but it routes the copy through the kernel's
    /// page cache and inode-size bookkeeping, which paired badly with the
    /// 1 s attribute TTL (observed in production: intermittent EIO and a
    /// silently truncated 0-byte copy from an app that saves via
    /// `copyFile`). Handling it here copies backing file → backing file
    /// with an exact byte count.
    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        _fh_in: FileHandle,
        offset_in: u64,
        _ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: fuser::CopyFileRangeFlags,
        reply: fuser::ReplyWrite,
    ) {
        let Some(rel_in) = self.rel_of_any(ino_in) else {
            return reply.error(Errno::ENOENT);
        };
        // The destination handle is authoritative for the path — its inode
        // may already have been recycled by a rename, same rationale as
        // `release`. A writable open always allocates a handle (fh ≥ 1);
        // fh 0 is the read-open marker and cannot be a copy destination.
        let (rel_out, mut dst) = {
            let handles = self.handles.lock().expect("handles mutex");
            let Some(handle) = handles.get(&fh_out.0) else {
                warn!(
                    fh = fh_out.0,
                    "copy_file_range: no write handle for destination"
                );
                return reply.error(Errno::EIO);
            };
            match handle.file.try_clone() {
                Ok(f) => (handle.rel.clone(), f),
                Err(e) => {
                    warn!(error = %e, path = %handle.rel, "copy_file_range: destination clone failed");
                    return reply.error(Errno::EIO);
                }
            }
        };
        let Some((src_is_dir, src_size, _mtime, _ready)) = self.node_meta(&rel_in) else {
            return reply.error(Errno::ENOENT);
        };
        if src_is_dir {
            return reply.error(Errno::EISDIR);
        }
        let Some(mut src) = self.wait_for_backing(&rel_in) else {
            warn!(path = %rel_in, "copy_file_range: source backing unavailable");
            return reply.error(Errno::EIO);
        };
        if src.seek(SeekFrom::Start(offset_in)).is_err() {
            warn!(path = %rel_in, offset = offset_in, "copy_file_range: source seek failed");
            return reply.error(Errno::EIO);
        }
        // Copy at most what the source advertises. A backing file shorter
        // than its snapshot size ends the copy early — a short result is
        // valid for copy_file_range, not an error (the kernel truncates
        // the destination to `written`).
        let mut remaining = len.min(src_size.saturating_sub(offset_in));
        let mut written = 0u64;
        let mut buf = vec![0u8; COPY_CHUNK.min(remaining.max(1)) as usize];
        while remaining > 0 {
            let want = remaining.min(COPY_CHUNK) as usize;
            let n = match src.read(&mut buf[..want]) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    warn!(error = %e, path = %rel_in, "copy_file_range: source read failed");
                    return reply.error(Errno::EIO);
                }
            };
            if dst.seek(SeekFrom::Start(offset_out + written)).is_err() {
                warn!(
                    path = %rel_out,
                    offset = offset_out + written,
                    "copy_file_range: destination seek failed"
                );
                return reply.error(Errno::EIO);
            }
            if let Err(e) = dst.write_all(&buf[..n]) {
                warn!(error = %e, path = %rel_out, "copy_file_range: destination write failed");
                return reply.error(Errno::EIO);
            }
            if let Some(h) = self
                .handles
                .lock()
                .expect("handles mutex")
                .get_mut(&fh_out.0)
            {
                h.dirty = true;
            }
            written += n as u64;
            remaining -= n as u64;
        }
        // Keep an overlay destination's size current while open, exactly
        // like `write` does (revision handles keep the snapshot size).
        if let Ok(meta) = dst.metadata()
            && let Some(entry) = self
                .overlay
                .lock()
                .expect("overlay mutex")
                .by_rel
                .get_mut(&rel_out)
        {
            entry.size = entry.size.max(meta.len());
        }
        debug!(
            from = %rel_in,
            to = %rel_out,
            bytes = written,
            "copy_file_range: server-side copy"
        );
        reply.written(written as u32);
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
        // fh 0 = read open (tracked for eviction protection); fh ≥ 1 =
        // write handle from `create` (overlay) or a revision open.
        if fh.0 == 0 {
            if let Some(rel) = self.rel_of_any(ino) {
                self.store.remove_open_handle(&rel);
            }
            return reply.ok();
        }
        let handle = self.handles.lock().expect("handles mutex").remove(&fh.0);
        let Some(h) = handle else {
            return reply.ok();
        };
        // The handle knows its path authoritatively (ino may already have
        // been recycled by an unlink during the handle's lifetime).
        let rel = h.rel.clone();
        let overlay = h.overlay;
        if h.dirty {
            let size = h.file.metadata().map(|m| m.len()).unwrap_or(0);
            h.file.sync_all().ok();
            drop(h);
            self.finalize_write(&rel, size);
        }
        if !overlay {
            // Revision handles are open-tracked; drop the protection.
            self.store.remove_open_handle(&rel);
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let upload = self
            .handles
            .lock()
            .expect("handles mutex")
            .get(&fh.0)
            .map(|h| (h.dirty, h.rel.clone(), h.file.metadata().map(|m| m.len()).unwrap_or(0)));
        if let Some((dirty, rel, size)) = upload {
            if let Some(handle) = self.handles.lock().expect("handles mutex").get(&fh.0) {
                handle.file.sync_all().ok();
            }
            if dirty {
                if let Some(h) = self.handles.lock().expect("handles mutex").get_mut(&fh.0) {
                    h.dirty = false;
                }
                self.finalize_write(&rel, size);
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
        self.handles.lock().expect("handles mutex").insert(
            fh,
            WriteHandle {
                rel,
                file,
                dirty: false,
                overlay: true,
            },
        );
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

    /// Rename/move through the mount. Moves the backing content, re-keys
    /// the write overlay, and queues the remote rename for the engine
    /// (which re-keys DB rows and calls `renameNode`/`moveNodes` — no
    /// trash + re-upload, so content and revision history survive). The
    /// snapshot tree flips on the engine's next publish; until then the
    /// overlay serves the new path and the old one is gone.
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let Some(old_parent_rel) = self.rel_of_any(parent) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(new_parent_rel) = self.rel_of_any(newparent) else {
            return reply.error(Errno::ENOENT);
        };
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            return reply.error(Errno::EINVAL);
        };
        for n in [name, newname] {
            if n.contains('/') || n == "." || n == ".." || n.is_empty() {
                return reply.error(Errno::EINVAL);
            }
        }
        // RENAME_EXCHANGE (renameat2) swaps two entries — no single-move
        // mapping; reject rather than fake it.
        if flags.contains(fuser::RenameFlags::RENAME_EXCHANGE) {
            return reply.error(Errno::EINVAL);
        }
        let old_rel = join_rel(&old_parent_rel, name);
        let new_rel = join_rel(&new_parent_rel, newname);
        // The `.pds` proxy and the root are not renamable.
        if old_rel == STATE_PROXY || new_rel == STATE_PROXY || old_rel.is_empty() {
            return reply.error(Errno::EPERM);
        }
        // Source must exist (overlay or snapshot).
        let Some((src_is_dir, src_size, _src_mtime, _)) = self.node_meta(&old_rel) else {
            return reply.error(Errno::ENOENT);
        };
        // Renaming a folder into its own subtree is invalid.
        if src_is_dir && new_rel.starts_with(&format!("{old_rel}/")) {
            return reply.error(Errno::EINVAL);
        }

        // Destination handling per rename(2)/renameat2 semantics: a file
        // replaces a file, a dir replaces an EMPTY dir, anything else is an
        // error. The replaced destination is mirrored as a remote delete —
        // queued together with the rename at the end (record_overwrite), so
        // the engine can never drain the delete without seeing the inbound
        // rename and tear down the content the rename is delivering.
        let mut replaced = false;
        if let Some((dst_is_dir, _, _, _)) = self.node_meta(&new_rel) {
            if flags.contains(fuser::RenameFlags::RENAME_NOREPLACE) {
                return reply.error(Errno::EEXIST);
            }
            if dst_is_dir && !src_is_dir {
                return reply.error(Errno::EISDIR);
            }
            if src_is_dir && !dst_is_dir {
                return reply.error(Errno::ENOTDIR);
            }
            if src_is_dir && dst_is_dir && !self.dir_is_empty(&new_rel) {
                return reply.error(Errno::ENOTEMPTY);
            }
            replaced = true;
            self.overlay.lock().expect("overlay mutex").remove(&new_rel);
            let abs = to_abs(&self.backing_dir, &new_rel);
            if dst_is_dir {
                std::fs::remove_dir(&abs).ok();
                self.store.forget_tree(&new_rel);
            } else {
                std::fs::remove_file(&abs).ok();
                self.store.forget(&new_rel);
            }
        }

        // 1. Move the backing content (hydrated or write-created). A pure
        //    placeholder has none — its content arrives by path later.
        let from = to_abs(&self.backing_dir, &old_rel);
        let to = to_abs(&self.backing_dir, &new_rel);
        if from.exists() {
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir).ok();
            }
            if let Err(e) = std::fs::rename(&from, &to) {
                warn!(error = %e, from = %old_rel, to = %new_rel, "backing rename failed");
                return reply.error(Errno::EIO);
            }
        }

        // 2. Overlay: re-key this entry and any children to the new prefix
        //    (same inos, so kernel dentries stay valid). A snapshot-backed
        //    source gets a fresh overlay entry so the new path serves
        //    until the engine's next snapshot publish re-keys the rows.
        {
            let mut overlay = self.overlay.lock().expect("overlay mutex");
            let old_prefix = format!("{old_rel}/");
            let moves: Vec<(String, WriteEntry)> = overlay
                .by_rel
                .iter()
                .filter(|(rel, _)| {
                    **rel == old_rel || (src_is_dir && rel.starts_with(&old_prefix))
                })
                .map(|(rel, e)| (rel.clone(), e.clone()))
                .collect();
            let mut had_source = false;
            for (rel, mut e) in moves {
                let new_path = if rel == old_rel {
                    had_source = true;
                    new_rel.clone()
                } else {
                    format!("{new_rel}/{}", &rel[old_rel.len() + 1..])
                };
                overlay.remove(&rel);
                e.mtime = self.now_secs();
                overlay.by_ino.insert(e.ino.0, new_path.clone());
                overlay.by_rel.insert(new_path, e);
            }
            if !had_source && self.tree.node(&new_rel).is_none() {
                let max_ino = overlay
                    .by_ino
                    .keys()
                    .filter(|k| **k >= OVERLAY_INO_BASE)
                    .max()
                    .copied()
                    .unwrap_or(OVERLAY_INO_BASE);
                overlay.insert(
                    &new_rel,
                    WriteEntry {
                        ino: INodeNo(max_ino + 1),
                        size: src_size,
                        mtime: self.now_secs(),
                        is_dir: src_is_dir,
                    },
                );
            }
        }

        // 3. Queue the remote work (engine re-keys rows, renameNode /
        //    moveNodes remotely, re-registers backing paths). An
        //    overwrite-rename queues the destination delete and the rename
        //    atomically so the engine always sees the pair together.
        if replaced {
            self.store.record_overwrite(&new_rel, &old_rel, &new_rel);
        } else {
            self.store.record_rename(&old_rel, &new_rel);
        }
        debug!(from = %old_rel, to = %new_rel, "vfs: rename queued");
        reply.ok();
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
        // children + overlay children. A folder still only in the overlay
        // (no snapshot row yet) has no snapshot children by definition —
        // resolving its ino would fall back to ROOT and make the root's
        // listing answer for it (spurious ENOTEMPTY on an empty folder).
        if !self.dir_is_empty(&rel) {
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
        fh: Option<FileHandle>,
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
        // truncate(2): resize the backing file. Overlay files created this
        // session are trivially writable; snapshot-backed files truncate in
        // place (M3.3 revision) — hydration must land first, and the
        // truncation marks the content dirty for the next upload.
        if let Some(new_size) = size {
            if is_dir {
                return reply.error(Errno::EISDIR);
            }
            let overlay = self.overlay_entry(&rel).is_some();
            if !overlay && self.store.backing_path(&rel).is_none() {
                // Placeholder content: hydrate first (bounded wait) so the
                // truncate lands on real content, not a missing file.
                if self.wait_for_backing(&rel).is_none() {
                    warn!(path = %rel, "truncate: backing hydration failed");
                    return reply.error(Errno::EIO);
                }
                self.store.record_access(&rel);
            }
            let abs = to_abs(&self.backing_dir, &rel);
            if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&abs) {
                if let Err(e) = f.set_len(new_size) {
                    warn!(error = %e, path = %rel, "truncate failed");
                    return reply.error(Errno::EIO);
                }
                f.sync_all().ok();
            } else {
                warn!(path = %rel, "truncate: backing file unopenable");
                return reply.error(Errno::EIO);
            }
            cur_size = new_size;
            if let Some(fh) = fh
                && self
                    .handles
                    .lock()
                    .expect("handles mutex")
                    .get_mut(&fh.0)
                    .is_some_and(|h| {
                        h.dirty = true;
                        h.rel == rel
                    })
            {
                // Truncation through an open write handle: defer the upload
                // to release/fsync — one revision for the whole write
                // sequence, no empty intermediate (open(O_TRUNC) pattern).
            } else if overlay {
                // Overlay (write-created) files carry no row yet; the create
                // path already queued the write.
                self.store.record_write(&rel);
            } else {
                // Truncate outside a handle (truncate(2)): queue the revision
                // upload directly (re-hash-deduped by the store).
                self.store.record_write(&rel);
            }
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

    fn listxattr(&self, _req: &Request, _ino: INodeNo, size: u32, reply: ReplyXattr) {
        // No extended attributes. `size == 0` is the size query; when the
        // kernel supplies a buffer the answer must be the (empty) data
        // itself — replying with the size form here makes the kernel fail
        // the caller with EIO (e.g. every `ls -l` entry).
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }
}
