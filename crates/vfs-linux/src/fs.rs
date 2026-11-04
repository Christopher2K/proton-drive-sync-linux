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
use std::os::unix::fs::FileExt as _;
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
    /// Open read handles: fh → backing file. Holding the file (not just
    /// the path) keeps the descriptor's bytes alive across rename(2) and
    /// unlink(2), as POSIX requires.
    read_handles: Mutex<HashMap<u64, ReadHandle>>,
    /// Local size of revision files whose upload has not republished the
    /// snapshot yet. Without it, getattr would report the old remote size
    /// right after close/truncate and the kernel would clip reads at the
    /// stale EOF even though the backing file already grew.
    pending_sizes: Mutex<HashMap<String, u64>>,
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
    mode: u32,
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
    /// The path was unlinked (or replaced by a rename) while this handle
    /// was open: the descriptor still points at the unlinked inode, so its
    /// writes must be discarded rather than uploaded to a recycled path.
    discarded: bool,
}

/// Per-open state of a read handle (fh ≥ 1). The open file is retained so
/// the descriptor keeps serving the inode it was opened on even after the
/// path is renamed over or unlinked.
struct ReadHandle {
    rel: String,
    file: File,
    /// Whether this handle registered with the store's open-handle guard
    /// (snapshot-backed files only; overlay files are not evictable).
    tracked: bool,
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
            read_handles: Mutex::new(HashMap::new()),
            pending_sizes: Mutex::new(HashMap::new()),
            next_fh: std::sync::atomic::AtomicU64::new(1),
            uid,
            gid,
        }
    }

    fn attr_for(&self, ino: INodeNo, is_dir: bool, size: u64, mtime: i64) -> FileAttr {
        let when = unix_time(mtime);
        let perm = self
            .rel_of_any(ino)
            .map(|rel| self.node_mode(&rel))
            .unwrap_or(if is_dir { 0o755 } else { 0o644 });
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
            perm: perm as u16,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// POSIX mode served for `rel`: overlay first (this session's writes),
    /// then the persisted snapshot mode. Synthetic dirs are 0755.
    fn node_mode(&self, rel: &str) -> u32 {
        if let Some(e) = self.overlay_entry(rel) {
            return e.mode;
        }
        if rel.is_empty() || rel == STATE_PROXY {
            return 0o755;
        }
        self.tree.node(rel).map(|n| n.mode).unwrap_or(0o644)
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

    /// Live size of a file with an open write handle (max across handles).
    /// Revision writes otherwise keep reporting the snapshot size until the
    /// upload republishes it — which would make `O_APPEND` compute its next
    /// offset from a stale length and let readers stop at the old EOF.
    fn open_write_size(&self, rel: &str) -> Option<u64> {
        self.handles
            .lock()
            .expect("handles mutex")
            .values()
            .filter(|h| h.rel == rel)
            .filter_map(|h| h.file.metadata().ok().map(|m| m.len()))
            .max()
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
        let node = self.tree.node(rel)?;
        let size = self.pending_size(rel).unwrap_or(node.size);
        Some((node.is_dir, size, node.mtime, node.content_ready))
    }

    /// Size of a locally modified revision file that the snapshot does not
    /// reflect yet. Entries clear themselves once the upload publishes the
    /// matching size (or the path disappears from the snapshot).
    fn pending_size(&self, rel: &str) -> Option<u64> {
        let mut pending = self.pending_sizes.lock().expect("pending sizes mutex");
        let size = *pending.get(rel)?;
        if self.tree.node(rel).is_none_or(|n| n.size == size) {
            pending.remove(rel);
            return None;
        }
        Some(size)
    }

    fn set_pending_size(&self, rel: &str, size: u64) {
        self.pending_sizes
            .lock()
            .expect("pending sizes mutex")
            .insert(rel.to_string(), size);
    }

    fn clear_pending_size(&self, rel: &str) {
        self.pending_sizes
            .lock()
            .expect("pending sizes mutex")
            .remove(rel);
    }

    /// A finished write (release with dirty data, or fsync): fix up the
    /// overlay size and hand the file to the engine for upload. The rel
    /// comes from [`Self::rel_of_any`] — overlay inos resolve here too.
    fn finalize_write(&self, rel: &str, size: u64) {
        if rel.is_empty() {
            return;
        }
        let overlay = if let Some(entry) = self
            .overlay
            .lock()
            .expect("overlay mutex")
            .by_rel
            .get_mut(rel)
        {
            entry.size = size;
            entry.mtime = self.now_secs();
            true
        } else {
            false
        };
        if !overlay {
            // Keep getattr/read at the real local size until the upload
            // republishes the snapshot (truncate-grow and append both
            // outrun it).
            self.set_pending_size(rel, size);
        }
        self.store.record_write(rel);
    }

    /// Re-key open handles after a rename/move: an fd stays valid across
    /// `rename(2)` and its writes belong to the new path on release.
    fn rekey_handles(&self, old_rel: &str, new_rel: &str, is_dir: bool) {
        let rekey = |rel: &str| -> Option<String> {
            if rel == old_rel {
                Some(new_rel.to_string())
            } else if is_dir && rel.starts_with(&format!("{old_rel}/")) {
                Some(format!("{new_rel}/{}", &rel[old_rel.len() + 1..]))
            } else {
                None
            }
        };
        for handle in self.handles.lock().expect("handles mutex").values_mut() {
            if let Some(rel) = rekey(&handle.rel) {
                handle.rel = rel;
            }
        }
        for handle in self
            .read_handles
            .lock()
            .expect("read handles mutex")
            .values_mut()
        {
            if let Some(rel) = rekey(&handle.rel) {
                handle.rel = rel;
            }
        }
    }

    /// A path was unlinked/replaced while handles were open: the kernel
    /// keeps the old inode readable through those fds, but writes to it
    /// must never be uploaded to the path's new occupant.
    fn discard_write_handles(&self, rel: &str) {
        for handle in self.handles.lock().expect("handles mutex").values_mut() {
            if handle.rel == rel {
                handle.discarded = true;
            }
        }
    }
}

fn unix_time(secs: i64) -> std::time::SystemTime {
    if secs > 0 {
        std::time::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        std::time::UNIX_EPOCH
    }
}

/// `statvfs(2)` for a path, `None` if it does not exist or fails.
fn statvfs(path: &std::path::Path) -> Option<libc::statvfs> {
    use std::os::unix::ffi::OsStrExt as _;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut out = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c_path.as_ptr(), out.as_mut_ptr()) } == 0 {
        Some(unsafe { out.assume_init() })
    } else {
        None
    }
}

/// Set only the modification time on an existing file. `set_times` needs a
/// writable fd on Linux; the backing files are ours, so this is safe.
fn set_file_mtime(path: &std::path::Path, secs: i64) {
    let times = std::fs::FileTimes::new().set_modified(unix_time(secs));
    if let Ok(f) = std::fs::OpenOptions::new().write(true).open(path) {
        f.set_times(times).ok();
    }
}

fn set_file_mode(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o777)).ok();
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
        let size = if is_dir {
            size
        } else {
            self.open_write_size(&rel).unwrap_or(size)
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
                let size = if is_dir {
                    size
                } else {
                    self.open_write_size(&rel).unwrap_or(size)
                };
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
                    discarded: false,
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
                    discarded: false,
                },
            );
            self.store.record_access(&rel);
            // Revision handles keep the file evict-protected too.
            self.store.add_open_handle(&rel);
            return reply.opened(FileHandle(fh), FopenFlags::empty());
        }
        // Read open: hydrate (bounded) and hold the backing file so this
        // descriptor keeps its bytes across rename(2)/unlink(2), exactly
        // like a POSIX fd on a local filesystem. A path-based read would
        // follow a recycled path instead.
        let Some(file) = self.wait_for_backing(&rel) else {
            warn!(path = %rel, "open: backing unavailable for read handle");
            return reply.error(Errno::EIO);
        };
        let fh = self
            .next_fh
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tracked = self.overlay_entry(&rel).is_none();
        self.read_handles
            .lock()
            .expect("read handles mutex")
            .insert(
                fh,
                ReadHandle {
                    rel: rel.clone(),
                    file,
                    tracked,
                },
            );
        self.store.record_access(&rel);
        // Open files are never evicted; overlay (write-created) files are
        // not tracked (they are never in the hydrated map).
        if tracked {
            self.store.add_open_handle(&rel);
        }
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }


    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        // A descriptor's open file is authoritative: after rename(2) or
        // unlink(2) the inode lives on under this fd even though the path
        // may now name different bytes (POSIX). Fall back to the path only
        // for legacy fh 0 requests.
        let held = self
            .handles
            .lock()
            .expect("handles mutex")
            .get(&fh.0)
            .and_then(|h| h.file.try_clone().ok().map(|f| (h.rel.clone(), f)))
            .or_else(|| {
                self.read_handles
                    .lock()
                    .expect("read handles mutex")
                    .get(&fh.0)
                    .and_then(|h| h.file.try_clone().ok().map(|f| (h.rel.clone(), f)))
            });
        let (rel, node_size, file) = match held {
            Some((rel, file)) => {
                let len = file.metadata().map(|m| m.len()).unwrap_or(0);
                (rel, len, file)
            }
            None => {
                let Some(rel) = self.rel_of_any(ino) else {
                    warn!(ino = ino.0, "read: unknown inode");
                    return reply.error(Errno::ENOENT);
                };
                let Some((is_dir, node_size, _mtime, _ready)) = self.node_meta(&rel) else {
                    warn!(ino = ino.0, path = %rel, "read: path vanished from the served tree");
                    return reply.error(Errno::ENOENT);
                };
                if is_dir {
                    return reply.error(Errno::EISDIR);
                }
                let Some(file) = self.wait_for_backing(&rel) else {
                    warn!(path = %rel, offset, "read: backing unavailable (hydration failed)");
                    return reply.error(Errno::EIO);
                };
                (rel, node_size, file)
            }
        };
        // LRU touch on every read pass (cheap; open() covers the cold case).
        if self.overlay_entry(&rel).is_none() {
            self.store.record_access(&rel);
        }
        if offset >= node_size {
            return reply.data(&[]);
        }
        let mut buf = vec![0u8; size as usize];
        match file.read_at(&mut buf, offset) {
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
        if let Some(h) = handle {
            // The handle knows its path authoritatively (ino may already
            // have been recycled by an unlink during the handle's lifetime).
            let rel = h.rel.clone();
            let overlay = h.overlay;
            if h.dirty && !h.discarded {
                let size = h.file.metadata().map(|m| m.len()).unwrap_or(0);
                h.file.sync_all().ok();
                drop(h);
                self.finalize_write(&rel, size);
            }
            if !overlay {
                // Revision handles are open-tracked; drop the protection.
                self.store.remove_open_handle(&rel);
            }
            return reply.ok();
        }
        if let Some(h) = self
            .read_handles
            .lock()
            .expect("read handles mutex")
            .remove(&fh.0)
            && h.tracked
        {
            self.store.remove_open_handle(&h.rel);
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
            .map(|h| {
                (
                    h.dirty && !h.discarded,
                    h.rel.clone(),
                    h.file.metadata().map(|m| m.len()).unwrap_or(0),
                )
            });
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
        // Report the backing filesystem's real capacity: placeholder sizes
        // are remote sizes, not local usage, so the cache directory is the
        // honest answer for `df` and for installers/copy tools that refuse
        // a 0-byte filesystem. Never answer zeros.
        let stats = self
            .backing_dir
            .ancestors()
            .find_map(statvfs)
            .or_else(|| self.state_dir.ancestors().find_map(statvfs));
        if let Some(v) = stats {
            let frsize = v.f_frsize.max(1);
            let bsize = v.f_bsize.max(1);
            reply.statfs(
                v.f_blocks.max(1),
                v.f_bfree,
                v.f_bavail.max(1),
                v.f_files.max(1),
                v.f_ffree,
                bsize as u32,
                v.f_namemax.max(255) as u32,
                frsize as u32,
            );
            return;
        }
        // No statvfs answer (path on a detached mount): a sane non-zero
        // synthetic filesystem beats a zero-byte one.
        const BLOCK: u32 = 4096;
        const BLOCKS: u64 = (1u64 << 40) / BLOCK as u64;
        reply.statfs(
            BLOCKS,
            BLOCKS,
            BLOCKS,
            1_000_000,
            1_000_000,
            BLOCK,
            255,
            BLOCK,
        );
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
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
        // Preserve the creator's requested mode locally (D2): an executable
        // script created through the mount keeps `+x`.
        let perm = if mode == 0 {
            0o644
        } else {
            mode & !umask & 0o777
        };
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&abs, std::fs::Permissions::from_mode(perm)).ok();
        }
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
                mode: perm,
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
                discarded: false,
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
                    mode: 0o755,
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
            // Fds open on the replaced destination keep the old inode
            // readable, but their writes must not land on the incoming
            // node's path.
            self.discard_write_handles(&new_rel);
            self.clear_pending_size(&new_rel);
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
        let src_mode = self.node_mode(&old_rel);
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
                        mode: src_mode,
                        is_dir: src_is_dir,
                    },
                );
            }
        }

        // 3. Queue the remote work (engine re-keys rows, renameNode /
        //    moveNodes remotely, re-registers backing paths). An
        //    overwrite-rename queues the destination delete and the rename
        //    atomically so the engine always sees the pair together.
        self.rekey_handles(&old_rel, &new_rel, src_is_dir);
        let moved_size = self
            .pending_sizes
            .lock()
            .expect("pending sizes mutex")
            .remove(&old_rel);
        if let Some(size) = moved_size {
            self.set_pending_size(&new_rel, size);
        }
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
        // Open fds keep reading the unlinked inode (read handles hold it);
        // their writes are discarded so a later recreate is never clobbered.
        self.discard_write_handles(&rel);
        self.clear_pending_size(&rel);
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
        mode: Option<u32>,
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
        let mut cur_mode = self.node_mode(&rel);
        let mtime_changed = mtime.is_some();
        let mode_changed = mode.is_some();
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
            if !overlay {
                self.set_pending_size(&rel, new_size);
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
        if let Some(new_mode) = mode {
            cur_mode = new_mode & 0o777;
        }
        // Apply the change to the backing content when present (hydrated
        // placeholder or overlay file). Times go before permissions so a
        // restrictive mode cannot block the write-open used for set_times.
        if mtime_changed || mode_changed {
            let abs = to_abs(&self.backing_dir, &rel);
            if mtime_changed && abs.is_file() {
                set_file_mtime(&abs, cur_mtime);
            }
            if mode_changed {
                set_file_mode(&abs, cur_mode);
            }
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
            entry.mode = cur_mode;
        } else if !rel.is_empty() && (mtime_changed || mode_changed) {
            // Snapshot-backed row: persist in SQLite and republish. No
            // content upload (hash-dedup/remote churn — see SPEC F5).
            self.store.record_metadata(
                &rel,
                mtime_changed.then_some(cur_mtime),
                mode_changed.then_some(cur_mode),
            );
        }
        let attr = FileAttr {
            perm: cur_mode as u16,
            ..self.attr_for(ino, is_dir, cur_size, cur_mtime)
        };
        reply.attr(&TTL, &attr);
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
