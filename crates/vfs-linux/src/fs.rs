//! FUSE filesystem presenting the remote tree with on-demand placeholders.
//!
//! Structure (lookup/getattr/readdir) is served from the engine's [`Store`]
//! snapshot — no Drive traffic, no local copies. Opening a placeholder
//! queues a hydrate request; the first `read()` blocks (bounded) until the
//! engine downloads the backing file, then serves bytes from it. `.pds` is
//! answered from the real `<root>/.pds` metadata so the CLI control socket
//! stays reachable through the mount (open/read of it are not served; the
//! CLI talks to the socket directly, `stat` just needs to succeed).
//!
//! Mounted read-only: `MountOption::RO` at the kernel level, and
//! open-for-write is refused with EROFS. Writes come in M3.2 (write-through
//! uploads) on the same request plumbing.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use fuser::{
    Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo, KernelConfig,
    OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyXattr, Request,
};
use pds_core::store::Store;
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

pub struct PdsFs {
    tree: Tree,
    store: Store,
    /// Real `<root>/.pds` directory, answered at the `.pds` mount entry.
    state_dir: PathBuf,
    uid: u32,
    gid: u32,
}

impl PdsFs {
    pub fn new(store: Store, state_dir: PathBuf) -> Self {
        // nolibc shims: fuser depends on libc anyway.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Self {
            tree: Tree::new(),
            store,
            state_dir,
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
        // Refresh on lookups too: cheap (generation check) and keeps
        // post-hydration metadata fresh for short-lived processes.
        self.tree.refresh(&self.store.snapshot());
        let Some(rel) = self.tree.child_rel(parent, name) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(node) = self.tree.node(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if rel == STATE_PROXY {
            let path = self.state_dir.join(name);
            let Ok(meta) = std::fs::metadata(&path) else {
                return reply.error(Errno::ENOENT);
            };
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return reply.entry(
                &TTL,
                &self.attr_for(node.ino, true, 0, mtime),
                Generation(0),
            );
        }
        reply.entry(
            &TTL,
            &self.attr_for(node.ino, node.is_dir, node.size, node.mtime),
            Generation(0),
        );
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        self.tree.refresh(&self.store.snapshot());
        let Some(rel) = self.tree.rel_of(ino) else {
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
        match self.tree.node(&rel) {
            Some(node) => reply.attr(
                &TTL,
                &self.attr_for(node.ino, node.is_dir, node.size, node.mtime),
            ),
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
        let Some(rel) = self.tree.rel_of(ino) else {
            return reply.error(Errno::ENOENT);
        };
        if rel == STATE_PROXY {
            return reply.error(Errno::EISDIR);
        }
        let Some(node) = self.tree.node(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if node.is_dir {
            return reply.opened(FileHandle(0), FopenFlags::empty());
        }
        use fuser::OpenAccMode;
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            return reply.error(Errno::EROFS);
        }
        // Kick the engine early; the bounded wait happens in read().
        if !node.content_ready {
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
        let Some(rel) = self.tree.rel_of(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(node) = self.tree.node(&rel) else {
            return reply.error(Errno::ENOENT);
        };
        if node.is_dir {
            return reply.error(Errno::EISDIR);
        }
        let Some(mut file) = self.wait_for_backing(&rel) else {
            return reply.error(Errno::EIO);
        };
        if offset >= node.size {
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

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok(); // read-only handles
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Placeholder sizes make free-space math meaningless; report an
        // effectively unlimited filesystem so tools don't refuse work.
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 4096);
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
