//! Shared snapshot store between the sync engine and the VFS.
//!
//! Placeholder mode never writes synced files into the visible root: the
//! FUSE filesystem serves structure from snapshots the engine publishes
//! here, and file contents from backing files the engine downloads on
//! demand. The flow for an open():
//!
//! 1. VFS [`Store::begin_hydrate`] queues the request and wakes the engine
//!    (a [`tokio::sync::Notify`], so it works from the FUSE thread).
//! 2. The engine downloads the file into the backing dir, flips the row's
//!    `content_ready`, and calls [`Store::publish_snapshot`].
//! 3. The VFS re-reads the snapshot and the read() retries succeed.
//!
//! All engine→VFS communication is `Arc<Mutex<…>>` snapshots (never live DB
//! handles): the VFS runs on its own blocking threads.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// What the VFS needs to know about one node. Path-keyed; mirrors the
/// relevant `nodes` row columns (see `db.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VNode {
    pub rel: String,
    pub parent: String,
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: i64,
    /// Backing file with the full content, ready to serve.
    pub content_ready: bool,
}

/// Immutable view of the tree the VFS serves.
#[derive(Debug, Clone, Default)]
pub struct RootSnapshot {
    pub generation: u64,
    pub by_path: HashMap<String, VNode>,
    /// parent rel → children rels, sorted by name for stable readdir.
    pub children: HashMap<String, Vec<String>>,
}

#[derive(Debug, Default)]
struct Inner {
    snapshot: RootSnapshot,
    /// Open requests awaiting hydration, oldest first.
    pending: VecDeque<String>,
    /// Requests the engine is currently downloading.
    hydrating: HashSet<String>,
    /// rel → backing file path for hydrated files.
    hydrated: HashMap<String, PathBuf>,
    /// Write-created or write-modified files awaiting upload, oldest first.
    pending_writes: VecDeque<String>,
    /// Files the engine is currently uploading from the write queue.
    uploading: HashSet<String>,
    /// Folders created through the mount, awaiting a remote mirror.
    pending_mkdirs: VecDeque<String>,
    /// Paths deleted through the mount, awaiting a remote trash.
    pending_deletes: VecDeque<String>,
    /// Last access per hydrated rel (LRU order for eviction; M3.2).
    /// Monotonic counter, not wall time: avoids clock skew and gives a
    /// total order even for same-second reads.
    last_access: HashMap<String, u64>,
    access_counter: u64,
    /// Open FUSE handles per rel — never evict open files.
    open_handles: HashMap<String, usize>,
    /// Monotonic generation for `publish_snapshot`.
    next_generation: u64,
}

/// Engine ↔ VFS shared state. Cheap to clone (`Arc`).
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Inner>>,
    /// Woken when a new hydrate request is queued.
    pub notify: Arc<tokio::sync::Notify>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    // ---- engine side --------------------------------------------------

    /// Atomically replace the tree snapshot and advance the generation.
    /// `hydrated` maps rel → backing file for files whose content is on
    /// disk; everything else without `content_ready` in the rows is a
    /// placeholder.
    pub fn publish_snapshot(&self, by_path: HashMap<String, VNode>) {
        let mut inner = self.lock();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for node in by_path.values() {
            children
                .entry(node.parent.clone())
                .or_default()
                .push(node.rel.clone());
        }
        for list in children.values_mut() {
            list.sort();
        }
        inner.next_generation += 1;
        inner.snapshot = RootSnapshot {
            generation: inner.next_generation,
            children,
            by_path,
        };
    }

    /// Register a hydrated backing file (engine, after a download lands).
    pub fn set_backing(&self, rel: &str, backing: PathBuf) {
        self.lock().hydrated.insert(rel.to_string(), backing);
    }

    /// Remove the backing entry (row deleted, or eviction).
    pub fn clear_backing(&self, rel: &str) {
        self.forget(rel);
    }

    /// Next queued hydrate request, if any (engine, from the main loop).
    pub fn next_hydrate_request(&self) -> Option<String> {
        let mut inner = self.lock();
        while let Some(rel) = inner.pending.pop_front() {
            if inner.hydrated.contains_key(&rel) {
                continue; // content landed meanwhile
            }
            inner.hydrating.insert(rel.clone());
            return Some(rel);
        }
        None
    }

    /// Finish a hydration: the engine calls this after the backing file is
    /// complete (or failed; `ok = false` just unblocks re-requesting).
    pub fn finish_hydrate(&self, rel: &str, ok: bool) {
        let mut inner = self.lock();
        if !ok {
            inner.hydrated.remove(rel);
        } else {
            // A fresh hydration is by definition the most recent use.
            inner.access_counter += 1;
            let stamp = inner.access_counter;
            inner.last_access.insert(rel.to_string(), stamp);
        }
        inner.hydrating.remove(rel);
    }

    // ---- pin / evict (M3.2) ----------------------------------------------

    /// VFS: record a read (or hydration-triggering open) of `rel` for the
    /// LRU order. Cheap: a mutex + counter bump.
    pub fn record_access(&self, rel: &str) {
        let mut inner = self.lock();
        inner.access_counter += 1;
        let stamp = inner.access_counter;
        inner.last_access.insert(rel.to_string(), stamp);
    }

    /// VFS: an open handle appeared/closed for `rel`. Open files are never
    /// evicted.
    pub fn add_open_handle(&self, rel: &str) {
        *self.lock().open_handles.entry(rel.to_string()).or_insert(0) += 1;
    }

    pub fn remove_open_handle(&self, rel: &str) {
        let mut inner = self.lock();
        if let Some(n) = inner.open_handles.get_mut(rel) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                inner.open_handles.remove(rel);
            }
        }
    }

    /// LRU-ordered hydrated paths (engine, eviction pass).
    pub fn hydrated_by_age(&self) -> Vec<(String, u64)> {
        let inner = self.lock();
        let mut v: Vec<(String, u64)> = inner
            .hydrated
            .keys()
            .map(|rel| {
                (
                    rel.clone(),
                    inner.last_access.get(rel).copied().unwrap_or(0),
                )
            })
            .collect();
        v.sort_by_key(|(_, age)| *age);
        v
    }

    /// Is `rel` held open by the VFS right now?
    pub fn is_open(&self, rel: &str) -> bool {
        self.lock().open_handles.contains_key(rel)
    }

    // ---- write-through (M3.2) ---------------------------------------------

    /// Record a finished write to a backing file (VFS `write`/`setattr`,
    /// after the bytes are on disk) and wake the engine to upload it.
    /// Re-queues while an upload is in flight: the newer content must be
    /// uploaded again after the current attempt (latest content wins —
    /// the engine re-hashes at upload time).
    pub fn record_write(&self, rel: &str) {
        let first = {
            let mut inner = self.lock();
            let rel = rel.to_string();
            if inner.pending_writes.contains(&rel) {
                false
            } else {
                inner.pending_writes.push_back(rel);
                true
            }
        };
        if first {
            self.notify.notify_one();
        }
    }

    /// Next write-created/-modified file to upload, if any (engine).
    /// Entries whose previous upload is still in flight stay queued.
    pub fn next_write_request(&self) -> Option<String> {
        let mut inner = self.lock();
        let mut skipped = Vec::new();
        let mut result = None;
        while let Some(rel) = inner.pending_writes.pop_front() {
            if inner.uploading.contains(&rel) {
                skipped.push(rel);
            } else {
                inner.uploading.insert(rel.clone());
                result = Some(rel);
                break;
            }
        }
        for rel in skipped.into_iter().rev() {
            inner.pending_writes.push_front(rel);
        }
        result
    }

    /// Engine finished one upload attempt from the write queue. On failure
    /// the file re-queues (retry on the next main-loop pass).
    pub fn finish_write(&self, rel: &str, ok: bool) {
        let mut inner = self.lock();
        inner.uploading.remove(rel);
        if !ok {
            inner.pending_writes.push_back(rel.to_string());
        }
    }

    /// True while the engine is uploading `rel` (VFS: hide the `.pdsw-`
    /// partial and answer reads from the last complete backing content).
    pub fn is_uploading(&self, rel: &str) -> bool {
        self.lock().uploading.contains(rel)
    }

    /// Number of queued + in-flight write uploads (status display).
    pub fn pending_writes(&self) -> usize {
        let inner = self.lock();
        inner.pending_writes.len() + inner.uploading.len()
    }

    /// Queue a folder created through the mount (VFS `mkdir`).
    pub fn record_mkdir(&self, rel: &str) {
        let first = {
            let mut inner = self.lock();
            let rel = rel.to_string();
            let first = !inner.pending_mkdirs.contains(&rel);
            if first {
                inner.pending_mkdirs.push_back(rel);
            }
            first
        };
        if first {
            self.notify.notify_one();
        }
    }

    /// Next folder to create remotely (engine). Parents sort before
    /// children because the VFS queues them parent-first.
    pub fn next_mkdir_request(&self) -> Option<String> {
        self.lock().pending_mkdirs.pop_front()
    }

    /// Queue a path removed through the mount (VFS `unlink`/`rmdir`).
    /// Deletion order is the VFS's problem (children are unlinked first);
    /// the engine mirrors each as a durable tombstone + trash.
    pub fn record_delete(&self, rel: &str) {
        let first = {
            let mut inner = self.lock();
            let rel = rel.to_string();
            let first = !inner.pending_deletes.contains(&rel);
            if first {
                inner.pending_deletes.push_back(rel);
            }
            first
        };
        if first {
            self.notify.notify_one();
        }
    }

    /// Next path to delete remotely (engine), oldest first.
    pub fn next_delete_request(&self) -> Option<String> {
        self.lock().pending_deletes.pop_front()
    }

    /// Drop a path from every queue (row deleted, or hydration failed).
    /// Called with the DB-rel form of the path.
    pub fn forget(&self, rel: &str) {
        let mut inner = self.lock();
        inner.hydrated.remove(rel);
        inner.hydrating.remove(rel);
        inner.pending.retain(|p| p != rel);
        inner.pending_writes.retain(|p| p != rel);
        inner.uploading.remove(rel);
        inner.pending_mkdirs.retain(|p| p != rel);
        inner.pending_deletes.retain(|p| p != rel);
        inner.last_access.remove(rel);
        inner.open_handles.remove(rel);
    }

    /// Drop `rel` and every path under it (recursive delete through the
    /// mount: children were queued explicitly, but drop leftovers anyway).
    pub fn forget_tree(&self, rel: &str) {
        let prefix = format!("{rel}/");
        let mut inner = self.lock();
        let keep = |p: &String| p != rel && !p.starts_with(&prefix);
        inner.hydrated.retain(|p, _| keep(p));
        inner.hydrating.retain(|p| keep(p));
        inner.pending.retain(|p| keep(p));
        inner.pending_writes.retain(|p| keep(p));
        inner.uploading.retain(|p| keep(p));
        inner.pending_mkdirs.retain(|p| keep(p));
        inner.pending_deletes.retain(|p| keep(p));
        inner.last_access.retain(|p, _| keep(p));
        inner.open_handles.retain(|p, _| keep(p));
    }

    // ---- VFS side ------------------------------------------------------

    /// Current snapshot (cheap Arc-free clone of the maps is avoided; the
    /// VFS clones the `RootSnapshot` values it needs per call instead).
    pub fn snapshot(&self) -> RootSnapshot {
        self.lock().snapshot.clone()
    }

    /// Queue an open of a not-yet-hydrated file. Returns `true` when this
    /// caller is the first requester (the engine gets one wake for many).
    pub fn begin_hydrate(&self, rel: &str) -> bool {
        let first = {
            let mut inner = self.lock();
            let rel_string = rel.to_string();
            if inner.hydrated.contains_key(&rel_string)
                || inner.hydrating.contains(&rel_string)
                || inner.pending.contains(&rel_string)
            {
                false
            } else {
                inner.pending.push_back(rel_string);
                true
            }
        };
        if first {
            self.notify.notify_one();
        }
        first
    }

    /// Backing file of a hydrated file, if ready.
    pub fn backing_path(&self, rel: &str) -> Option<PathBuf> {
        self.lock().hydrated.get(rel).cloned()
    }

    /// Number of files currently being downloaded (status display).
    pub fn hydrating_count(&self) -> usize {
        self.lock().hydrating.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("store mutex poisoned")
    }
}

/// Backing file path for a hydrated placeholder: `<backing_dir>/<rel>`
/// (mirroring the tree keeps eviction and debugging simple).
pub fn backing_path(backing_dir: &Path, rel: &str) -> PathBuf {
    backing_dir.join(rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(rel: &str, is_dir: bool) -> VNode {
        let (parent, name) = match rel.rsplit_once('/') {
            Some((p, n)) => (p.to_string(), n.to_string()),
            None => (String::new(), rel.to_string()),
        };
        VNode {
            rel: rel.to_string(),
            parent,
            name,
            is_dir,
            size: 123,
            mtime: 0,
            content_ready: false,
        }
    }

    #[test]
    fn snapshot_builds_sorted_children() {
        let store = Store::new();
        let mut map = HashMap::new();
        map.insert("b.txt".to_string(), node("b.txt", false));
        map.insert("a.txt".to_string(), node("a.txt", false));
        map.insert("d/x".to_string(), node("d/x", false));
        map.insert("d".to_string(), node("d", true));
        store.publish_snapshot(map);

        let snap = store.snapshot();
        assert_eq!(snap.by_path.len(), 4);
        assert_eq!(
            snap.children[""],
            vec!["a.txt".to_string(), "b.txt".to_string(), "d".to_string()]
        );
        assert_eq!(snap.children["d"], vec!["d/x".to_string()]);
    }

    #[test]
    fn hydrate_queue_dedups_and_tracks() {
        let store = Store::new();
        assert!(store.begin_hydrate("a.txt"));
        assert!(!store.begin_hydrate("a.txt"), "second open is not first");
        assert_eq!(store.next_hydrate_request(), Some("a.txt".into()));
        assert_eq!(store.hydrating_count(), 1);
        assert_eq!(store.next_hydrate_request(), None);

        store.finish_hydrate("a.txt", false);
        assert_eq!(store.hydrating_count(), 0);
        // failed hydration can be re-requested
        assert!(store.begin_hydrate("a.txt"));
    }

    #[test]
    fn backing_registration_unblocks() {
        let store = Store::new();
        assert!(store.begin_hydrate("a.txt"));
        store.set_backing("a.txt", PathBuf::from("/tmp/x"));
        // engine drops the queued request because content is already there
        assert_eq!(store.next_hydrate_request(), None);
        assert_eq!(store.backing_path("a.txt"), Some(PathBuf::from("/tmp/x")));
    }

    #[test]
    fn write_queue_roundtrip_and_retry() {
        let store = Store::new();
        assert_eq!(store.pending_writes(), 0);
        store.record_write("new.txt");
        store.record_write("new.txt"); // dedup while queued
        assert_eq!(store.pending_writes(), 1);
        assert_eq!(store.next_write_request(), Some("new.txt".into()));
        assert!(store.is_uploading("new.txt"));
        // re-queue during upload is kept for the retry pass
        store.record_write("new.txt");
        assert_eq!(store.pending_writes(), 2);
        assert_eq!(store.next_write_request(), None, "in-flight not re-served");

        store.finish_write("new.txt", true);
        assert!(!store.is_uploading("new.txt"));
        assert_eq!(store.next_write_request(), Some("new.txt".into()));
        store.finish_write("new.txt", false); // failure re-queues
        assert_eq!(store.next_write_request(), Some("new.txt".into()));
        store.finish_write("new.txt", true);

        store.record_write("gone.txt");
        store.forget("gone.txt");
        assert_eq!(store.next_write_request(), None);
    }

    #[test]
    fn mkdir_and_delete_queues() {
        let store = Store::new();
        store.record_mkdir("docs");
        store.record_mkdir("docs/sub");
        assert_eq!(store.next_mkdir_request(), Some("docs".into()));
        assert_eq!(store.next_mkdir_request(), Some("docs/sub".into()));
        assert_eq!(store.next_mkdir_request(), None);

        store.record_delete("docs/sub/a.txt");
        store.record_delete("docs/sub/a.txt"); // dedup
        assert_eq!(store.pending_writes(), 0);
        assert_eq!(store.next_delete_request(), Some("docs/sub/a.txt".into()));
        assert_eq!(store.next_delete_request(), None);

        // forget_tree drops queued work under a removed folder.
        store.record_mkdir("gone");
        store.record_write("gone/f.txt");
        store.forget_tree("gone");
        assert_eq!(store.next_mkdir_request(), None);
        assert_eq!(store.next_write_request(), None);
    }

    #[test]
    fn access_tracking_and_open_handles() {
        let store = Store::new();
        store.set_backing("a.txt", PathBuf::from("/tmp/a"));
        store.set_backing("b.txt", PathBuf::from("/tmp/b"));

        // Hydration itself counts as the most recent access.
        store.record_access("a.txt");
        store.record_access("b.txt");
        let ages = store.hydrated_by_age();
        assert_eq!(ages[0].0, "a.txt", "older access evicted first");
        assert_eq!(ages[1].0, "b.txt");

        // Re-read bumps a.txt to newest.
        store.record_access("a.txt");
        let ages = store.hydrated_by_age();
        assert_eq!(ages[0].0, "b.txt");

        // Open handles tracked independently; forget clears them.
        store.add_open_handle("b.txt");
        assert!(store.is_open("b.txt"));
        store.add_open_handle("b.txt");
        store.remove_open_handle("b.txt");
        assert!(store.is_open("b.txt"), "still one handle open");
        store.remove_open_handle("b.txt");
        assert!(!store.is_open("b.txt"));
        store.forget("a.txt");
        assert!(store.hydrated_by_age().iter().all(|(r, _)| r != "a.txt"));
    }
}
