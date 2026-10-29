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
        let mut inner = self.lock();
        inner.hydrated.remove(rel);
        inner.hydrating.remove(rel);
        inner.pending.retain(|p| p != rel);
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
        }
        inner.hydrating.remove(rel);
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
}
