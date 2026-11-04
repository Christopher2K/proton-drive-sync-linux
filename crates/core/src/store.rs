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
    /// POSIX permission bits for the VFS attrs (files default 0644, dirs
    /// 0755; a locally persisted executable bit survives eviction).
    pub mode: u32,
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
    /// Metadata-only changes from the VFS (`setattr(mtime/mode)`):
    /// (rel, mtime, mode). Drained by the engine into the DB/snapshot
    /// WITHOUT queueing a content upload — the hash is unchanged.
    pending_metadata: VecDeque<(String, Option<i64>, Option<u32>)>,
    /// Folders created through the mount, awaiting a remote mirror.
    pending_mkdirs: VecDeque<String>,
    /// Paths deleted through the mount, awaiting a remote trash.
    pending_deletes: VecDeque<String>,
    /// Renames made through the mount: (old rel, new rel), oldest first.
    /// The engine turns each into a remote `renameNode` (never a
    /// trash+re-upload), re-keying the DB rows beneath the paths.
    pending_renames: VecDeque<(String, String)>,
    /// Consecutive name-conflict failures per rename pair. A doomed pair
    /// (the remote destination name never frees up) is dropped at
    /// [`RENAME_MAX_CONFLICT_RETRIES`] instead of re-queueing forever:
    /// renames drain before `pump_work`, so a perpetual retry starves
    /// the tombstone drain that could unblock it.
    rename_conflicts: HashMap<(String, String), u32>,
    /// Paths deleted through the mount that the served snapshot may still
    /// list until the engine drains their tombstones and republishes.
    /// Outlives `pending_deletes` (which the engine pops immediately);
    /// cleared when a published snapshot no longer contains the path, or
    /// the path is re-created. The VFS's rmdir emptiness check ignores
    /// these — without that, `rm -rf` hits ENOTEMPTY milliseconds after
    /// its last unlink.
    deleted: HashSet<String>,
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

/// How many consecutive name-conflict failures a rename pair survives
/// before it is dropped. Each failure is a round trip to the API; more
/// than a handful of collisions on one move means the remote name is not
/// freeing up (see `Store::finish_rename`).
pub const RENAME_MAX_CONFLICT_RETRIES: u32 = 5;

/// One unit of local work for the engine's main loop, popped atomically
/// from the store's queues with delete → rename → write priority
/// ([`Store::next_work_request`]).
pub enum WorkRequest {
    Delete(String),
    Rename(String, String),
    Write(String),
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
        // Deletes the new snapshot has absorbed are settled; forget them.
        let absorbed: Vec<String> = inner
            .deleted
            .iter()
            .filter(|rel| !inner.snapshot.by_path.contains_key(*rel))
            .cloned()
            .collect();
        for rel in absorbed {
            inner.deleted.remove(&rel);
        }
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
            // Re-created after a delete: drop the stale snapshot mark
            // (same reasoning as record_mkdir).
            inner.deleted.remove(&rel);
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

    /// Record a metadata-only change (`setattr` mtime/mode) for the engine.
    /// Coalesced per path (latest value wins) and deliberately separate from
    /// [`Store::record_write`]: unchanged content must not create a revision.
    /// `None` fields are left untouched.
    pub fn record_metadata(&self, rel: &str, mtime: Option<i64>, mode: Option<u32>) {
        let mut inner = self.lock();
        inner.deleted.remove(rel);
        if let Some(entry) = inner.pending_metadata.iter_mut().find(|(r, _, _)| r == rel) {
            if mtime.is_some() {
                entry.1 = mtime;
            }
            if mode.is_some() {
                entry.2 = mode;
            }
        } else {
            inner
                .pending_metadata
                .push_back((rel.to_string(), mtime, mode));
        }
        drop(inner);
        self.notify.notify_one();
    }

    /// Next metadata-only change to apply (engine main loop).
    pub fn next_metadata_request(&self) -> Option<(String, Option<i64>, Option<u32>)> {
        self.lock().pending_metadata.pop_front()
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
                inner.pending_mkdirs.push_back(rel.clone());
            }
            // Re-created after a delete: the stale tombstone snapshot mark
            // (if any) no longer describes this path.
            inner.deleted.remove(&rel);
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

    /// Put a folder back after a failed remote create (engine retry).
    /// Without this the folder would vanish from the queue and never
    /// reach the remote.
    pub fn requeue_mkdir(&self, rel: &str) {
        let mut inner = self.lock();
        let rel = rel.to_string();
        if !inner.pending_mkdirs.contains(&rel) {
            inner.pending_mkdirs.push_back(rel);
        }
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
                inner.pending_deletes.push_back(rel.clone());
            }
            inner.deleted.insert(rel);
            first
        };
        if first {
            self.notify.notify_one();
        }
    }

    /// Queue an overwrite-rename atomically: `dest` is replaced by a rename
    /// of `old_rel` onto `new_rel` (VFS rename over an existing destination).
    /// Equivalent to `record_delete(dest)` + `record_rename(old, new)` under
    /// a single lock with a single wake-up, so the engine can never drain
    /// the delete before the rename is queued (it would see no inbound
    /// rename and tear down the content the rename is delivering).
    pub fn record_overwrite(&self, dest_rel: &str, old_rel: &str, new_rel: &str) {
        let mut inner = self.lock();
        let dest = dest_rel.to_string();
        if !inner.pending_deletes.contains(&dest) {
            inner.pending_deletes.push_back(dest.clone());
        }
        inner.deleted.insert(dest);

        let old = old_rel.to_string();
        let new = new_rel.to_string();
        // Re-key queued renames whose destination is this source: the
        // file they describe is the one now moving again.
        for r in inner.pending_renames.iter_mut() {
            if r.1 == old {
                r.1 = new.clone();
            }
        }
        // A pair renamed back onto itself cancels.
        inner.pending_renames.retain(|(o, n)| o != n);
        // The overwrite delivers NEW content to the destination, so its
        // upload must follow the pair: drop the source's queued upload (its
        // row follows the rename; the content now lives at the destination)
        // and queue the destination. Queue even while a destination upload
        // is in flight — next_write_request holds in-flight entries until
        // they finish, then this entry delivers the fresh content. Without
        // the unconditional queue, an overwrite arriving after the source's
        // write already drained (vanished tmp) queued nothing and the new
        // content never reached the remote.
        inner.pending_writes.retain(|w| *w != old && *w != new);
        inner.pending_writes.push_back(new.clone());
        if inner.pending_renames.iter().any(|(o, n)| o == &old && n == &new) {
            // Already queued; the delete above is the only new work.
        } else {
            inner.pending_renames.push_back((old, new));
        }
        drop(inner);
        self.notify.notify_one();
    }

    /// Queue depths for tests and diagnostics: (delete requests, rename
    /// requests, write requests). The queues are in-memory only.
    pub fn pending_counts(&self) -> (usize, usize, usize) {
        let inner = self.lock();
        (
            inner.pending_deletes.len(),
            inner.pending_renames.len(),
            inner.pending_writes.len(),
        )
    }

    /// Next path to delete remotely (engine), oldest first.
    pub fn next_delete_request(&self) -> Option<String> {
        self.lock().pending_deletes.pop_front()
    }

    /// One structural/unit-of-work pop for the engine's main loop, with the
    /// full delete → rename → write priority resolved ATOMICALLY under a
    /// single lock. The three queues are fed by concurrent producers (VFS
    /// threads); separate per-queue pops let a producer's multi-queue
    /// operation land between two pops and invert the priority — an
    /// overwrite-rename (delete + rename + write queued together) draining
    /// its write BEFORE its delete trashed the destination node it had just
    /// uploaded (observed: v2 lands, then `trashed remotely`). A single pop
    /// cannot interleave with a producer's locked queueing, so the triple
    /// always drains delete → rename → write. Write entries whose upload is
    /// in flight stay queued (same rule as `next_write_request`).
    pub fn next_work_request(&self) -> Option<WorkRequest> {
        let mut inner = self.lock();
        if let Some(rel) = inner.pending_deletes.pop_front() {
            return Some(WorkRequest::Delete(rel));
        }
        if let Some((old, new)) = inner.pending_renames.pop_front() {
            return Some(WorkRequest::Rename(old, new));
        }
        let mut skipped = Vec::new();
        let mut result = None;
        while let Some(rel) = inner.pending_writes.pop_front() {
            if inner.uploading.contains(&rel) {
                skipped.push(rel);
            } else {
                inner.uploading.insert(rel.clone());
                result = Some(WorkRequest::Write(rel));
                break;
            }
        }
        for rel in skipped.into_iter().rev() {
            inner.pending_writes.push_front(rel);
        }
        result
    }

    /// True if `rel` has a delete queued (or in flight) from the mount.
    /// The VFS uses this to answer rmdir's emptiness check for children
    /// that are gone on disk but still served by the last snapshot until
    /// the engine drains their tombstones — without it, an `rm -rf` gets
    /// ENOTEMPTY milliseconds after the last unlink.
    pub fn has_pending_delete(&self, rel: &str) -> bool {
        let inner = self.lock();
        inner.pending_deletes.iter().any(|p| p == rel) || inner.deleted.contains(rel)
    }

    /// Queue a rename made through the mount (VFS `rename`). Chained
    /// renames merge: a→b queued, then b→c arrives → a→c; a rename back
    /// onto itself (a→b→a) cancels out entirely.
    pub fn record_rename(&self, old_rel: &str, new_rel: &str) {
        let first = {
            let mut inner = self.lock();
            let old = old_rel.to_string();
            let new = new_rel.to_string();
            // Re-key queued renames whose destination is this source: the
            // file they describe is the one now moving again.
            let mut rekeyed = false;
            for r in inner.pending_renames.iter_mut() {
                if r.1 == old {
                    r.1 = new.clone();
                    rekeyed = true;
                }
            }
            // A pair renamed back onto itself cancels.
            inner.pending_renames.retain(|(o, n)| o != n);
            // The source's queued upload (if any) must follow the renamed
            // backing file, or the engine finds "backing file vanished".
            // A queued upload of the DESTINATION is dropped: the overwrite
            // delete + rename pair supersedes it.
            if inner.pending_writes.contains(&old) {
                inner.pending_writes.retain(|w| *w != old && *w != new);
                inner.pending_writes.push_back(new.clone());
            } else {
                inner.pending_writes.retain(|w| *w != new);
            }
            // An IN-FLIGHT upload of the source refers to the old path and
            // will fail ("backing file vanished") — re-queue it under the
            // new path so the content still uploads after the rename.
            if inner.uploading.contains(&old) {
                inner.uploading.remove(&old);
                if !inner.pending_writes.contains(&new) {
                    inner.pending_writes.push_back(new.clone());
                }
            }
            let notify = if rekeyed {
                // The new rename is expressed by the merged pair (or
                // cancelled); the standalone (old, new) entry would move
                // a source path that no longer exists.
                !inner.pending_renames.is_empty()
            } else if inner
                .pending_renames
                .iter()
                .any(|(o, n)| o == &old && n == &new)
            {
                false
            } else {
                inner.pending_renames.push_back((old, new.clone()));
                true
            };
            // A re-keyed write queue entry may need the engine awake even
            // when the rename itself deduped away.
            notify || (!inner.pending_writes.is_empty() && !inner.uploading.contains(&new))
        };
        if first {
            self.notify.notify_one();
        }
    }

    /// Next queued rename (engine). None if only cancelled pairs remain.
    pub fn next_rename_request(&self) -> Option<(String, String)> {
        let mut inner = self.lock();
        loop {
            let (old, new) = inner.pending_renames.pop_front()?;
            if old == new {
                continue; // renamed back to itself; no-op
            }
            return Some((old, new));
        }
    }

    /// Finish a drained rename. `ok=false` re-queues it (transient
    /// failures retry immediately; the engine's main loop only reaches
    /// `pump_work` when this queue is empty, so an instant retry keeps
    /// the API path alive). `permanent=true` marks a name conflict — a
    /// failure that cannot heal by retrying alone — and counts toward
    /// [`RENAME_MAX_CONFLICT_RETRIES`]; the pair is dropped (returns
    /// false) once the cap is reached, so a doomed move cannot starve
    /// the tombstone drain that would free the name. Dropping clears the
    /// count: a rename the user makes again later gets a fresh budget.
    pub fn finish_rename(&self, old_rel: &str, new_rel: &str, ok: bool, permanent: bool) -> bool {
        let mut inner = self.lock();
        let key = (old_rel.to_string(), new_rel.to_string());
        if ok {
            inner.rename_conflicts.remove(&key);
            return true;
        }
        if permanent {
            let attempts = {
                let count = inner.rename_conflicts.entry(key.clone()).or_insert(0);
                *count += 1;
                *count
            };
            if attempts >= RENAME_MAX_CONFLICT_RETRIES {
                inner.rename_conflicts.remove(&key);
                return false;
            }
        }
        if !inner
            .pending_renames
            .iter()
            .any(|(o, n)| o == old_rel && n == new_rel)
        {
            inner.pending_renames.push_back(key);
        }
        true
    }

    /// True while a queued or in-flight rename targets `rel` (an
    /// overwrite-rename's destination): the delete queue entry for the
    /// same path must not tear down the backing content the rename is
    /// delivering.
    pub fn rename_inbound(&self, rel: &str) -> bool {
        let inner = self.lock();
        inner.pending_renames.iter().any(|(_, n)| n == rel)
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
        inner
            .pending_renames
            .retain(|(o, n)| o != rel && n != rel);
        inner
            .rename_conflicts
            .retain(|(o, n), _| o != rel && n != rel);
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
        inner.pending_renames.retain(|(o, n)| keep(o) && keep(n));
    }

    // ---- VFS side ------------------------------------------------------

    /// Current snapshot (cheap Arc-free clone of the maps is avoided; the
    /// VFS clones the `RootSnapshot` values it needs per call instead).
    pub fn snapshot(&self) -> RootSnapshot {
        self.lock().snapshot.clone()
    }

    /// Monotonic snapshot generation (cheap; no map clone). Tests use it to
    /// wait for a publish without copying the whole tree.
    pub fn generation(&self) -> u64 {
        self.lock().snapshot.generation
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
            mode: 0o644,
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

        // A failed remote create re-queues (no dedup loss, no drop).
        store.record_mkdir("retry-me");
        assert_eq!(store.next_mkdir_request(), Some("retry-me".into()));
        store.requeue_mkdir("retry-me");
        assert_eq!(store.next_mkdir_request(), Some("retry-me".into()));
        store.requeue_mkdir("retry-me"); // no duplicate when already queued
        assert_eq!(store.next_mkdir_request(), Some("retry-me".into()));
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
    fn rename_queue_roundtrip_and_chaining() {
        let store = Store::new();
        store.record_rename("a.txt", "b.txt");
        store.record_rename("a.txt", "b.txt"); // dedup
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        store.finish_rename("a.txt", "b.txt", true, false);
        assert_eq!(store.next_rename_request(), None);

        // Failure re-queues; rename-back cancels on drain.
        store.record_rename("a.txt", "b.txt");
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        store.finish_rename("a.txt", "b.txt", false, false);
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        store.finish_rename("a.txt", "b.txt", true, false);

        store.record_rename("a.txt", "b.txt");
        store.record_rename("b.txt", "a.txt"); // chained → a→a, cancels
        assert_eq!(store.next_rename_request(), None);

        // Chained destination re-key: a→b then b→c merges into a→c.
        store.record_rename("a.txt", "b.txt");
        store.record_rename("b.txt", "c.txt");
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "c.txt".into()))
        );
        store.finish_rename("a.txt", "c.txt", true, false);

        // forget drops renames touching the path.
        store.record_rename("x.txt", "y.txt");
        store.forget("x.txt");
        assert_eq!(store.next_rename_request(), None);
    }

    #[test]
    fn rename_conflicts_are_capped_but_transient_failures_are_not() {
        let store = Store::new();
        // Transient failures re-queue without limit.
        store.record_rename("a.txt", "b.txt");
        for _ in 0..(RENAME_MAX_CONFLICT_RETRIES * 2) {
            assert_eq!(
                store.next_rename_request(),
                Some(("a.txt".into(), "b.txt".into()))
            );
            assert!(store.finish_rename("a.txt", "b.txt", false, false));
        }

        // Name conflicts stop after the cap: the pair is dropped, so the
        // main loop can reach pump_work and free the remote name.
        for _ in 1..RENAME_MAX_CONFLICT_RETRIES {
            assert_eq!(
                store.next_rename_request(),
                Some(("a.txt".into(), "b.txt".into()))
            );
            assert!(store.finish_rename("a.txt", "b.txt", false, true));
        }
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        assert!(!store.finish_rename("a.txt", "b.txt", false, true));
        assert_eq!(store.next_rename_request(), None);

        // Dropping cleared the count: the same pair queued again gets a
        // full budget instead of dying on its first conflict.
        store.record_rename("a.txt", "b.txt");
        for _ in 1..RENAME_MAX_CONFLICT_RETRIES {
            assert_eq!(
                store.next_rename_request(),
                Some(("a.txt".into(), "b.txt".into()))
            );
            assert!(store.finish_rename("a.txt", "b.txt", false, true));
        }
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        assert!(!store.finish_rename("a.txt", "b.txt", false, true));
        assert_eq!(store.next_rename_request(), None);

        // Success clears the count too.
        store.record_rename("a.txt", "b.txt");
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
        assert!(store.finish_rename("a.txt", "b.txt", true, false));
        store.record_rename("a.txt", "b.txt");
        for _ in 1..RENAME_MAX_CONFLICT_RETRIES {
            assert_eq!(
                store.next_rename_request(),
                Some(("a.txt".into(), "b.txt".into()))
            );
            assert!(store.finish_rename("a.txt", "b.txt", false, true));
        }
        assert_eq!(
            store.next_rename_request(),
            Some(("a.txt".into(), "b.txt".into()))
        );
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
