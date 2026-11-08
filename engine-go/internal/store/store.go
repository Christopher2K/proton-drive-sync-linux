// Package store is the shared snapshot store between the sync engine and
// the VFS.
//
// Placeholder mode never writes synced files into the visible root: the
// FUSE filesystem serves structure from snapshots the engine publishes
// here, and file contents from backing files the engine downloads on
// demand. The flow for an open():
//
//  1. VFS BeginHydrate queues the request and wakes the engine
//     (Notify, so it works from the FUSE thread).
//  2. The engine downloads the file into the backing dir, flips the row's
//     content_ready, and calls PublishSnapshot.
//  3. The VFS re-reads the snapshot and the read() retries succeed.
//
// All engine→VFS communication is mutex-guarded snapshots (never live DB
// handles): the VFS runs on its own goroutines.
//
// Port of crates/core/src/store.rs.
package store

import (
	"container/list"
	"path/filepath"
	"sync"
)

// VNode is what the VFS needs to know about one node. Path-keyed; mirrors
// the relevant nodes row columns (see the db package).
type VNode struct {
	Rel    string
	Parent string
	Name   string
	IsDir  bool
	Size   uint64
	Mtime  int64
	// Mode is the POSIX permission bits for the VFS attrs (files default
	// 0644, dirs 0755; a locally persisted executable bit survives
	// eviction).
	Mode uint32
	// ContentReady marks a backing file with the full content, ready to
	// serve.
	ContentReady bool
}

// RootSnapshot is an immutable view of the tree the VFS serves.
type RootSnapshot struct {
	Generation uint64
	ByPath     map[string]VNode
	// Children maps parent rel → children rels, sorted by name for stable
	// readdir.
	Children map[string][]string
}

// NewRootSnapshot returns an empty snapshot (generation 0).
func NewRootSnapshot() RootSnapshot {
	return RootSnapshot{ByPath: map[string]VNode{}, Children: map[string][]string{}}
}

// Clone returns a deep copy of the snapshot (the VFS clones the values it
// needs per call).
func (s RootSnapshot) Clone() RootSnapshot {
	byPath := make(map[string]VNode, len(s.ByPath))
	for k, v := range s.ByPath {
		byPath[k] = v
	}
	children := make(map[string][]string, len(s.Children))
	for k, v := range s.Children {
		l := make([]string, len(v))
		copy(l, v)
		children[k] = l
	}
	return RootSnapshot{Generation: s.Generation, ByPath: byPath, Children: children}
}

// renamePair is one queued rename: (old rel, new rel).
type renamePair struct{ oldRel, newRel string }

// metadataChange is a metadata-only change from the VFS (setattr
// mtime/mode): (rel, mtime, mode).
type metadataChange struct {
	rel   string
	mtime *int64
	mode  *uint32
}

// RenameMaxConflictRetries is how many consecutive name-conflict failures
// a rename pair survives before it is dropped. Each failure is a round
// trip to the API; more than a handful of collisions on one move means
// the remote name is not freeing up (see FinishRename).
const RenameMaxConflictRetries uint32 = 5

// WorkRequest is one unit of local work for the engine's main loop, popped
// atomically from the store's queues with delete → rename → write priority
// (NextWorkRequest).
type WorkRequest struct {
	// Kind is one of WorkDelete, WorkRename, WorkWrite.
	Kind WorkKind
	// Rel is the primary path (delete/write, rename source).
	Rel string
	// NewRel is the rename destination (rename only).
	NewRel string
}

// WorkKind enumerates WorkRequest kinds.
type WorkKind int

const (
	WorkDelete WorkKind = iota
	WorkRename
	WorkWrite
)

type inner struct {
	snapshot RootSnapshot
	// pending holds open requests awaiting hydration, oldest first.
	pending    *list.List // of string
	pendingSet map[string]bool
	// hydrating holds requests the engine is currently downloading.
	hydrating map[string]bool
	// hydrated maps rel → backing file path for hydrated files.
	hydrated map[string]string
	// pendingWrites holds write-created or write-modified files awaiting
	// upload, oldest first.
	pendingWrites *orderedSet
	// uploading holds files the engine is currently uploading from the
	// write queue.
	uploading map[string]bool
	// pendingMetadata holds metadata-only changes from the VFS
	// (setattr(mtime/mode)), drained by the engine into the DB/snapshot
	// WITHOUT queueing a content upload — the hash is unchanged.
	pendingMetadata *list.List // of metadataChange
	// pendingMkdirs holds folders created through the mount, awaiting a
	// remote mirror.
	pendingMkdirs *orderedSet
	// pendingDeletes holds paths deleted through the mount, awaiting a
	// remote trash.
	pendingDeletes *orderedSet
	// pendingRenames holds renames made through the mount, oldest first.
	// The engine turns each into a remote renameNode (never a
	// trash+re-upload), re-keying the DB rows beneath the paths.
	pendingRenames *list.List // of renamePair
	// renameConflicts counts consecutive name-conflict failures per rename
	// pair. A doomed pair (the remote destination name never frees up) is
	// dropped at RenameMaxConflictRetries instead of re-queueing forever:
	// renames drain before pump_work, so a perpetual retry starves the
	// tombstone drain that could unblock it.
	renameConflicts map[renamePair]uint32
	// deleted holds paths deleted through the mount that the served
	// snapshot may still list until the engine drains their tombstones and
	// republishes. Outlives pendingDeletes (which the engine pops
	// immediately); cleared when a published snapshot no longer contains
	// the path, or the path is re-created. The VFS's rmdir emptiness check
	// ignores these — without that, `rm -rf` hits ENOTEMPTY milliseconds
	// after its last unlink.
	deleted map[string]bool
	// lastAccess records the last access per hydrated rel (LRU order for
	// eviction; M3.2). Monotonic counter, not wall time: avoids clock skew
	// and gives a total order even for same-second reads.
	lastAccess    map[string]uint64
	accessCounter uint64
	// openHandles counts open FUSE handles per rel — never evict open
	// files.
	openHandles map[string]int
	// nextGeneration is the monotonic generation for PublishSnapshot.
	nextGeneration uint64
}

// Store is the engine ↔ VFS shared state.
type Store struct {
	mu sync.Mutex
	in inner
	// Notify is woken when a new hydrate request is queued. Works from the
	// FUSE goroutine (no lock held while firing).
	Notify *Notify
}

// New creates an empty store.
func New() *Store {
	return &Store{
		in: inner{
			snapshot:        NewRootSnapshot(),
			pending:         list.New(),
			pendingSet:      map[string]bool{},
			hydrating:       map[string]bool{},
			hydrated:        map[string]string{},
			pendingWrites:   newOrderedSet(),
			uploading:       map[string]bool{},
			pendingMetadata: list.New(),
			pendingMkdirs:   newOrderedSet(),
			pendingDeletes:  newOrderedSet(),
			pendingRenames:  list.New(),
			renameConflicts: map[renamePair]uint32{},
			deleted:         map[string]bool{},
			lastAccess:      map[string]uint64{},
			openHandles:     map[string]int{},
		},
		Notify: NewNotify(),
	}
}

// orderedSet is a FIFO queue of unique strings (Rust VecDeque + contains).
type orderedSet struct {
	queue *list.List // of string
	set   map[string]bool
}

func newOrderedSet() *orderedSet {
	return &orderedSet{queue: list.New(), set: map[string]bool{}}
}

func (o *orderedSet) contains(rel string) bool { return o.set[rel] }

func (o *orderedSet) pushBack(rel string) {
	if !o.set[rel] {
		o.set[rel] = true
		o.queue.PushBack(rel)
	}
}

func (o *orderedSet) popFront() (string, bool) {
	e := o.queue.Front()
	if e == nil {
		return "", false
	}
	o.queue.Remove(e)
	rel := e.Value.(string)
	delete(o.set, rel)
	return rel, true
}

func (o *orderedSet) len() int { return o.queue.Len() }

func (o *orderedSet) retain(keep func(string) bool) {
	for e := o.queue.Front(); e != nil; {
		next := e.Next()
		if !keep(e.Value.(string)) {
			o.queue.Remove(e)
			delete(o.set, e.Value.(string))
		}
		e = next
	}
}

// ---- engine side ---------------------------------------------------------

// PublishSnapshot atomically replaces the tree snapshot and advances the
// generation. hydrated maps rel → backing file for files whose content is
// on disk; everything else without ContentReady in the rows is a
// placeholder.
func (s *Store) PublishSnapshot(byPath map[string]VNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	children := map[string][]string{}
	for _, node := range byPath {
		children[node.Parent] = append(children[node.Parent], node.Rel)
	}
	for _, list := range children {
		sortStrings(list)
	}
	s.in.nextGeneration++
	s.in.snapshot = RootSnapshot{
		Generation: s.in.nextGeneration,
		Children:   children,
		ByPath:     byPath,
	}
	// Deletes the new snapshot has absorbed are settled; forget them.
	for rel := range s.in.deleted {
		if _, ok := s.in.snapshot.ByPath[rel]; !ok {
			delete(s.in.deleted, rel)
		}
	}
}

// SetBacking registers a hydrated backing file (engine, after a download
// lands).
func (s *Store) SetBacking(rel, backing string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.in.hydrated[rel] = backing
}

// ClearBacking removes the backing entry (row deleted, or eviction).
func (s *Store) ClearBacking(rel string) { s.Forget(rel) }

// NextHydrateRequest returns the next queued hydrate request, if any
// (engine, from the main loop).
func (s *Store) NextHydrateRequest() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		rel, ok := s.in.popPendingLocked()
		if !ok {
			return "", false
		}
		if _, done := s.in.hydrated[rel]; done {
			continue // content landed meanwhile
		}
		s.in.hydrating[rel] = true
		return rel, true
	}
}

// popPendingLocked pops the front of the pending hydrate queue.
func (in *inner) popPendingLocked() (string, bool) {
	e := in.pending.Front()
	if e == nil {
		return "", false
	}
	in.pending.Remove(e)
	rel := e.Value.(string)
	delete(in.pendingSet, rel)
	return rel, true
}

// FinishHydrate finishes a hydration: the engine calls this after the
// backing file is complete (or failed; ok=false just unblocks
// re-requesting).
func (s *Store) FinishHydrate(rel string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ok {
		delete(s.in.hydrated, rel)
	} else {
		// A fresh hydration is by definition the most recent use.
		s.in.accessCounter++
		s.in.lastAccess[rel] = s.in.accessCounter
	}
	delete(s.in.hydrating, rel)
}

// ---- pin / evict (M3.2) --------------------------------------------------

// RecordAccess records a read (or hydration-triggering open) of rel for
// the LRU order. Cheap: a mutex + counter bump.
func (s *Store) RecordAccess(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.in.accessCounter++
	s.in.lastAccess[rel] = s.in.accessCounter
}

// AddOpenHandle records that an open handle appeared for rel. Open files
// are never evicted.
func (s *Store) AddOpenHandle(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.in.openHandles[rel]++
}

// RemoveOpenHandle records that an open handle for rel closed.
func (s *Store) RemoveOpenHandle(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.in.openHandles[rel]; ok {
		if n > 0 {
			n--
		}
		if n == 0 {
			delete(s.in.openHandles, rel)
		} else {
			s.in.openHandles[rel] = n
		}
	}
}

// HydratedByAge returns LRU-ordered hydrated paths (engine, eviction
// pass): oldest access first.
func (s *Store) HydratedByAge() []HydratedAge {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := make([]HydratedAge, 0, len(s.in.hydrated))
	for rel := range s.in.hydrated {
		age := s.in.lastAccess[rel] // missing key → 0, same as Rust
		v = append(v, HydratedAge{Rel: rel, Age: age})
	}
	// Map iteration order is random in Go; sort by age AND rel for a
	// stable total order (Rust sort_by_key is stable, but its input order
	// is the HashMap's arbitrary order — tests only compare relative
	// order, and ties never occur because the access counter is unique).
	sortHydratedByAge(v)
	return v
}

// HydratedAge is one entry of HydratedByAge.
type HydratedAge struct {
	Rel string
	Age uint64
}

// IsOpen reports whether rel is held open by the VFS right now.
func (s *Store) IsOpen(rel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.openHandles[rel] > 0
}

// ---- write-through (M3.2) ------------------------------------------------

// RecordWrite records a finished write to a backing file (VFS
// write/setattr, after the bytes are on disk) and wakes the engine to
// upload it. Re-queues while an upload is in flight: the newer content
// must be uploaded again after the current attempt (latest content wins —
// the engine re-hashes at upload time).
func (s *Store) RecordWrite(rel string) {
	s.mu.Lock()
	// Re-created after a delete: drop the stale snapshot mark (same
	// reasoning as RecordMkdir).
	delete(s.in.deleted, rel)
	first := !s.in.pendingWrites.contains(rel)
	if first {
		s.in.pendingWrites.pushBack(rel)
	}
	s.mu.Unlock()
	if first {
		s.Notify.NotifyOne()
	}
}

// RecordMetadata records a metadata-only change (setattr mtime/mode) for
// the engine. Coalesced per path (latest value wins) and deliberately
// separate from RecordWrite: unchanged content must not create a
// revision. nil fields are left untouched.
func (s *Store) RecordMetadata(rel string, mtime *int64, mode *uint32) {
	s.mu.Lock()
	var entry *metadataChange
	for e := s.in.pendingMetadata.Front(); e != nil; e = e.Next() {
		mc := e.Value.(*metadataChange)
		if mc.rel == rel {
			entry = mc
			break
		}
	}
	if entry == nil {
		entry = &metadataChange{rel: rel}
		s.in.pendingMetadata.PushBack(entry)
	}
	if mtime != nil {
		entry.mtime = mtime
	}
	if mode != nil {
		entry.mode = mode
	}
	s.mu.Unlock()
	s.Notify.NotifyOne()
}

// NextMetadataRequest returns the next metadata-only change to apply
// (engine main loop).
func (s *Store) NextMetadataRequest() (rel string, mtime *int64, mode *uint32, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.in.pendingMetadata.Front()
	if e == nil {
		return "", nil, nil, false
	}
	s.in.pendingMetadata.Remove(e)
	mc := e.Value.(*metadataChange)
	return mc.rel, mc.mtime, mc.mode, true
}

// NextWriteRequest returns the next write-created/-modified file to
// upload, if any (engine). Entries whose previous upload is still in
// flight stay queued.
func (s *Store) NextWriteRequest() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextWriteLocked()
}

func (s *Store) nextWriteLocked() (string, bool) {
	var skipped []string
	for {
		rel, ok := s.in.pendingWrites.popFront()
		if !ok {
			break
		}
		if s.in.uploading[rel] {
			skipped = append(skipped, rel)
			continue
		}
		s.in.uploading[rel] = true
		// Push skipped entries back to the FRONT, newest-skipped first,
		// restoring their original relative order (Rust pushes rev()).
		for i := len(skipped) - 1; i >= 0; i-- {
			s.in.pendingWrites.pushFront(skipped[i])
		}
		return rel, true
	}
	for i := len(skipped) - 1; i >= 0; i-- {
		s.in.pendingWrites.pushFront(skipped[i])
	}
	return "", false
}

// pushFront re-inserts rel at the front of the queue, deduplicating.
func (o *orderedSet) pushFront(rel string) {
	if o.set[rel] {
		return
	}
	o.set[rel] = true
	o.queue.PushFront(rel)
}

// FinishWrite marks one upload attempt from the write queue finished. On
// failure the file re-queues (retry on the next main-loop pass).
func (s *Store) FinishWrite(rel string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.in.uploading, rel)
	if !ok {
		s.in.pendingWrites.pushBack(rel)
	}
}

// IsUploading reports true while the engine is uploading rel (VFS: hide
// the `.pdsw-` partial and answer reads from the last complete backing
// content).
func (s *Store) IsUploading(rel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.uploading[rel]
}

// PendingWrites returns the number of queued + in-flight write uploads
// (status display).
func (s *Store) PendingWrites() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.pendingWrites.len() + len(s.in.uploading)
}

// RecordMkdir queues a folder created through the mount (VFS mkdir).
func (s *Store) RecordMkdir(rel string) {
	s.mu.Lock()
	first := !s.in.pendingMkdirs.contains(rel)
	if first {
		s.in.pendingMkdirs.pushBack(rel)
	}
	// Re-created after a delete: the stale tombstone snapshot mark (if
	// any) no longer describes this path.
	delete(s.in.deleted, rel)
	s.mu.Unlock()
	if first {
		s.Notify.NotifyOne()
	}
}

// NextMkdirRequest returns the next folder to create remotely (engine).
// Parents sort before children because the VFS queues them parent-first.
func (s *Store) NextMkdirRequest() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.pendingMkdirs.popFront()
}

// RequeueMkdir puts a folder back after a failed remote create (engine
// retry). Without this the folder would vanish from the queue and never
// reach the remote.
func (s *Store) RequeueMkdir(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.in.pendingMkdirs.pushBack(rel)
}

// RecordDelete queues a path removed through the mount (VFS unlink/rmdir).
// Deletion order is the VFS's problem (children are unlinked first); the
// engine mirrors each as a durable tombstone + trash.
func (s *Store) RecordDelete(rel string) {
	s.mu.Lock()
	first := !s.in.pendingDeletes.contains(rel)
	if first {
		s.in.pendingDeletes.pushBack(rel)
	}
	s.in.deleted[rel] = true
	s.mu.Unlock()
	if first {
		s.Notify.NotifyOne()
	}
}

// RecordOverwrite queues an overwrite-rename atomically: dest is replaced
// by a rename of oldRel onto newRel (VFS rename over an existing
// destination). Equivalent to RecordDelete(dest) + RecordRename(old, new)
// under a single lock with a single wake-up, so the engine can never drain
// the delete before the rename is queued (it would see no inbound rename
// and tear down the content the rename is delivering).
func (s *Store) RecordOverwrite(destRel, oldRel, newRel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.in.pendingDeletes.contains(destRel) {
		s.in.pendingDeletes.pushBack(destRel)
	}
	s.in.deleted[destRel] = true

	// Re-key queued renames whose destination is this source: the file
	// they describe is the one now moving again.
	for e := s.in.pendingRenames.Front(); e != nil; e = e.Next() {
		r := e.Value.(*renamePair)
		if r.newRel == oldRel {
			r.newRel = newRel
		}
	}
	// A pair renamed back onto itself cancels.
	s.retainRenamesLocked(func(o, n string) bool { return o != n })
	// The overwrite delivers NEW content to the destination, so its
	// upload must follow the pair: drop the source's queued upload (its
	// row follows the rename; the content now lives at the destination)
	// and queue the destination. Queue even while a destination upload is
	// in flight — NextWriteRequest holds in-flight entries until they
	// finish, then this entry delivers the fresh content. Without the
	// unconditional queue, an overwrite arriving after the source's write
	// already drained (vanished tmp) queued nothing and the new content
	// never reached the remote.
	s.in.pendingWrites.retain(func(w string) bool { return w != oldRel && w != newRel })
	s.in.pendingWrites.pushBack(newRel)
	if !s.pendingRenamesContainsLocked(oldRel, newRel) {
		// Already queued; the delete above is the only new work.
		s.in.pendingRenames.PushBack(&renamePair{oldRel: oldRel, newRel: newRel})
	}
	s.Notify.NotifyOne()
}

// PendingCounts returns the queue depths for tests and diagnostics:
// (delete requests, rename requests, write requests). The queues are
// in-memory only.
func (s *Store) PendingCounts() (deletes, renames, writes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.pendingDeletes.len(), s.in.pendingRenames.Len(), s.in.pendingWrites.len()
}

// NextDeleteRequest returns the next path to delete remotely (engine),
// oldest first.
func (s *Store) NextDeleteRequest() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.pendingDeletes.popFront()
}

// NextWorkRequest is one structural/unit-of-work pop for the engine's main
// loop, with the full delete → rename → write priority resolved
// ATOMICALLY under a single lock. The three queues are fed by concurrent
// producers (VFS threads); separate per-queue pops let a producer's
// multi-queue operation land between two pops and invert the priority —
// an overwrite-rename (delete + rename + write queued together) draining
// its write BEFORE its delete trashed the destination node it had just
// uploaded (observed: v2 lands, then "trashed remotely"). A single pop
// cannot interleave with a producer's locked queueing, so the triple
// always drains delete → rename → write. Write entries whose upload is in
// flight stay queued (same rule as NextWriteRequest).
func (s *Store) NextWorkRequest() (WorkRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rel, ok := s.in.pendingDeletes.popFront(); ok {
		return WorkRequest{Kind: WorkDelete, Rel: rel}, true
	}
	if e := s.in.pendingRenames.Front(); e != nil {
		s.in.pendingRenames.Remove(e)
		r := e.Value.(*renamePair)
		return WorkRequest{Kind: WorkRename, Rel: r.oldRel, NewRel: r.newRel}, true
	}
	if rel, ok := s.nextWriteLocked(); ok {
		return WorkRequest{Kind: WorkWrite, Rel: rel}, true
	}
	return WorkRequest{}, false
}

// HasPendingDelete reports true if rel has a delete queued (or in flight)
// from the mount. The VFS uses this to answer rmdir's emptiness check for
// children that are gone on disk but still served by the last snapshot
// until the engine drains their tombstones — without it, an `rm -rf` gets
// ENOTEMPTY milliseconds after its last unlink.
func (s *Store) HasPendingDelete(rel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.pendingDeletes.contains(rel) || s.in.deleted[rel]
}

// RecordRename queues a rename made through the mount (VFS rename).
// Chained renames merge: a→b queued, then b→c arrives → a→c; a rename
// back onto itself (a→b→a) cancels out entirely.
func (s *Store) RecordRename(oldRel, newRel string) {
	s.mu.Lock()
	// Re-key queued renames whose destination is this source: the file
	// they describe is the one now moving again.
	rekeyed := false
	for e := s.in.pendingRenames.Front(); e != nil; e = e.Next() {
		r := e.Value.(*renamePair)
		if r.newRel == oldRel {
			r.newRel = newRel
			rekeyed = true
		}
	}
	// A pair renamed back onto itself cancels.
	s.retainRenamesLocked(func(o, n string) bool { return o != n })
	// The source's queued upload (if any) must follow the renamed backing
	// file, or the engine finds "backing file vanished". A queued upload
	// of the DESTINATION is dropped: the overwrite delete + rename pair
	// supersedes it.
	if s.in.pendingWrites.contains(oldRel) {
		s.in.pendingWrites.retain(func(w string) bool { return w != oldRel && w != newRel })
		s.in.pendingWrites.pushBack(newRel)
	} else {
		s.in.pendingWrites.retain(func(w string) bool { return w != newRel })
	}
	// An IN-FLIGHT upload of the source refers to the old path and will
	// fail ("backing file vanished") — re-queue it under the new path so
	// the content still uploads after the rename.
	if s.in.uploading[oldRel] {
		delete(s.in.uploading, oldRel)
		s.in.pendingWrites.pushBack(newRel)
	}
	notify := false
	if rekeyed {
		// The new rename is expressed by the merged pair (or cancelled);
		// the standalone (old, new) entry would move a source path that
		// no longer exists.
		notify = s.in.pendingRenames.Len() > 0
	} else if s.pendingRenamesContainsLocked(oldRel, newRel) {
		notify = false
	} else {
		s.in.pendingRenames.PushBack(&renamePair{oldRel: oldRel, newRel: newRel})
		notify = true
	}
	// A re-keyed write queue entry may need the engine awake even when
	// the rename itself deduped away.
	notify = notify || (s.in.pendingWrites.len() > 0 && !s.in.uploading[newRel])
	s.mu.Unlock()
	if notify {
		s.Notify.NotifyOne()
	}
}

// NextRenameRequest returns the next queued rename (engine). False if
// only cancelled pairs remain.
func (s *Store) NextRenameRequest() (oldRel, newRel string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		e := s.in.pendingRenames.Front()
		if e == nil {
			return "", "", false
		}
		s.in.pendingRenames.Remove(e)
		r := e.Value.(*renamePair)
		if r.oldRel == r.newRel {
			continue // renamed back to itself; no-op
		}
		return r.oldRel, r.newRel, true
	}
}

// FinishRename finishes a drained rename. ok=false re-queues it
// (transient failures retry immediately; the engine's main loop only
// reaches pump_work when this queue is empty, so an instant retry keeps
// the API path alive). permanent=true marks a name conflict — a failure
// that cannot heal by retrying alone — and counts toward
// RenameMaxConflictRetries; the pair is dropped (returns false) once the
// cap is reached, so a doomed move cannot starve the tombstone drain that
// would free the name. Dropping clears the count: a rename the user makes
// again later gets a fresh budget.
func (s *Store) FinishRename(oldRel, newRel string, ok, permanent bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := renamePair{oldRel: oldRel, newRel: newRel}
	if ok {
		delete(s.in.renameConflicts, key)
		return true
	}
	if permanent {
		s.in.renameConflicts[key]++
		if s.in.renameConflicts[key] >= RenameMaxConflictRetries {
			delete(s.in.renameConflicts, key)
			return false
		}
	}
	if !s.pendingRenamesContainsLocked(oldRel, newRel) {
		s.in.pendingRenames.PushBack(&renamePair{oldRel: oldRel, newRel: newRel})
	}
	return true
}

// RenameInbound reports true while a queued or in-flight rename targets
// rel (an overwrite-rename's destination): the delete queue entry for the
// same path must not tear down the backing content the rename is
// delivering.
func (s *Store) RenameInbound(rel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for e := s.in.pendingRenames.Front(); e != nil; e = e.Next() {
		if e.Value.(*renamePair).newRel == rel {
			return true
		}
	}
	return false
}

// Forget drops a path from every queue (row deleted, or hydration
// failed). Called with the DB-rel form of the path.
func (s *Store) Forget(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.in.hydrated, rel)
	delete(s.in.hydrating, rel)
	s.retainListLocked(s.in.pending, func(p string) bool { return p != rel })
	delete(s.in.pendingSet, rel)
	s.in.pendingWrites.retain(func(p string) bool { return p != rel })
	delete(s.in.uploading, rel)
	s.in.pendingMkdirs.retain(func(p string) bool { return p != rel })
	s.in.pendingDeletes.retain(func(p string) bool { return p != rel })
	delete(s.in.lastAccess, rel)
	delete(s.in.openHandles, rel)
	s.retainRenamesLocked(func(o, n string) bool { return o != rel && n != rel })
	for key := range s.in.renameConflicts {
		if key.oldRel == rel || key.newRel == rel {
			delete(s.in.renameConflicts, key)
		}
	}
}

// ForgetTree drops rel and every path under it (recursive delete through
// the mount: children were queued explicitly, but drop leftovers anyway).
func (s *Store) ForgetTree(rel string) {
	prefix := rel + "/"
	keep := func(p string) bool { return p != rel && !hasPrefix(p, prefix) }
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retainMapLocked(s.in.hydrated, keep)
	s.retainBoolMapLocked(s.in.hydrating, keep)
	s.retainListLocked(s.in.pending, keep)
	s.retainBoolMapLocked(s.in.pendingSet, keep)
	s.in.pendingWrites.retain(keep)
	s.retainBoolMapLocked(s.in.uploading, keep)
	s.in.pendingMkdirs.retain(keep)
	s.in.pendingDeletes.retain(keep)
	s.retainU64MapLocked(s.in.lastAccess, keep)
	s.retainIntMapLocked(s.in.openHandles, keep)
	s.retainRenamesLocked(func(o, n string) bool { return keep(o) && keep(n) })
}

// ---- VFS side ------------------------------------------------------------

// Snapshot returns the current snapshot.
func (s *Store) Snapshot() RootSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.snapshot.Clone()
}

// Generation returns the monotonic snapshot generation (cheap; no map
// clone). Tests use it to wait for a publish without copying the whole
// tree.
func (s *Store) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in.snapshot.Generation
}

// BeginHydrate queues an open of a not-yet-hydrated file. Returns true
// when this caller is the first requester (the engine gets one wake for
// many).
func (s *Store) BeginHydrate(rel string) bool {
	s.mu.Lock()
	_, hasBacking := s.in.hydrated[rel]
	first := !hasBacking && !s.in.hydrating[rel] && !s.in.pendingSet[rel]
	if first {
		s.in.pending.PushBack(rel)
		s.in.pendingSet[rel] = true
	}
	s.mu.Unlock()
	if first {
		s.Notify.NotifyOne()
	}
	return first
}

// BackingPath returns the backing file of a hydrated file, if ready.
func (s *Store) BackingPath(rel string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.in.hydrated[rel]
	return p, ok
}

// HydratingCount returns the number of files currently being downloaded
// (status display).
func (s *Store) HydratingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.in.hydrating)
}

// pendingRenamesContainsLocked reports whether (old,new) is queued.
func (s *Store) pendingRenamesContainsLocked(oldRel, newRel string) bool {
	for e := s.in.pendingRenames.Front(); e != nil; e = e.Next() {
		r := e.Value.(*renamePair)
		if r.oldRel == oldRel && r.newRel == newRel {
			return true
		}
	}
	return false
}

// retainRenamesLocked filters the rename queue in place.
func (s *Store) retainRenamesLocked(keep func(old, newRel string) bool) {
	for e := s.in.pendingRenames.Front(); e != nil; {
		next := e.Next()
		r := e.Value.(*renamePair)
		if !keep(r.oldRel, r.newRel) {
			s.in.pendingRenames.Remove(e)
		}
		e = next
	}
}

// retainListLocked filters a list.List of string values in place.
func (s *Store) retainListLocked(l *list.List, keep func(string) bool) {
	for e := l.Front(); e != nil; {
		next := e.Next()
		if !keep(e.Value.(string)) {
			l.Remove(e)
		}
		e = next
	}
}

func (s *Store) retainMapLocked(m map[string]string, keep func(string) bool) {
	for k := range m {
		if !keep(k) {
			delete(m, k)
		}
	}
}

func (s *Store) retainBoolMapLocked(m map[string]bool, keep func(string) bool) {
	for k := range m {
		if !keep(k) {
			delete(m, k)
		}
	}
}

func (s *Store) retainU64MapLocked(m map[string]uint64, keep func(string) bool) {
	for k := range m {
		if !keep(k) {
			delete(m, k)
		}
	}
}

func (s *Store) retainIntMapLocked(m map[string]int, keep func(string) bool) {
	for k := range m {
		if !keep(k) {
			delete(m, k)
		}
	}
}

// BackingFilePath returns the backing file path for a hydrated
// placeholder: `<backing_dir>/<rel>` (mirroring the tree keeps eviction
// and debugging simple).
func BackingFilePath(backingDir, rel string) string {
	return filepath.Join(backingDir, filepath.FromSlash(rel))
}
