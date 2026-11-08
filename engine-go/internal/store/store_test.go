package store

import (
	"slices"
	"testing"
)

// node mirrors the Rust test helper: split rel into (parent, name).
func node(rel string, isDir bool) VNode {
	parent, name := "", rel
	if i := lastIndexSlash(rel); i >= 0 {
		parent, name = rel[:i], rel[i+1:]
	}
	return VNode{
		Rel: rel, Parent: parent, Name: name,
		IsDir: isDir, Size: 123, Mtime: 0, Mode: 0o644, ContentReady: false,
	}
}

func lastIndexSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func pubMap(nodes ...VNode) map[string]VNode {
	m := make(map[string]VNode, len(nodes))
	for _, n := range nodes {
		m[n.Rel] = n
	}
	return m
}

// TestSnapshotBuildsSortedChildren: children lists are name-sorted for
// stable readdir (store.rs).
func TestSnapshotBuildsSortedChildren(t *testing.T) {
	s := New()
	s.PublishSnapshot(pubMap(
		node("b.txt", false),
		node("a.txt", false),
		node("d/x", false),
		node("d", true),
	))

	snap := s.Snapshot()
	if len(snap.ByPath) != 4 {
		t.Fatalf("by_path len = %d, want 4", len(snap.ByPath))
	}
	if got := snap.Children[""]; !slices.Equal(got, []string{"a.txt", "b.txt", "d"}) {
		t.Errorf("root children = %v", got)
	}
	if got := snap.Children["d"]; !slices.Equal(got, []string{"d/x"}) {
		t.Errorf("d children = %v", got)
	}
}

// TestSnapshotGenerationAdvances (store.go doc: monotonic generation).
func TestSnapshotGenerationAdvances(t *testing.T) {
	s := New()
	if g := s.Generation(); g != 0 {
		t.Fatalf("initial generation = %d, want 0", g)
	}
	s.PublishSnapshot(pubMap(node("a.txt", false)))
	if g := s.Generation(); g != 1 {
		t.Fatalf("generation after first publish = %d, want 1", g)
	}
	s.PublishSnapshot(pubMap(node("a.txt", false), node("b.txt", false)))
	if g := s.Generation(); g != 2 {
		t.Fatalf("generation after second publish = %d, want 2", g)
	}
}

// TestPublishSnapshotSettlesDeletedMarks: deletes the new snapshot
// absorbed are forgotten; paths still served keep the rm -rf ENOTEMPTY
// guard mark (store.rs publish_snapshot).
func TestPublishSnapshotSettlesDeletedMarks(t *testing.T) {
	s := New()
	s.PublishSnapshot(pubMap(node("a.txt", false), node("b.txt", false)))
	s.RecordDelete("a.txt")
	s.RecordDelete("b.txt")
	s.NextDeleteRequest() // engine pops; deleted marks must outlive the queue

	// a.txt gone from the new snapshot → mark settled; b.txt still served
	// → mark kept for the rmdir emptiness check.
	s.PublishSnapshot(pubMap(node("b.txt", false)))
	if s.HasPendingDelete("a.txt") {
		t.Error("a.txt delete mark should be settled by the new snapshot")
	}
	if !s.HasPendingDelete("b.txt") {
		t.Error("b.txt delete mark must outlive the queue while still served")
	}
}

// TestHydrateQueueDedupsAndTracks (store.rs).
func TestHydrateQueueDedupsAndTracks(t *testing.T) {
	s := New()
	if !s.BeginHydrate("a.txt") {
		t.Fatal("first begin_hydrate should be first")
	}
	if s.BeginHydrate("a.txt") {
		t.Fatal("second open is not first")
	}
	if rel, ok := s.NextHydrateRequest(); !ok || rel != "a.txt" {
		t.Fatalf("next_hydrate_request = %q,%v; want a.txt", rel, ok)
	}
	if n := s.HydratingCount(); n != 1 {
		t.Fatalf("hydrating_count = %d, want 1", n)
	}
	if _, ok := s.NextHydrateRequest(); ok {
		t.Fatal("queue should be empty")
	}

	s.FinishHydrate("a.txt", false)
	if n := s.HydratingCount(); n != 0 {
		t.Fatalf("hydrating_count after failure = %d, want 0", n)
	}
	// failed hydration can be re-requested
	if !s.BeginHydrate("a.txt") {
		t.Fatal("re-request after failure should be first")
	}
}

// TestBackingRegistrationUnblocks (store.rs).
func TestBackingRegistrationUnblocks(t *testing.T) {
	s := New()
	s.BeginHydrate("a.txt")
	s.SetBacking("a.txt", "/tmp/x")
	// engine drops the queued request because content is already there
	if _, ok := s.NextHydrateRequest(); ok {
		t.Fatal("queued request should be dropped; content already there")
	}
	if p, ok := s.BackingPath("a.txt"); !ok || p != "/tmp/x" {
		t.Fatalf("backing_path = %q,%v; want /tmp/x", p, ok)
	}
}

// TestWriteQueueRoundtripAndRetry (store.rs).
func TestWriteQueueRoundtripAndRetry(t *testing.T) {
	s := New()
	if n := s.PendingWrites(); n != 0 {
		t.Fatalf("pending_writes = %d, want 0", n)
	}
	s.RecordWrite("new.txt")
	s.RecordWrite("new.txt") // dedup while queued
	if n := s.PendingWrites(); n != 1 {
		t.Fatalf("pending_writes after dedup = %d, want 1", n)
	}
	if rel, ok := s.NextWriteRequest(); !ok || rel != "new.txt" {
		t.Fatalf("next_write_request = %q,%v", rel, ok)
	}
	if !s.IsUploading("new.txt") {
		t.Fatal("new.txt should be uploading")
	}
	// re-queue during upload is kept for the retry pass
	s.RecordWrite("new.txt")
	if n := s.PendingWrites(); n != 2 {
		t.Fatalf("pending_writes after re-queue = %d, want 2", n)
	}
	if _, ok := s.NextWriteRequest(); ok {
		t.Fatal("in-flight not re-served")
	}

	s.FinishWrite("new.txt", true)
	if s.IsUploading("new.txt") {
		t.Fatal("new.txt should not be uploading")
	}
	if rel, ok := s.NextWriteRequest(); !ok || rel != "new.txt" {
		t.Fatalf("re-queued write = %q,%v", rel, ok)
	}
	s.FinishWrite("new.txt", false) // failure re-queues
	if rel, ok := s.NextWriteRequest(); !ok || rel != "new.txt" {
		t.Fatalf("retry write = %q,%v", rel, ok)
	}
	s.FinishWrite("new.txt", true)

	s.RecordWrite("gone.txt")
	s.Forget("gone.txt")
	if _, ok := s.NextWriteRequest(); ok {
		t.Fatal("forgotten write should be gone")
	}
}

// TestMkdirAndDeleteQueues (store.rs).
func TestMkdirAndDeleteQueues(t *testing.T) {
	s := New()
	s.RecordMkdir("docs")
	s.RecordMkdir("docs/sub")
	if rel, ok := s.NextMkdirRequest(); !ok || rel != "docs" {
		t.Fatalf("mkdir #1 = %q,%v", rel, ok)
	}
	if rel, ok := s.NextMkdirRequest(); !ok || rel != "docs/sub" {
		t.Fatalf("mkdir #2 = %q,%v", rel, ok)
	}
	if _, ok := s.NextMkdirRequest(); ok {
		t.Fatal("mkdir queue should be empty")
	}

	// A failed remote create re-queues (no dedup loss, no drop).
	s.RecordMkdir("retry-me")
	if rel, ok := s.NextMkdirRequest(); !ok || rel != "retry-me" {
		t.Fatalf("retry mkdir = %q,%v", rel, ok)
	}
	s.RequeueMkdir("retry-me")
	if rel, ok := s.NextMkdirRequest(); !ok || rel != "retry-me" {
		t.Fatalf("requeued mkdir = %q,%v", rel, ok)
	}
	s.RequeueMkdir("retry-me") // no duplicate when already queued
	if rel, ok := s.NextMkdirRequest(); !ok || rel != "retry-me" {
		t.Fatalf("deduped requeue = %q,%v", rel, ok)
	}
	if _, ok := s.NextMkdirRequest(); ok {
		t.Fatal("mkdir queue should be empty")
	}

	s.RecordDelete("docs/sub/a.txt")
	s.RecordDelete("docs/sub/a.txt") // dedup
	if n := s.PendingWrites(); n != 0 {
		t.Fatalf("pending_writes = %d, want 0", n)
	}
	if rel, ok := s.NextDeleteRequest(); !ok || rel != "docs/sub/a.txt" {
		t.Fatalf("delete = %q,%v", rel, ok)
	}
	if _, ok := s.NextDeleteRequest(); ok {
		t.Fatal("delete queue should be empty")
	}

	// forget_tree drops queued work under a removed folder.
	s.RecordMkdir("gone")
	s.RecordWrite("gone/f.txt")
	s.ForgetTree("gone")
	if _, ok := s.NextMkdirRequest(); ok {
		t.Fatal("forget_tree should drop the mkdir")
	}
	if _, ok := s.NextWriteRequest(); ok {
		t.Fatal("forget_tree should drop the write")
	}
}

// TestRenameQueueRoundtripAndChaining (store.rs).
func TestRenameQueueRoundtripAndChaining(t *testing.T) {
	s := New()
	s.RecordRename("a.txt", "b.txt")
	s.RecordRename("a.txt", "b.txt") // dedup
	if o, n, ok := s.NextRenameRequest(); !ok || o != "a.txt" || n != "b.txt" {
		t.Fatalf("rename = %q→%q,%v", o, n, ok)
	}
	s.FinishRename("a.txt", "b.txt", true, false)
	if _, _, ok := s.NextRenameRequest(); ok {
		t.Fatal("rename queue should be empty")
	}

	// Failure re-queues; rename-back cancels on drain.
	s.RecordRename("a.txt", "b.txt")
	if o, n, ok := s.NextRenameRequest(); !ok || o != "a.txt" || n != "b.txt" {
		t.Fatalf("rename = %q→%q,%v", o, n, ok)
	}
	s.FinishRename("a.txt", "b.txt", false, false)
	if o, n, ok := s.NextRenameRequest(); !ok || o != "a.txt" || n != "b.txt" {
		t.Fatalf("retry rename = %q→%q,%v", o, n, ok)
	}
	s.FinishRename("a.txt", "b.txt", true, false)

	s.RecordRename("a.txt", "b.txt")
	s.RecordRename("b.txt", "a.txt") // chained → a→a, cancels
	if _, _, ok := s.NextRenameRequest(); ok {
		t.Fatal("rename-back should cancel on drain")
	}

	// Chained destination re-key: a→b then b→c merges into a→c.
	s.RecordRename("a.txt", "b.txt")
	s.RecordRename("b.txt", "c.txt")
	if o, n, ok := s.NextRenameRequest(); !ok || o != "a.txt" || n != "c.txt" {
		t.Fatalf("chained rename = %q→%q,%v; want a.txt→c.txt", o, n, ok)
	}
	s.FinishRename("a.txt", "c.txt", true, false)

	// forget drops renames touching the path.
	s.RecordRename("x.txt", "y.txt")
	s.Forget("x.txt")
	if _, _, ok := s.NextRenameRequest(); ok {
		t.Fatal("forgotten rename should be gone")
	}
}

// TestRenameConflictsAreCappedButTransientFailuresAreNot (store.rs).
func TestRenameConflictsAreCappedButTransientFailuresAreNot(t *testing.T) {
	s := New()
	// Transient failures re-queue without limit.
	s.RecordRename("a.txt", "b.txt")
	for i := 0; i < int(RenameMaxConflictRetries)*2; i++ {
		if o, n, ok := s.NextRenameRequest(); !ok || o != "a.txt" || n != "b.txt" {
			t.Fatalf("transient %d: rename = %q→%q,%v", i, o, n, ok)
		}
		if !s.FinishRename("a.txt", "b.txt", false, false) {
			t.Fatalf("transient failure %d should re-queue", i)
		}
	}

	// Name conflicts stop after the cap: the pair is dropped, so the main
	// loop can reach pump_work and free the remote name.
	for i := 1; i < int(RenameMaxConflictRetries); i++ {
		if _, _, ok := s.NextRenameRequest(); !ok {
			t.Fatalf("conflict %d: queue empty", i)
		}
		if !s.FinishRename("a.txt", "b.txt", false, true) {
			t.Fatalf("conflict %d below cap should re-queue", i)
		}
	}
	if _, _, ok := s.NextRenameRequest(); !ok {
		t.Fatal("final attempt should still be queued")
	}
	if s.FinishRename("a.txt", "b.txt", false, true) {
		t.Fatal("conflict at cap should drop the pair")
	}
	if _, _, ok := s.NextRenameRequest(); ok {
		t.Fatal("dropped pair should leave the queue empty")
	}

	// Dropping cleared the count: the same pair queued again gets a full
	// budget instead of dying on its first conflict.
	s.RecordRename("a.txt", "b.txt")
	for i := 1; i < int(RenameMaxConflictRetries); i++ {
		if _, _, ok := s.NextRenameRequest(); !ok {
			t.Fatalf("budget %d: queue empty", i)
		}
		if !s.FinishRename("a.txt", "b.txt", false, true) {
			t.Fatalf("budget conflict %d should re-queue", i)
		}
	}
	if _, _, ok := s.NextRenameRequest(); !ok {
		t.Fatal("final budget attempt should be queued")
	}
	if s.FinishRename("a.txt", "b.txt", false, true) {
		t.Fatal("budget exhausted should drop the pair")
	}
	if _, _, ok := s.NextRenameRequest(); ok {
		t.Fatal("queue should be empty after drop")
	}

	// Success clears the count too.
	s.RecordRename("a.txt", "b.txt")
	if _, _, ok := s.NextRenameRequest(); !ok {
		t.Fatal("queue empty before success")
	}
	if !s.FinishRename("a.txt", "b.txt", true, false) {
		t.Fatal("success should return true")
	}
	s.RecordRename("a.txt", "b.txt")
	for i := 1; i < int(RenameMaxConflictRetries); i++ {
		if _, _, ok := s.NextRenameRequest(); !ok {
			t.Fatalf("post-success %d: queue empty", i)
		}
		if !s.FinishRename("a.txt", "b.txt", false, true) {
			t.Fatalf("post-success conflict %d should re-queue", i)
		}
	}
	if _, _, ok := s.NextRenameRequest(); !ok {
		t.Fatal("post-success final attempt should be queued")
	}
}

// TestAccessTrackingAndOpenHandles (store.rs).
func TestAccessTrackingAndOpenHandles(t *testing.T) {
	s := New()
	s.SetBacking("a.txt", "/tmp/a")
	s.SetBacking("b.txt", "/tmp/b")

	// Hydration itself counts as the most recent access.
	s.RecordAccess("a.txt")
	s.RecordAccess("b.txt")
	ages := s.HydratedByAge()
	if len(ages) != 2 || ages[0].Rel != "a.txt" {
		t.Fatalf("older access should evict first: %v", ages)
	}
	if ages[1].Rel != "b.txt" {
		t.Fatalf("ages = %v", ages)
	}

	// Re-read bumps a.txt to newest.
	s.RecordAccess("a.txt")
	ages = s.HydratedByAge()
	if ages[0].Rel != "b.txt" {
		t.Fatalf("after re-read, oldest = %s; want b.txt", ages[0].Rel)
	}

	// Open handles tracked independently; forget clears them.
	s.AddOpenHandle("b.txt")
	if !s.IsOpen("b.txt") {
		t.Fatal("b.txt should be open")
	}
	s.AddOpenHandle("b.txt")
	s.RemoveOpenHandle("b.txt")
	if !s.IsOpen("b.txt") {
		t.Fatal("still one handle open")
	}
	s.RemoveOpenHandle("b.txt")
	if s.IsOpen("b.txt") {
		t.Fatal("b.txt should be closed")
	}
	s.Forget("a.txt")
	for _, a := range s.HydratedByAge() {
		if a.Rel == "a.txt" {
			t.Fatal("forgotten path still in LRU list")
		}
	}
}

// TestNextWorkRequestDrainsInPriorityOrder: delete → rename → write,
// resolved atomically under one lock (store.rs next_work_request — the
// overwrite-rename priority inversion fix).
func TestNextWorkRequestDrainsInPriorityOrder(t *testing.T) {
	s := New()
	// Simulate the VFS's overwrite-rename: delete + rename + write queued
	// together.
	s.RecordOverwrite("dest.txt", "tmp-dest", "dest.txt")
	s.RecordWrite("plain.txt")

	d, r, w := s.PendingCounts()
	if d != 1 || r != 1 || w != 2 {
		t.Fatalf("pending counts = %d/%d/%d; want 1/1/2", d, r, w)
	}

	req, ok := s.NextWorkRequest()
	if !ok || req.Kind != WorkDelete || req.Rel != "dest.txt" {
		t.Fatalf("first pop = %+v,%v; want delete dest.txt", req, ok)
	}
	req, _ = s.NextWorkRequest()
	if req.Kind != WorkRename || req.Rel != "tmp-dest" || req.NewRel != "dest.txt" {
		t.Fatalf("second pop = %+v; want rename tmp-dest→dest.txt", req)
	}
	req, _ = s.NextWorkRequest()
	if req.Kind != WorkWrite || req.Rel != "dest.txt" {
		t.Fatalf("third pop = %+v; want write dest.txt", req)
	}
	req, _ = s.NextWorkRequest()
	if req.Kind != WorkWrite || req.Rel != "plain.txt" {
		t.Fatalf("fourth pop = %+v; want write plain.txt", req)
	}
	if _, ok := s.NextWorkRequest(); ok {
		t.Fatal("queue should be empty")
	}
}

// TestNextWorkRequestHoldsInFlightWrites: entries whose upload is in
// flight stay queued (same rule as next_write_request).
func TestNextWorkRequestHoldsInFlightWrites(t *testing.T) {
	s := New()
	s.RecordWrite("a.txt")
	if _, ok := s.NextWorkRequest(); !ok {
		t.Fatal("first pop should serve the write")
	}
	s.RecordWrite("a.txt") // re-queued while in flight
	s.RecordWrite("b.txt")
	req, ok := s.NextWorkRequest()
	if !ok || req.Kind != WorkWrite || req.Rel != "b.txt" {
		t.Fatalf("pop = %+v,%v; want write b.txt (a.txt in flight)", req, ok)
	}
	s.FinishWrite("a.txt", true)
	req, ok = s.NextWorkRequest()
	if !ok || req.Kind != WorkWrite || req.Rel != "a.txt" {
		t.Fatalf("pop after finish = %+v,%v; want write a.txt", req, ok)
	}
}

// TestRecordOverwriteQueuesAtomically: the delete is queued, the rename
// follows, and the destination write follows the pair (store.rs
// record_overwrite).
func TestRecordOverwriteQueuesAtomically(t *testing.T) {
	s := New()
	s.RecordOverwrite("dest.txt", "tmp-abc", "dest.txt")

	if !s.HasPendingDelete("dest.txt") {
		t.Fatal("dest.txt delete should be queued")
	}
	if !s.RenameInbound("dest.txt") {
		t.Fatal("rename should be inbound for dest.txt")
	}
	d, r, w := s.PendingCounts()
	if d != 1 || r != 1 || w != 1 {
		t.Fatalf("counts = %d/%d/%d; want 1/1/1", d, r, w)
	}
	// Drain: delete, then rename, then the destination write.
	req, _ := s.NextWorkRequest()
	if req.Kind != WorkDelete {
		t.Fatalf("first = %+v; want delete", req)
	}
	req, _ = s.NextWorkRequest()
	if req.Kind != WorkRename || req.Rel != "tmp-abc" || req.NewRel != "dest.txt" {
		t.Fatalf("second = %+v; want rename", req)
	}
	req, _ = s.NextWorkRequest()
	if req.Kind != WorkWrite || req.Rel != "dest.txt" {
		t.Fatalf("third = %+v; want write dest.txt", req)
	}
}

// TestRecordRenameRekeysInFlightUpload: an in-flight upload of the source
// is re-queued under the new path so the content still uploads after the
// rename (store.rs record_rename).
func TestRecordRenameRekeysInFlightUpload(t *testing.T) {
	s := New()
	s.RecordWrite("a.txt")
	if rel, ok := s.NextWriteRequest(); !ok || rel != "a.txt" {
		t.Fatalf("write = %q,%v", rel, ok)
	}
	s.RecordRename("a.txt", "b.txt")
	if s.IsUploading("a.txt") {
		t.Fatal("old path should no longer be uploading")
	}
	if rel, ok := s.NextWriteRequest(); !ok || rel != "b.txt" {
		t.Fatalf("re-keyed write = %q,%v; want b.txt", rel, ok)
	}
}

// TestRecordRenameMovesQueuedUpload: a queued (not in-flight) upload of
// the source follows the rename; a queued destination upload is
// superseded.
func TestRecordRenameMovesQueuedUpload(t *testing.T) {
	s := New()
	s.RecordWrite("a.txt")
	s.RecordWrite("b.txt") // would-be destination upload, superseded
	s.RecordRename("a.txt", "b.txt")
	if n := s.PendingWrites(); n != 1 {
		t.Fatalf("pending_writes = %d, want 1", n)
	}
	if rel, ok := s.NextWriteRequest(); !ok || rel != "b.txt" {
		t.Fatalf("write = %q,%v; want b.txt", rel, ok)
	}
}

// TestRecordMetadataCoalescesPerPath: latest value wins, nil fields left
// untouched, FIFO across paths (store.rs record_metadata).
func TestRecordMetadataCoalescesPerPath(t *testing.T) {
	s := New()
	mtime := int64(123)
	mode := uint32(0o755)
	s.RecordMetadata("a.txt", &mtime, nil)
	mtime2 := int64(456)
	s.RecordMetadata("a.txt", &mtime2, &mode) // coalesce onto a.txt
	s.RecordMetadata("b.txt", nil, &mode)

	rel, mt, md, ok := s.NextMetadataRequest()
	if !ok || rel != "a.txt" || mt == nil || *mt != 456 || md == nil || *md != 0o755 {
		t.Fatalf("first metadata = %q,%v,%v,%v", rel, mt, md, ok)
	}
	rel, mt, md, ok = s.NextMetadataRequest()
	if !ok || rel != "b.txt" || mt != nil || md == nil || *md != 0o755 {
		t.Fatalf("second metadata = %q,%v,%v,%v", rel, mt, md, ok)
	}
	if _, _, _, ok := s.NextMetadataRequest(); ok {
		t.Fatal("metadata queue should be empty")
	}
}

// TestRecordWriteClearsStaleDeleteMark: re-created after a delete, the
// stale snapshot mark must drop (same reasoning as record_mkdir).
func TestRecordWriteClearsStaleDeleteMark(t *testing.T) {
	s := New()
	s.RecordDelete("a.txt")
	s.NextDeleteRequest() // engine popped it; deleted mark lingers
	if !s.HasPendingDelete("a.txt") {
		t.Fatal("mark should outlive the queue")
	}
	s.RecordWrite("a.txt")
	if s.HasPendingDelete("a.txt") {
		t.Fatal("re-created path must drop the stale delete mark")
	}
}

// TestBackingFilePath: backing path mirrors the tree (store.rs
// backing_path).
func TestBackingFilePath(t *testing.T) {
	if got := BackingFilePath("/cache", "docs/a.txt"); got != "/cache/docs/a.txt" {
		t.Fatalf("backing path = %q", got)
	}
}

// TestNotifyCoalescesWhilePending: tokio Notify semantics — one pending
// wake, further NotifyOne calls coalesce, one Wait consumes it.
func TestNotifyCoalescesWhilePending(t *testing.T) {
	n := NewNotify()
	n.NotifyOne()
	n.NotifyOne() // coalesces
	n.Wait()
	select {
	case <-n.ch:
		t.Fatal("wake should be consumed")
	default:
	}
}

// TestBeginHydrateWakesOncePerRequest: only the first requester wakes the
// engine (store.rs begin_hydrate).
func TestBeginHydrateWakesOncePerRequest(t *testing.T) {
	s := New()
	s.BeginHydrate("a.txt")
	select {
	case <-s.Notify.ch:
	default:
		t.Fatal("first request should wake the engine")
	}
	s.BeginHydrate("a.txt") // dedup: no second wake pending
	select {
	case <-s.Notify.ch:
		t.Fatal("duplicate request must not wake again")
	default:
	}
}
