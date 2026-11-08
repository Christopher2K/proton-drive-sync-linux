package db

import (
	"path/filepath"
	"testing"
)

func sp(s string) *string { return &s }
func si(v int64) *int64   { return &v }
func su(v uint32) *uint32 { return &v }

func row(localPath, uid string) *NodeRow {
	parent := "root"
	return &NodeRow{
		LocalPath:  localPath,
		NodeUID:    uid,
		ParentUID:  &parent,
		Name:       localPath,
		Kind:       KindFile,
		Size:       nil,
		SHA1:       nil,
		Mtime:      nil,
		Mode:       nil,
		SyncState:  StateSynced,
		Pinned:     false,
		SyncedSHA1: nil,
	}
}

func mustOpenMemory(t *testing.T) *Db {
	t.Helper()
	d, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// Port of db.rs rekey_nodes_is_all_or_nothing.
func TestRekeyNodesIsAllOrNothing(t *testing.T) {
	d := mustOpenMemory(t)
	d.UpsertNode(row("a.txt", "uid-a"))
	d.UpsertNode(row("b.txt", "uid-b"))

	// Two rows moved onto the same destination: the insert conflicts and
	// the whole transaction must roll back (the old paths stay intact).
	bad := []Move{
		{Old: "a.txt", Row: row("dest.txt", "uid-a")},
		{Old: "b.txt", Row: row("dest.txt", "uid-b")},
	}
	if err := d.Rekey(bad); err == nil {
		t.Fatal("conflicting rekey must fail")
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if r, _ := d.NodeByPath(p); r == nil {
			t.Errorf("%s vanished after failed rekey", p)
		}
	}
	if r, _ := d.NodeByPath("dest.txt"); r != nil {
		t.Error("dest.txt must not exist after failed rekey")
	}

	// A valid multi-row re-key moves every row atomically.
	good := []Move{
		{Old: "a.txt", Row: row("c.txt", "uid-a")},
		{Old: "b.txt", Row: row("d.txt", "uid-b")},
	}
	if err := d.Rekey(good); err != nil {
		t.Fatalf("valid rekey: %v", err)
	}
	if r, _ := d.NodeByPath("a.txt"); r != nil {
		t.Error("a.txt must be gone")
	}
	if r, _ := d.NodeByPath("b.txt"); r != nil {
		t.Error("b.txt must be gone")
	}
	if r, _ := d.NodeByPath("c.txt"); r == nil || r.NodeUID != "uid-a" {
		t.Errorf("c.txt = %+v, want uid-a", r)
	}
	if r, _ := d.NodeByPath("d.txt"); r == nil || r.NodeUID != "uid-b" {
		t.Errorf("d.txt = %+v, want uid-b", r)
	}
}

func TestNodeCRUDAndLookups(t *testing.T) {
	d := mustOpenMemory(t)
	r := row("docs/report.pdf", "uid-1")
	r.Size, r.Mtime, r.SHA1 = si(1024), si(1731000000), sp("abc")
	r.Mode = su(0o755)
	r.SyncedSHA1 = sp("abc")
	if err := d.UpsertNode(r); err != nil {
		t.Fatal(err)
	}

	got, err := d.NodeByPath("docs/report.pdf")
	if err != nil || got == nil {
		t.Fatalf("NodeByPath: %v %v", got, err)
	}
	if got.NodeUID != "uid-1" || got.Kind != KindFile || got.SyncState != StateSynced {
		t.Errorf("roundtrip mismatch: %+v", got)
	}
	if got.Size == nil || *got.Size != 1024 || got.Mode == nil || *got.Mode != 0o755 {
		t.Errorf("size/mode mismatch: %+v", got)
	}
	byUID, _ := d.NodeByUID("uid-1")
	if byUID == nil || byUID.LocalPath != "docs/report.pdf" {
		t.Errorf("NodeByUID: %+v", byUID)
	}

	// Update via upsert (same path) keeps one row.
	r2 := *r
	r2.SyncState = StateDirty
	r2.Pinned = true
	if err := d.UpsertNode(&r2); err != nil {
		t.Fatal(err)
	}
	dirty, _ := d.NodesWithState(StateDirty)
	if len(dirty) != 1 || dirty[0].Pinned != true {
		t.Errorf("dirty rows: %+v", dirty)
	}
	n, _ := d.CountSynced()
	if n != 0 {
		t.Errorf("CountSynced = %d, want 0 after dirty update", n)
	}

	if err := d.RemoveNode("docs/report.pdf"); err != nil {
		t.Fatal(err)
	}
	if r, _ := d.NodeByPath("docs/report.pdf"); r != nil {
		t.Error("row must be removed")
	}
}

func TestPinsAndTree(t *testing.T) {
	d := mustOpenMemory(t)
	for _, p := range []string{"dir", "dir/a.txt", "dir/sub/b.txt", "other/c.txt"} {
		r := row(p, "uid-"+p)
		if p == "dir" || p == "dir/sub" || p == "other" {
			r.Kind = KindFolder
		}
		if err := d.UpsertNode(r); err != nil {
			t.Fatal(err)
		}
	}
	d.UpsertNode(row("dir/sub", "uid-dir/sub")) // ensure folder row exists
	if err := d.SetPinnedTree("dir", true); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"dir", "dir/a.txt", "dir/sub/b.txt"} {
		r, _ := d.NodeByPath(p)
		if r == nil || !r.Pinned {
			t.Errorf("%s must be pinned", p)
		}
	}
	if r, _ := d.NodeByPath("other/c.txt"); r == nil || r.Pinned {
		t.Error("other/c.txt must not be pinned")
	}
	if err := d.SetPinned("dir/a.txt", false); err != nil {
		t.Fatal(err)
	}
	if r, _ := d.NodeByPath("dir/a.txt"); r == nil || r.Pinned {
		t.Error("dir/a.txt pin must be cleared")
	}
}

func TestFailureBookkeeping(t *testing.T) {
	d := mustOpenMemory(t)
	d.UpsertNode(row("f.txt", "uid-f"))

	// Retry counter increments and survives a row rewrite (upsert does not
	// clobber retry_count/last_error — same contract as db.rs).
	for want := int64(1); want <= 3; want++ {
		got, err := d.RecordRetryableFailure("f.txt")
		if err != nil || got != want {
			t.Fatalf("RecordRetryableFailure = %d, %v; want %d", got, err, want)
		}
	}
	r := row("f.txt", "uid-f")
	r.SyncState = StateDirty
	d.UpsertNode(r)
	if got, _ := d.RecordRetryableFailure("f.txt"); got != 4 {
		t.Errorf("retry_count must survive upsert; got %d", got)
	}

	if err := d.MarkUploadErrored("f.txt", "boom"); err != nil {
		t.Fatal(err)
	}
	if e, _ := d.UploadError("f.txt"); e != "boom" {
		t.Errorf("UploadError = %q", e)
	}
	paths, _ := d.ErroredPaths()
	if len(paths) != 1 || paths[0] != "f.txt" {
		t.Errorf("ErroredPaths = %v", paths)
	}
	n, _ := d.CountErrored()
	if n != 1 {
		t.Errorf("CountErrored = %d", n)
	}
	if err := d.ClearUploadFailures("f.txt"); err != nil {
		t.Fatal(err)
	}
	if e, _ := d.UploadError("f.txt"); e != "" {
		t.Errorf("UploadError after clear = %q", e)
	}
}

func TestConflictParking(t *testing.T) {
	d := mustOpenMemory(t)
	d.UpsertNode(row("c.txt", "uid-c"))
	if on, _ := d.IsRemoteConflicted("c.txt"); on {
		t.Error("default must be unparked")
	}
	if on, _ := d.IsRemoteConflicted("missing"); on {
		t.Error("unknown path must read as unparked")
	}
	d.SetRemoteConflicted("c.txt", true)
	if on, _ := d.IsRemoteConflicted("c.txt"); !on {
		t.Error("must be parked")
	}
	paths, _ := d.RemoteConflictedPaths()
	if len(paths) != 1 || paths[0] != "c.txt" {
		t.Errorf("RemoteConflictedPaths = %v", paths)
	}
	if n, _ := d.CountRemoteConflicted(); n != 1 {
		t.Errorf("CountRemoteConflicted = %d", n)
	}
	d.SetRemoteConflicted("c.txt", false)
	if n, _ := d.CountRemoteConflicted(); n != 0 {
		t.Errorf("after unpark: %d", n)
	}
}

func TestTombstonesTransfersCursorMeta(t *testing.T) {
	d := mustOpenMemory(t)

	d.AddTombstone(&Tombstone{LocalPath: "x.txt", NodeUID: "uid-x", DeletedAt: 2})
	d.AddTombstone(&Tombstone{LocalPath: "y.txt", NodeUID: "uid-y", DeletedAt: 1})
	tbs, _ := d.Tombstones()
	if len(tbs) != 2 || tbs[0].LocalPath != "y.txt" {
		t.Errorf("tombstones not oldest-first: %+v", tbs)
	}
	d.RemoveTombstone("y.txt")
	if tbs, _ = d.Tombstones(); len(tbs) != 1 || tbs[0].LocalPath != "x.txt" {
		t.Errorf("after remove: %+v", tbs)
	}

	lp := "x.txt"
	if err := d.UpsertTransfer(&Transfer{
		TransferID: "t1", Kind: "upload", LocalPath: &lp, NodeUID: sp("uid-x"),
		Bytes: 10, Total: si(100), State: "running", UpdatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertTransfer(&Transfer{
		TransferID: "t1", Kind: "upload", Bytes: 50, State: "done", UpdatedAt: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if id, _ := d.EventCursor("scope"); id != "" {
		t.Errorf("empty cursor must read as \"\", got %q", id)
	}
	d.SetEventCursor("scope", "ev-1")
	if id, _ := d.EventCursor("scope"); id != "ev-1" {
		t.Errorf("cursor = %q", id)
	}
	d.SetEventCursor("scope", "ev-2")
	if id, _ := d.EventCursor("scope"); id != "ev-2" {
		t.Errorf("cursor after replace = %q", id)
	}

	if v, _ := d.Meta("k"); v != "" {
		t.Errorf("unset meta must read as \"\", got %q", v)
	}
	d.SetMeta("k", "v1")
	d.SetMeta("k", "v2")
	if v, _ := d.Meta("k"); v != "v2" {
		t.Errorf("meta = %q", v)
	}
	d.RemoveMeta("k")
	if v, _ := d.Meta("k"); v != "" {
		t.Errorf("meta after remove = %q", v)
	}
}

// NodesUnder must match the subtree LIKE pattern used for subtree removal.
func TestNodesUnder(t *testing.T) {
	d := mustOpenMemory(t)
	for _, p := range []string{"dir", "dir/a.txt", "dir/sub", "dir/sub/b.txt", "dirother/x"} {
		if err := d.UpsertNode(row(p, "uid-"+p)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := d.NodesUnder("dir/%")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 { // dir/a.txt, dir/sub, dir/sub/b.txt — NOT dirother/x
		var paths []string
		for _, r := range got {
			paths = append(paths, r.LocalPath)
		}
		t.Errorf("NodesUnder = %v, want the 3 dir/* rows", paths)
	}
}

// Schema compatibility: a DB file created by this Go package must be openable
// by the Rust engine, and vice versa — the same state.db serves both engines
// during migration. The Rust leg runs in db_compat_rust_test.go (skipped when
// the Rust workspace is not built).
func TestOpenFilePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertNode(row("persist.txt", "uid-p")); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if r, _ := d2.NodeByPath("persist.txt"); r == nil || r.NodeUID != "uid-p" {
		t.Errorf("row did not persist: %+v", r)
	}
}
