// Wave-2 ports (part B): special files, local path lifecycle (delete /
// re-create, folder rename/move), remote trash→restore, event ordering,
// metadata fidelity (mtime, exec bit), case-only renames, and the unusual
// names battery. Ported from crates/core/tests/engine.rs (mirror-mode).
package engine_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

func mtimeSecs(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.ModTime().Unix()
}

// lockDir/unlockDir/mtimeSecs helpers live in engine_wave_test.go (wave 1).

// ---------------------------------------------------------------------------
// TG-29 — special files are skipped before hashing
// ---------------------------------------------------------------------------

func TestSpecialFilesAreSkippedBeforeHashingAndEngineStaysResponsive(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}

	passwdBefore, err := os.ReadFile("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	// Symlink inside the root (target exists later) and outside it.
	if err := os.Symlink("hello.txt", filepath.Join(root, "link-inside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "link-outside")); err != nil {
		t.Fatal(err)
	}
	// FIFO: opening it for read would block forever.
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Unix socket node. mknod creates the filesystem socket without binding
	// (a real bind would need a path short enough for sun_path, and t.TempDir
	// dirs routinely exceed the 108-byte limit).
	if err := syscall.Mknod(filepath.Join(root, "sock"), syscall.S_IFSOCK|0o666, 0); err != nil {
		t.Fatal(err)
	}
	// Hardlinked regular file: both names upload independently.
	if err := os.WriteFile(filepath.Join(root, "hard.txt"), []byte("hard data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "hard.txt"), filepath.Join(root, "hard-link.txt")); err != nil {
		t.Fatal(err)
	}

	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Subscribe BEFORE Run so the initial scan's warnings are captured.
	activity := h.Activity()
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() { h.Shutdown(); <-done; client.Close() }()
	if !h.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}

	waitFor(t, func() bool { return fxExists(root, "hello.txt") })
	waitFor(t, func() bool { return contains(handle.UploadedNames(), "hard.txt") })
	waitFor(t, func() bool { return contains(handle.UploadedNames(), "hard-link.txt") })

	// Hardlinks upload as two independent nodes with the same bytes.
	hardUID, ok := handle.FindUID("root", "hard.txt")
	if !ok {
		t.Fatal("hard.txt uid missing")
	}
	linkUID, ok := handle.FindUID("root", "hard-link.txt")
	if !ok {
		t.Fatal("hard-link.txt uid missing")
	}
	if hardUID == linkUID {
		t.Fatalf("hardlinks must upload as distinct nodes, both %s", hardUID)
	}
	if c, _ := handle.FileContent(hardUID); string(c) != "hard data" {
		t.Fatalf("hard.txt content = %q", c)
	}
	if c, _ := handle.FileContent(linkUID); string(c) != "hard data" {
		t.Fatalf("hard-link.txt content = %q", c)
	}

	// Nothing special was uploaded (and the outside target was not read).
	specials := []string{"link-inside", "link-outside", "pipe", "sock"}
	for _, name := range specials {
		if contains(handle.ChildNames("root"), name) {
			t.Fatalf("%s must never be uploaded: %v", name, handle.ChildNames("root"))
		}
	}
	if passwdAfter, err := os.ReadFile("/etc/passwd"); err != nil || string(passwdAfter) != string(passwdBefore) {
		t.Fatal("symlink target outside the root must stay untouched")
	}

	// One Skipped warning per special path.
	warnings := collectSkippedPaths(t, activity, specials, 10*time.Second)
	for _, path := range specials {
		n := 0
		for _, w := range warnings {
			if w == path {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s must warn exactly once, got %d (%v)", path, n, warnings)
		}
	}

	// Responsive: a normal file added afterwards still syncs.
	if err := os.WriteFile(filepath.Join(root, "after.txt"), []byte("after specials"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains(handle.UploadedNames(), "after.txt") })
	st, err := h.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q", st.State)
	}
}

// collectSkippedPaths drains the activity feed until every wanted path has
// a Skipped entry (bounded).
func collectSkippedPaths(t *testing.T, ch <-chan engine.ActivityItem, wanted []string, timeout time.Duration) []string {
	t.Helper()
	var out []string
	deadline := time.After(timeout)
	for {
		have := map[string]bool{}
		for _, w := range out {
			have[w] = true
		}
		all := true
		for _, w := range wanted {
			if !have[w] {
				all = false
			}
		}
		if all {
			return out
		}
		select {
		case item := <-ch:
			if item.Kind == engine.ActivitySkipped {
				out = append(out, item.Path)
			}
		case <-deadline:
			t.Fatalf("skipped warnings incomplete: %v (got %v)", wanted, out)
		}
	}
}

// ---------------------------------------------------------------------------
// Wave 1a — local delete / re-create lifecycle (TG-05)
// ---------------------------------------------------------------------------

func TestLocalDeleteThenRecreateInSameDebounceWindowKeepsNewContent(t *testing.T) {
	fx := start(t)
	fx.write("churn.txt", []byte("v1"))
	waitForSynced(t, fx.db, "churn.txt")
	uid, ok := fx.mock.FindUID("root", "churn.txt")
	if !ok {
		t.Fatal("churn.txt not uploaded")
	}

	// Remove and re-create inside one watcher debounce window.
	if err := os.Remove(filepath.Join(fx.root, "churn.txt")); err != nil {
		t.Fatal(err)
	}
	fx.write("churn.txt", []byte("v2 re-created"))

	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "v2 re-created"
	})
	waitForSynced(t, fx.db, "churn.txt")
	// Settle: a stale tombstone would drain after the upload and trash the
	// very node that just received the new content.
	time.Sleep(1200 * time.Millisecond)

	if got, _ := fx.mock.FindUID("root", "churn.txt"); got != uid {
		t.Fatalf("the re-created content must stay on the original node %q, got %q", uid, got)
	}
	n := 0
	for _, name := range fx.mock.ChildNames("root") {
		if name == "churn.txt" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("exactly one remote node at the path: %v", fx.mock.ChildNames("root"))
	}
	if contains(fx.mock.TrashedUIDs(), uid) {
		t.Fatalf("stale delete reached the remote: %v", fx.mock.TrashedUIDs())
	}
	if _, ok := conflictEntry(fx.root, "churn"); ok {
		t.Fatal("unexpected conflict copy")
	}
	if tombs := mustTombstones(t, fx.db); len(tombs) != 0 {
		t.Fatalf("tombstones: %v", tombs)
	}
	row, _ := fx.db.NodeByPath("churn.txt")
	if row == nil || row.SyncState != db.StateSynced {
		t.Fatalf("row = %+v", row)
	}
	if row.SHA1 == nil || *row.SHA1 != hashSHA1([]byte("v2 re-created")) {
		t.Fatalf("row sha1 = %v", row.SHA1)
	}
}

func TestRecreateAfterDeleteDrainedUploadsNewNode(t *testing.T) {
	fx := start(t)
	seedSyncedFillers(t, fx, 10)
	fx.write("churn.txt", []byte("v1"))
	waitForSynced(t, fx.db, "churn.txt")
	oldUID, _ := fx.mock.FindUID("root", "churn.txt")

	// Delete and let the tombstone drain completely.
	if err := os.Remove(filepath.Join(fx.root, "churn.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains(fx.mock.TrashedUIDs(), oldUID) })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("churn.txt")
		return row == nil
	})

	// Re-create while paused so its upload cannot start yet: this is the
	// "delete drained, upload not completed" window.
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	fx.write("churn.txt", []byte("v2 replacement"))
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("churn.txt")
		return row != nil && row.SyncState == db.StateDirty
	})
	if _, ok := fx.mock.FindUID("root", "churn.txt"); ok {
		t.Fatal("the re-created file must not exist remotely until its upload runs")
	}

	fx.handle.Resume()
	newUID := waitForMockUpload(t, fx, "churn.txt")
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(newUID)
		return string(c) == "v2 replacement"
	})
	waitForSynced(t, fx.db, "churn.txt")
	time.Sleep(1200 * time.Millisecond)

	// Exactly one node at the path, carrying the replacement bytes; the old
	// node was trashed once, not the new one.
	n := 0
	for _, name := range fx.mock.ChildNames("root") {
		if name == "churn.txt" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("remote churn.txt nodes = %d", n)
	}
	if got, _ := fx.mock.FindUID("root", "churn.txt"); got != newUID {
		t.Fatalf("churn.txt uid = %q, want the new node %q", got, newUID)
	}
	oldTrashed := 0
	for _, u := range fx.mock.TrashedUIDs() {
		if u == oldUID {
			oldTrashed++
		}
	}
	if oldTrashed != 1 {
		t.Fatalf("old uid trashed %d times, want 1", oldTrashed)
	}
	if contains(fx.mock.TrashedUIDs(), newUID) {
		t.Fatal("the new node was trashed")
	}
	if tombs := mustTombstones(t, fx.db); len(tombs) != 0 {
		t.Fatalf("tombstones: %v", tombs)
	}
	if _, ok := conflictEntry(fx.root, "churn"); ok {
		t.Fatal("unexpected conflict copy")
	}
}

// ---------------------------------------------------------------------------
// TG-06 — full local folder rename / move cycle
// ---------------------------------------------------------------------------

func TestLocalFolderRenameAndMoveFullCyclePreservesUIDs(t *testing.T) {
	fx := start(t)
	fx.write("Project/sub/c.txt", []byte("gamma"))
	fx.write("Project/a.txt", []byte("alpha"))
	fx.write("Project/b.txt", []byte("beta"))
	for _, rel := range []string{"Project/a.txt", "Project/b.txt", "Project/sub/c.txt"} {
		waitForSynced(t, fx.db, rel)
	}
	waitFor(t, func() bool {
		_, ok := fx.mock.FindUID("root", "Project")
		return ok
	})
	projectUID, _ := fx.mock.FindUID("root", "Project")
	subUID, ok := fx.mock.FindUID(projectUID, "sub")
	if !ok {
		t.Fatal("sub remote missing")
	}
	aUID, _ := fx.mock.FindUID(projectUID, "a.txt")
	bUID, _ := fx.mock.FindUID(projectUID, "b.txt")
	cUID, _ := fx.mock.FindUID(subUID, "c.txt")
	allUIDs := []string{projectUID, subUID, aUID, bUID, cUID}
	downloads := len(fx.mock.DownloadedUIDs())
	uploads := len(fx.uploadedNames())

	// A real folder elsewhere for the move step.
	if err := os.Mkdir(filepath.Join(fx.root, "Archive"), 0o755); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, ok := fx.mock.FindUID("root", "Archive")
		return ok
	})
	archiveUID, _ := fx.mock.FindUID("root", "Archive")

	// 1. Rename at the root.
	if err := os.Rename(filepath.Join(fx.root, "Project"), filepath.Join(fx.root, "Renamed")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("Renamed/sub/c.txt")
		old, _ := fx.db.NodeByPath("Project")
		return row != nil && old == nil
	})
	waitFor(t, func() bool {
		got, ok := fx.mock.FindUID("root", "Renamed")
		return ok && got == projectUID
	})
	if got, ok := fx.mock.FindUID(projectUID, "sub"); !ok || got != subUID {
		t.Fatalf("sub uid changed: %q", got)
	}

	// 2. Move under Archive.
	if err := os.Rename(filepath.Join(fx.root, "Renamed"), filepath.Join(fx.root, "Archive", "Renamed")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		// Re-keying deletes the old rows and inserts children before their
		// parent, so wait for the parent row too.
		row, _ := fx.db.NodeByPath("Archive/Renamed/sub/c.txt")
		frow, _ := fx.db.NodeByPath("Archive/Renamed")
		old, _ := fx.db.NodeByPath("Renamed")
		return row != nil && frow != nil && old == nil
	})
	waitFor(t, func() bool { return contains(fx.mock.ChildNames(archiveUID), "Renamed") })
	frow, _ := fx.db.NodeByPath("Archive/Renamed")
	if frow == nil || frow.ParentUID == nil || *frow.ParentUID != archiveUID {
		t.Fatalf("Archive/Renamed parent = %+v", frow)
	}

	// 3. Move back to the root.
	if err := os.Rename(filepath.Join(fx.root, "Archive", "Renamed"), filepath.Join(fx.root, "Renamed")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("Renamed/sub/c.txt")
		frow, _ := fx.db.NodeByPath("Renamed")
		old, _ := fx.db.NodeByPath("Archive/Renamed")
		return row != nil && frow != nil && old == nil
	})
	waitFor(t, func() bool { return contains(fx.mock.ChildNames("root"), "Renamed") })

	// Rename/move only: no trash, no re-upload, same uids throughout.
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("folder rename/move must never trash: %v", trashed)
	}
	if got := len(fx.uploadedNames()); got != uploads {
		t.Fatalf("uploads = %d, want %d (no re-uploads)", got, uploads)
	}
	if got := len(fx.mock.DownloadedUIDs()); got != downloads {
		t.Fatalf("downloads = %d, want %d (no re-downloads)", got, downloads)
	}
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range allUIDs {
		n := 0
		for _, r := range rows {
			if r.NodeUID == uid {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("uid %s must still have exactly one row, got %d", uid, n)
		}
	}
	// Content stays intact and the mock saw the remote rename/move calls.
	if c, _ := fx.mock.FileContent(aUID); string(c) != "alpha" {
		t.Fatalf("a.txt = %q", c)
	}
	if c, _ := fx.mock.FileContent(bUID); string(c) != "beta" {
		t.Fatalf("b.txt = %q", c)
	}
	if c, _ := fx.mock.FileContent(cUID); string(c) != "gamma" {
		t.Fatalf("c.txt = %q", c)
	}
	seenRename, seenMove := false, false
	for _, r := range fx.mock.Renames() {
		if r[0] == projectUID && r[1] == "Renamed" {
			seenRename = true
		}
	}
	for _, m := range fx.mock.Moves() {
		if m[0] == projectUID && m[1] == archiveUID {
			seenMove = true
		}
	}
	if !seenRename {
		t.Fatalf("expected a remote rename of %s: %v", projectUID, fx.mock.Renames())
	}
	if !seenMove {
		t.Fatalf("expected a remote move of %s under %s: %v", projectUID, archiveUID, fx.mock.Moves())
	}
	// DB paths re-keyed including the nested descendant.
	nested, _ := fx.db.NodeByPath("Renamed/sub/c.txt")
	if nested == nil || nested.NodeUID != cUID || nested.ParentUID == nil || *nested.ParentUID != subUID {
		t.Fatalf("nested row = %+v", nested)
	}
}

// ---------------------------------------------------------------------------
// TG-08 — remote trash → restore round trip; TG-02 event ordering
// ---------------------------------------------------------------------------

func TestRemoteTrashThenRestoreRoundTripsMirror(t *testing.T) {
	fx := start(t)
	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q", got)
	}
	downloads := len(fx.mock.DownloadedUIDs())
	cursorBefore, _ := fx.db.EventCursor("scope-1")

	fx.mock.RemoteTrash("node-hello")
	waitFor(t, func() bool {
		// The cursor is persisted after the event is applied, so require it
		// here too: the file/row can be gone a moment before the cursor
		// write.
		if fx.exists("hello.txt") {
			return false
		}
		row, _ := fx.db.NodeByPath("hello.txt")
		if row != nil {
			return false
		}
		cur, _ := fx.db.EventCursor("scope-1")
		return cur != "" && cur != cursorBefore
	})
	cursorTrashed, _ := fx.db.EventCursor("scope-1")
	if cursorTrashed == "" || cursorTrashed == cursorBefore {
		t.Fatalf("trash event must advance the cursor (%q → %q)", cursorBefore, cursorTrashed)
	}

	fx.mock.RemoteRestore("node-hello")
	waitFor(t, func() bool {
		b, err := os.ReadFile(filepath.Join(fx.root, "hello.txt"))
		if err != nil || string(b) != "hello drive" {
			return false
		}
		row, _ := fx.db.NodeByPath("hello.txt")
		if row == nil || row.NodeUID != "node-hello" {
			return false
		}
		cur, _ := fx.db.EventCursor("scope-1")
		return cur != "" && cur != cursorTrashed
	})
	cursorRestored, _ := fx.db.EventCursor("scope-1")
	if cursorRestored == "" || cursorRestored == cursorTrashed {
		t.Fatalf("restore event must advance the cursor (%q → %q)", cursorTrashed, cursorRestored)
	}

	if got := len(fx.mock.DownloadedUIDs()); got <= downloads {
		t.Fatalf("restore must fetch the content again: %d <= %d", got, downloads)
	}
	if _, ok := conflictEntry(fx.root, "hello"); ok {
		t.Fatal("unexpected conflict copy")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("remote-originated trash never calls back: %v", trashed)
	}
}

func TestChildUpdateBeforeMovedParentLandsAtFinalPath(t *testing.T) {
	fx := start(t)
	fx.mock.RemoteCreateFile("node-docs", "nested.txt", []byte("before move"))
	waitFor(t, func() bool { return fx.exists("Documents/nested.txt") })
	nestedUID, ok := fx.mock.FindUID("node-docs", "nested.txt")
	if !ok {
		t.Fatal("nested remote missing")
	}

	// Remote state: a new Outer folder holds Documents, but the engine has
	// not seen Outer yet (its event is still queued behind the child's).
	outer := fx.mock.RemoteCreateFolderSilent("root", "Outer")
	fx.mock.RemoteMoveSilent("node-docs", outer)

	// Child update first: its parent row still points at the old root path.
	fx.mock.RemoteUpdateFile(nestedUID, []byte("changed before parent event"))
	waitFor(t, func() bool {
		b, err := os.ReadFile(filepath.Join(fx.root, "Documents", "nested.txt"))
		return err == nil && string(b) == "changed before parent event"
	})

	// Now the parent folder's move update, without an Outer creation event.
	fx.mock.EmitEvent(&sidecar.DriveEvent{
		ScopeID:       "scope-1",
		EventID:       "evt-parent-move",
		Kind:          sidecar.EventNodeUpdated,
		NodeUID:       strPtr("node-docs"),
		ParentNodeUID: strPtr(outer),
	})

	waitFor(t, func() bool {
		b, err := os.ReadFile(filepath.Join(fx.root, "Outer", "Documents", "nested.txt"))
		if err != nil || string(b) != "changed before parent event" {
			return false
		}
		row, _ := fx.db.NodeByPath("Outer/Documents/nested.txt")
		return row != nil && row.NodeUID == nestedUID
	})
	if fx.exists("Documents") {
		t.Fatal("the old root-level folder must not linger")
	}
	if row, _ := fx.db.NodeByPath("Documents"); row != nil {
		t.Fatalf("old Documents row lingered: %+v", row)
	}
	if row, _ := fx.db.NodeByPath("Documents/nested.txt"); row != nil {
		t.Fatalf("old nested row lingered: %+v", row)
	}
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if r.NodeUID == nestedUID {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("child must have exactly one row, got %d", n)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// TG-10/TG-11 — metadata fidelity
// ---------------------------------------------------------------------------

func TestDownloadAppliesRemoteMtimeExactly(t *testing.T) {
	fx := start(t)
	remoteMtime, ok := fx.mock.NodeMtime("node-hello")
	if !ok {
		t.Fatal("mock mtime missing")
	}
	if remoteMtime != 1_700_000_000 {
		t.Fatalf("distinctive seeded mtime expected, got %d", remoteMtime)
	}
	waitFor(t, func() bool { return mtimeSecs(t, filepath.Join(fx.root, "hello.txt")) == remoteMtime })
}

func TestLocalExplicitMtimeIsUploadedAndStored(t *testing.T) {
	fx := start(t)
	stamped := int64(1_650_000_000)
	path := filepath.Join(fx.root, "stamped.txt")
	if err := os.WriteFile(path, []byte("build output"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Unix(stamped, 0), time.Unix(stamped, 0)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		for _, e := range fx.mock.UploadedMtimes() {
			if e.Name == "stamped.txt" && e.Mtime == stamped {
				return true
			}
		}
		return false
	})
	waitForSynced(t, fx.db, "stamped.txt")

	row, _ := fx.db.NodeByPath("stamped.txt")
	if row == nil || row.Mtime == nil || *row.Mtime != stamped {
		t.Fatalf("row mtime = %+v", row)
	}
	uid, ok := fx.mock.FindUID("root", "stamped.txt")
	if !ok {
		t.Fatal("stamped.txt not uploaded")
	}
	if mt, _ := fx.mock.NodeMtime(uid); mt != stamped {
		t.Fatalf("remote mtime = %d, want %d", mt, stamped)
	}
}

func TestLocalExecutableBitIsCapturedOnUpload(t *testing.T) {
	fx := start(t)
	path := filepath.Join(fx.root, "tool.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho ok\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return contains(fx.uploadedNames(), "tool.sh") })
	waitForSynced(t, fx.db, "tool.sh")

	row, _ := fx.db.NodeByPath("tool.sh")
	if row == nil || row.Mode == nil || *row.Mode != 0o755 {
		t.Fatalf("owner-exec must be persisted locally: %+v", row)
	}
}

// ---------------------------------------------------------------------------
// TG-12 — case-only renames and case collisions
// ---------------------------------------------------------------------------

func TestRemoteCaseOnlyRenameRekeysRowAndPreservesIdentity(t *testing.T) {
	fx := start(t)
	fx.mock.RemoteCreateFile("root", "README.md", []byte("read me"))
	waitForSynced(t, fx.db, "README.md")
	uid, _ := fx.mock.FindUID("root", "README.md")
	downloads := len(fx.mock.DownloadedUIDs())

	// Case-only remote rename: the row is resolved by uid, so this is a
	// pure relocation (no conflict, no trash, no re-download).
	fx.mock.RemoteRename(uid, "readme.md")

	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("readme.md")
		old, _ := fx.db.NodeByPath("README.md")
		return row != nil && row.NodeUID == uid && old == nil
	})
	if got := string(fx.read("readme.md")); got != "read me" {
		t.Fatalf("readme.md = %q", got)
	}
	if fx.exists("README.md") {
		t.Fatal("old casing still on disk")
	}
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if r.NodeUID == uid {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the renamed node must keep exactly one row, got %d", n)
	}
	if _, ok := conflictEntry(fx.root, "readme"); ok {
		t.Fatal("a case-only rename must not create a conflict copy")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if got := len(fx.mock.DownloadedUIDs()); got != downloads {
		t.Fatalf("downloads = %d, want %d", got, downloads)
	}
}

func TestLocalCaseOnlyFolderRenameKeepsSubtreeAndUsesInodePairing(t *testing.T) {
	fx := start(t)
	if err := os.Mkdir(filepath.Join(fx.root, "Docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	fx.write("Docs/a.txt", []byte("alpha"))
	waitForSynced(t, fx.db, "Docs/a.txt")
	waitFor(t, func() bool {
		_, ok := fx.mock.FindUID("root", "Docs")
		return ok
	})
	docsUID, _ := fx.mock.FindUID("root", "Docs")
	aUID, _ := fx.mock.FindUID(docsUID, "a.txt")
	uploads := len(fx.uploadedNames())

	// A case-only directory rename must be paired by inode, never mistaken
	// for "delete the folder + create a new one".
	if err := os.Rename(filepath.Join(fx.root, "Docs"), filepath.Join(fx.root, "docs")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("docs/a.txt")
		frow, _ := fx.db.NodeByPath("docs")
		old, _ := fx.db.NodeByPath("Docs")
		return row != nil && row.NodeUID == aUID && frow != nil && old == nil
	})
	waitFor(t, func() bool {
		got, ok := fx.mock.FindUID("root", "docs")
		return ok && got == docsUID
	})
	if got, ok := fx.mock.FindUID(docsUID, "a.txt"); !ok || got != aUID {
		t.Fatalf("a.txt uid changed: %q", got)
	}
	if got := string(fx.read("docs/a.txt")); got != "alpha" {
		t.Fatalf("docs/a.txt = %q", got)
	}
	if fx.exists("Docs") {
		t.Fatal("old casing still on disk")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if got := len(fx.uploadedNames()); got != uploads {
		t.Fatalf("uploads = %d, want %d", got, uploads)
	}
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if r.NodeUID == docsUID || r.NodeUID == aUID {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("folder and child must keep exactly one row each, got %d", n)
	}
}

func TestCaseDifferingRemoteSiblingsStayDistinct(t *testing.T) {
	fx := start(t)
	lower := fx.mock.RemoteCreateFile("root", "case.txt", []byte("lower bytes"))
	waitForSynced(t, fx.db, "case.txt")
	upper := fx.mock.RemoteCreateFile("root", "Case.txt", []byte("UPPER bytes"))
	waitForSynced(t, fx.db, "Case.txt")

	if got := string(fx.read("case.txt")); got != "lower bytes" {
		t.Fatalf("case.txt = %q", got)
	}
	if got := string(fx.read("Case.txt")); got != "UPPER bytes" {
		t.Fatalf("Case.txt = %q", got)
	}
	row, _ := fx.db.NodeByPath("case.txt")
	if row == nil || row.NodeUID != lower {
		t.Fatalf("case.txt row = %+v", row)
	}
	row, _ = fx.db.NodeByPath("Case.txt")
	if row == nil || row.NodeUID != upper {
		t.Fatalf("Case.txt row = %+v", row)
	}
	if lower == upper {
		t.Fatal("siblings must be distinct nodes")
	}
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if r.NodeUID == lower || r.NodeUID == upper {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("two distinct rows expected, got %d", n)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if _, ok := conflictEntry(fx.root, "case"); ok {
		t.Fatal("unexpected conflict copy (case)")
	}
	if _, ok := conflictEntry(fx.root, "Case"); ok {
		t.Fatal("unexpected conflict copy (Case)")
	}
}

// ---------------------------------------------------------------------------
// TG-13 — Unicode, emoji, and odd-but-legal names (full battery)
// ---------------------------------------------------------------------------

var unusualNames = []string{
	"caf\u00e9.txt",         // NFC
	"cafe\u0301.txt",        // NFD
	"rocket \U0001f680.txt", // emoji
	"with space.txt",
	"it's \"quoted\".txt",
	"line\nbreak.txt",
	"trailing dot.",
	"trailing space ",
	"~leading.txt",
}

func TestUnusualNamesUploadRoundTripFullBattery(t *testing.T) {
	fx := start(t)
	for _, name := range unusualNames {
		fx.write(name, []byte("upload:"+name))
	}
	for _, name := range unusualNames {
		waitForSynced(t, fx.db, name)
		if !contains(fx.uploadedNames(), name) {
			t.Fatalf("name %q must reach the remote byte-for-byte: %v", name, fx.uploadedNames())
		}
		row, err := fx.db.NodeByPath(name)
		if err != nil || row == nil {
			t.Fatalf("missing row for %q", name)
		}
		if row.LocalPath != name || row.Name != name {
			t.Fatalf("row path/name = %q/%q, want %q", row.LocalPath, row.Name, name)
		}
		if got := string(fx.read(name)); got != "upload:"+name {
			t.Fatalf("%q content = %q", name, got)
		}
		uid, ok := fx.mock.FindUID("root", name)
		if !ok || uid != row.NodeUID {
			t.Fatalf("remote uid mismatch for %q: %q vs %q", name, uid, row.NodeUID)
		}
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

func TestUnusualNamesDownloadRoundTripFullBattery(t *testing.T) {
	fx := start(t)
	var uids []struct{ name, uid string }
	for _, name := range unusualNames {
		uid := fx.mock.RemoteCreateFile("root", name, []byte("remote:"+name))
		uids = append(uids, struct{ name, uid string }{name, uid})
	}
	for _, e := range uids {
		waitForSynced(t, fx.db, e.name)
		if got := string(fx.read(e.name)); got != "remote:"+e.name {
			t.Fatalf("downloaded bytes for %q = %q", e.name, got)
		}
		row, err := fx.db.NodeByPath(e.name)
		if err != nil || row == nil {
			t.Fatalf("missing row for %q", e.name)
		}
		if row.NodeUID != e.uid {
			t.Fatalf("%q uid = %q, want %q", e.name, row.NodeUID, e.uid)
		}
		if row.Name != e.name {
			t.Fatalf("%q row name = %q", e.name, row.Name)
		}
	}
	// The local namespace keeps NFC and NFD as distinct files (Linux does
	// not normalise), so the rows agree on nine paths, not eight.
	rows, err := fx.db.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if contains(unusualNames, r.LocalPath) {
			n++
		}
	}
	if n != len(unusualNames) {
		t.Fatalf("unusual rows = %d, want %d", n, len(unusualNames))
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

// conflictPrefixFor matches the engine's conflict-name extension handling
// (dotfiles/extension-less names keep the suffix whole).
func conflictPrefixFor(name string) string {
	if i := strings.LastIndex(name, "."); i > 0 {
		return name[:i] + " (conflict "
	}
	return name + " (conflict "
}

func entriesWithPrefix(t *testing.T, dir, prefix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func TestConflictCopiesForUnusualNamesAreValidSiblings(t *testing.T) {
	fx := start(t)
	// One fixture, sequential collisions: each local original is displaced
	// by a remote node of the same name, then must survive as a sibling.
	names := []string{
		"report.txt",
		"trailing dot.",
		"line\nbreak.txt",
		"cafe\u0301.txt",
	}
	for _, name := range names {
		local := "local:" + name
		fx.write(name, []byte(local))
		waitForSynced(t, fx.db, name)

		remoteUID := fx.mock.RemoteCreateFile("root", name, []byte("remote:"+name))
		prefix := conflictPrefixFor(name)
		waitFor(t, func() bool {
			siblings := entriesWithPrefix(t, fx.root, prefix)
			if len(siblings) == 0 {
				return false
			}
			b, err := os.ReadFile(filepath.Join(fx.root, siblings[0]))
			if err != nil || string(b) != local {
				return false
			}
			if got := string(fx.read(name)); got != "remote:"+name {
				return false
			}
			row, _ := fx.db.NodeByPath(name)
			return row != nil && row.NodeUID == remoteUID
		})

		// The incoming node keeps the original path; the displaced node
		// keeps its identity under a conflict sibling with the extension
		// intact (never corrupting the original).
		row, _ := fx.db.NodeByPath(name)
		if row == nil || row.NodeUID != remoteUID {
			t.Fatalf("%q row uid = %+v, want the remote node %q", name, row, remoteUID)
		}
		siblings := entriesWithPrefix(t, fx.root, prefix)
		if len(siblings) != 1 {
			t.Fatalf("one conflict sibling expected for %q, got %v", name, siblings)
		}
		sibling := siblings[0]
		if i := strings.LastIndex(name, "."); i > 0 && !strings.HasPrefix(name, ".") {
			ext := name[i+1:]
			if !strings.HasSuffix(sibling, "."+ext) {
				t.Fatalf("extension must survive conflict naming for %q: %q", name, sibling)
			}
		}
		if row, _ := fx.db.NodeByPath(sibling); row == nil {
			t.Fatalf("conflict sibling %q has no row", sibling)
		}
	}

	// Dedup: a second collision for the same name must pick a NEW sibling
	// instead of overwriting the first conflict copy.
	fx.write("report.txt", []byte("local once more"))
	waitForSynced(t, fx.db, "report.txt")
	fx.mock.RemoteCreateFile("root", "report.txt", []byte("remote once more"))
	waitFor(t, func() bool {
		siblings := entriesWithPrefix(t, fx.root, "report (conflict ")
		if len(siblings) != 2 {
			return false
		}
		for _, s := range siblings {
			if row, _ := fx.db.NodeByPath(s); row == nil {
				return false
			}
		}
		return true
	})
	siblings := entriesWithPrefix(t, fx.root, "report (conflict ")
	if len(siblings) != 2 {
		t.Fatalf("dedup must not overwrite a sibling: %v", siblings)
	}
	for _, s := range siblings {
		if !strings.HasPrefix(s, "report (conflict ") {
			t.Fatalf("unexpected conflict sibling name: %q", s)
		}
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}
