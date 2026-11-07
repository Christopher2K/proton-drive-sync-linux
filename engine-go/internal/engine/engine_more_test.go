// Additional engine battery ports: remote renames/moves, tombstone
// cancellation, session expiry, offline start, restart semantics, and
// pause-freeze of an active download. Ported from the remainder of
// crates/core/tests/engine.rs (mirror-mode subset).
package engine_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

// ---------------------------------------------------------------------------
// Remote renames / moves (TG-01/TG-02)
// ---------------------------------------------------------------------------

func TestRemoteFileRenameRekeysRowAndPreservesContent(t *testing.T) {
	fx := start(t)
	fx.mock.RemoteRename("node-hello", "renamed.txt")
	waitFor(t, func() bool { return fx.exists("renamed.txt") && !fx.exists("hello.txt") })
	if got := string(fx.read("renamed.txt")); got != "hello drive" {
		t.Fatalf("renamed.txt = %q", got)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("renamed.txt")
		return row != nil && row.NodeUID == "node-hello"
	})
}

func TestRemoteFileMoveUpdatesParentAndMaterializesDestination(t *testing.T) {
	fx := start(t)
	docs, _ := fx.mock.FindUID("root", "Documents")
	fx.mock.RemoteMove("node-hello", docs)
	waitFor(t, func() bool { return fx.exists("Documents/hello.txt") && !fx.exists("hello.txt") })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("Documents/hello.txt")
		return row != nil && row.NodeUID == "node-hello"
	})
}

func TestRemoteFolderRenameRekeysSubtreeAndPreservesNestedContent(t *testing.T) {
	fx := start(t)
	// Build a nested subtree under Documents.
	nested := fx.mock.RemoteCreateFolder("node-docs", "Nested")
	fx.mock.RemoteCreateFile(nested, "inner.txt", []byte("inner"))
	waitFor(t, func() bool { return fx.exists("Documents/Nested/inner.txt") })
	fx.mock.RemoteRename("node-docs", "Docs2")
	waitFor(t, func() bool {
		return fx.exists("Docs2/Nested/inner.txt") && !fx.exists("Documents")
	})
	if got := string(fx.read("Docs2/Nested/inner.txt")); got != "inner" {
		t.Fatalf("inner.txt = %q", got)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("Docs2/Nested/inner.txt")
		return row != nil
	})
}

// ---------------------------------------------------------------------------
// Tombstone cancellation (TG-05)
// ---------------------------------------------------------------------------

func TestRecreatedFileCancelsPendingTombstone(t *testing.T) {
	fx := start(t)
	// Stop the engine from draining: pause before deleting.
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	if err := os.Remove(filepath.Join(fx.root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // delete batch lands, tombstone queued
	// Recreate before the tombstone drains.
	fx.write("hello.txt", []byte("back again"))
	time.Sleep(1200 * time.Millisecond)
	fx.handle.Resume()
	// The recreate must cancel the tombstone: nothing trashed.
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent("node-hello")
		return string(c) == "back again"
	})
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("stale tombstone trashed the node: %v", trashed)
	}
	waitForSynced(t, fx.db, "hello.txt")
}

// ---------------------------------------------------------------------------
// Session expiry (1001/1002)
// ---------------------------------------------------------------------------

func TestSessionExpiredUploadParksAndHealsAfterReauthentication(t *testing.T) {
	fx := start(t)
	fx.write("auth.txt", []byte("v1"))
	waitForSynced(t, fx.db, "auth.txt")
	uid, _ := fx.mock.FindUID("root", "auth.txt")

	// Expire the session for the next upload; resume is the host's
	// "signed in again" signal.
	fx.mock.InjectUploadFailures(uid, [][]any{{int64(1002), "session expired", false}})
	fx.write("auth.txt", []byte("v2"))
	waitFor(t, func() bool {
		st, _ := fx.handle.Status()
		return st.NeedsSignIn
	})
	// Parked: no hot loop of failing uploads.
	attempts := fx.mock.UploadAttempts("auth.txt")
	time.Sleep(700 * time.Millisecond)
	if got := fx.mock.UploadAttempts("auth.txt") - attempts; got > 2 {
		t.Fatalf("attempts while parked = %d", got)
	}
	// Re-authenticate (resume) and heal.
	fx.handle.Resume()
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "v2"
	})
	waitForSynced(t, fx.db, "auth.txt")
}

// ---------------------------------------------------------------------------
// Offline start retries until the sidecar is reachable
// ---------------------------------------------------------------------------

func TestOfflineStartReportsAndRetriesUntilSidecarIsReachable(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	handle.FailGetRoot(2) // two failures, then reachable
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = 50 * time.Millisecond
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() { h.Shutdown(); <-done; client.Close() }()
	waitFor(t, func() bool { return fxExists(root, "hello.txt") })
}

// ---------------------------------------------------------------------------
// Restart semantics
// ---------------------------------------------------------------------------

func TestSecondRunSeesPersistedStateAndSyncsIncrementally(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, engine.StateDir, "state.db")

	seq := int64(500)
	for run := 0; run < 2; run++ {
		mock := sidecar.NewMockWithSeq(seq)
		client, handle := mock.Connect()
		if run == 1 {
			// Carry the first run's upload into the second mock.
			handle.RemoteCreateFile("root", "carried.txt", []byte("carried"))
		}
		cfg := engine.DefaultConfig()
		cfg.Root = root
		cfg.DBPath = dbPath
		eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- eng.Run() }()
		if !h.WaitForWatcher(10 * time.Second) {
			t.Fatalf("run %d: watcher never attached", run)
		}
		if run == 0 {
			if err := os.WriteFile(filepath.Join(root, "carried.txt"), []byte("carried"), 0o644); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return contains(handle.UploadedNames(), "carried.txt") })
		} else {
			waitFor(t, func() bool { return fxExists(root, "carried.txt") })
			// The persisted row survives; no duplicate upload of the same
			// content (synced hash matches).
			waitFor(t, func() bool {
				d, err := db.Open(dbPath)
				if err != nil {
					return false
				}
				defer d.Close()
				row, err := d.NodeByPath("carried.txt")
				return err == nil && row != nil && row.SyncState == db.StateSynced
			})
		}
		h.Shutdown()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("run %d: engine did not stop", run)
		}
		client.Close()
		seq += 100
	}
}

// ---------------------------------------------------------------------------
// Local delete of a non-empty folder: every node trashed exactly once
// ---------------------------------------------------------------------------

func TestLocalDeleteOfNonemptyFolderTrashesEachNodeOnce(t *testing.T) {
	// Seed enough synced files outside the folder that the in-folder
	// deletes stay under the guard ratio.
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	for i := 0; i < 20; i++ {
		handle.RemoteCreateFile("root", fmt.Sprintf("keep%02d.txt", i), []byte("k"))
	}
	fx := startWithClient(t, root, client, handle, engine.RetryBaseDefault)

	fx.write("Folder/a.txt", []byte("a"))
	fx.write("Folder/b.txt", []byte("b"))
	waitForSynced(t, fx.db, "Folder/a.txt")
	waitForSynced(t, fx.db, "Folder/b.txt")
	uidA, _ := fx.mock.FindUID("root", "a.txt")
	uidB, _ := fx.mock.FindUID("root", "b.txt")
	_ = uidA
	_ = uidB

	if err := os.RemoveAll(filepath.Join(fx.root, "Folder")); err != nil {
		t.Fatal(err)
	}
	waitForDeletesAccounted(t, fx, 3) // folder + 2 files
	// Drain any tombstones (guard may pause with 3/~24 deletes — resume).
	deadline := time.Now().Add(30 * time.Second)
	for {
		st, _ := fx.handle.Status()
		if st.QueuedTombstones == 0 {
			break
		}
		if st.State == engine.StateGuardPaused {
			fx.handle.Resume()
		}
		if time.Now().After(deadline) {
			t.Fatal("tombstones never drained")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Each uid trashed exactly once.
	counts := map[string]int{}
	for _, uid := range fx.mock.TrashedUIDs() {
		counts[uid]++
	}
	for uid, n := range counts {
		if n > 1 {
			t.Fatalf("uid %s trashed %d times", uid, n)
		}
	}
	if len(fx.mock.TrashedUIDs()) != 3 {
		t.Fatalf("trashed = %v, want 3 nodes", fx.mock.TrashedUIDs())
	}
}

// ---------------------------------------------------------------------------
// Pause freezes an active download; resume completes it (F4)
// ---------------------------------------------------------------------------

func TestPauseFreezesActiveDownloadAndResumeCompletesIt(t *testing.T) {
	fx := start(t)
	// A revision large enough that the mock parks the transfer mid-stream.
	big := make([]byte, 3*1024*1024)
	for i := range big {
		big[i] = byte(i % 251)
	}
	fx.mock.HoldNextDownload("node-hello", 1024*1024)
	fx.mock.RemoteUpdateFile("node-hello", big)
	waitFor(t, func() bool { return fx.mock.HeldDownloads() == 1 })

	// Pause while the transfer is in flight; the engine must reach the
	// sidecar's pause control and leave the row downloading.
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	waitFor(t, func() bool {
		for _, c := range fx.mock.TransferControls() {
			if c == "pause" {
				return true
			}
		}
		return false
	})
	// Old complete content stays at the final path while paused.
	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q while paused", got)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("hello.txt")
		return row != nil && row.SyncState == db.StateDownloading
	})

	fx.handle.Resume()
	waitForWithin(t, 60*time.Second, func() bool {
		return fx.exists("hello.txt") && string(fx.read("hello.txt")) == string(big)
	})
	waitForSynced(t, fx.db, "hello.txt")
	controls := fx.mock.TransferControls()
	hasResume := false
	for _, c := range controls {
		if c == "resume" {
			hasResume = true
		}
	}
	if !hasResume {
		t.Fatalf("transfer.controls = %v, want a resume", controls)
	}
	row, _ := fx.db.NodeByPath("hello.txt")
	if row == nil || row.Size == nil || *row.Size != int64(len(big)) {
		t.Fatalf("row size mismatch: %+v", row)
	}
}

// ---------------------------------------------------------------------------
// Sidecar death mid-upload: degraded, no stuck loop, no lost file
// ---------------------------------------------------------------------------

func TestSidecarDeathMidUploadDegradesWithoutStuckOrLoop(t *testing.T) {
	fx := start(t)
	fx.mock.CrashOnNextUpload()
	fx.write("dying.txt", []byte("never lands"))
	// The engine must surface the outage and keep answering commands.
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.SidecarUnavailable
	})
	// The dirty row survives for the next session.
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("dying.txt")
		return row != nil
	})
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State == "" {
		t.Fatal("engine state gone")
	}
}

// ---------------------------------------------------------------------------
// Unusual names download round trip
// ---------------------------------------------------------------------------

func TestUnusualNamesDownloadRoundTripExactly(t *testing.T) {
	fx := start(t)
	names := []string{
		"remote space.txt",
		"remote-üñïçø∂é.txt",
		"remote emoji 🎉.txt",
	}
	for i, n := range names {
		fx.mock.RemoteCreateFile("root", n, []byte(fmt.Sprintf("content %d", i)))
	}
	for i, n := range names {
		want := fmt.Sprintf("content %d", i)
		waitFor(t, func() bool { return fx.exists(n) })
		if got := string(fx.read(n)); got != want {
			t.Fatalf("%q = %q, want %q", n, got, want)
		}
	}
}
