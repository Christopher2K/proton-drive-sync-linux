// Engine battery port (wave 1): mass-delete guard re-trip and restart
// durability, unchanged-head fast path, skewed-clock conflict copies, 409
// healing, crash recovery, Retry-After, and resume-drops-backoff. Ported
// from crates/core/tests/engine.rs (mirror-mode subset).
package engine_test

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

// ---------------------------------------------------------------------------
// Wave helpers (shared by all engine_wave*_test.go files)
// ---------------------------------------------------------------------------

// runEngine starts a manual engine (no fixture cleanup); the caller must
// Shutdown the handle, drain done, and Close the client.
func runEngine(t *testing.T, client *sidecar.SidecarClient, root, dbPath string, retryBase time.Duration) (*engine.Handle, chan error) {
	t.Helper()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = retryBase
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	return h, done
}

func stopEngine(t *testing.T, h *engine.Handle, done chan error, client *sidecar.SidecarClient) {
	t.Helper()
	h.Shutdown()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("engine did not stop")
	}
	client.Close()
}

// activityRecorder drains the activity feed in the background so entries are
// never dropped (the engine channel is drop-on-full).
type activityRecorder struct {
	mu    sync.Mutex
	items []engine.ActivityItem
}

func recordActivity(h *engine.Handle) *activityRecorder {
	r := &activityRecorder{}
	ch := h.Activity()
	go func() {
		for it := range ch {
			r.mu.Lock()
			r.items = append(r.items, it)
			r.mu.Unlock()
		}
	}()
	return r
}

func (r *activityRecorder) snapshot() []engine.ActivityItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]engine.ActivityItem(nil), r.items...)
}

func (r *activityRecorder) count(kind engine.ActivityKind, path string) int {
	n := 0
	for _, it := range r.snapshot() {
		if it.Kind == kind && (path == "" || it.Path == path) {
			n++
		}
	}
	return n
}

// waitForActivity polls until an entry of the kind (and path, if non-empty)
// appears and returns it.
func waitForActivity(t *testing.T, r *activityRecorder, kind engine.ActivityKind, path string) engine.ActivityItem {
	t.Helper()
	var found engine.ActivityItem
	waitFor(t, func() bool {
		for _, it := range r.snapshot() {
			if it.Kind == kind && (path == "" || it.Path == path) {
				found = it
				return true
			}
		}
		return false
	})
	return found
}

func hashSHA1(b []byte) string {
	s := sha1.Sum(b)
	return hex.EncodeToString(s[:])
}

// downloadTempName mirrors the engine's transfer-temp naming for rel.
func downloadTempName(rel string) string {
	return ".pds-" + hashSHA1([]byte(rel)) + ".pds-part"
}

func strPtrT(s string) *string { return &s }

// mustTombstones fails the test on DB errors and returns pending tombstones.
func mustTombstones(t *testing.T, d *db.Db) []*db.Tombstone {
	t.Helper()
	tombs, err := d.Tombstones()
	if err != nil {
		t.Fatal(err)
	}
	return tombs
}

func lockDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
}

func unlockDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mtimeSecsOf(path string) (int64, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	return fi.ModTime().Unix(), true
}

func waitForEngineState(t *testing.T, fx *fixture, state engine.EngineState) {
	t.Helper()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == state
	})
}

// seedRemoteSyncedFillers seeds n remote filler files (the Rust battery's
// seed_synced_fillers): keeps single deletes under the 25% guard limb.
func seedRemoteSyncedFillers(t *testing.T, fx *fixture, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		fx.mock.RemoteCreateFile("root", fmt.Sprintf("filler%02d.txt", i), []byte("filler"))
	}
	for i := 0; i < n; i++ {
		waitForSynced(t, fx.db, fmt.Sprintf("filler%02d.txt", i))
	}
}

// ---------------------------------------------------------------------------
// Mass-delete guard: re-trip and restart durability
// ---------------------------------------------------------------------------

func TestMassDeleteGuardReTripsOnNewDeletesBeyondApprovedSet(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	for i := 0; i < 30; i++ {
		handle.RemoteCreateFile("root", fmt.Sprintf("f%02d.txt", i), []byte("x"))
	}
	fx := startWithClient(t, root, client, handle, engine.RetryBaseDefault)

	// First batch: 25 deletes → guard trips → one resume drains them all.
	for i := 0; i < 25; i++ {
		if err := os.Remove(filepath.Join(root, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitForEngineState(t, fx, engine.StateGuardPaused)
	waitForDeletesAccounted(t, fx, 25)
	fx.handle.Resume()
	waitFor(t, func() bool { return len(fx.mock.TrashedUIDs()) == 25 })
	waitFor(t, func() bool {
		tombs, _ := fx.db.Tombstones()
		return len(tombs) == 0
	})

	// NEW deletes beyond the approved set must re-trip the guard: 7 synced
	// nodes remain, so the second new delete breaks the 25% ratio.
	for i := 25; i < 30; i++ {
		if err := os.Remove(filepath.Join(root, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitForEngineState(t, fx, engine.StateGuardPaused)
	if trashed := len(fx.mock.TrashedUIDs()); trashed >= 30 {
		t.Fatalf("guard must re-trip before the new batch drains: %d", trashed)
	}
	tombs, _ := fx.db.Tombstones()
	if len(tombs) == 0 {
		t.Fatal("no pending tombstones after re-trip")
	}
	waitForDeletesAccounted(t, fx, 30)

	fx.handle.Resume()
	waitFor(t, func() bool { return len(fx.mock.TrashedUIDs()) == 30 })
	waitFor(t, func() bool {
		tombs, _ := fx.db.Tombstones()
		return len(tombs) == 0
	})
	time.Sleep(500 * time.Millisecond)
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q, want running", st.State)
	}
}

func TestMassDeleteGuardPauseSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	// Same seeding order on both sides of the restart keeps the generated
	// node uids identical, so the second mock can trash every tombstone.
	for i := 0; i < 40; i++ {
		handle.RemoteCreateFile("root", fmt.Sprintf("f%02d.txt", i), []byte("x"))
	}
	fx := startWithClient(t, root, client, handle, engine.RetryBaseDefault)

	for i := 0; i < 15; i++ {
		if err := os.Remove(filepath.Join(root, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitForEngineState(t, fx, engine.StateGuardPaused)
	waitForDeletesAccounted(t, fx, 15)
	trashedBefore := len(fx.mock.TrashedUIDs())
	if trashedBefore >= 15 {
		t.Fatalf("guard must stop the first run before the wave drains: %d", trashedBefore)
	}
	tombs, _ := fx.db.Tombstones()
	pendingBefore := len(tombs)
	if pendingBefore == 0 {
		t.Fatal("remaining deletes must stay tombstoned")
	}
	if trashedBefore+pendingBefore != 15 {
		t.Fatalf("accounted = %d+%d, want 15", trashedBefore, pendingBefore)
	}

	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	fx.shutdown()

	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := d.Meta("guard_paused"); v != "1" {
		t.Fatalf("guard_paused = %q, want 1 (persisted before shutdown)", v)
	}
	tombs2, _ := d.Tombstones()
	if len(tombs2) != pendingBefore {
		t.Fatalf("tombstones across shutdown = %d, want %d", len(tombs2), pendingBefore)
	}
	d.Close()

	// Fresh engine over the SAME root+db, with a matching mock tree.
	mock2 := sidecar.NewMock()
	client2, handle2 := mock2.Connect()
	for i := 0; i < 40; i++ {
		handle2.RemoteCreateFile("root", fmt.Sprintf("f%02d.txt", i), []byte("x"))
	}
	h, done := runEngine(t, client2, root, dbPath, engine.RetryBaseDefault)
	defer func() { stopEngine(t, h, done, client2) }()
	waitFor(t, func() bool {
		st, err := h.Status()
		return err == nil && st.State == engine.StateGuardPaused
	})

	// No silent resume: nothing drains until the user approves again.
	time.Sleep(1200 * time.Millisecond)
	if trashed := handle2.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("restart drained tombstones while guard-paused: %v", trashed)
	}
	d, err = db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	tombs3, _ := d.Tombstones()
	if len(tombs3) != pendingBefore {
		t.Fatalf("tombstones after paused restart = %d, want %d", len(tombs3), pendingBefore)
	}
	d.Close()

	h.Resume()
	waitFor(t, func() bool { return len(handle2.TrashedUIDs()) == pendingBefore })
	d, err = db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		tombs, _ := d.Tombstones()
		return len(tombs) == 0
	})
	st, _ := h.Status()
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q, want running", st.State)
	}
	if v, _ := d.Meta("guard_paused"); v != "" {
		t.Fatalf("guard_paused = %q, want removed", v)
	}
	d.Close()
}

// ---------------------------------------------------------------------------
// Conflicts: unchanged-head fast path, skewed clocks, 409 healing
// ---------------------------------------------------------------------------

func TestDirtyUploadWithUnchangedRemoteHeadUploadsWithoutConflict(t *testing.T) {
	fx := start(t)
	fx.write("hello.txt", []byte("v2"))
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent("node-hello")
		return string(c) == "v2"
	})
	time.Sleep(1200 * time.Millisecond) // settle

	// Exactly one revision upload, no conflict file anywhere.
	count := 0
	for _, n := range fx.uploadedNames() {
		if n == "hello.txt" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("hello.txt uploads = %d, want 1", count)
	}
	if _, ok := conflictEntry(fx.root, "hello"); ok {
		t.Fatal("unexpected conflict copy")
	}
	row, _ := fx.db.NodeByPath("hello.txt")
	if row == nil || row.SyncState != db.StateSynced {
		t.Fatalf("row = %+v, want synced", row)
	}
	expected := hashSHA1([]byte("v2"))
	if row.SyncedSHA1 == nil || *row.SyncedSHA1 != expected {
		t.Fatalf("synced head = %v, want %s", row.SyncedSHA1, expected)
	}
}

func TestConflictCopiesFromSkewedClocksAllSurvive(t *testing.T) {
	fx := start(t)
	future := "hello (conflict 2035-06-01 1200).txt"
	past := "hello (conflict 1999-01-01 0000).txt"
	fx.write(future, []byte("from the future"))
	fx.write(past, []byte("from the past"))

	fx.handle.Pause()
	waitForEngineState(t, fx, engine.StatePaused)
	fx.write("hello.txt", []byte("local one"))
	time.Sleep(1200 * time.Millisecond)
	fx.mock.RemoteUpdateFile("node-hello", []byte("remote one"))
	fx.handle.Resume()

	waitFor(t, func() bool {
		content, err := os.ReadFile(filepath.Join(fx.root, "hello.txt"))
		if err != nil || string(content) != "remote one" {
			return false
		}
		entries, err := os.ReadDir(fx.root)
		if err != nil {
			return false
		}
		for _, e := range entries {
			n := e.Name()
			if n == future || n == past {
				continue
			}
			if strings.HasPrefix(n, "hello (conflict ") {
				b, err := os.ReadFile(filepath.Join(fx.root, n))
				return err == nil && string(b) == "local one"
			}
		}
		return false
	})

	if got := string(fx.read(future)); got != "from the future" {
		t.Fatalf("future copy = %q", got)
	}
	if got := string(fx.read(past)); got != "from the past" {
		t.Fatalf("past copy = %q", got)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

func TestRevisionConflict409HealsViaRemoteEvent(t *testing.T) {
	fx := start(t)
	// Several scripted 409s: an inline-retry regression would burn attempts.
	var failures [][]any
	for i := 0; i < 5; i++ {
		failures = append(failures, []any{int64(1099), "POST /drive/v2/volumes/x/links/y/revisions 409 'Current revision is no longer up to date'", false})
	}
	fx.mock.InjectUploadFailures("node-hello", failures)

	fx.write("hello.txt", []byte("local edit"))

	// Exactly ONE rejected attempt, then parked: no retry, no conflict copy.
	waitFor(t, func() bool {
		c, _ := fx.db.IsRemoteConflicted("hello.txt")
		return c
	})
	time.Sleep(1500 * time.Millisecond)
	if n := fx.mock.UploadAttempts("hello.txt"); n != 1 {
		t.Fatalf("upload attempts = %d, want 1 (no inline retry)", n)
	}
	if _, ok := conflictEntry(fx.root, "hello"); ok {
		t.Fatal("conflict copy must not exist while parked")
	}
	st, _ := fx.handle.Status()
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q", st.State)
	}
	if st.ConflictedNodes != 1 || st.ErroredNodes != 0 {
		t.Fatalf("conflicted = %d, errored = %d", st.ConflictedNodes, st.ErroredNodes)
	}

	// The remote event carrying the fresh head arrives and heals the node.
	fx.mock.RemoteUpdateFile("node-hello", []byte("remote fresh"))
	waitFor(t, func() bool {
		if string(fx.read("hello.txt")) != "remote fresh" {
			return false
		}
		name, ok := conflictEntry(fx.root, "hello")
		if !ok {
			return false
		}
		b, err := os.ReadFile(filepath.Join(fx.root, name))
		return err == nil && string(b) == "local edit"
	})

	// The conflict copy (local edit) syncs up as a NEW node…
	conflictUID := waitForMockUpload(t, fx, "hello (conflict ")
	if c, _ := fx.mock.FileContent(conflictUID); string(c) != "local edit" {
		t.Fatalf("conflict upload = %q", c)
	}
	// …the original node holds the fresh remote content…
	if c, _ := fx.mock.FileContent("node-hello"); string(c) != "remote fresh" {
		t.Fatalf("node-hello = %q", c)
	}
	// …and the node healed without a restart.
	waitFor(t, func() bool {
		c, _ := fx.db.IsRemoteConflicted("hello.txt")
		return !c
	})
	st, _ = fx.handle.Status()
	if st.ConflictedNodes != 0 || st.ErroredNodes != 0 {
		t.Fatalf("conflicted = %d, errored = %d", st.ConflictedNodes, st.ErroredNodes)
	}
	if n := fx.mock.UploadAttempts("hello.txt"); n != 1 {
		t.Fatalf("upload attempts after heal = %d, want 1", n)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

func TestErroredNodeRecoversOnNextRemoteEvent(t *testing.T) {
	fx := start(t)
	var failures [][]any
	for i := 0; i < 30; i++ {
		failures = append(failures, []any{int64(1099), "integrity check failed", false})
	}
	fx.mock.InjectUploadFailures("node-hello", failures)

	fx.write("hello.txt", []byte("broken local edit"))
	waitFor(t, func() bool {
		st, _ := fx.handle.Status()
		return st.ErroredNodes == 1
	})
	if n := fx.mock.UploadAttempts("hello.txt"); n != 1 {
		t.Fatalf("upload attempts = %d, want 1", n)
	}

	// The remote side moves on; its event reconciles the errored node.
	fx.mock.RemoteUpdateFile("node-hello", []byte("remote fix"))
	waitFor(t, func() bool {
		content, err := os.ReadFile(filepath.Join(fx.root, "hello.txt"))
		if err != nil || string(content) != "remote fix" {
			return false
		}
		name, ok := conflictEntry(fx.root, "hello")
		if !ok {
			return false
		}
		b, err := os.ReadFile(filepath.Join(fx.root, name))
		return err == nil && string(b) == "broken local edit"
	})

	waitFor(t, func() bool {
		st, _ := fx.handle.Status()
		return st.ErroredNodes == 0
	})
	conflictUID := waitForMockUpload(t, fx, "hello (conflict ")
	if c, _ := fx.mock.FileContent(conflictUID); string(c) != "broken local edit" {
		t.Fatalf("conflict upload = %q", c)
	}
	if n := fx.mock.UploadAttempts("hello.txt"); n != 1 {
		t.Fatalf("upload attempts after heal = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

func TestCrashedUploadWithVanishedFileTombstones(t *testing.T) {
	fx := start(t)
	if err := fx.db.SetSyncState("hello.txt", db.StateUploading); err != nil {
		t.Fatal(err)
	}
	root, dbPath := fx.root, filepath.Join(fx.root, engine.StateDir, "state.db")
	if err := os.Remove(filepath.Join(root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	fx.shutdown()

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	h, done := runEngine(t, client, root, dbPath, engine.RetryBaseDefault)
	defer func() { stopEngine(t, h, done, client) }()
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	waitFor(t, func() bool {
		row, _ := d.NodeByPath("hello.txt")
		return row == nil
	})
	waitFor(t, func() bool { return len(mock.Handle().TrashedUIDs()) > 0 })
	for _, u := range mock.Handle().TrashedUIDs() {
		if u != "node-hello" {
			t.Fatalf("only the interrupted file may be trashed: %v", mock.Handle().TrashedUIDs())
		}
	}
}

func TestInterruptedDownloadRecoversOnRestart(t *testing.T) {
	NEW := []byte("the complete new remote revision that must win")
	fx := start(t)

	fx.mock.HoldNextDownload("node-hello", 5)
	fx.mock.RemoteUpdateFile("node-hello", NEW)
	waitFor(t, func() bool { return fx.mock.HeldDownloads() == 1 })

	tmpRel := downloadTempName("hello.txt")
	waitFor(t, func() bool { return fx.exists(tmpRel) })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("hello.txt")
		return row != nil && row.SyncState == db.StateDownloading
	})

	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q while interrupted", got)
	}
	b, err := os.ReadFile(filepath.Join(fx.root, tmpRel))
	if err != nil || len(b) != 5 {
		t.Fatalf("temp = %v (%d bytes, err %v), want the 5-byte prefix", b, len(b), err)
	}
	if string(b) != string(NEW[:5]) {
		t.Fatalf("temp prefix = %q", b)
	}

	root, dbPath := fx.root, filepath.Join(fx.root, engine.StateDir, "state.db")
	fx.shutdown()

	// Same root+db; the remote still holds the new revision.
	mock := sidecar.NewMockWithSeq(500)
	client, handle := mock.Connect()
	handle.RemoteUpdateFileSilent("node-hello", NEW)
	h, done := runEngine(t, client, root, dbPath, engine.RetryBaseDefault)
	defer func() { stopEngine(t, h, done, client) }()

	waitFor(t, func() bool {
		b, err := os.ReadFile(filepath.Join(root, "hello.txt"))
		return err == nil && string(b) == string(NEW)
	})
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	waitForSynced(t, d, "hello.txt")

	row, _ := d.NodeByPath("hello.txt")
	if row == nil || row.NodeUID != "node-hello" {
		t.Fatalf("row = %+v", row)
	}
	if row.Size == nil || *row.Size != int64(len(NEW)) {
		t.Fatalf("row size = %v", row.Size)
	}
	wantSHA, _ := handle.FileSHA1("node-hello")
	if row.SHA1 == nil || *row.SHA1 != wantSHA {
		t.Fatalf("row sha1 = %v, want %s", row.SHA1, wantSHA)
	}
	if row.SyncedSHA1 == nil || *row.SyncedSHA1 != wantSHA {
		t.Fatalf("row synced sha1 = %v", row.SyncedSHA1)
	}
	if fxExistsLocal(root, tmpRel) {
		t.Fatal("stale partial temp must be swept")
	}
	downloaded := handle.DownloadedUIDs()
	if len(downloaded) != 1 || downloaded[0] != "node-hello" {
		t.Fatalf("downloads = %v, want exactly one recovery download", downloaded)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	hellos := 0
	for _, e := range entries {
		if e.Name() == "hello.txt" {
			hellos++
		}
		if strings.Contains(e.Name(), "conflict") || strings.Contains(e.Name(), "pds-part") {
			t.Fatalf("leftover %q", e.Name())
		}
	}
	if hellos != 1 {
		t.Fatalf("hello.txt entries = %d", hellos)
	}
	helloCount := 0
	for _, n := range handle.ChildNames("root") {
		if n == "hello.txt" {
			helloCount++
		}
	}
	if helloCount != 1 {
		t.Fatalf("remote hello.txt nodes = %d", helloCount)
	}
	if trashed := handle.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("no trash round-trip: %v", trashed)
	}
}

func fxExistsLocal(root, rel string) bool {
	_, err := os.Stat(filepath.Join(root, rel))
	return err == nil
}

// ---------------------------------------------------------------------------
// Retry-After hint and resume-drops-backoff
// ---------------------------------------------------------------------------

func TestRateLimitRetryAfterHintIsHonored(t *testing.T) {
	fx := startWithRetryBase(t, 20*time.Millisecond)
	// Hint 1s ≫ computed backoff (20ms). The engine must wait ≥ 1s.
	fx.mock.InjectUploadFailuresFull("node-hello", [][]any{{int64(1003), "rate limited", true, float64(1)}})

	t0 := time.Now()
	fx.write("hello.txt", []byte("throttled"))
	waitFor(t, func() bool { return fx.mock.UploadAttempts("hello.txt") == 2 })
	if elapsed := time.Since(t0); elapsed < 900*time.Millisecond {
		t.Fatalf("retryAfter hint ignored: 2nd attempt after only %v", elapsed)
	}
}

func TestResumeDropsPendingBackoff(t *testing.T) {
	fx := startWithRetryBase(t, 20*time.Millisecond)
	// First failure schedules a LONG backoff via the server hint (60s).
	fx.mock.InjectUploadFailuresFull("node-hello", [][]any{{int64(1003), "rate limited", true, float64(60)}})
	fx.write("hello.txt", []byte("v2"))
	waitFor(t, func() bool { return fx.mock.UploadAttempts("hello.txt") == 1 })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("hello.txt")
		return row != nil && row.SyncState == db.StateDirty
	})

	// Nothing further happens while the 60s backoff runs…
	time.Sleep(400 * time.Millisecond)
	if n := fx.mock.UploadAttempts("hello.txt"); n != 1 {
		t.Fatalf("attempts during backoff = %d, want 1", n)
	}

	// …but an explicit resume is "try now".
	fx.handle.Resume()
	waitFor(t, func() bool { return fx.mock.UploadAttempts("hello.txt") == 2 })
	if c, _ := fx.mock.FileContent("node-hello"); string(c) != "v2" {
		t.Fatalf("node-hello = %q, want v2", c)
	}
}
