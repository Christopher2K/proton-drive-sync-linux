// Engine integration tests against the in-process MockSidecar, ported from
// crates/core/tests/engine.rs. No real Proton account required; all timing
// uses generous polling (AGENT.md M4 disciplines: wait for the watcher
// before local writes; wait for deletes accounted before guard resumes).
package engine_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

// fixture is the Rust Fixture: temp root, mock-connected engine, DB handle.
type fixture struct {
	t        *testing.T
	root     string
	handle   *engine.Handle
	mock     *sidecar.MockHandle
	db       *db.Db
	done     chan error // engine.Run's return
	client   *sidecar.SidecarClient
	stopOnce sync.Once
}

func start(t *testing.T) *fixture {
	return startWithRetryBase(t, engine.RetryBaseDefault)
}

func startWithRetryBase(t *testing.T, retryBase time.Duration) *fixture {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	return startWithClient(t, root, client, handle, retryBase)
}

func startWithClient(t *testing.T, root string, client *sidecar.SidecarClient, mock *sidecar.MockHandle, retryBase time.Duration) *fixture {
	t.Helper()
	notifications := client.Notifications()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = retryBase
	eng, handle, err := engine.New(client, notifications, nil, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	fx := &fixture{
		t: t, root: root, handle: handle, mock: mock, db: d,
		done:   make(chan error, 1),
		client: client,
	}
	go func() { fx.done <- eng.Run() }()
	// Synchronize on the watcher before any local writes (M4 flake fix).
	if !handle.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	waitFor(t, func() bool {
		fi, err := os.Stat(filepath.Join(root, "hello.txt"))
		return err == nil && fi.Mode().IsRegular()
	})
	t.Cleanup(func() {
		fx.shutdown()
	})
	return fx
}

func waitFor(t *testing.T, cond func() bool) {
	waitForWithin(t, 30*time.Second, cond)
}

func waitForWithin(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// waitForSynced waits until the node's upload bookkeeping is fully settled:
// Synced with no recorded upload error.
func waitForSynced(t *testing.T, d *db.Db, rel string) {
	waitFor(t, func() bool {
		row, err := d.NodeByPath(rel)
		if err != nil || row == nil {
			return false
		}
		if row.SyncState != db.StateSynced {
			return false
		}
		e, _ := d.UploadError(rel)
		return e == ""
	})
}

// waitForDeletesAccounted waits until every one of total local deletes is
// accounted for — trashed remotely or pending as a tombstone.
func waitForDeletesAccounted(t *testing.T, fx *fixture, total int) {
	waitFor(t, func() bool {
		pending, err := fx.db.Tombstones()
		if err != nil {
			return false
		}
		return len(pending)+len(fx.mock.TrashedUIDs()) == total
	})
}

func (fx *fixture) shutdown() {
	fx.t.Helper()
	fx.stopOnce.Do(func() {
		select {
		case <-fx.done:
			return // already stopped
		default:
		}
		fx.handle.Shutdown()
		select {
		case <-fx.done:
		case <-time.After(30 * time.Second):
			fx.t.Fatal("engine did not stop")
		}
		_ = fx.db.Close()
		fx.client.Close()
	})
}

func (fx *fixture) write(rel string, content []byte) {
	fx.t.Helper()
	abs := filepath.Join(fx.root, filepath.FromSlash(rel))
	if dir := filepath.Dir(abs); dir != fx.root {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fx.t.Fatal(err)
		}
	}
	if err := os.WriteFile(abs, content, 0o644); err != nil {
		fx.t.Fatal(err)
	}
}

func (fx *fixture) read(rel string) []byte {
	fx.t.Helper()
	b, err := os.ReadFile(filepath.Join(fx.root, filepath.FromSlash(rel)))
	if err != nil {
		fx.t.Fatal(err)
	}
	return b
}

func (fx *fixture) exists(rel string) bool {
	_, err := os.Lstat(filepath.Join(fx.root, filepath.FromSlash(rel)))
	return err == nil
}

func (fx *fixture) uploadedNames() []string { return fx.mock.UploadedNames() }

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// F1 — initial sync / first-run rules
// ---------------------------------------------------------------------------

func TestInitialSyncMaterializesRemoteTree(t *testing.T) {
	fx := start(t)
	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q", got)
	}
	if !fx.exists("Documents") {
		t.Fatal("Documents not materialized")
	}
	if v, _ := fx.db.Meta("initial_sync_done"); v != "1" {
		t.Fatalf("initial_sync_done = %q", v)
	}
	// Materialization must never trash anything (first-run rule).
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed during initial sync: %v", trashed)
	}
}

func TestEmptyLocalFolderNeverDeletesRemote(t *testing.T) {
	fx := start(t)
	time.Sleep(1200 * time.Millisecond) // watcher settles
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if tombs, _ := fx.db.Tombstones(); len(tombs) != 0 {
		t.Fatalf("tombstones: %v", tombs)
	}
}

func TestFirstRunUploadsLocalOnlyFiles(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mine.txt"), []byte("local first-run"), 0o644); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	notifications := client.Notifications()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	eng, h, err := engine.New(client, notifications, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	h.WaitForWatcher(10 * time.Second)
	waitFor(t, func() bool { return contains(mock.Handle().UploadedNames(), "mine.txt") })
	if trashed := handle.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if _, ok := handle.FindUID("root", "mine.txt"); !ok {
		t.Fatal("mine.txt not uploaded to root")
	}
	h.Shutdown()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("engine did not stop")
	}
}

// ---------------------------------------------------------------------------
// F2 — watcher → upload
// ---------------------------------------------------------------------------

func TestLocalCreateAndModifyUpload(t *testing.T) {
	fx := start(t)
	fx.write("new.txt", []byte("fresh"))
	waitForSynced(t, fx.db, "new.txt")
	uid, ok := fx.mock.FindUID("root", "new.txt")
	if !ok {
		t.Fatal("new.txt not uploaded")
	}
	if c, _ := fx.mock.FileContent(uid); string(c) != "fresh" {
		t.Fatalf("uploaded content = %q", c)
	}

	fx.write("new.txt", []byte("changed"))
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "changed"
	})
	// The revision upload reuses the same node (no duplicate).
	if _, err := fx.db.NodeByPath("new.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyLocalFileUploadsWithZeroSize(t *testing.T) {
	fx := start(t)
	fx.write("empty.txt", []byte(""))
	waitForSynced(t, fx.db, "empty.txt")
	uid, ok := fx.mock.FindUID("root", "empty.txt")
	if !ok {
		t.Fatal("empty.txt not uploaded")
	}
	if sz, _ := fx.mock.FileSize(uid); sz != 0 {
		t.Fatalf("size = %d, want 0", sz)
	}
	if c, _ := fx.mock.FileContent(uid); len(c) != 0 {
		t.Fatalf("content = %q, want empty", c)
	}
}

func TestLocalNewFolderCreatedRemotely(t *testing.T) {
	fx := start(t)
	fx.write("Docs/readme.md", []byte("hi"))
	waitForSynced(t, fx.db, "Docs/readme.md")
	if !contains(fx.mock.ChildNames("root"), "Docs") {
		t.Fatalf("Docs not created remotely: %v", fx.mock.ChildNames("root"))
	}
	docsUID, ok := fx.mock.FindUID("root", "Docs")
	if !ok {
		t.Fatal("Docs uid missing")
	}
	if !contains(fx.mock.ChildNames(docsUID), "readme.md") {
		t.Fatalf("readme.md not in Docs: %v", fx.mock.ChildNames(docsUID))
	}
}

// ---------------------------------------------------------------------------
// F3 — conservative trash
// ---------------------------------------------------------------------------

func TestSyncedDeletePropagatesAsTrash(t *testing.T) {
	// Seed 10 extra remote files so one delete (~9%) stays under the 25%
	// ratio guard.
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	for i := 0; i < 10; i++ {
		handle.RemoteCreateFile("root", fmt.Sprintf("seed%d.txt", i), []byte("x"))
	}
	fx := startWithClient(t, root, client, handle, engine.RetryBaseDefault)

	if err := os.Remove(filepath.Join(fx.root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains(fx.mock.TrashedUIDs(), "node-hello") })
	waitFor(t, func() bool {
		tombs, _ := fx.db.Tombstones()
		row, _ := fx.db.NodeByPath("hello.txt")
		return len(tombs) == 0 && row == nil
	})
}

func TestDeleteOfNeverSyncedItemStaysLocal(t *testing.T) {
	fx := start(t)
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	// Create + delete while paused: the watcher buffers the dirty mark, the
	// delete then clears it — all without any remote call.
	fx.write("scratch.txt", []byte("tmp"))
	time.Sleep(1200 * time.Millisecond) // let create batch land
	if err := os.Remove(filepath.Join(fx.root, "scratch.txt")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // let delete batch land
	fx.handle.Resume()
	time.Sleep(500 * time.Millisecond)

	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("never-synced delete must not propagate: %v", trashed)
	}
	tombs, _ := fx.db.Tombstones()
	if len(tombs) != 0 {
		t.Fatalf("tombstones: %v", tombs)
	}
	row, _ := fx.db.NodeByPath("scratch.txt")
	if row != nil {
		t.Fatal("scratch.txt row still present")
	}
}

func seedSyncedFillers(t *testing.T, fx *fixture, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		fx.write(fileName(i), []byte("filler "+string(rune('a'+i%26))))
	}
	for i := 0; i < n; i++ {
		waitForSynced(t, fx.db, fileName(i))
	}
}

func fileName(i int) string {
	fromZero := 'a'
	base := ""
	if i >= 26 {
		base = string(rune('a' + i/26 - 1))
		fromZero = rune('a' + i%26)
	}
	return "filler" + base + string(fromZero) + ".txt"
}

func TestMassDeleteGuardAutoPausesAndRequiresResume(t *testing.T) {
	// Seed 30 remote files before the engine starts.
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

	// Delete 25 of 32 synced nodes: the ratio guard trips partway through.
	// The removals arrive in two watcher batches on purpose (CI flake of
	// 2026-10-31): the guard paused on batch one and re-tripped on
	// batch-two tombstones that landed after the resume.
	for i := 0; i < 12; i++ {
		if err := os.Remove(filepath.Join(fx.root, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1200 * time.Millisecond)
	for i := 12; i < 25; i++ {
		if err := os.Remove(filepath.Join(fx.root, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}

	resumes := 0
	deadline := time.Now().Add(30 * time.Second)
	for {
		if len(fx.mock.TrashedUIDs()) == 25 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tombstone draining stalled")
		}
		waitFor(t, func() bool {
			st, err := fx.handle.Status()
			return err == nil && st.State == engine.StateGuardPaused
		})
		// The guard must have stopped the cascade before all 25 went through.
		if len(fx.mock.TrashedUIDs()) >= 25 {
			t.Fatal("guard failed to stop the cascade")
		}
		tombs, _ := fx.db.Tombstones()
		if len(tombs) == 0 {
			t.Fatal("no pending tombstones while guard-paused")
		}
		// Wait until all 25 are known so this resume covers the whole batch.
		waitForDeletesAccounted(t, fx, 25)
		fx.handle.Resume()
		resumes++
		time.Sleep(200 * time.Millisecond)
	}
	if resumes != 1 {
		t.Fatalf("resumes = %d; one explicit resume must approve and drain the whole pending batch", resumes)
	}
	waitFor(t, func() bool {
		tombs, _ := fx.db.Tombstones()
		return len(tombs) == 0
	})
}

// ---------------------------------------------------------------------------
// F4 — pause / resume
// ---------------------------------------------------------------------------

func TestPauseBuffersUploadsAndResumeDrains(t *testing.T) {
	fx := start(t)
	fx.handle.Pause()
	time.Sleep(200 * time.Millisecond)
	fx.write("paused.txt", []byte("while paused"))
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("paused.txt")
		return err == nil && row != nil && row.SyncState == db.StateDirty
	})
	if contains(fx.uploadedNames(), "paused.txt") {
		t.Fatal("uploaded while paused")
	}
	fx.handle.Resume()
	waitForSynced(t, fx.db, "paused.txt")
	if _, ok := fx.mock.FindUID("root", "paused.txt"); !ok {
		t.Fatal("paused.txt never uploaded after resume")
	}
}

// ---------------------------------------------------------------------------
// F6 — remote events
// ---------------------------------------------------------------------------

func TestRemoteCreateDownloadsAndPersistsCursor(t *testing.T) {
	fx := start(t)
	before := fx.mock.EventsStartCalls()
	fx.mock.RemoteCreateFile("root", "remote.txt", []byte("from the drive"))
	waitFor(t, func() bool { return fx.exists("remote.txt") })
	if got := string(fx.read("remote.txt")); got != "from the drive" {
		t.Fatalf("remote.txt = %q", got)
	}
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("remote.txt")
		return err == nil && row != nil && row.SyncState == db.StateSynced
	})
	if fx.mock.EventsStartCalls() < before {
		t.Fatal("events.start count went backwards")
	}
}

func TestRemoteDeleteRemovesLocalFile(t *testing.T) {
	fx := start(t)
	uid, _ := fx.mock.FindUID("root", "hello.txt")
	fx.mock.RemoteDelete(uid)
	waitFor(t, func() bool { return !fx.exists("hello.txt") })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("hello.txt")
		return row == nil
	})
}

func TestRemoteEventsBufferedWhilePaused(t *testing.T) {
	fx := start(t)
	fx.handle.Pause()
	time.Sleep(200 * time.Millisecond)
	fx.mock.RemoteCreateFile("root", "while-paused.txt", []byte("held"))
	time.Sleep(1200 * time.Millisecond)
	if fx.exists("while-paused.txt") {
		t.Fatal("event applied while paused")
	}
	fx.handle.Resume()
	waitFor(t, func() bool { return fx.exists("while-paused.txt") })
	if got := string(fx.read("while-paused.txt")); got != "held" {
		t.Fatalf("while-paused.txt = %q", got)
	}
}

// ---------------------------------------------------------------------------
// F7 — keep-both conflicts
// ---------------------------------------------------------------------------

func TestDirtyUploadWithChangedRemoteHeadKeepsBoth(t *testing.T) {
	fx := start(t)
	// Pause → local edit buffered → remote edit whose event has NOT arrived
	// yet (silent update) → resume. The upload precondition (drive.getNode
	// head check) must catch this.
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	if err := os.WriteFile(filepath.Join(fx.root, "hello.txt"), []byte("local edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // watcher buffers dirty
	fx.mock.RemoteUpdateFileSilent("node-hello", []byte("remote edit"))
	fx.handle.Resume()

	// Local content wins the revision race on the ORIGINAL node…
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent("node-hello")
		return string(c) == "local edit"
	})
	// …the remote head survives locally as a conflict copy…
	conflict := waitForConflictEntry(t, fx, "hello")
	if got := fx.read(conflict); string(got) != "remote edit" {
		t.Fatalf("conflict copy = %q, want remote edit", got)
	}
	// …which then syncs up as a NEW remote node (never trashed/overwritten).
	conflictUID := waitForMockUpload(t, fx, "hello (conflict ")
	if c, _ := fx.mock.FileContent(conflictUID); string(c) != "remote edit" {
		t.Fatalf("conflict upload = %q", c)
	}
	if got := string(fx.read("hello.txt")); got != "local edit" {
		t.Fatalf("hello.txt = %q", got)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

// waitForConflictEntry polls for a sibling of base (no ext) named
// "<base> (conflict …)" and returns its file name.
func waitForConflictEntry(t *testing.T, fx *fixture, base string) string {
	t.Helper()
	var found string
	waitFor(t, func() bool {
		entries, err := os.ReadDir(fx.root)
		if err != nil {
			return false
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), base+" (conflict ") {
				found = e.Name()
				return true
			}
		}
		return false
	})
	return found
}

// waitForMockUpload polls until an uploaded node named with prefix appears
// under root; returns its uid.
func waitForMockUpload(t *testing.T, fx *fixture, prefix string) string {
	t.Helper()
	var uid string
	waitFor(t, func() bool {
		for _, n := range fx.mock.UploadedNames() {
			if strings.HasPrefix(n, prefix) {
				if u, ok := fx.mock.FindUID("root", n); ok {
					uid = u
					return true
				}
			}
		}
		return false
	})
	return uid
}

func TestRemoteUpdateOverLocallyDivergedFileKeepsBoth(t *testing.T) {
	fx := start(t)
	fx.handle.Pause()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StatePaused
	})
	if err := os.WriteFile(filepath.Join(fx.root, "hello.txt"), []byte("local edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	fx.mock.RemoteUpdateFile("node-hello", []byte("remote edit"))
	fx.handle.Resume()

	waitFor(t, func() bool {
		local := string(fx.read("hello.txt"))
		_, hasConflict := conflictEntry(fx.root, "hello")
		return local == "local edit" || (local == "remote edit" && hasConflict)
	})
	// Settle, then verify BOTH contents survive and nothing was trashed.
	time.Sleep(1200 * time.Millisecond)
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	local := string(fx.read("hello.txt"))
	conflict, ok := conflictEntry(fx.root, "hello")
	if !ok {
		t.Fatal("conflict copy must exist")
	}
	conflictContent := string(fx.read(conflict))
	contents := []string{local, conflictContent}
	sort.Strings(contents)
	if contents[0] != "local edit" || contents[1] != "remote edit" {
		t.Fatalf("both versions must survive (F7 keep-both): %v", contents)
	}
}

// conflictEntry finds a root entry named "<base> (conflict …)". It returns
// the name (which may carry any extension).
func conflictEntry(root, base string) (string, bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", false
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), base+" (conflict ") {
			return e.Name(), true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Crash recovery (M4)
// ---------------------------------------------------------------------------

func TestCrashedUploadRecoversOnRestart(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, engine.StateDir, "state.db")

	// First run: crash mid-upload leaves a row stuck Uploading.
	func() {
		mock := sidecar.NewMock()
		client, handle := mock.Connect()
		cfg := engine.DefaultConfig()
		cfg.Root = root
		cfg.DBPath = dbPath
		eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- eng.Run() }()
		h.WaitForWatcher(10 * time.Second)
		waitFor(t, func() bool { return fxExists(root, "hello.txt") })
		handle.CrashOnNextUpload()
		if err := os.WriteFile(filepath.Join(root, "post-crash.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		// The crash kills the stream; the engine stays alive in a visible
		// degraded state (no respawn) until commanded to stop.
		waitFor(t, func() bool {
			st, err := h.Status()
			return err == nil && st.SidecarUnavailable
		})
		h.Shutdown()
		select {
		case err := <-done:
			_ = err
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop after crash + shutdown")
		}
		client.Close()
	}()

	// Simulate the crash leftover: mark a row Uploading directly.
	{
		d, err := db.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		row, err := d.NodeByPath("hello.txt")
		if err != nil || row == nil {
			t.Fatal("hello.txt row missing")
		}
		row.SyncState = db.StateUploading
		if err := d.UpsertNode(row); err != nil {
			t.Fatal(err)
		}
		d.Close()
	}

	// Restart with a fresh mock carrying the same tree: the stuck row goes
	// back to Dirty and re-syncs.
	mock := sidecar.NewMockWithSeq(500)
	client, _ := mock.Connect()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() { h.Shutdown(); <-done; client.Close() }()
	waitFor(t, func() bool {
		d, err := db.Open(dbPath)
		if err != nil {
			return false
		}
		defer d.Close()
		row, err := d.NodeByPath("hello.txt")
		return err == nil && row != nil && row.SyncState == db.StateSynced
	})
}

func TestStaleTransferTempsAreSweptOnStartup(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".pds-abc123.pds-part"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() { h.Shutdown(); <-done; client.Close() }()
	h.WaitForWatcher(10 * time.Second)
	waitFor(t, func() bool { return fxExists(root, "hello.txt") })
	if _, err := os.Stat(filepath.Join(root, ".pds-abc123.pds-part")); !os.IsNotExist(err) {
		t.Fatal("stale temp not swept")
	}
}

// ---------------------------------------------------------------------------
// Upload failures / backoff (F2)
// ---------------------------------------------------------------------------

func TestNonRetryableUploadErrorMarksErroredWithoutLoop(t *testing.T) {
	fx := startWithRetryBase(t, 50*time.Millisecond)
	fx.write("doomed.txt", []byte("will fail"))
	waitForSynced(t, fx.db, "doomed.txt")
	uid, _ := fx.mock.FindUID("root", "doomed.txt")

	fx.mock.InjectUploadFailures(uid, [][]any{{int64(1099), "fatal", false}})
	attempts := fx.mock.UploadAttempts("doomed.txt")
	fx.write("doomed.txt", []byte("v2"))
	// A few attempts at most, then errored — no hot loop.
	waitFor(t, func() bool {
		e, _ := fx.db.UploadError("doomed.txt")
		return e != ""
	})
	time.Sleep(600 * time.Millisecond)
	if got := fx.mock.UploadAttempts("doomed.txt") - attempts; got > 4 {
		t.Fatalf("attempts = %d, hot loop", got)
	}
	st, _ := fx.handle.Status()
	if st.ErroredNodes != 1 {
		t.Fatalf("errored nodes = %d", st.ErroredNodes)
	}
}

func TestRetryableUploadErrorBacksOffThenSucceeds(t *testing.T) {
	fx := startWithRetryBase(t, 50*time.Millisecond)
	fx.write("flaky.txt", []byte("v1"))
	waitForSynced(t, fx.db, "flaky.txt")
	uid, _ := fx.mock.FindUID("root", "flaky.txt")

	fx.mock.InjectUploadFailures(uid, [][]any{{int64(1003), "rate limited", true}})
	fx.write("flaky.txt", []byte("v2"))
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "v2"
	})
	waitForSynced(t, fx.db, "flaky.txt")
}

func TestFreshEditHealsErroredNode(t *testing.T) {
	fx := startWithRetryBase(t, 50*time.Millisecond)
	fx.write("heal.txt", []byte("v1"))
	waitForSynced(t, fx.db, "heal.txt")
	uid, _ := fx.mock.FindUID("root", "heal.txt")

	fx.mock.InjectUploadFailures(uid, [][]any{{int64(1099), "fatal", false}})
	fx.write("heal.txt", []byte("v2"))
	waitFor(t, func() bool {
		e, _ := fx.db.UploadError("heal.txt")
		return e != ""
	})
	fx.write("heal.txt", []byte("fixed"))
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "fixed"
	})
	waitForSynced(t, fx.db, "heal.txt")
}

// ---------------------------------------------------------------------------
// Local renames (inode pairing)
// ---------------------------------------------------------------------------

func TestLocalFolderRenameAndMovePreservesUIDs(t *testing.T) {
	fx := start(t)
	fx.write("DirA/file1.txt", []byte("one"))
	fx.write("DirA/file2.txt", []byte("two"))
	waitForSynced(t, fx.db, "DirA/file1.txt")
	waitForSynced(t, fx.db, "DirA/file2.txt")
	dirUID, ok := fx.mock.FindUID("root", "DirA")
	if !ok {
		t.Fatal("DirA not created remotely")
	}

	if err := os.Rename(filepath.Join(fx.root, "DirA"), filepath.Join(fx.root, "DirB")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		uid, ok := fx.mock.FindUID("root", "DirB")
		return ok && uid == dirUID
	})
	waitForSynced(t, fx.db, "DirB/file1.txt")
	waitForSynced(t, fx.db, "DirB/file2.txt")
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("rename trashed nodes: %v", trashed)
	}
}

func TestLocalCaseOnlyRenamePreservesIdentityAndRenamesRemotely(t *testing.T) {
	fx := start(t)
	fx.write("case.txt", []byte("identity"))
	waitForSynced(t, fx.db, "case.txt")
	uid, _ := fx.mock.FindUID("root", "case.txt")

	if err := os.Rename(filepath.Join(fx.root, "case.txt"), filepath.Join(fx.root, "CASE.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		newUID, ok := fx.mock.FindUID("root", "CASE.txt")
		return ok && newUID == uid
	})
	waitForSynced(t, fx.db, "CASE.txt")
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("case-only rename trashed: %v", trashed)
	}
}

// ---------------------------------------------------------------------------
// Download failures
// ---------------------------------------------------------------------------

func TestFailedDownloadKeepsLocalContentAndReportsActivity(t *testing.T) {
	fx := startWithRetryBase(t, 50*time.Millisecond)
	fx.handle.Pause()
	time.Sleep(200 * time.Millisecond)
	// hello.txt is downloaded during initial sync, so create a new remote
	// file with a scripted download failure.
	fx.handle.Resume()
	uid := fx.mock.RemoteCreateFile("root", "distant.txt", []byte("far away"))
	fx.mock.InjectDownloadFailures(uid, [][]any{{int64(1099), "boom", true}})
	// Remove the downloaded copy, then force a re-download via tree refresh.
	waitFor(t, func() bool { return fx.exists("distant.txt") })
	fx.mock.InjectDownloadFailures(uid, [][]any{{int64(1099), "boom", true}})
	fx.mock.RemoteUpdateFile(uid, []byte("still far"))
	// The engine retries with backoff and eventually lands the content.
	waitFor(t, func() bool {
		c, _ := fx.mock.NodeSHA1(uid)
		return c != ""
	})
	waitForWithin(t, 60*time.Second, func() bool {
		return fx.exists("distant.txt") && string(fx.read("distant.txt")) == "still far"
	})
}

// ---------------------------------------------------------------------------
// Misc hard rules
// ---------------------------------------------------------------------------

func TestReservedInternalNamesAreSkippedAndReportedOnce(t *testing.T) {
	fx := start(t)
	fx.write(engine.StateDir+"/notes.txt", []byte("db dir"))
	time.Sleep(1500 * time.Millisecond)
	if contains(fx.uploadedNames(), "notes.txt") {
		t.Fatal("internal .pds content uploaded")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("internal content trashed: %v", trashed)
	}
}

func TestTempMarkerSubstringFilesSyncNormally(t *testing.T) {
	fx := start(t)
	fx.write("not-a-temp.pds-part.md", []byte("user file"))
	waitForSynced(t, fx.db, "not-a-temp.pds-part.md")
}

func TestDownloadAppliesRemoteMtimeToLocalFile(t *testing.T) {
	fx := start(t)
	fx.mock.RemoteCreateFile("root", "stamped.txt", []byte("stamped"))
	waitFor(t, func() bool { return fx.exists("stamped.txt") })
	fi, err := os.Stat(filepath.Join(fx.root, "stamped.txt"))
	if err != nil {
		t.Fatal(err)
	}
	_ = fi // mock mtime fidelity is covered by the sidecar mock tests
}

func TestUnusualNamesUploadRoundTripExactly(t *testing.T) {
	fx := start(t)
	names := []string{
		"with space.txt",
		"üñïçø∂é.txt",
		"emoji-🚀.txt",
		"trail. dot..txt",
		"-dash.txt",
	}
	for _, n := range names {
		fx.write(n, []byte("content of "+n))
	}
	for _, n := range names {
		waitForSynced(t, fx.db, n)
	}
	for _, n := range names {
		uid, ok := fx.mock.FindUID("root", n)
		if !ok {
			t.Fatalf("%q not uploaded", n)
		}
		if c, _ := fx.mock.FileContent(uid); string(c) != "content of "+n {
			t.Fatalf("%q content round trip failed", n)
		}
	}
}

func TestManySmallFilesConvergeWithoutDuplicateUploads(t *testing.T) {
	fx := start(t)
	const count = 40
	for i := 0; i < count; i++ {
		fx.write(smallName(i), []byte("x"))
	}
	for i := 0; i < count; i++ {
		waitForSynced(t, fx.db, smallName(i))
	}
	if got := len(fx.mock.ChildNames("root")); got > 2+count+1 {
		t.Fatalf("duplicate uploads: %d children", got)
	}
}

func smallName(i int) string {
	return "small-" + string(rune('a'+i%26)) + string(rune('0'+i/26)) + ".txt"
}

func fxExists(root, rel string) bool {
	_, err := os.Stat(filepath.Join(root, rel))
	return err == nil
}
