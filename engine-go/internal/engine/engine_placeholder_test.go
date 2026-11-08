// Placeholder-mode engine tests (SPEC F5, Phase 2 step 5): no
// materialization, VFS snapshot publication, hydrate round trip, LRU
// eviction, pins, and write-through uploads. Ported from
// crates/core/tests/placeholders.rs (backing-dir adoption / mode
// conversion excluded: convert_layout is owned outside this workstream;
// large-sparse excludes: the Go mock has no sparse-file API; real-kernel
// FUSE e2e excludes: it needs a mount, which the VFS workstream owns).
package engine_test

import (
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
	storepkg "pds.dev/engine-go/internal/store"
)

// phFixture is the placeholder counterpart of fixture: the engine runs
// with cfg.Placeholders and a real *store.Store the test drives
// directly (BeginHydrate / Record* / Snapshot / BackingPath). The store
// instance is shared with the engine (engine.New keeps the caller's).
type phFixture struct {
	t        *testing.T
	root     string
	backing  string
	handle   *engine.Handle
	mock     *sidecar.MockHandle
	db       *db.Db
	dbPath   string
	store    *storepkg.Store
	done     chan error
	client   *sidecar.SidecarClient
	stopOnce sync.Once
}

func phCap(v uint64) *uint64 { return &v }

func startPlaceholder(t *testing.T) *phFixture {
	t.Helper()
	return startPlaceholderWithCap(t, nil, engine.RetryBaseDefault)
}

func startPlaceholderWithCap(t *testing.T, capVal *uint64, retryBase time.Duration) *phFixture {
	t.Helper()
	// Resolve placeholder mode deterministically in this test process.
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	notifications := client.Notifications()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = retryBase
	cfg.Placeholders = true
	cfg.EvictionCap = capVal
	cfg.WatchLocalWrites = true
	st := storepkg.New()
	eng, eh, err := engine.New(client, notifications, st, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	fx := &phFixture{
		t: t, root: root, backing: filepath.Join(root, engine.VFSBackingDir),
		handle: eh, mock: handle, db: d, dbPath: dbPath, store: st,
		done: make(chan error, 1), client: client,
	}
	go func() { fx.done <- eng.Run() }()
	if !eh.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	t.Cleanup(func() { fx.shutdown() })
	return fx
}

func (fx *phFixture) shutdown() {
	fx.t.Helper()
	fx.stopOnce.Do(func() {
		select {
		case <-fx.done:
			return
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

func (fx *phFixture) snapshotHas(rel string) bool {
	_, ok := fx.store.Snapshot().ByPath[rel]
	return ok
}

func (fx *phFixture) ready(rel string) bool {
	n, ok := fx.store.Snapshot().ByPath[rel]
	return ok && n.ContentReady
}

func (fx *phFixture) backingPath(rel string) string {
	p, _ := fx.store.BackingPath(rel)
	return p
}

func (fx *phFixture) backingBytes(rel string) []byte {
	p := fx.backingPath(rel)
	if p == "" {
		return nil
	}
	b, _ := os.ReadFile(p)
	return b
}

func (fx *phFixture) backingAbs(rel string) string {
	return filepath.Join(fx.backing, filepath.FromSlash(rel))
}

func waitForSnapshot(t *testing.T, fx *phFixture, rel string) {
	t.Helper()
	waitFor(t, func() bool { return fx.snapshotHas(rel) })
}

func waitForReady(t *testing.T, fx *phFixture, rel string) {
	t.Helper()
	waitFor(t, func() bool { return fx.ready(rel) })
}

func waitForBackingFile(t *testing.T, fx *phFixture, rel string) {
	t.Helper()
	waitFor(t, func() bool { return fx.backingPath(rel) != "" })
}

func phWaitState(t *testing.T, fx *phFixture, state engine.EngineState) {
	t.Helper()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == state
	})
}

// phWaitSettled waits until the store queues and the tombstone drain are
// empty (the Rust overwrite test's settle barrier).
func phWaitSettled(t *testing.T, fx *phFixture) {
	t.Helper()
	waitFor(t, func() bool {
		d, r, w := fx.store.PendingCounts()
		if d != 0 || r != 0 || w != 0 {
			return false
		}
		st, err := fx.handle.Status()
		if err != nil {
			return false
		}
		return st.QueuedTombstones == 0
	})
}

// TestExternalBackingAdoptsPreviouslyMaterializedContent: the desktop
// layout keeps backing outside the root (the app mounts the placeholder
// view at the root itself). A root that was previously mirrored has real
// files in it; adopting it into placeholder mode must move them into the
// backing cache (no re-download) and mark them content-ready.
func TestExternalBackingAdoptsPreviouslyMaterializedContent(t *testing.T) {
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	stateDir := filepath.Join(dir, "state")
	backing := filepath.Join(stateDir, "backing")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello drive"), 0o644); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(stateDir, "state.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rootUID := "root"
	mtime := int64(1_700_000_000)
	size := int64(11)
	if err := d.UpsertNode(&db.NodeRow{
		LocalPath: "hello.txt",
		NodeUID:   "node-hello",
		ParentUID: &rootUID,
		Name:      "hello.txt",
		Kind:      db.KindFile,
		Size:      &size,
		Mtime:     &mtime,
		SyncState: db.StateSynced,
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	notifications := client.Notifications()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = engine.RetryBaseDefault
	cfg.Placeholders = true
	cfg.BackingDir = backing
	cfg.WatchLocalWrites = false
	st := storepkg.New()
	eng, eh, err := engine.New(client, notifications, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() {
		eh.Shutdown()
		<-done
		client.Close()
	}()
	if !eh.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	// The previously materialized file moved into the backing cache, so
	// the snapshot must show it hydrated from the start — no download.
	waitFor(t, func() bool {
		n, ok := st.Snapshot().ByPath["hello.txt"]
		return ok && n.ContentReady
	})
	if _, err := os.Stat(filepath.Join(root, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("adoption leaves nothing materialized in the root")
	}
	b, err := os.ReadFile(filepath.Join(backing, "hello.txt"))
	if err != nil || string(b) != "hello drive" {
		t.Fatalf("backing = %q, err %v", b, err)
	}
}

// ---------------------------------------------------------------------------
// No materialization / snapshot publication
// ---------------------------------------------------------------------------

func TestPlaceholdersSkipMaterializationAndPublishSnapshot(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if _, err := os.Stat(filepath.Join(fx.root, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("placeholder mode must not materialize files")
	}
	snap := fx.store.Snapshot()
	hello, ok := snap.ByPath["hello.txt"]
	if !ok {
		t.Fatal("snapshot missing hello.txt")
	}
	if hello.IsDir {
		t.Fatal("hello must be a file")
	}
	if hello.ContentReady {
		t.Fatal("hello must not be hydrated yet")
	}
	if hello.Size == 0 {
		t.Fatal("placeholder must report remote size")
	}
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q", st.State)
	}
	if st.CachePath == "" {
		t.Fatal("cache path must be surfaced")
	}
}

func TestVFSnapshotReflectsRemoteEvents(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	fx.mock.RemoteCreateFile("root", "later.txt", []byte("later"))
	waitForSnapshot(t, fx, "later.txt")
	if _, err := os.Stat(filepath.Join(fx.root, "later.txt")); !os.IsNotExist(err) {
		t.Fatal("placeholder must not materialize later.txt")
	}
	if fx.ready("later.txt") {
		t.Fatal("later.txt must not be ready")
	}
}

// ---------------------------------------------------------------------------
// Hydration
// ---------------------------------------------------------------------------

func TestHydrateRequestDownloadsToBacking(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("first BeginHydrate must queue")
	}
	waitForBackingFile(t, fx, "hello.txt")
	abs := fx.backingPath("hello.txt")
	b, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello drive" {
		t.Fatalf("backing content = %q", b)
	}
	waitForReady(t, fx, "hello.txt")
	if _, err := os.Stat(filepath.Join(fx.root, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("visible root must stay clean")
	}
	row, err := fx.db.NodeByPath("hello.txt")
	if err != nil || row == nil || row.SyncState != db.StateSynced {
		t.Fatalf("row = %+v", row)
	}
}

func TestEmptyRemoteFilePlaceholderHydratesAndRehydrates(t *testing.T) {
	fx := startPlaceholder(t)
	uid := fx.mock.RemoteCreateFile("root", "empty.txt", []byte(""))
	waitForSnapshot(t, fx, "empty.txt")
	snap := fx.store.Snapshot()
	node := snap.ByPath["empty.txt"]
	if node.IsDir || node.ContentReady || node.Size != 0 {
		t.Fatalf("empty placeholder = %+v, want file size 0 not-ready", node)
	}
	if !fx.store.BeginHydrate("empty.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitForReady(t, fx, "empty.txt")
	waitFor(t, func() bool { return fx.store.HydratingCount() == 0 })
	b, err := os.ReadFile(fx.backingPath("empty.txt"))
	if err != nil || len(b) != 0 {
		t.Fatalf("backing = %q, err %v", b, err)
	}
	// Evict: hydrate hello so bytes are over the cap, then cap 0.
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate hello must queue")
	}
	waitFor(t, func() bool {
		return fx.backingPath("hello.txt") != ""
	})
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !fx.ready("empty.txt") })
	if _, err := os.Stat(fx.backingAbs("empty.txt")); !os.IsNotExist(err) {
		t.Fatal("evicted backing must be gone")
	}
	// Rehydrate after eviction.
	if err := fx.handle.SetEvictionCap(nil); err != nil {
		t.Fatal(err)
	}
	if !fx.store.BeginHydrate("empty.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitForReady(t, fx, "empty.txt")
	waitFor(t, func() bool { return fx.store.HydratingCount() == 0 })
	n := 0
	for _, u := range fx.mock.DownloadedUIDs() {
		if u == uid {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("downloads of empty.txt = %d, want 2 (one per hydration)", n)
	}
}

func TestStatusReportsBackingCacheUsage(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.CacheUsed != 0 {
		t.Fatalf("cache_used before hydration = %d, want 0", st.CacheUsed)
	}
	if st.CachePath == "" {
		t.Fatal("cache path must be surfaced")
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitForReady(t, fx, "hello.txt")
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.CacheUsed == 11 // "hello drive"
	})
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.CacheUsed == 0
	})
}

// A cache-cap change mid-hydration applies on the next pass (the in-flight
// hydration is not re-issued) and leaves the placeholder intact.
func TestCacheCapChangeDuringHydrationEvictsNextPassWithoutLoss(t *testing.T) {
	fx := startPlaceholderWithCap(t, nil, engine.RetryBaseDefault)
	waitForSnapshot(t, fx, "hello.txt")
	fx.mock.SetDownloadDelay(400 * time.Millisecond)
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return contains(fx.mock.DownloadedUIDs(), "node-hello") })
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.CacheUsed == 0
	})
	n := 0
	for _, u := range fx.mock.DownloadedUIDs() {
		if u == "node-hello" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("in-flight hydration re-issued: %d downloads", n)
	}
	if !fx.snapshotHas("hello.txt") {
		t.Fatal("eviction must leave the placeholder")
	}
	if fx.ready("hello.txt") {
		t.Fatal("hello must be a placeholder after eviction")
	}
	fx.mock.SetDownloadDelay(0)
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitForReady(t, fx, "hello.txt")
	if got := string(fx.backingBytes("hello.txt")); got != "hello drive" {
		t.Fatalf("rehydrated = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Pins + eviction
// ---------------------------------------------------------------------------

func TestPinDownloadsAndUnpinAllowsEviction(t *testing.T) {
	fx := startPlaceholderWithCap(t, phCap(1), engine.RetryBaseDefault)
	waitForSnapshot(t, fx, "hello.txt")
	if err := fx.handle.Pin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		p := fx.backingPath("hello.txt")
		return p != "" && fx.ready("hello.txt")
	})
	if err := fx.handle.Unpin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	fx.mock.RemoteCreateFile("root", "b.txt", []byte("bbbb"))
	waitForSnapshot(t, fx, "b.txt")
	if !fx.store.BeginHydrate("b.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("b.txt") != "" })
	waitFor(t, func() bool { return fx.backingPath("hello.txt") == "" })
	waitFor(t, func() bool { return !fx.ready("hello.txt") })
}

func TestPinnedFileSurvivesEviction(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	handle.RemoteCreateFile("root", "a.txt", []byte("aaa"))
	handle.RemoteCreateFile("root", "b.txt", []byte("bbb"))
	notifications := client.Notifications()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.Placeholders = true
	cfg.EvictionCap = phCap(1)
	cfg.WatchLocalWrites = true
	st := storepkg.New()
	eng, eh, err := engine.New(client, notifications, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() {
		eh.Shutdown()
		<-done
		client.Close()
	}()
	if !eh.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	waitFor(t, func() bool {
		_, ok := st.Snapshot().ByPath["a.txt"]
		return ok
	})
	if err := eh.Pin("a.txt"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		p, _ := st.BackingPath("a.txt")
		return p != "" && st.Snapshot().ByPath["a.txt"].ContentReady
	})
	st.BeginHydrate("b.txt")
	waitFor(t, func() bool {
		p, _ := st.BackingPath("b.txt")
		return p != ""
	})
	time.Sleep(300 * time.Millisecond)
	p, _ := st.BackingPath("a.txt")
	if p == "" {
		t.Fatal("pinned file must survive the eviction pass")
	}
}

func TestSetCacheCapAppliesLiveWithImmediateEviction(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if err := fx.handle.Pin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	if err := fx.handle.Unpin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	if err := fx.handle.SetEvictionCap(phCap(1)); err != nil {
		t.Fatal(err)
	}
	st, err := fx.handle.Status()
	if err != nil || st.CacheCap == nil || *st.CacheCap != 1 {
		t.Fatalf("cache_cap = %+v", st)
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") == "" })
	waitFor(t, func() bool { return !fx.ready("hello.txt") })
	if err := fx.handle.SetEvictionCap(nil); err != nil {
		t.Fatal(err)
	}
	st, err = fx.handle.Status()
	if err != nil || st.CacheCap != nil {
		t.Fatalf("cache_cap = %+v", st)
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	time.Sleep(300 * time.Millisecond)
	if fx.backingPath("hello.txt") == "" {
		t.Fatal("unlimited cap must not evict")
	}
}

func TestOpenHandleSurvivesEvictionAndEvictsAfterRelease(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	fx.store.AddOpenHandle("hello.txt")
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return string(fx.backingBytes("hello.txt")) == "hello drive" })
	if err := fx.handle.SetEvictionCap(phCap(1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State != engine.StateRunning {
		t.Fatalf("state = %q", st.State)
	}
	if st.CacheCap == nil || *st.CacheCap != 1 {
		t.Fatalf("cache_cap = %+v", st)
	}
	if st.CacheUsed <= 1 {
		t.Fatalf("cache_used = %d, want over cap while open", st.CacheUsed)
	}
	if fx.backingPath("hello.txt") == "" {
		t.Fatal("open file must stay hydrated even over the cap")
	}
	fx.store.RemoveOpenHandle("hello.txt")
	if err := fx.handle.SetEvictionCap(phCap(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") == "" })
	waitFor(t, func() bool { return !fx.ready("hello.txt") })
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("eviction must not trash: %v", fx.mock.TrashedUIDs())
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitFor(t, func() bool { return string(fx.backingBytes("hello.txt")) == "hello drive" })
}

func TestEvictedPlaceholderRehydratesOnRequest(t *testing.T) {
	fx := startPlaceholderWithCap(t, phCap(1), engine.RetryBaseDefault)
	waitForSnapshot(t, fx, "hello.txt")
	if err := fx.handle.Pin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	if err := fx.handle.Unpin("hello.txt"); err != nil {
		t.Fatal(err)
	}
	fx.mock.RemoteCreateFile("root", "c.txt", []byte("cccc"))
	waitForSnapshot(t, fx, "c.txt")
	if !fx.store.BeginHydrate("c.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") == "" })
	time.Sleep(1200 * time.Millisecond)
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("eviction must not trash: %v", fx.mock.TrashedUIDs())
	}
	if !fx.snapshotHas("hello.txt") {
		t.Fatal("evicted file must remain a placeholder")
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitFor(t, func() bool {
		p := fx.backingPath("hello.txt")
		return p != "" && string(fx.backingBytes("hello.txt")) == "hello drive"
	})
}

// ---------------------------------------------------------------------------
// Write-through: mkdir / delete / rename / write
// ---------------------------------------------------------------------------

func TestBareMkdirAndDeleteFromVFS(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	fx.store.RecordMkdir("NewFolder")
	fx.store.RecordMkdir("NewFolder/inner")
	waitFor(t, func() bool { return contains(fx.mock.ChildNames("root"), "NewFolder") })
	outerUID, ok := fx.mock.FindUID("root", "NewFolder")
	if !ok {
		t.Fatal("NewFolder uid missing")
	}
	waitFor(t, func() bool { return contains(fx.mock.ChildNames(outerUID), "inner") })
	waitFor(t, func() bool {
		n, ok := fx.store.Snapshot().ByPath["NewFolder"]
		return ok && n.IsDir
	})
	waitFor(t, func() bool {
		n, ok := fx.store.Snapshot().ByPath["NewFolder/inner"]
		return ok && n.IsDir
	})
	fx.store.RecordDelete("NewFolder/inner")
	waitFor(t, func() bool { return !fx.snapshotHas("NewFolder/inner") })
	waitFor(t, func() bool { return len(fx.mock.ChildNames(outerUID)) == 0 })
	fx.store.RecordDelete("NewFolder")
	// Tiny tree: the guard trips; resume until the row leaves the snapshot.
	deadline := time.Now().Add(30 * time.Second)
	for fx.snapshotHas("NewFolder") {
		if time.Now().After(deadline) {
			t.Fatal("NewFolder never left the snapshot")
		}
		fx.handle.Resume()
		time.Sleep(50 * time.Millisecond)
	}
	waitFor(t, func() bool { return !contains(fx.mock.ChildNames("root"), "NewFolder") })
	if !contains(fx.mock.TrashedUIDs(), outerUID) {
		t.Fatalf("NewFolder (%s) must be trashed remotely, got %v", outerUID, fx.mock.TrashedUIDs())
	}
}

func TestVFSRenameRekeysAndRenamesRemotely(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	fx.store.RecordRename("hello.txt", "hi.txt")
	waitForSnapshot(t, fx, "hi.txt")
	waitFor(t, func() bool { return contains(fx.mock.ChildNames("root"), "hi.txt") })
	if fx.snapshotHas("hello.txt") {
		t.Fatal("old path must be gone from the snapshot")
	}
	if contains(fx.mock.TrashedUIDs(), "node-hello") {
		t.Fatalf("rename must never trash: %v", fx.mock.TrashedUIDs())
	}
	renames := fx.mock.Renames()
	seen := false
	for _, r := range renames {
		if r[0] == "node-hello" && r[1] == "hi.txt" {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("expected remote rename of node-hello: %v", renames)
	}
	fx.store.RecordRename("hi.txt", "Documents/moved.txt")
	waitForSnapshot(t, fx, "Documents/moved.txt")
	docsUID, ok := fx.mock.FindUID("root", "Documents")
	if !ok {
		t.Fatal("Documents uid missing")
	}
	waitFor(t, func() bool { return contains(fx.mock.ChildNames(docsUID), "moved.txt") })
	if fx.snapshotHas("hi.txt") {
		t.Fatal("hi.txt must be gone")
	}
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("no trashes at all expected: %v", fx.mock.TrashedUIDs())
	}
	before := len(fx.mock.Renames())
	fx.store.RecordRename("ghost.txt", "phantom.txt")
	time.Sleep(300 * time.Millisecond)
	if n := len(fx.mock.Renames()); n != before {
		t.Fatalf("ghost rename must be a no-op: %d != %d", n, before)
	}
}

func TestVFSWriteThroughUploadsNewFileAndRevision(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if err := os.MkdirAll(fx.backing, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fx.backingAbs("new.txt"), []byte("fresh bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	fx.store.RecordWrite("new.txt")
	waitForSnapshot(t, fx, "new.txt")
	newUID, ok := fx.mock.FindUID("root", "new.txt")
	if !ok {
		t.Fatal("new.txt not uploaded")
	}
	if c, _ := fx.mock.FileContent(newUID); string(c) != "fresh bytes" {
		t.Fatalf("uploaded = %q", c)
	}
	waitForSynced(t, fx.db, "new.txt")
	// Revision write: same node, new bytes.
	if err := os.WriteFile(fx.backingAbs("new.txt"), []byte("changed bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	fx.store.RecordWrite("new.txt")
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(newUID)
		return string(c) == "changed bytes"
	})
	waitForSynced(t, fx.db, "new.txt")
	if got, _ := fx.mock.FindUID("root", "new.txt"); got != newUID {
		t.Fatalf("revision must reuse the node: %q != %q", got, newUID)
	}
	n := 0
	for _, name := range fx.mock.ChildNames("root") {
		if name == "new.txt" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("exactly one remote new.txt: %v", fx.mock.ChildNames("root"))
	}
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("trashed: %v", fx.mock.TrashedUIDs())
	}
}

func TestOverwriteRenameDoesNotLoseDestination(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	// Extra siblings keep the mass-deletion guard's ratio limb out of this
	// test (1 delete of 3 synced trips it).
	for i := 0; i < 10; i++ {
		fx.mock.RemoteCreateFile("root", fmt.Sprintf("fill%02d.bin", i), []byte("filler"))
	}
	modelUID := fx.mock.RemoteCreateFile("root", "model.plasticity", []byte("v1 content"))
	waitForSnapshot(t, fx, "fill09.bin")
	waitForSnapshot(t, fx, "model.plasticity")
	fx.store.BeginHydrate("model.plasticity")
	waitFor(t, func() bool { return fx.backingPath("model.plasticity") != "" })

	tmp := "model.plasticity.1793648988585.3.tmp"
	if err := os.WriteFile(fx.backingAbs(tmp), []byte("v2 content"), 0o644); err != nil {
		t.Fatal(err)
	}
	fx.store.RecordWrite(tmp)
	destAbs := fx.backingAbs("model.plasticity")
	if err := os.Rename(fx.backingAbs(tmp), destAbs); err != nil {
		t.Fatal(err)
	}
	fx.store.RecordOverwrite("model.plasticity", tmp, "model.plasticity")
	phWaitSettled(t, fx)
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(modelUID)
		return string(c) == "v2 content"
	})
	for _, n := range fx.mock.UploadedNames() {
		if strings.Contains(n, ".tmp") {
			t.Fatalf("temp file must not upload as its own node: %v", fx.mock.UploadedNames())
		}
	}
	count := 0
	for _, n := range fx.mock.ChildNames("root") {
		if n == "model.plasticity" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("exactly one model.plasticity remotely: %v", fx.mock.ChildNames("root"))
	}
	if contains(fx.mock.TrashedUIDs(), modelUID) {
		t.Fatalf("renamed-onto node must never be trashed: %v", fx.mock.TrashedUIDs())
	}
	waitForSnapshot(t, fx, "model.plasticity")
}

func TestPlaceholderFolderDeleteTrashesOnceAndLeavesSnapshot(t *testing.T) {
	fx := startPlaceholder(t)
	for i := 0; i < 20; i++ {
		fx.mock.RemoteCreateFile("root", fmt.Sprintf("filler%02d.txt", i), []byte("filler"))
	}
	vault := fx.mock.RemoteCreateFolder("root", "Vault")
	a := fx.mock.RemoteCreateFile(vault, "a.txt", []byte("a bytes"))
	b := fx.mock.RemoteCreateFile(vault, "b.txt", []byte("b bytes"))
	sub := fx.mock.RemoteCreateFolder(vault, "sub")
	c := fx.mock.RemoteCreateFile(sub, "c.txt", []byte("c bytes"))
	waitForSnapshot(t, fx, "Vault/sub/c.txt")
	fx.handle.Pause()
	phWaitState(t, fx, engine.StatePaused)
	for _, rel := range []string{"Vault/a.txt", "Vault/b.txt", "Vault/sub/c.txt", "Vault/sub", "Vault"} {
		fx.store.RecordDelete(rel)
	}
	waitFor(t, func() bool {
		tombs, err := fx.db.Tombstones()
		return err == nil && len(tombs) == 5
	})
	fx.handle.Resume()
	waitFor(t, func() bool {
		for rel := range fx.store.Snapshot().ByPath {
			if strings.HasPrefix(rel, "Vault") {
				return false
			}
		}
		return true
	})
	waitFor(t, func() bool { return !contains(fx.mock.ChildNames("root"), "Vault") })
	trashed := fx.mock.TrashedUIDs()
	for _, uid := range []string{vault, a, b, sub, c} {
		n := 0
		for _, u := range trashed {
			if u == uid {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("uid %s trashed %d times: %v", uid, n, trashed)
		}
	}
}

func TestPlaceholderMassDeleteGuardTripsAndOneResumeDrains(t *testing.T) {
	fx := startPlaceholder(t)
	for i := 0; i < 40; i++ {
		fx.mock.RemoteCreateFile("root", fmt.Sprintf("f%02d.txt", i), []byte("filler"))
	}
	waitForSnapshot(t, fx, "f39.txt")
	synced, err := fx.db.CountSynced()
	if err != nil || synced < 42 {
		t.Fatalf("synced = %d (%v), want >=42", synced, err)
	}
	fx.handle.Pause()
	phWaitState(t, fx, engine.StatePaused)
	for i := 0; i < 15; i++ {
		fx.store.RecordDelete(fmt.Sprintf("f%02d.txt", i))
	}
	waitFor(t, func() bool {
		tombs, err := fx.db.Tombstones()
		return err == nil && len(tombs) == 15
	})
	fx.handle.Resume()
	waitFor(t, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StateGuardPaused
	})
	trashedAtTrip := len(fx.mock.TrashedUIDs())
	if trashedAtTrip >= 15 {
		t.Fatalf("guard must stop the wave: %d already trashed", trashedAtTrip)
	}
	tombs, err := fx.db.Tombstones()
	if err != nil {
		t.Fatal(err)
	}
	if len(tombs) == 0 {
		t.Fatal("pending must be > 0 while guard-paused")
	}
	if trashedAtTrip+len(tombs) != 15 {
		t.Fatalf("accounted = %d+%d, want 15", trashedAtTrip, len(tombs))
	}
	fx.handle.Resume()
	waitFor(t, func() bool { return len(fx.mock.TrashedUIDs()) == 15 })
	waitFor(t, func() bool {
		tombs, err := fx.db.Tombstones()
		return err == nil && len(tombs) == 0
	})
	st, err := fx.handle.Status()
	if err != nil || st.State != engine.StateRunning {
		t.Fatalf("state = %+v", st)
	}
	snap := fx.store.Snapshot()
	for i := 0; i < 15; i++ {
		p := fmt.Sprintf("f%02d.txt", i)
		if _, ok := snap.ByPath[p]; ok {
			t.Fatalf("%s must be gone", p)
		}
	}
	for i := 15; i < 40; i++ {
		p := fmt.Sprintf("f%02d.txt", i)
		if _, ok := snap.ByPath[p]; !ok {
			t.Fatalf("%s must remain", p)
		}
	}
}

// ---------------------------------------------------------------------------
// Remote renames / trash round trips through the snapshot
// ---------------------------------------------------------------------------

func TestRemoteTrashThenRestoreRoundTripsPlaceholders(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	cursorBefore, _ := fx.db.EventCursor("scope-1")
	fx.mock.RemoteTrash("node-hello")
	waitFor(t, func() bool { return !fx.snapshotHas("hello.txt") })
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("hello.txt")
		return row == nil
	})
	cursorTrashed, _ := fx.db.EventCursor("scope-1")
	if cursorTrashed == "" || cursorTrashed == cursorBefore {
		t.Fatalf("trash must advance the cursor (%q -> %q)", cursorBefore, cursorTrashed)
	}
	fx.mock.RemoteRestore("node-hello")
	waitFor(t, func() bool {
		n, ok := fx.store.Snapshot().ByPath["hello.txt"]
		return ok && !n.ContentReady
	})
	row, err := fx.db.NodeByPath("hello.txt")
	if err != nil || row == nil || row.NodeUID != "node-hello" {
		t.Fatalf("row = %+v", row)
	}
	cursorRestored, _ := fx.db.EventCursor("scope-1")
	if cursorRestored == cursorTrashed {
		t.Fatalf("restore must advance the cursor (%q -> %q)", cursorTrashed, cursorRestored)
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return string(fx.backingBytes("hello.txt")) == "hello drive" })
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("remote trash must never call back: %v", fx.mock.TrashedUIDs())
	}
}

func TestRemoteFolderRenameMovesHydratedBackingSubtree(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	fx.mock.RemoteCreateFile("node-docs", "nested.txt", []byte("nested bytes"))
	waitForSnapshot(t, fx, "Documents/nested.txt")
	if !fx.store.BeginHydrate("Documents/nested.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitForReady(t, fx, "Documents/nested.txt")
	downloads := len(fx.mock.DownloadedUIDs())
	fx.mock.RemoteRename("node-docs", "Docs")
	waitFor(t, func() bool {
		snap := fx.store.Snapshot()
		n, ok := snap.ByPath["Docs/nested.txt"]
		return ok && n.ContentReady && !func() bool {
			_, present := snap.ByPath["Documents/nested.txt"]
			return present
		}()
	})
	if got := string(fx.backingBytes("Docs/nested.txt")); got != "nested bytes" {
		t.Fatalf("backing = %q", got)
	}
	if fx.backingPath("Documents/nested.txt") != "" {
		t.Fatal("old backing entry must be gone")
	}
	if n := len(fx.mock.DownloadedUIDs()); n != downloads {
		t.Fatalf("rename must not re-download: %d != %d", n, downloads)
	}
	row, err := fx.db.NodeByPath("Docs/nested.txt")
	if err != nil || row == nil || row.ParentUID == nil || *row.ParentUID != "node-docs" {
		t.Fatalf("row = %+v", row)
	}
	if row, err := fx.db.NodeByPath("Documents/nested.txt"); err != nil || row != nil {
		t.Fatalf("old row must be gone: %+v", row)
	}
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("trashed: %v", fx.mock.TrashedUIDs())
	}
}

func TestLocalFolderRenameMovesHydratedSubtreeAndRehydrates(t *testing.T) {
	fx := startPlaceholder(t)
	photos := fx.mock.RemoteCreateFolder("root", "Photos")
	fx.mock.RemoteCreateFile(photos, "a.txt", []byte("alpha"))
	fx.mock.RemoteCreateFile(photos, "b.txt", []byte("beta"))
	sub := fx.mock.RemoteCreateFolder(photos, "sub")
	fx.mock.RemoteCreateFile(sub, "c.txt", []byte("gamma"))
	waitForSnapshot(t, fx, "Photos/sub/c.txt")
	for _, rel := range []string{"Photos/a.txt", "Photos/b.txt", "Photos/sub/c.txt"} {
		if !fx.store.BeginHydrate(rel) {
			t.Fatalf("BeginHydrate %s must queue", rel)
		}
	}
	for _, rel := range []string{"Photos/a.txt", "Photos/b.txt", "Photos/sub/c.txt"} {
		waitForReady(t, fx, rel)
	}
	downloads := len(fx.mock.DownloadedUIDs())
	fx.store.RecordRename("Photos", "Albums")
	waitFor(t, func() bool {
		snap := fx.store.Snapshot()
		n, ok := snap.ByPath["Albums/sub/c.txt"]
		if !ok || !n.ContentReady {
			return false
		}
		_, old := snap.ByPath["Photos"]
		return !old
	})
	for rel, want := range map[string]string{
		"Albums/a.txt": "alpha", "Albums/b.txt": "beta", "Albums/sub/c.txt": "gamma",
	} {
		if got := string(fx.backingBytes(rel)); got != want {
			t.Fatalf("backing %s = %q, want %q", rel, got, want)
		}
		old := strings.Replace(rel, "Albums", "Photos", 1)
		if fx.backingPath(old) != "" {
			t.Fatalf("old backing %s must be gone", old)
		}
	}
	if n := len(fx.mock.DownloadedUIDs()); n != downloads {
		t.Fatalf("rename must not re-download: %d != %d", n, downloads)
	}
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("trashed: %v", fx.mock.TrashedUIDs())
	}
	albumsUID, ok := fx.mock.FindUID("root", "Albums")
	if !ok || albumsUID != photos {
		t.Fatalf("Albums uid = %q, want %q", albumsUID, photos)
	}
	seenRename := false
	for _, r := range fx.mock.Renames() {
		if r[0] == photos && r[1] == "Albums" {
			seenRename = true
		}
	}
	if !seenRename {
		t.Fatalf("expected remote rename: %v", fx.mock.Renames())
	}
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("Albums/sub/c.txt") == "" })
	downloads = len(fx.mock.DownloadedUIDs())
	if !fx.store.BeginHydrate("Albums/sub/c.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitFor(t, func() bool { return string(fx.backingBytes("Albums/sub/c.txt")) == "gamma" })
	if n := len(fx.mock.DownloadedUIDs()); n <= downloads {
		t.Fatalf("rehydrate must download again: %d <= %d", n, downloads)
	}
}

// ---------------------------------------------------------------------------
// Metadata fidelity (mtime, exec bit)
// ---------------------------------------------------------------------------

func TestPlaceholderMtimeSurvivesHydrateEvictRehydrate(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	remoteMtime, ok := fx.mock.NodeMtime("node-hello")
	if !ok {
		t.Fatal("mock mtime missing")
	}
	if remoteMtime != 1_700_000_000 {
		t.Fatalf("distinctive seeded mtime expected, got %d", remoteMtime)
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	abs := fx.backingPath("hello.txt")
	waitFor(t, func() bool {
		m, ok := mtimeSecsOf(abs)
		return ok && m == remoteMtime
	})
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") == "" })
	if _, err := os.Stat(abs); !os.IsNotExist(err) {
		t.Fatal("evicted backing must be gone")
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("rehydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	waitFor(t, func() bool {
		m, ok := mtimeSecsOf(fx.backingPath("hello.txt"))
		return ok && m == remoteMtime
	})
}

func TestSetattrMtimeUpdatesRowAndSnapshotWithoutUploadChurn(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	stamped := int64(1_600_000_000)
	fx.store.RecordMetadata("hello.txt", &stamped, nil)
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("hello.txt")
		if err != nil || row == nil || row.Mtime == nil || *row.Mtime != stamped {
			return false
		}
		n, ok := fx.store.Snapshot().ByPath["hello.txt"]
		return ok && n.Mtime == stamped
	})
	if n := len(fx.mock.UploadedNames()); n != 0 {
		t.Fatalf("timestamp-only change must not upload: %v", fx.mock.UploadedNames())
	}
	row, err := fx.db.NodeByPath("hello.txt")
	if err != nil || row == nil || row.SyncState != db.StateSynced {
		t.Fatalf("row = %+v", row)
	}
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	waitFor(t, func() bool {
		m, ok := mtimeSecsOf(fx.backingPath("hello.txt"))
		return ok && m == stamped
	})
}

func TestExecutableBitSurvivesEvictAndRehydrate(t *testing.T) {
	fx := startPlaceholder(t)
	fx.mock.RemoteCreateFile("root", "script.sh", []byte("#!/bin/sh\necho ok\n"))
	waitForSnapshot(t, fx, "script.sh")
	if !fx.store.BeginHydrate("script.sh") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("script.sh") != "" })
	mode755 := uint32(0o755)
	fx.store.RecordMetadata("script.sh", nil, &mode755)
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("script.sh")
		if err != nil || row == nil || row.Mode == nil || *row.Mode != 0o755 {
			return false
		}
		n, ok := fx.store.Snapshot().ByPath["script.sh"]
		return ok && n.Mode&0o111 == 0o111
	})
	if err := fx.handle.SetEvictionCap(phCap(0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fx.backingPath("script.sh") == "" })
	if !fx.store.BeginHydrate("script.sh") {
		t.Fatal("rehydrate must queue")
	}
	waitFor(t, func() bool {
		p := fx.backingPath("script.sh")
		if p == "" {
			return false
		}
		m, ok := mtimeSecsOf(p)
		_ = m
		_ = ok
		fi, err := os.Stat(p)
		return err == nil && uint32(fi.Mode().Perm()) == 0o755
	})
}

// ---------------------------------------------------------------------------
// Environment resilience in placeholder mode (TG-19)
// ---------------------------------------------------------------------------

func TestHydrationPermissionDeniedIsSurfacedAndRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if err := os.MkdirAll(fx.backing, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(fx.backing, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(fx.backing, 0o755) })
	rec := recordActivity(fx.handle)
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	item := waitForActivity(t, rec, engine.ActivityDownloadErrored, "hello.txt")
	if !strings.Contains(strings.ToLower(item.Detail), "permission denied") {
		t.Fatalf("detail should name the cause: %q", item.Detail)
	}
	if fx.backingPath("hello.txt") != "" {
		t.Fatal("no backing file may be registered")
	}
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("hello.txt")
		if err != nil || row == nil || row.SyncState != db.StateDownloading {
			return false
		}
		e, _ := fx.db.UploadError("hello.txt")
		return e != ""
	})
	if err := os.Chmod(fx.backing, 0o755); err != nil {
		t.Fatal(err)
	}
	fx.handle.Resume()
	waitFor(t, func() bool {
		return fx.backingPath("hello.txt") != "" && fx.ready("hello.txt")
	})
	waitFor(t, func() bool {
		row, err := fx.db.NodeByPath("hello.txt")
		return err == nil && row != nil && row.SyncState == db.StateSynced
	})
	if got := string(fx.backingBytes("hello.txt")); got != "hello drive" {
		t.Fatalf("hydrated = %q", got)
	}
}

func TestUnreadableBackingFileIsErroredWithoutHotLooping(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	fx := startPlaceholder(t)
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	abs := fx.backingPath("hello.txt")
	remoteBefore, _ := fx.mock.FileContent("node-hello")
	if err := os.Chmod(abs, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p := fx.backingPath("hello.txt"); p != "" {
			_ = os.Chmod(p, 0o644)
		}
	})
	uploadsBefore := fx.mock.UploadAttempts("hello.txt")
	rec := recordActivity(fx.handle)
	fx.store.RecordWrite("hello.txt")
	item := waitForActivity(t, rec, engine.ActivityUploadErrored, "hello.txt")
	if !strings.Contains(strings.ToLower(item.Detail), "permission denied") {
		t.Fatalf("detail should name the cause: %q", item.Detail)
	}
	if n := fx.mock.UploadAttempts("hello.txt"); n != uploadsBefore {
		t.Fatalf("unreadable source must not reach the uploader: %d != %d", n, uploadsBefore)
	}
	if c, _ := fx.mock.FileContent("node-hello"); string(c) != string(remoteBefore) {
		t.Fatalf("remote content changed: %q", c)
	}
	waitFor(t, func() bool { return fx.store.PendingWrites() == 0 })
	if e, _ := fx.db.UploadError("hello.txt"); e == "" {
		t.Fatal("upload error must be recorded")
	}
	if err := os.Chmod(abs, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte("hello v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	fx.store.RecordWrite("hello.txt")
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent("node-hello")
		if string(c) != "hello v2" {
			return false
		}
		row, err := fx.db.NodeByPath("hello.txt")
		if err != nil || row == nil || row.SyncState != db.StateSynced {
			return false
		}
		e, _ := fx.db.UploadError("hello.txt")
		return e == ""
	})
}

// ---------------------------------------------------------------------------
// Odd-but-legal names and backing-dir isolation (TG-13 / M3.3)
// ---------------------------------------------------------------------------

func TestUnusualNamesAreIdenticalInSnapshotDbAndBacking(t *testing.T) {
	names := []string{
		"caf\u00e9.txt",
		"cafe\u0301.txt",
		"rocket \U0001f680.txt",
		"with space.txt",
		"it's \"quoted\".txt",
		"line\nbreak.txt",
		"trailing dot.",
		"trailing space ",
		"~leading.txt",
	}
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	for _, name := range names {
		fx.mock.RemoteCreateFile("root", name, []byte("remote:"+name))
	}
	for _, name := range names {
		waitFor(t, func() bool {
			if !fx.snapshotHas(name) {
				return false
			}
			row, err := fx.db.NodeByPath(name)
			return err == nil && row != nil
		})
		node := fx.store.Snapshot().ByPath[name]
		if node.Rel != name || node.Name != name || node.Parent != "" || node.IsDir {
			t.Fatalf("snapshot node for %q = %+v", name, node)
		}
		row, err := fx.db.NodeByPath(name)
		if err != nil || row == nil || row.LocalPath != name || row.Name != name {
			t.Fatalf("row for %q = %+v", name, row)
		}
		if !fx.store.BeginHydrate(name) {
			t.Fatalf("BeginHydrate %q must queue", name)
		}
	}
	for _, name := range names {
		want := "remote:" + name
		waitFor(t, func() bool { return string(fx.backingBytes(name)) == want })
		abs := fx.backingPath(name)
		if filepath.Base(abs) != name {
			t.Fatalf("backing file name = %q, want %q", filepath.Base(abs), name)
		}
	}
	if len(fx.mock.TrashedUIDs()) != 0 {
		t.Fatalf("trashed: %v", fx.mock.TrashedUIDs())
	}
}

func TestBackingDirEventsNeverEnterTheSyncPipeline(t *testing.T) {
	fx := startPlaceholder(t)
	waitForSnapshot(t, fx, "hello.txt")
	if !fx.store.BeginHydrate("hello.txt") {
		t.Fatal("BeginHydrate must queue")
	}
	waitFor(t, func() bool { return fx.backingPath("hello.txt") != "" })
	time.Sleep(1200 * time.Millisecond)
	for _, n := range fx.mock.UploadedNames() {
		if strings.Contains(n, ".pds-vfs-backing") {
			t.Fatalf("backing-dir files leaked into the upload pipeline: %v", fx.mock.UploadedNames())
		}
	}
	if n := len(fx.mock.UploadedNames()); n != 0 {
		t.Fatalf("hydration must not upload: %v", fx.mock.UploadedNames())
	}
}
