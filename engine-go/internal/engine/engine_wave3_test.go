// Engine battery port (wave 3): mode conversion across restarts and the
// large-sparse upload, translated from crates/core/tests/engine.rs.
//
// The two conversion tests exercise the external-layout conversion that
// landed in 5b3e4c3 (mode.ConvertLayout + engine.New with BackingDir):
// a dirty pending upload survives the mirror→placeholders move (and a
// crash before the direction marker), and a guard pause survives a mode
// switch without trashing anything.
package engine_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/mode"
	"pds.dev/engine-go/internal/sidecar"
)

func wave3Config(root, dbPath, backing string) engine.Config {
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = engine.RetryBaseDefault
	cfg.Placeholders = true
	cfg.BackingDir = backing
	cfg.WatchLocalWrites = false
	return cfg
}

func TestModeConversionPreservesPendingUploadAcrossRestart(t *testing.T) {
	t.Setenv("PDS_SYNC_MODE", "placeholders")

	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	state := filepath.Join(dir, "state")
	backing := filepath.Join(state, "backing")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("pending bytes")
	if err := os.WriteFile(filepath.Join(root, "draft.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(state, "state.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	sha := hashSHA1(content)
	size := int64(len(content))
	parent := "root"
	if err := d.UpsertNode(&db.NodeRow{
		LocalPath: "draft.txt",
		NodeUID:   "pending:draft.txt",
		ParentUID: &parent,
		Name:      "draft.txt",
		Kind:      db.KindFile,
		Size:      &size,
		SHA1:      &sha,
		SyncState: db.StateDirty,
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.SetMeta("initial_sync_done", "1"); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := wave3Config(root, dbPath, backing)

	// First construction performs the conversion, then "crashes" before
	// the marker is written: remove the marker to simulate that.
	{
		mock := sidecar.NewMock()
		client, _ := mock.Connect()
		eng, _, err := engine.New(client, client.Notifications(), nil, cfg)
		if err != nil {
			t.Fatalf("first engine.New: %v", err)
		}
		_ = eng
		client.Close()
	}
	b, err := os.ReadFile(filepath.Join(backing, "draft.txt"))
	if err != nil || string(b) != string(content) {
		t.Fatalf("backing draft.txt = %q, err %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(root, "draft.txt")); !os.IsNotExist(err) {
		t.Fatal("conversion must move draft.txt out of the root")
	}
	if err := os.Remove(filepath.Join(backing, mode.LayoutConvertedFile)); err != nil {
		t.Fatalf("remove marker: %v", err)
	}

	// A fresh engine re-runs the interrupted pass: content stays exactly
	// once in the backing cache, still dirty.
	mock := sidecar.NewMock()
	client, mhandle := mock.Connect()
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatalf("second engine.New: %v", err)
	}
	d2, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	row, err := d2.NodeByPath("draft.txt")
	if err != nil || row == nil {
		t.Fatalf("draft.txt row missing: %+v, %v", row, err)
	}
	if row.SyncState != db.StateDirty {
		t.Fatalf("sync_state = %v, want dirty", row.SyncState)
	}
	if row.NodeUID != "pending:draft.txt" {
		t.Fatalf("node_uid = %q, want pending:draft.txt", row.NodeUID)
	}
	b, err = os.ReadFile(filepath.Join(backing, "draft.txt"))
	if err != nil || string(b) != string(content) {
		t.Fatalf("backing after rerun = %q, err %v", b, err)
	}

	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() {
		h.Shutdown()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop")
		}
		client.Close()
	}()

	waitFor(t, func() bool { return contains(mhandle.UploadedNames(), "draft.txt") })
	waitForSynced(t, d2, "draft.txt")
	uid, ok := mhandle.FindUID("root", "draft.txt")
	if !ok {
		t.Fatal("draft.txt never uploaded")
	}
	if c, _ := mhandle.FileContent(uid); string(c) != string(content) {
		t.Fatalf("uploaded = %q, want %q", c, content)
	}
}

func TestModeSwitchWhileGuardPausedKeepsTombstonesAndTrashesNothing(t *testing.T) {
	t.Setenv("PDS_SYNC_MODE", "mirror")
	fx := start(t)

	// A single delete trips the ratio limb on this tiny tree (2 synced
	// nodes → 1 > 25%).
	if err := os.Remove(filepath.Join(fx.root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitForEngineState(t, fx, engine.StateGuardPaused)
	tombs, err := fx.db.Tombstones()
	if err != nil || len(tombs) != 1 {
		t.Fatalf("tombstones = %v, %v, want exactly 1", tombs, err)
	}
	if v, _ := fx.db.Meta("guard_paused"); v != "1" {
		t.Fatalf("guard_paused = %q, want 1", v)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed before switch = %v, want none", trashed)
	}

	// Content queued while the guard is paused (local-only files waiting
	// for their first upload) must survive the conversion too.
	for i := 0; i < 3; i++ {
		rel := "filler-" + string(rune('0'+i)) + ".txt"
		if err := os.WriteFile(filepath.Join(fx.root, rel), []byte("keep "+string(rune('0'+i))), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		rel := "filler-" + string(rune('0'+i)) + ".txt"
		waitFor(t, func() bool {
			row, err := fx.db.NodeByPath(rel)
			return err == nil && row != nil && row.SyncState == db.StateDirty
		})
	}

	// Persist the new mode, shut the engine down (keeping the dir), and
	// build a fresh one with an external backing cache.
	if err := mode.SetSyncMode(fx.db, mode.ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	root := fx.root
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	backing := filepath.Join(filepath.Dir(root), "backing")
	fx.shutdown()

	// The env override would win over the persisted value; clear it so the
	// persisted placeholders choice takes effect.
	if err := os.Unsetenv("PDS_SYNC_MODE"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PDS_SYNC_MODE", "placeholders")

	mock := sidecar.NewMock()
	client, mhandle := mock.Connect()
	cfg := wave3Config(root, dbPath, backing)
	eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatalf("restart engine.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	defer func() {
		h.Shutdown()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop")
		}
		client.Close()
	}()

	waitForEngineState2(t, h, engine.StateGuardPaused)
	// Conversion moved the survivors into the backing cache; the pause
	// held back the tombstone.
	for i := 0; i < 3; i++ {
		rel := "filler-" + string(rune('0'+i)) + ".txt"
		b, err := os.ReadFile(filepath.Join(backing, rel))
		if err != nil || string(b) != "keep "+string(rune('0'+i)) {
			t.Fatalf("backing %s = %q, err %v", rel, b, err)
		}
		if _, err := os.Stat(filepath.Join(root, rel)); !os.IsNotExist(err) {
			t.Fatalf("root %s must be gone after conversion", rel)
		}
	}
	st, err := h.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.QueuedTombstones != 1 {
		t.Fatalf("queued_tombstones = %d, want 1", st.QueuedTombstones)
	}
	time.Sleep(800 * time.Millisecond)
	if trashed := mhandle.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("paused engine trashed %v, want nothing", trashed)
	}

	// Explicit resume is the approval: exactly the one queued tombstone
	// is trashed, none of the converted files.
	h.Resume()
	waitFor(t, func() bool {
		trashed := mhandle.TrashedUIDs()
		return len(trashed) == 1 && trashed[0] == "node-hello"
	})
	waitFor(t, func() bool {
		st, err := h.Status()
		return err == nil && st.State == engine.StateRunning && st.QueuedTombstones == 0
	})
	if trashed := mhandle.TrashedUIDs(); len(trashed) != 1 || trashed[0] != "node-hello" {
		t.Fatalf("trashed = %v, want exactly [node-hello]", trashed)
	}
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if v, _ := d.Meta("guard_paused"); v != "" {
		t.Fatalf("guard_paused = %q, want removed", v)
	}
}

func waitForEngineState2(t *testing.T, h *engine.Handle, state engine.EngineState) {
	t.Helper()
	waitFor(t, func() bool {
		st, err := h.Status()
		return err == nil && st.State == state
	})
}

func TestLargeSparseLocalFileUploadsWithoutTruncation(t *testing.T) {
	t.Skip("production seam missing: the Go mock has no streaming-upload mode or sparse-file API (Rust MockSidecar::enable_streaming_uploads + remote_create_sparse_file); handleUpload buffers the whole file with os.ReadFile, so a 5 GiB sparse upload cannot complete without multi-GiB memory — the size/metadata assertions need a mock streaming seam")
}
