// Conflict soak (M4 hardening), ported from crates/core/tests/soak.rs:
// repeated and simultaneous local/remote mutations must keep BOTH versions
// every time (F7) and leave the engine in a consistent terminal state.
//
// Mirror mode only: the engine runs with store == nil and watches the real
// sync root. Placeholder-mode variants are deferred (step 5 engine wiring
// pending: the hydrate pump is a step-5 stub) and skipped explicitly.
//
// Discipline (AGENT.md M4): fixtures synchronize on the watcher before local
// writes; all waits are bounded. Mass-delete guard tests (engine_test.go)
// synchronize on deletes-accounted before resuming.
package engine_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

// soakConflictContents returns the contents of every conflict copy of stem
// currently in root. Later rounds accumulate copies, so "the counterpart
// survived" means "some copy holds it" — order must not matter.
func soakConflictContents(root, stem string) []string {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), stem+" (conflict ") {
			if b, err := os.ReadFile(filepath.Join(root, e.Name())); err == nil {
				out = append(out, string(b))
			}
		}
	}
	return out
}

func soakReadFile(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

// soakRootHasContent walks root recursively for a file holding exactly
// content.
func soakRootHasContent(root, content string) bool {
	var walk func(dir string) bool
	walk = func(dir string) bool {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return false
		}
		for _, e := range entries {
			p := filepath.Join(dir, e.Name())
			if e.IsDir() {
				if walk(p) {
					return true
				}
			} else if soakReadFile(p) == content {
				return true
			}
		}
		return false
	}
	return walk(root)
}

// soakAssertQuiescent: the engine reached a quiescent state — running, and
// nothing uploading or dirty (rows parked as errored/conflicted are no
// longer dirty).
func soakAssertQuiescent(t *testing.T, fx *fixture) {
	t.Helper()
	waitForWithin(t, 30*time.Second, func() bool {
		st, err := fx.handle.Status()
		return err == nil && st.State == engine.StateRunning
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		dirty, _ := fx.db.NodesWithState(db.StateDirty)
		uploading, _ := fx.db.NodesWithState(db.StateUploading)
		if len(dirty) == 0 && len(uploading) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rows never reached a terminal state: dirty=%v uploading=%v", dirty, uploading)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// soakStart takes the fast retry base so backoff scheduling never stretches
// a round (Rust soak.rs uses 20 ms).
func soakStart(t *testing.T) *fixture {
	t.Helper()
	return startWithRetryBase(t, 20*time.Millisecond)
}

func soakWaitForFile(t *testing.T, root, rel string) {
	t.Helper()
	waitFor(t, func() bool {
		_, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		return err == nil
	})
}

// Soak 1: N rounds of simultaneous local edit + remote edit of the same
// file. Every round must keep both versions somewhere and end synced.
func TestSoakRepeatedSimultaneousEditsKeepBoth(t *testing.T) {
	fx := soakStart(t)
	for round := 1; round <= 5; round++ {
		local := "local r" + itoa(round)
		remote := "remote r" + itoa(round)
		fx.write("hello.txt", []byte(local))
		fx.mock.RemoteUpdateFile("node-hello", []byte(remote))
		// Wait until BOTH survived: the file shows one version, and some
		// conflict copy holds the other.
		waitFor(t, func() bool {
			disk := soakReadFile(filepath.Join(fx.root, "hello.txt"))
			copies := soakConflictContents(fx.root, "hello")
			if disk == local {
				for _, c := range copies {
					if c == remote {
						return true
					}
				}
			}
			if disk == remote {
				for _, c := range copies {
					if c == local {
						return true
					}
				}
			}
			return false
		})
		soakAssertQuiescent(t, fx)
	}
	// No data loss across all rounds: the latest remote version still
	// exists on disk somewhere.
	var all []byte
	all = append(all, []byte(soakReadFile(filepath.Join(fx.root, "hello.txt")))...)
	for _, c := range soakConflictContents(fx.root, "hello") {
		all = append(all, c...)
	}
	if !strings.Contains(string(all), "remote r5") {
		t.Fatalf("latest remote lost: %s", all)
	}
	st, err := fx.handle.Status()
	if err != nil || st.State != engine.StateRunning {
		t.Fatalf("engine state = %v, %v", st.State, err)
	}
}

func itoa(v int) string {
	return string(rune('0' + v))
}

// Soak 2: remote delete vs concurrent local edit — the local edit must
// survive; the remote deletion must never destroy unsynced local content
// (the M4 fix: applyRemoteDelete hashes on-disk content first).
func TestSoakRemoteDeleteVsLocalEditKeepsEdit(t *testing.T) {
	for round := 1; round <= 3; round++ {
		fx := soakStart(t)
		local := "precious edit r" + itoa(round)
		fx.write("hello.txt", []byte(local))
		fx.mock.RemoteDelete("node-hello")
		// The local content must survive somewhere in the root.
		waitFor(t, func() bool { return soakRootHasContent(fx.root, local) })
		soakAssertQuiescent(t, fx)
		st, err := fx.handle.Status()
		if err != nil || st.State != engine.StateRunning {
			t.Fatalf("engine state = %v, %v", st.State, err)
		}
	}
}

// Soak 3: burst of remote edits racing one local edit stream — the final
// state must converge to the last remote version with the local edits
// preserved in conflict copies.
func TestSoakRemoteEditBurstConverges(t *testing.T) {
	fx := soakStart(t)
	for i := 1; i <= 6; i++ {
		fx.mock.RemoteUpdateFile("node-hello", []byte("burst "+itoa(i)))
		fx.write("hello.txt", []byte("local burst "+itoa(i)))
		time.Sleep(120 * time.Millisecond)
	}
	// Converged: hello.txt eventually holds the last remote version...
	waitFor(t, func() bool {
		return soakReadFile(filepath.Join(fx.root, "hello.txt")) == "burst 6"
	})
	soakAssertQuiescent(t, fx)
	// ...some local version survived in a conflict copy...
	if _, ok := conflictEntry(fx.root, "hello"); !ok {
		t.Fatal("local edits lost in the burst")
	}
	// ...and the engine never errored or wedged.
	st, err := fx.handle.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.State != engine.StateRunning {
		t.Fatalf("engine state = %v", st.State)
	}
	if st.ErroredNodes != 0 {
		t.Fatalf("errored nodes = %d", st.ErroredNodes)
	}
}

// Soak 4: repeated rename-churn — the conflict copy path picks fresh names
// even when many copies pile up (never overwrites an existing copy).
func TestSoakConflictCopiesNeverCollide(t *testing.T) {
	fx := soakStart(t)
	for round := 1; round <= 8; round++ {
		// Diverge local content, then let the remote event hit the same path.
		fx.write("hello.txt", []byte("copy r"+itoa(round)))
		fx.mock.RemoteUpdateFile("node-hello", []byte("head r"+itoa(round)))
		waitFor(t, func() bool {
			return soakReadFile(filepath.Join(fx.root, "hello.txt")) == "head r"+itoa(round)
		})
	}
	soakAssertQuiescent(t, fx)
	// Every surviving conflict copy must have distinct, non-empty content.
	seen := map[string]bool{}
	for _, content := range soakConflictContents(fx.root, "hello") {
		if content == "" {
			t.Fatal("conflict copy is empty")
		}
		if seen[content] {
			t.Fatalf("duplicate conflict copy content: %q", content)
		}
		seen[content] = true
	}
	if len(seen) == 0 {
		t.Fatal("no conflict copies survived the churn")
	}
}

// Soak 5 (M4): crash-interrupted transfer recovery. A revision upload killed
// mid-transfer recovers on restart — the stuck row re-uploads, stale temps
// sweep, and nothing is trashed.
func TestSoakCrashInterruptedTransferRecovery(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, engine.StateDir, "state.db")

	// Run 1: baseline sync, then a revision edit, then crash mid-upload.
	func() {
		mock := sidecar.NewMock()
		client, handle := mock.Connect()
		cfg := engine.DefaultConfig()
		cfg.Root = root
		cfg.DBPath = dbPath
		cfg.RetryBase = 20 * time.Millisecond
		eng, h, err := engine.New(client, client.Notifications(), nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- eng.Run() }()
		h.WaitForWatcher(10 * time.Second)
		soakWaitForFile(t, root, "hello.txt")
		handle.CrashOnNextUpload()
		if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("revision-in-flight"), 0o644); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			st, err := h.Status()
			return err == nil && st.SidecarUnavailable
		})
		h.Shutdown()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop after crash + shutdown")
		}
		client.Close()
	}()

	// Plant crash leftovers: a stale temp and a row stuck mid-upload.
	if err := os.WriteFile(filepath.Join(root, ".pds-abc123.pds-part"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
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

	// Run 2 with a fresh mock carrying the same tree: the stuck row goes
	// back to Dirty and re-syncs, the stale temp sweeps away.
	mock := sidecar.NewMockWithSeq(500)
	client, handle := mock.Connect()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = 20 * time.Millisecond
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
	uid, ok := handle.FindUID("root", "hello.txt")
	if !ok {
		t.Fatal("hello.txt uid missing after recovery")
	}
	waitFor(t, func() bool {
		c, _ := handle.FileContent(uid)
		return string(c) == "revision-in-flight"
	})
	if _, err := os.Stat(filepath.Join(root, ".pds-abc123.pds-part")); !os.IsNotExist(err) {
		t.Fatal("stale temp not swept")
	}
	// Recovery never trashes (the guard never trips on it).
	if trashed := handle.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed during recovery: %v", trashed)
	}
}

// Placeholder-mode soak variants: deferred (the soak fixtures here are
// mirror-only, store == nil; in-process placeholder scenarios live in
// engine_placeholder_test.go). The mirror-mode tests above are the runnable
// battery.
func TestSoakPlaceholderModePendingStep5(t *testing.T) {
	scenarios := []string{
		"repeated-simultaneous-edits",
		"remote-delete-vs-edit",
		"remote-burst-convergence",
		"conflict-copy-churn",
		"crash-recovery",
	}
	for _, s := range scenarios {
		t.Run(s, func(t *testing.T) {
			t.Skip("step 5 engine wiring pending: placeholder-mode soak variants deferred — these fixtures are mirror-only (store==nil); placeholder scenarios live in engine_placeholder_test.go and the placeholder soak battery is follow-up work")
		})
	}
}
