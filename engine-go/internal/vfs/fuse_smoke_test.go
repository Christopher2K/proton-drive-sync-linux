//go:build linux

// SMOKE subset of crates/vfs-linux/tests/fuse_e2e.rs: mount, ls, read a
// placeholder (forcing hydration), and a write-through create→release.
// The full e2e suite (revision writes, rename trees, eviction, watcher
// interplay) ports in migration step 6.
//
// Needs a real kernel FUSE setup: /dev/fuse plus a working fusermount
// helper. On hosts without one the test skips itself (behind -short, the
// default for CI) instead of hanging until the CI timeout.

package vfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// fuseUnavailableReason returns why this host cannot run the real-kernel
// smoke test, or "" if it can.
func fuseUnavailableReason(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return "/dev/fuse does not exist on this host"
	}
	for _, helper := range []string{"fusermount3", "fusermount"} {
		if path, err := exec.LookPath(helper); err == nil {
			if err := exec.Command(path, "--version").Run(); err == nil {
				return ""
			}
		}
	}
	return "no runnable fusermount helper"
}

func TestFUSESmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("real-kernel FUSE smoke test skipped in -short mode")
	}
	if reason := fuseUnavailableReason(t); reason != "" {
		t.Skipf("FUSE unavailable: %s", reason)
	}

	root := t.TempDir()
	// Engine state lives OUTSIDE the sync root (AGENT.md M3.4: mounting
	// over the root shadows everything under it; serving the live state
	// dir through FUSE would deadlock — a stat of <root>/.pds would
	// round-trip back into this filesystem).
	stateRoot := t.TempDir()
	s := snap(1, "hello.txt")
	s.byPath["hello.txt"] = VNode{Rel: "hello.txt", Parent: "", Name: "hello.txt", Size: 11, Mtime: 1, Mode: 0o644, ContentReady: false}
	st := newMemStore(t, s)
	stateDir := filepath.Join(stateRoot, ".pds")
	backingDir := filepath.Join(stateRoot, ".pds-vfs-backing")
	for _, d := range []string{stateDir, backingDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Engine simulation: hydrate hello.txt shortly after BeginHydrate.
	st.hydrate(t, "hello.txt", "hello world")
	fs := NewPdsFs(st, stateDir, backingDir)

	server, err := Mount(root, fs, false)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer func() {
		if err := server.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
		}
	}()

	// ls: the snapshot entry + the .pds proxy are visible.
	deadline := time.Now().Add(10 * time.Second)
	var entries []os.DirEntry
	for {
		entries, err = os.ReadDir(root)
		if err == nil && len(entries) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount never became listable: %v (%d entries)", err, len(entries))
		}
		time.Sleep(50 * time.Millisecond)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	if !names["hello.txt"] || !names[StateProxy] {
		t.Errorf("root listing = %v, want hello.txt and .pds", entries)
	}

	// read the placeholder: forces hydration through the BeginHydrate
	// callback and serves the backing bytes.
	data, err := os.ReadFile(filepath.Join(root, "hello.txt"))
	if err != nil {
		t.Fatalf("read placeholder: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("placeholder content = %q", data)
	}
	st.mu.Lock()
	hydrates := len(st.beginHydrate)
	st.mu.Unlock()
	if hydrates == 0 {
		t.Errorf("reading a placeholder must queue a hydration")
	}

	// write-through: create a file, write, close → upload queued on release.
	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("created"), 0o644); err != nil {
		t.Fatalf("create+write: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		for _, w := range st.writes {
			if w == "new.txt" {
				return true
			}
		}
		return false
	}, "release of new.txt must queue the upload")

	// The written file reads back through the mount.
	got, err := os.ReadFile(filepath.Join(root, "new.txt"))
	if err != nil || string(got) != "created" {
		t.Errorf("read back new.txt = %q, %v", got, err)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
