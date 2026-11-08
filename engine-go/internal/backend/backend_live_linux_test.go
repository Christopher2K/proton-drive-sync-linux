//go:build linux

package backend_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"pds.dev/engine-go/internal/backend"
	"pds.dev/engine-go/internal/protocol"
	"pds.dev/engine-go/internal/sidecar"
)

// TestBackendLivePlaceholderMount exercises the real go-fuse stack through
// the backend: probe + mount + unmount against the mock sidecar. Gated
// behind -short like the vfs smoke test: on hosts without /dev/fuse it
// skips instead of hanging.
func TestBackendLivePlaceholderMount(t *testing.T) {
	if testing.Short() {
		t.Skip("live FUSE integration test skipped in -short mode")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("FUSE unavailable: %v", err)
	}
	ok := false
	for _, helper := range []string{"fusermount3", "fusermount"} {
		if path, err := exec.LookPath(helper); err == nil {
			if err := exec.Command(path, "--version").Run(); err == nil {
				ok = true
				break
			}
		}
	}
	if !ok {
		t.Skip("no runnable fusermount helper")
	}

	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	root := filepath.Join(dir, "root")

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	defer client.Close()

	// Platform defaults: real probe + real mount (nil Hooks).
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: root}, client, func() {}, collect)

	be.Start(context.Background())
	defer func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(10 * time.Second):
		}
	}()

	waitFor(t, 30*time.Second, func() bool {
		for _, u := range snapshot() {
			if u.Update == protocol.UpdEngineStarted {
				return true
			}
			if u.Update == protocol.UpdMountUnavailable {
				return true
			}
		}
		return false
	}, "EngineStarted or MountUnavailable for live placeholder session")

	for _, u := range snapshot() {
		if u.Update == protocol.UpdMountUnavailable {
			t.Skipf("FUSE probe unavailable on this host: %s", u.Error)
		}
	}
	// The mount is up: the root is listable through the FUSE view.
	deadline := time.Now().Add(10 * time.Second)
	listed := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(root); err == nil {
			listed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !listed {
		t.Fatal("placeholder mount at sync root never became stat-able")
	}
}
