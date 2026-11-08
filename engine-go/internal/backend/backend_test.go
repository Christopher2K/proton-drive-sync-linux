package backend_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"pds.dev/engine-go/internal/backend"
	"pds.dev/engine-go/internal/protocol"
	"pds.dev/engine-go/internal/sidecar"
	storepkg "pds.dev/engine-go/internal/store"
)

type fakeMount struct {
	mu        sync.Mutex
	events    []string
	probeOK   bool
	mounted   int
	unmounted int
}

func (f *fakeMount) record(ev string) {
	f.mu.Lock()
	f.events = append(f.events, ev)
	f.mu.Unlock()
}

func (f *fakeMount) hooks() backend.Hooks {
	return backend.Hooks{
		Preflight: func(_ string, _ time.Duration) error {
			f.record("preflight")
			return nil
		},
		Probe: func() bool {
			f.record("probe")
			return f.probeOK
		},
		Mount: func(_ *storepkg.Store, _, _, _ string) (func(), error) {
			f.record("mount")
			f.mu.Lock()
			f.mounted++
			f.mu.Unlock()
			return func() {
				f.record("unmount")
				f.mu.Lock()
				f.unmounted++
				f.mu.Unlock()
			}, nil
		},
	}
}

func (f *fakeMount) eventsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func updatesCollector() (func(protocol.Update), func() []protocol.Update) {
	var mu sync.Mutex
	var got []protocol.Update
	return func(u protocol.Update) {
			mu.Lock()
			got = append(got, u)
			mu.Unlock()
		}, func() []protocol.Update {
			mu.Lock()
			defer mu.Unlock()
			return append([]protocol.Update(nil), got...)
		}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}

func TestBackendProbeBeforeEngineMountAfterWatcher(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	root := filepath.Join(dir, "root")

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	defer client.Close()

	fake := &fakeMount{probeOK: true}
	collect, snapshot := updatesCollector()
	h := fake.hooks()
	be := backend.New(backend.Config{Root: root, Hooks: &h}, client, func() {}, collect)

	be.Start(context.Background())
	defer func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(5 * time.Second):
		}
	}()

	// Probe must run (before the engine starts) and mount after the
	// watcher; EngineStarted follows the mount.
	waitFor(t, 10*time.Second, func() bool {
		for _, u := range snapshot() {
			if u.Update == protocol.UpdEngineStarted {
				return true
			}
		}
		return false
	}, "EngineStarted for placeholder session")

	evs := fake.eventsCopy()
	if len(evs) == 0 || evs[0] != "preflight" {
		t.Fatalf("first hook event = %v, want preflight", evs)
	}
	// probe is the only pre-engine hook; mount follows it.
	idxProbe, idxMount := -1, -1
	for i, e := range evs {
		if e == "probe" && idxProbe < 0 {
			idxProbe = i
		}
		if e == "mount" {
			idxMount = i
			break
		}
	}
	if idxProbe < 0 {
		t.Fatalf("probe hook never ran: %v", evs)
	}
	if idxMount < 0 {
		t.Fatalf("mount hook never ran: %v", evs)
	}
	if idxMount < idxProbe {
		t.Fatalf("mount (%d) before probe (%d): %v", idxMount, idxProbe, evs)
	}
}

func TestBackendProbeFailureFallsBackToMirror(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	root := filepath.Join(dir, "root")

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	defer client.Close()

	fake := &fakeMount{probeOK: false}
	h := fake.hooks()
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: root, Hooks: &h}, client, func() {}, collect)

	be.Start(context.Background())
	defer func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(5 * time.Second):
		}
	}()

	waitFor(t, 10*time.Second, func() bool {
		foundMountUnavailable, foundStarted := false, false
		for _, u := range snapshot() {
			if u.Update == protocol.UpdMountUnavailable {
				foundMountUnavailable = true
			}
			if u.Update == protocol.UpdEngineStarted {
				foundStarted = true
			}
		}
		return foundMountUnavailable && foundStarted
	}, "MountUnavailable fallback + EngineStarted")

	fake.mu.Lock()
	mounted := fake.mounted
	fake.mu.Unlock()
	if mounted != 0 {
		t.Fatalf("mount called %d times after probe failure, want 0", mounted)
	}
}

func TestBackendModeSwitchRestartsIntoNewMode(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("PDS_SYNC_MODE", "mirror")
	root := filepath.Join(dir, "root")

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	defer client.Close()

	fake := &fakeMount{probeOK: true}
	h := fake.hooks()
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: root, Hooks: &h}, client, func() {}, collect)

	be.Start(context.Background())
	defer func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(5 * time.Second):
		}
	}()

	waitFor(t, 10*time.Second, func() bool {
		for _, u := range snapshot() {
			if u.Update == protocol.UpdEngineStarted {
				return true
			}
		}
		return false
	}, "initial EngineStarted")

	// The session-only env override would win over the persisted value on
	// restart (SPEC: $PDS_SYNC_MODE is session-only); clear it so the
	// persisted placeholders choice takes effect.
	if err := os.Unsetenv("PDS_SYNC_MODE"); err != nil {
		t.Fatal(err)
	}
	// Mirror uses the in-root .pds path: no mount expected yet.
	if err := be.SetMode(protocol.ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		for _, u := range snapshot() {
			if u.Update == protocol.UpdModeSwitched && u.Mode == protocol.ModePlaceholders {
				return true
			}
		}
		return false
	}, "ModeSwitched to placeholders")

	// Restart: a second EngineStarted and a placeholder mount.
	waitFor(t, 15*time.Second, func() bool {
		started, mounted := 0, 0
		for _, u := range snapshot() {
			if u.Update == protocol.UpdEngineStarted {
				started++
			}
		}
		fake.mu.Lock()
		mounted = fake.mounted
		fake.mu.Unlock()
		return started >= 2 && mounted >= 1
	}, "restart into placeholders (second EngineStarted + mount)")

	// Unmount on Shutdown.
	be.Shutdown()
	select {
	case <-be.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("backend never exited after Shutdown")
	}
	fake.mu.Lock()
	unmounted := fake.unmounted
	fake.mu.Unlock()
	if unmounted == 0 {
		t.Fatal("placeholder view never unmounted on Shutdown")
	}
}
