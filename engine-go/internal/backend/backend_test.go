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

type loginAttempt struct {
	email   string
	started chan struct{}
	release chan struct{}
}

func newLoginAttempt(email string) *loginAttempt {
	return &loginAttempt{
		email:   email,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// controlledAuthAPI wraps the real mock Drive API and replaces only auth and
// notifications. Login completion is held open so tests can prove the URL is
// forwarded while auth.loginFork is still waiting for browser authorization.
type controlledAuthAPI struct {
	sidecar.API

	mu                sync.Mutex
	loggedIn          bool
	email             string
	attempts          chan *loginAttempt
	authNotifications chan sidecar.Notification
	notifications     chan sidecar.Notification
	logoutStarted     chan struct{}
	logoutRelease     chan struct{}
}

func newControlledAuthAPI(api sidecar.API, loggedIn bool, email string) *controlledAuthAPI {
	a := &controlledAuthAPI{
		API:               api,
		loggedIn:          loggedIn,
		email:             email,
		attempts:          make(chan *loginAttempt, 2),
		authNotifications: make(chan sidecar.Notification, 16),
		notifications:     make(chan sidecar.Notification, 256),
		logoutStarted:     make(chan struct{}, 1),
	}
	if notifier, ok := api.(interface {
		Notifications() <-chan sidecar.Notification
	}); ok {
		go func() {
			for notification := range notifier.Notifications() {
				a.notifications <- notification
			}
		}()
	}
	go func() {
		for notification := range a.authNotifications {
			a.notifications <- notification
		}
	}()
	return a
}

func (a *controlledAuthAPI) Notifications() <-chan sidecar.Notification {
	return a.notifications
}

func (a *controlledAuthAPI) AuthStatus(_ context.Context) (sidecar.AuthStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := sidecar.AuthStatus{LoggedIn: a.loggedIn}
	if a.loggedIn && a.email != "" {
		email := a.email
		st.Email = &email
	}
	return st, nil
}

func (a *controlledAuthAPI) LoginFork(ctx context.Context) (string, error) {
	var attempt *loginAttempt
	select {
	case attempt = <-a.attempts:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	close(attempt.started)
	select {
	case <-attempt.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	a.mu.Lock()
	a.loggedIn = true
	a.email = attempt.email
	a.mu.Unlock()
	return attempt.email, nil
}

func (a *controlledAuthAPI) Logout(ctx context.Context) error {
	select {
	case a.logoutStarted <- struct{}{}:
	default:
	}
	if a.logoutRelease != nil {
		select {
		case <-a.logoutRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.mu.Lock()
	a.loggedIn = false
	a.email = ""
	a.mu.Unlock()
	return nil
}

func newControlledBackend(t *testing.T, loggedIn bool, email string) (*backend.Backend, *controlledAuthAPI, func(protocol.Update), func() []protocol.Update) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("PDS_SYNC_MODE", "mirror")

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	api := newControlledAuthAPI(client, loggedIn, email)
	fake := &fakeMount{probeOK: true}
	h := fake.hooks()
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: filepath.Join(dir, "root"), Hooks: &h}, api, func() {}, collect)
	be.Start(context.Background())
	t.Cleanup(func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(5 * time.Second):
			t.Error("backend never exited after Shutdown")
		}
		client.Close()
	})
	return be, api, collect, snapshot
}

func startTestLogin(be *backend.Backend, collect func(protocol.Update)) <-chan struct{} {
	updates := make(chan protocol.Update, 32)
	done := make(chan struct{})
	go func() {
		be.StartLogin(context.Background(), updates)
		close(updates)
	}()
	go func() {
		for update := range updates {
			collect(update)
		}
		close(done)
	}()
	return done
}

func updateCount(updates []protocol.Update, name string) int {
	count := 0
	for _, update := range updates {
		if update.Update == name {
			count++
		}
	}
	return count
}

type blockingAuthAPI struct {
	sidecar.API
	started      chan struct{}
	release      chan struct{}
	logoutCalled chan struct{}
	mu           sync.Mutex
	calls        int
}

func (a *blockingAuthAPI) AuthStatus(ctx context.Context) (sidecar.AuthStatus, error) {
	a.mu.Lock()
	a.calls++
	first := a.calls == 1
	a.mu.Unlock()
	if first {
		close(a.started)
		if a.release != nil {
			select {
			case <-a.release:
				email := "starting@example.com"
				return sidecar.AuthStatus{LoggedIn: true, Email: &email}, nil
			case <-ctx.Done():
				return sidecar.AuthStatus{}, ctx.Err()
			}
		}
	}
	<-ctx.Done()
	return sidecar.AuthStatus{}, ctx.Err()
}

func (a *blockingAuthAPI) Logout(_ context.Context) error {
	if a.logoutCalled != nil {
		close(a.logoutCalled)
	}
	return nil
}

func TestBackendShutdownCancelsPendingAuthentication(t *testing.T) {
	api := &blockingAuthAPI{started: make(chan struct{})}
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: t.TempDir()}, api, func() {}, collect)
	be.Start(context.Background())
	select {
	case <-api.started:
	case <-time.After(2 * time.Second):
		t.Fatal("auth.status never started")
	}
	be.Shutdown()
	select {
	case <-be.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("backend ignored Shutdown while auth.status was pending")
	}
	for _, update := range snapshot() {
		if update.Update == protocol.UpdFailed || update.Update == protocol.UpdEngineStarted {
			t.Fatalf("unexpected update after Shutdown: %+v", update)
		}
	}
}

func TestBackendSignOutDuringAuthenticationDoesNotStartEngine(t *testing.T) {
	api := &blockingAuthAPI{
		started:      make(chan struct{}),
		release:      make(chan struct{}),
		logoutCalled: make(chan struct{}),
	}
	collect, snapshot := updatesCollector()
	be := backend.New(backend.Config{Root: t.TempDir()}, api, func() {}, collect)
	be.Start(context.Background())
	defer func() {
		be.Shutdown()
		select {
		case <-be.Done():
		case <-time.After(2 * time.Second):
			t.Error("backend never exited after Shutdown")
		}
	}()
	select {
	case <-api.started:
	case <-time.After(2 * time.Second):
		t.Fatal("auth.status never started")
	}
	if err := be.SignOut(context.Background()); err != nil {
		t.Fatalf("SignOut without an engine handle: %v", err)
	}
	close(api.release)
	select {
	case <-api.logoutCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("queued sign-out never called auth.logout")
	}
	waitFor(t, 2*time.Second, func() bool {
		return updateCount(snapshot(), protocol.UpdSignedOut) == 1
	}, "SignedOut for queued request")
	if got := updateCount(snapshot(), protocol.UpdEngineStarted); got != 0 {
		t.Fatalf("engine started %d times after queued sign-out, want 0", got)
	}
}

func TestBackendRestoredSessionEmitsAccountBeforeEngineStart(t *testing.T) {
	_, _, _, snapshot := newControlledBackend(t, true, "saved@example.com")

	waitFor(t, 10*time.Second, func() bool {
		authOK, started := false, false
		for _, update := range snapshot() {
			switch update.Update {
			case protocol.UpdAuthOk:
				authOK = update.Email == "saved@example.com"
			case protocol.UpdEngineStarted:
				started = true
			}
		}
		return authOK && started
	}, "restored AuthOk with email followed by EngineStarted")
	if got := updateCount(snapshot(), protocol.UpdAuthRequired); got != 0 {
		t.Fatalf("AuthRequired count = %d, want 0 for restored session", got)
	}
}

func TestBackendSignOutThenLoginAgain(t *testing.T) {
	be, api, collect, snapshot := newControlledBackend(t, false, "")

	waitFor(t, 10*time.Second, func() bool {
		return updateCount(snapshot(), protocol.UpdAuthRequired) == 1
	}, "initial AuthRequired")
	if got := updateCount(snapshot(), protocol.UpdEngineStarted); got != 0 {
		t.Fatalf("engine started %d times while logged out, want 0", got)
	}

	first := newLoginAttempt("first@example.com")
	api.attempts <- first
	firstLoginDone := startTestLogin(be, collect)
	select {
	case <-first.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first auth.loginFork never started")
	}
	const firstURL = "https://account.proton.me/desktop/login?mock=first"
	api.authNotifications <- &sidecar.AuthSignInUrlNotification{URL: firstURL, UserCode: "FIRST-CODE"}
	waitFor(t, 2*time.Second, func() bool {
		for _, update := range snapshot() {
			if update.Update == protocol.UpdLoginUrl {
				return update.URL == firstURL && update.UserCode == "FIRST-CODE"
			}
		}
		return false
	}, "first LoginUrl before login completion")
	if got := updateCount(snapshot(), protocol.UpdAuthOk); got != 0 {
		t.Fatalf("AuthOk emitted %d times before browser login completed, want 0", got)
	}
	if got := updateCount(snapshot(), protocol.UpdEngineStarted); got != 0 {
		t.Fatalf("engine started %d times before browser login completed, want 0", got)
	}

	close(first.release)
	waitFor(t, 10*time.Second, func() bool {
		authOK, started := false, false
		for _, update := range snapshot() {
			switch update.Update {
			case protocol.UpdAuthOk:
				authOK = update.Email == "first@example.com"
			case protocol.UpdEngineStarted:
				started = true
			}
		}
		return authOK && started
	}, "first AuthOk with email and EngineStarted")
	select {
	case <-firstLoginDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first StartLogin did not return")
	}

	// Hold logout open: no signed-out/login updates or replacement engine may
	// race ahead while the old session is still being cleared.
	api.logoutRelease = make(chan struct{})
	signOutDone := make(chan error, 1)
	go func() { signOutDone <- be.SignOut(context.Background()) }()
	select {
	case <-api.logoutStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("auth.logout never started")
	}
	if got := updateCount(snapshot(), protocol.UpdSignedOut); got != 0 {
		t.Fatalf("SignedOut emitted %d times before logout completed, want 0", got)
	}
	if got := updateCount(snapshot(), protocol.UpdEngineStarted); got != 1 {
		t.Fatalf("EngineStarted count while logout pending = %d, want 1", got)
	}
	close(api.logoutRelease)
	select {
	case err := <-signOutDone:
		if err != nil {
			t.Fatalf("SignOut: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SignOut did not return")
	}
	waitFor(t, 10*time.Second, func() bool {
		return updateCount(snapshot(), protocol.UpdSignedOut) == 1 &&
			updateCount(snapshot(), protocol.UpdAuthRequired) == 2
	}, "SignedOut followed by a fresh AuthRequired")

	second := newLoginAttempt("second@example.com")
	api.attempts <- second
	secondLoginDone := startTestLogin(be, collect)
	select {
	case <-second.started:
	case <-time.After(2 * time.Second):
		t.Fatal("second auth.loginFork never started")
	}
	const secondURL = "https://account.proton.me/desktop/login?mock=second"
	api.authNotifications <- &sidecar.AuthSignInUrlNotification{URL: secondURL, UserCode: "SECOND-CODE"}
	waitFor(t, 2*time.Second, func() bool {
		for _, update := range snapshot() {
			if update.Update == protocol.UpdLoginUrl && update.URL == secondURL {
				return update.UserCode == "SECOND-CODE"
			}
		}
		return false
	}, "second LoginUrl before login completion")
	close(second.release)
	waitFor(t, 10*time.Second, func() bool {
		authOK, started := false, false
		for _, update := range snapshot() {
			switch update.Update {
			case protocol.UpdAuthOk:
				if update.Email == "second@example.com" {
					authOK = true
				}
			case protocol.UpdEngineStarted:
				started = updateCount(snapshot(), protocol.UpdEngineStarted) >= 2
			}
		}
		return authOK && started
	}, "second AuthOk with email and replacement EngineStarted")
	select {
	case <-secondLoginDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second StartLogin did not return")
	}
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
