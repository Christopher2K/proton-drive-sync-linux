// Package backend wires the ported sync engine (internal/engine) behind
// session.EngineBackend: it owns the placeholder-mode orchestration
// (SPEC F5 / AGENT.md M3.4) — layout resolution via mode.ResolveSessionLayout,
// FUSE probe before the engine starts, layout conversion before the mount,
// mount after the engine's first watcher attach, and session-loop restarts
// on SetMode/SignOut/Shutdown.
//
// Replaces the Phase 2 stubbackend in pds-engine-go when a sidecar is
// available. The engine is only ever driven through its exported surface;
// the store seam needs no adapter because *store.Store satisfies
// engine.StoreAPI structurally (sibling placeholder workstream).
package backend

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/mode"
	"pds.dev/engine-go/internal/protocol"
	"pds.dev/engine-go/internal/sidecar"
	storepkg "pds.dev/engine-go/internal/store"
)

// Config configures a Backend. Root is the sync root; the DB/backing paths
// are resolved per session via mode.ResolveSessionLayout (external state dir
// in placeholder mode, in-root .pds in plain mirror mode). CacheCapFlag is
// the explicit CLI flag choice (flag > env > persisted > 10GiB default).
type Config struct {
	Root         string
	CacheCapFlag mode.CapOpt
	// Hooks overrides the FUSE probe/mount seams (tests). Nil means the
	// platform defaults from defaultHooks.
	Hooks *Hooks
}

// Hooks are the testable seams for FUSE availability, mountpoint preflight,
// and mounting. Production defaults (Linux) probe with a throwaway
// temp-dir mount and mount the placeholder view at the sync root; other
// platforms report FUSE unavailable so the session falls back to mirror.
type Hooks struct {
	// Preflight refuses to start a placeholder session at an unusable
	// mountpoint (live/stale mount). Mirrors vfs.EnsurePlaceholderMountpoint.
	Preflight func(root string, timeout time.Duration) error
	// Probe reports whether FUSE is usable, proven by a served throwaway
	// mount. Called BEFORE the engine starts (single-consumer sidecar).
	Probe func() bool
	// Mount mounts the placeholder view at the sync root (after the engine
	// initial sync / watcher attach) and returns an idempotent unmount.
	Mount func(st *storepkg.Store, stateDir, backingDir, root string) (unmount func(), err error)
}

type loopExit int

const (
	exitNone loopExit = iota
	exitQuit
	exitSignOut
	exitModeSwitch
)

// Backend is the real EngineBackend over internal/engine.
type Backend struct {
	cfg      Config
	api      sidecar.API
	closeAPI func()
	log      *log.Logger
	updating func(protocol.Update)
	hooks    Hooks

	mu         sync.Mutex
	handle     *engine.Handle
	store      *storepkg.Store
	unmount    func()
	dbPath     string
	exitReason loopExit

	startOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once

	loginResults     chan string
	ctx              context.Context
	cancel           context.CancelFunc
	authWaiting      bool // guarded by mu
	loginRunning     bool // guarded by mu
	signOutRequested bool // guarded by mu
}

// New creates the backend. api is the sidecar connection (SidecarClient or
// mock); notifications are taken from it. updates is called from any
// goroutine to emit protocol updates.
func New(cfg Config, api sidecar.API, closeAPI func(), updates func(protocol.Update)) *Backend {
	hooks := defaultHooks()
	if cfg.Hooks != nil {
		hooks = *cfg.Hooks
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Backend{
		cfg:          cfg,
		api:          api,
		closeAPI:     closeAPI,
		log:          log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime),
		updating:     updates,
		hooks:        hooks,
		done:         make(chan struct{}),
		loginResults: make(chan string, 1),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start runs the engine session loop until Shutdown. Safe to call once.
func (b *Backend) Start(_ context.Context) {
	b.startOnce.Do(func() { go b.runLoop() })
}

func (b *Backend) closeDone() {
	b.doneOnce.Do(func() { close(b.done) })
}

func (b *Backend) runLoop() {
	defer b.closeDone()
	defer func() {
		if b.closeAPI != nil {
			b.closeAPI()
		}
	}()
	for {
		if b.takeSignOutRequest() {
			b.logout()
			continue
		}
		if !b.authenticate() || b.ctx.Err() != nil {
			return
		}
		// SignOut can arrive while auth.status is in flight, before an
		// engine handle exists. Honor it before starting a new session.
		if b.takeSignOutRequest() {
			b.logout()
			continue
		}
		switch b.runSession() {
		case exitModeSwitch:
			// The previous engine stopped consuming the sidecar channel;
			// the next engine resumes from the persisted cursor.
			continue
		case exitSignOut:
			b.logout()
		default:
			return
		}
	}
}

func (b *Backend) takeSignOutRequest() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	requested := b.signOutRequested
	b.signOutRequested = false
	return requested
}

// logout runs only between engine sessions. This ordering prevents a new
// engine from restoring the old account while auth.logout is still in flight.
func (b *Backend) logout() {
	b.takeSignOutRequest() // coalesce duplicate SignOut commands
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	err := b.api.Logout(ctx)
	cancel()
	if err != nil {
		// The sidecar clears its in-memory session even when server-side
		// logout fails, so it is safe to return to the login screen.
		b.log.Printf("sign out: %v", err)
	}
	b.updating(protocol.Update{Update: protocol.UpdSignedOut})
}

// authenticate gates every engine session on a saved or newly completed
// login. While logged out no engine exists, so StartLogin exclusively owns
// the sidecar notification stream and can forward auth.signInUrl immediately.
func (b *Backend) authenticate() bool {
	if b.ctx.Err() != nil {
		return false
	}

	ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
	st, err := b.api.AuthStatus(ctx)
	cancel()
	if err != nil {
		if b.ctx.Err() != nil {
			return false
		}
		b.log.Printf("auth.status: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: fmt.Sprintf("auth.status: %v", err)})
		return false
	}
	if b.ctx.Err() != nil {
		return false
	}
	if st.LoggedIn {
		email := "unknown account"
		if st.Email != nil && *st.Email != "" {
			email = *st.Email
		}
		b.updating(protocol.Update{Update: protocol.UpdAuthOk, Email: email})
		return true
	}

	b.mu.Lock()
	b.authWaiting = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.authWaiting = false
		b.mu.Unlock()
	}()
	b.updating(protocol.Update{Update: protocol.UpdAuthRequired})
	select {
	case email := <-b.loginResults:
		if b.ctx.Err() != nil {
			return false
		}
		if email == "" {
			email = "unknown account"
		}
		b.updating(protocol.Update{Update: protocol.UpdAuthOk, Email: email})
		return true
	case <-b.ctx.Done():
		return false
	}
}

// runSession is one engine session: resolve layout/mode, probe, convert,
// construct + run the engine, mount after the watcher, then unmount.
// It returns the exit reason the session loop should act on.
func (b *Backend) runSession() loopExit {
	if b.ctx.Err() != nil {
		return exitQuit
	}
	layout, err := mode.ResolveSessionLayout(b.cfg.Root)
	if err != nil {
		b.log.Printf("layout: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: fmt.Sprintf("layout: %v", err)})
		return exitQuit
	}
	if layout.AdoptedLegacyDB {
		b.updating(protocol.Update{Update: protocol.UpdLayoutMigrated})
	}
	wantPlaceholders := layout.EffectiveMode == mode.ModePlaceholders
	// Mountpoint preflight (TG-30): refuse a placeholder session at an
	// unusable mountpoint instead of syncing into a dead mount.
	if wantPlaceholders {
		if err := b.hooks.Preflight(layout.Root, 2*time.Second); err != nil {
			b.log.Printf("placeholder mountpoint unusable: %v", err)
			b.updating(protocol.Update{Update: protocol.UpdMountUnavailable, Error: err.Error()})
			return exitQuit
		}
	}
	// FUSE probe BEFORE the engine starts: the engine consumes the
	// sidecar's single notification stream, so a post-hoc mirror fallback
	// would be deaf to remote events. Probe failure downgrades THIS
	// session to mirror (the persisted mode stays placeholders).
	sessionPlaceholders := wantPlaceholders
	if wantPlaceholders && !b.hooks.Probe() {
		b.log.Print("FUSE unavailable; running mirror mode for this session")
		b.updating(protocol.Update{Update: protocol.UpdMountUnavailable, Error: "FUSE is not available on this system"})
		sessionPlaceholders = false
	}

	// One-time-per-direction local layout conversion (external backing
	// only). Runs before the engine opens the DB and before the mount.
	if layout.UseExternal {
		if d, err := db.Open(layout.DBPath); err != nil {
			b.log.Printf("layout conversion: open DB: %v (continuing)", err)
		} else {
			if err := mode.ConvertLayoutForSession(d, layout); err != nil {
				b.log.Printf("layout conversion failed; continuing: %v", err)
			}
			_ = d.Close()
		}
	}

	evictionCap := mode.ResolveCacheCap(
		b.cfg.CacheCapFlag,
		mode.CacheCapFromEnv(),
		mode.PersistedCacheCap(layout.DBPath),
		capPtr(mode.DefaultCacheCap),
	)
	cfg := engine.DefaultConfig()
	cfg.Root = layout.Root
	cfg.DBPath = layout.DBPath
	cfg.BackingDir = layout.BackingDir
	cfg.EvictionCap = evictionCap
	cfg.Placeholders = sessionPlaceholders
	cfg.WatchLocalWrites = !sessionPlaceholders

	var st *storepkg.Store
	var storeAPI engine.StoreAPI
	if layout.EffectiveMode == mode.ModePlaceholders {
		st = storepkg.New()
		storeAPI = st
	}
	eng, handle, err := engine.New(b.api, b.notifications(), storeAPI, cfg)
	if err != nil {
		b.log.Printf("engine init: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: fmt.Sprintf("engine init: %v", err)})
		return exitQuit
	}
	b.mu.Lock()
	b.handle = handle
	b.store = st
	b.dbPath = layout.DBPath
	b.exitReason = exitNone
	if b.signOutRequested {
		b.signOutRequested = false
		b.exitReason = exitSignOut
	}
	stop := b.exitReason == exitSignOut
	b.mu.Unlock()
	// Shutdown/SignOut may have arrived after authenticate returned but
	// before the handle was published. Do not leave that race running a
	// new engine.
	if stop || b.ctx.Err() != nil {
		handle.Shutdown()
	}

	engineErr := make(chan error, 1)
	go func() { engineErr <- eng.Run() }()
	// Mount only AFTER the engine initial sync: its first-run scan and the
	// layout conversion above must see the real folder. WaitForWatcher
	// flips after that.
	if sessionPlaceholders {
		if !handle.WaitForWatcher(60 * time.Second) {
			b.log.Print("engine readiness timed out; mounting the placeholder view anyway")
		}
		select {
		case err := <-engineErr:
			// Engine already exited (fatal init): do not mount.
			return b.finishSession(handle, err, true)
		default:
		}
		unmount, merr := b.hooks.Mount(st, layout.StateDir, layout.BackingDir, layout.Root)
		if merr != nil {
			failure := merr.Error()
			if perr := b.hooks.Preflight(layout.Root, time.Second); perr != nil {
				failure = perr.Error()
			}
			b.log.Printf("placeholder mount failed after the FUSE probe: %v", merr)
			b.updating(protocol.Update{Update: protocol.UpdMountUnavailable, Error: failure})
			handle.Shutdown()
			err := <-engineErr
			return b.finishSession(handle, err, true)
		}
		b.mu.Lock()
		b.unmount = unmount
		b.mu.Unlock()
	}
	b.updating(protocol.Update{Update: protocol.UpdEngineStarted})

	// Per-session pumps: activity + status broadcasts.
	sessionDone := make(chan struct{})
	defer func() {
		close(sessionDone)
		b.mu.Lock()
		b.handle = nil
		b.store = nil
		b.unmount = nil
		b.mu.Unlock()
	}()
	go b.pumpActivity(handle, sessionDone)
	go b.pumpStatus(handle, sessionDone)

	err = <-engineErr
	if err != nil && b.currentExit() == exitNone {
		b.log.Printf("engine stopped: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: err.Error()})
	}
	return b.finishSession(handle, err, false)
}

// finishSession unmounts the placeholder view, clears the current handle,
// and reports the exit reason. alreadyDrained means engineErr was consumed.
func (b *Backend) finishSession(_ *engine.Handle, _ error, _ bool) loopExit {
	b.mu.Lock()
	unmount := b.unmount
	reason := b.exitReason
	if reason == exitNone {
		reason = exitQuit
	}
	// Reset for the next session; the deferred clear in runSession also
	// nils handle/store, but unmount must run exactly once here.
	b.unmount = nil
	b.exitReason = exitNone
	b.mu.Unlock()
	if unmount != nil {
		b.log.Print("unmounting placeholder view")
		unmount()
	}
	return reason
}

func (b *Backend) currentExit() loopExit {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exitReason
}

func (b *Backend) pumpActivity(handle *engine.Handle, done <-chan struct{}) {
	for {
		select {
		case item := <-handle.Activity():
			b.updating(protocol.Update{
				Update: protocol.UpdActivity,
				Path:   item.Path,
				Detail: fmt.Sprintf("%s: %s", item.Kind, item.Detail),
				AtUnix: item.AtUnix,
			})
		case <-done:
			return
		}
	}
}

type statusHandle interface {
	Status() (engine.Status, error)
}

func (b *Backend) pumpStatus(handle statusHandle, done <-chan struct{}) {
	b.pumpStatusEvery(handle, done, 2*time.Second)
}

func (b *Backend) pumpStatusEvery(handle statusHandle, done <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			st, err := handle.Status()
			if err != nil {
				// Initial sync and large transfers can occupy the engine loop
				// longer than Status's timeout. Keep polling: a busy engine is
				// still running and later snapshots must reach the UI.
				continue
			}
			b.updating(protocol.Update{
				Update:             protocol.UpdStatus,
				State:              string(st.State),
				SyncedNodes:        st.SyncedNodes,
				DirtyNodes:         st.DirtyNodes,
				QueuedTombstones:   st.QueuedTombstones,
				ErroredNodes:       st.ErroredNodes,
				ConflictedNodes:    st.ConflictedNodes,
				Mode:               st.Mode.Label(),
				CacheCap:           st.CacheCap,
				CacheUsed:          st.CacheUsed,
				NeedsSignIn:        st.NeedsSignIn,
				SidecarUnavailable: st.SidecarUnavailable,
			})
		case <-done:
			return
		}
	}
}

func capPtr(v uint64) *uint64 { return &v }

func (b *Backend) notifications() <-chan sidecar.Notification {
	type notifier interface {
		Notifications() <-chan sidecar.Notification
	}
	if c, ok := b.api.(notifier); ok {
		return c.Notifications()
	}
	return nil
}

func (b *Backend) currentHandle() (*engine.Handle, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.handle == nil {
		return nil, fmt.Errorf("engine not running")
	}
	return b.handle, nil
}

// StartLogin begins the fork-login flow. The RPC resolves only after the
// browser flow completes; auth.signInUrl arrives separately and must be
// forwarded while that RPC is still pending.
func (b *Backend) StartLogin(ctx context.Context, updates chan<- protocol.Update) {
	b.mu.Lock()
	if !b.authWaiting || b.loginRunning {
		b.mu.Unlock()
		return
	}
	b.loginRunning = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.loginRunning = false
		b.mu.Unlock()
	}()

	updates <- protocol.Update{Update: protocol.UpdLoginStarted}
	type loginResult struct {
		email string
		err   error
	}
	result := make(chan loginResult, 1)
	go func() {
		email, err := b.api.LoginFork(ctx)
		result <- loginResult{email: email, err: err}
	}()

	notifications := b.notifications()
	for {
		select {
		case res := <-result:
			if res.err != nil {
				updates <- protocol.Update{Update: protocol.UpdLoginFailed, Error: res.err.Error()}
				return
			}
			// Close the login window before waking runSession. A second
			// StartLogin must never overlap the new engine and compete for
			// its single notification stream.
			b.mu.Lock()
			if !b.authWaiting {
				b.mu.Unlock()
				return
			}
			b.authWaiting = false
			b.mu.Unlock()
			select {
			case b.loginResults <- res.email:
			case <-b.ctx.Done():
			}
			return
		case n, ok := <-notifications:
			if !ok {
				updates <- protocol.Update{Update: protocol.UpdLoginFailed, Error: "sidecar connection lost during sign-in"}
				return
			}
			if signIn, ok := n.(*sidecar.AuthSignInUrlNotification); ok {
				updates <- protocol.Update{
					Update:   protocol.UpdLoginUrl,
					URL:      signIn.URL,
					UserCode: signIn.UserCode,
				}
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// SignOut stops the engine. runLoop performs Logout after the session has
// fully stopped, then reports SignedOut and waits for the next StartLogin.
func (b *Backend) SignOut(_ context.Context) error {
	b.mu.Lock()
	h := b.handle
	if h == nil {
		// Mode switches and auth checks have a handle-less interval. Queue
		// the request so runLoop performs it at the next safe boundary.
		b.signOutRequested = true
	} else if b.exitReason == exitNone {
		b.exitReason = exitSignOut
	}
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
	}
	return nil
}

func (b *Backend) Pause() error {
	h, err := b.currentHandle()
	if err != nil {
		return err
	}
	h.Pause()
	return nil
}

func (b *Backend) Resume() error {
	h, err := b.currentHandle()
	if err != nil {
		return err
	}
	h.Resume()
	return nil
}

func (b *Backend) SetCacheCap(cap *uint64) error {
	h, err := b.currentHandle()
	if err != nil {
		return err
	}
	return h.SetEvictionCap(cap)
}

// SetMode persists the new sync mode, emits ModeSwitched, and restarts
// into it via the backend session loop (the next runSession re-resolves
// the layout and runs ConvertLayout before the mount).
func (b *Backend) SetMode(m string) error {
	var sm mode.SyncMode
	switch m {
	case protocol.ModeMirror:
		sm = mode.ModeMirror
	case protocol.ModePlaceholders:
		sm = mode.ModePlaceholders
	default:
		return fmt.Errorf("unknown mode %q", m)
	}
	b.mu.Lock()
	dbPath := b.dbPath
	b.mu.Unlock()
	if dbPath == "" {
		layout, err := mode.ResolveSessionLayout(b.cfg.Root)
		if err != nil {
			return err
		}
		dbPath = layout.DBPath
	}
	d, err := db.Open(dbPath)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := mode.SetSyncMode(d, sm); err != nil {
		return err
	}
	b.updating(protocol.Update{Update: protocol.UpdModeSwitched, Mode: m})
	b.mu.Lock()
	h := b.handle
	if h != nil && b.exitReason == exitNone {
		b.exitReason = exitModeSwitch
	}
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
	}
	return nil
}

func (b *Backend) Shutdown() {
	b.cancel()
	b.mu.Lock()
	h := b.handle
	if h != nil && b.exitReason == exitNone {
		b.exitReason = exitQuit
	}
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
	}
}

func (b *Backend) Done() <-chan struct{} { return b.done }
