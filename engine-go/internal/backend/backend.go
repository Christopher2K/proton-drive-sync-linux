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
}

// New creates the backend. api is the sidecar connection (SidecarClient or
// mock); notifications are taken from it. updates is called from any
// goroutine to emit protocol updates.
func New(cfg Config, api sidecar.API, closeAPI func(), updates func(protocol.Update)) *Backend {
	hooks := defaultHooks()
	if cfg.Hooks != nil {
		hooks = *cfg.Hooks
	}
	return &Backend{
		cfg:      cfg,
		api:      api,
		closeAPI: closeAPI,
		log:      log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime),
		updating: updates,
		hooks:    hooks,
		done:     make(chan struct{}),
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
	for {
		switch b.runSession() {
		case exitModeSwitch, exitSignOut:
			// Restart into the new mode (or still-logged-out state).
			// The sidecar channel is reused: the previous engine
			// stopped consuming it (EventsStop) and the next engine
			// resumes from the persisted cursor.
			continue
		default:
			if b.closeAPI != nil {
				b.closeAPI()
			}
			return
		}
	}
}

// runSession is one engine session: resolve layout/mode, probe, convert,
// construct + run the engine, mount after the watcher, then unmount.
// It returns the exit reason the session loop should act on.
func (b *Backend) runSession() loopExit {
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
	b.mu.Unlock()

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

func (b *Backend) pumpStatus(handle *engine.Handle, done <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			st, err := handle.Status()
			if err != nil {
				return
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

// StartLogin begins the fork-login flow.
func (b *Backend) StartLogin(ctx context.Context, updates chan<- protocol.Update) {
	updates <- protocol.Update{Update: protocol.UpdLoginStarted}
	url, err := b.api.LoginFork(ctx)
	if err != nil {
		updates <- protocol.Update{Update: protocol.UpdLoginFailed, Error: err.Error()}
		return
	}
	updates <- protocol.Update{Update: protocol.UpdLoginUrl, URL: url}
	// Completion arrives as an auth.signInUrl notification consumed by the
	// engine; poll auth status until it flips.
	go func() {
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			st, err := b.api.AuthStatus(sctx)
			cancel()
			if err == nil && st.LoggedIn {
				updates <- protocol.Update{Update: protocol.UpdAuthOk}
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
}

// SignOut stops the engine (unmounting via the session loop), logs out,
// and reports SignedOut. The session loop restarts into the (now logged-
// out) state, so the socket stays alive for the next StartLogin.
func (b *Backend) SignOut(ctx context.Context) error {
	b.mu.Lock()
	h := b.handle
	if h != nil && b.exitReason == exitNone {
		b.exitReason = exitSignOut
	}
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
	}
	if err := b.api.Logout(ctx); err != nil {
		return err
	}
	b.updating(protocol.Update{Update: protocol.UpdSignedOut})
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
	b.mu.Lock()
	h := b.handle
	if h != nil && b.exitReason == exitNone {
		b.exitReason = exitQuit
	}
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
		return
	}
	// Not started (or between sessions): close done so the session can exit.
	b.closeDone()
}

func (b *Backend) Done() <-chan struct{} { return b.done }
