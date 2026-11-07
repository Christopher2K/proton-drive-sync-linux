// Package backend wires the ported sync engine (internal/engine) behind
// session.EngineBackend: it spawns (or dials) the sidecar, constructs the
// engine, runs it, and translates engine state/activity into protocol
// updates. Replaces the Phase 2 stubbackend in pds-engine-go when a
// sidecar is available.
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
)

// Backend is the real EngineBackend over internal/engine.
type Backend struct {
	cfg      engine.Config
	api      sidecar.API
	closeAPI func()
	log      *log.Logger
	updating func(protocol.Update)

	mu      sync.Mutex
	handle  *engine.Handle
	started bool

	startOnce sync.Once
	done      chan struct{}
}

// New creates the backend. api is the sidecar connection (SidecarClient or
// mock); notifications are taken from it. updates is called from any
// goroutine to emit protocol updates.
func New(cfg engine.Config, api sidecar.API, closeAPI func(), updates func(protocol.Update)) *Backend {
	return &Backend{
		cfg:      cfg,
		api:      api,
		closeAPI: closeAPI,
		log:      log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime),
		updating: updates,
		done:     make(chan struct{}),
	}
}

// Start runs the engine until Shutdown. Safe to call once.
func (b *Backend) Start(ctx context.Context) {
	b.startOnce.Do(func() { go b.run() })
}

func (b *Backend) run() {
	defer close(b.done)
	notifications := b.notifications()
	eng, handle, err := engine.New(b.api, notifications, nil, b.cfg)
	if err != nil {
		b.log.Printf("engine init: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: fmt.Sprintf("engine init: %v", err)})
		return
	}
	b.mu.Lock()
	b.handle = handle
	b.started = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.handle = nil
		b.mu.Unlock()
	}()

	b.updating(protocol.Update{Update: protocol.UpdEngineStarted})
	// Pump engine activity into protocol Activity updates.
	go func() {
		for {
			select {
			case item := <-handle.Activity():
				b.updating(protocol.Update{
					Update: protocol.UpdActivity,
					Path:   item.Path,
					Detail: fmt.Sprintf("%s: %s", item.Kind, item.Detail),
					AtUnix: item.AtUnix,
				})
			case <-b.done:
				return
			}
		}
	}()
	// Periodic status broadcasts.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
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
				NeedsSignIn:        st.NeedsSignIn,
				SidecarUnavailable: st.SidecarUnavailable,
			})
		}
	}()

	if err := eng.Run(); err != nil {
		b.log.Printf("engine stopped: %v", err)
		b.updating(protocol.Update{Update: protocol.UpdFailed, Error: err.Error()})
	}
	if b.closeAPI != nil {
		b.closeAPI()
	}
}

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
	if !b.started || b.handle == nil {
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

// SignOut stops the engine and clears the session.
func (b *Backend) SignOut(ctx context.Context) error {
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

// SetMode persists the new sync mode; a restart into it is required, so
// this reports the pending change (the Rust engine restarts itself; the Go
// session keeps the same lifecycle contract as the stub for now).
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
	d, err := db.Open(b.cfg.DBPath)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := mode.SetSyncMode(d, sm); err != nil {
		return err
	}
	b.updating(protocol.Update{Update: protocol.UpdModeSwitched, Mode: m})
	return nil
}

func (b *Backend) Shutdown() {
	b.mu.Lock()
	h := b.handle
	b.mu.Unlock()
	if h != nil {
		h.Shutdown()
		return
	}
	// Never started: close done so the session can exit.
	select {
	case <-b.done:
	default:
		close(b.done)
	}
}

func (b *Backend) Done() <-chan struct{} { return b.done }
