// Package stubbackend is the Phase 2 skeleton engine backend: it implements
// the full session lifecycle and protocol surface, with the sync internals
// (sidecar, DB, watcher, VFS) stubbed as "not implemented yet". It exists so
// the socket protocol and shell integration are final and testable before
// the real engine lands incrementally (docs/research/go-engine-migration.md,
// steps 2–6).
package stubbackend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pds.dev/engine-go/internal/protocol"
)

// Backend is the skeleton EngineBackend.
type Backend struct {
	mu       sync.Mutex
	stopped  bool
	done     chan struct{}
	updating func(protocol.Update) // how updates reach the session
}

// New creates a skeleton backend. updates is called (from any goroutine) to
// emit protocol updates.
func New(updates func(protocol.Update)) *Backend {
	return &Backend{done: make(chan struct{}), updating: updates}
}

// Start reports the initial auth state like the real engine does at startup.
func (b *Backend) Start(ctx context.Context) {
	// Skeleton: no sidecar yet, so always report sign-in required.
	go b.updating(protocol.Update{Update: protocol.UpdAuthRequired})
}

// StartLogin runs the login flow; the skeleton emits the URL-ish updates with
// a clear not-implemented error, so the shell shows a truthful state.
func (b *Backend) StartLogin(ctx context.Context, updates chan<- protocol.Update) {
	updates <- protocol.Update{Update: protocol.UpdLoginStarted}
	updates <- protocol.Update{
		Update: protocol.UpdLoginFailed,
		Error:  "Go engine skeleton: login not implemented yet (Phase 2 step 3)",
	}
}

func (b *Backend) SignOut(ctx context.Context) error { return nil }

func (b *Backend) Pause() error  { return fmt.Errorf("not implemented yet (Phase 2 step 4)") }
func (b *Backend) Resume() error { return fmt.Errorf("not implemented yet (Phase 2 step 4)") }

func (b *Backend) SetCacheCap(cap *uint64) error {
	return fmt.Errorf("not implemented yet (Phase 2 step 4)")
}

func (b *Backend) SetMode(mode string) error {
	return fmt.Errorf("not implemented yet (Phase 2 step 4)")
}

func (b *Backend) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	b.stopped = true
	// Give in-flight update writes a moment before reporting done.
	time.AfterFunc(100*time.Millisecond, func() { close(b.done) })
}

func (b *Backend) Done() <-chan struct{} { return b.done }
