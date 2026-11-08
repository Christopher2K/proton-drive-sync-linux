// Package session is the top-level engine session state machine: it owns the
// control socket, tracks auth/login state, and (in this skeleton phase) emits
// protocol updates without doing real syncing. The engine internals (sidecar,
// DB, sync loop, VFS) plug in behind the Session interface boundary in later
// phases; the socket protocol and lifecycle are final from day one.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"pds.dev/engine-go/internal/protocol"
)

// EngineBackend is the sync engine behind the session. The skeleton ships a
// stub; the ported Rust engine (Phase 2 steps 4–6) implements it for real.
type EngineBackend interface {
	// StartLogin begins the fork-login flow, reporting progress via the
	// update channel.
	StartLogin(ctx context.Context, updates chan<- protocol.Update)
	// SignOut stops the engine and clears the session; the session loops
	// back to the auth-required state.
	SignOut(ctx context.Context) error
	// Pause freezes queues and event application.
	Pause() error
	// Resume drains tombstones (with guard), then uploads, then events.
	Resume() error
	// SetCacheCap applies a live cache cap change (nil = unlimited).
	SetCacheCap(cap *uint64) error
	// SetMode persists the new sync mode and restarts into it.
	SetMode(mode string) error
	// Shutdown stops the engine; the session then emits Exited.
	Shutdown()
	// Done closes when the backend has fully stopped.
	Done() <-chan struct{}
}

// Config configures a Session.
type Config struct {
	Root       string
	SocketPath string
}

// Session owns the Unix control socket and drives the backend through the
// auth/engine lifecycle, broadcasting protocol updates to every client.
type Session struct {
	cfg     Config
	backend EngineBackend

	mu      sync.Mutex
	clients map[net.Conn]chan protocol.Update // per-client outbound queue
	cancel  context.CancelFunc                // stops Run on Shutdown

	updates chan protocol.Update
	joined  chan struct{} // closed when broadcast loop exits

	log *log.Logger
}

// New creates a Session.
func New(cfg Config, backend EngineBackend) *Session {
	return &Session{
		cfg:     cfg,
		backend: backend,
		clients: make(map[net.Conn]chan protocol.Update),
		updates: make(chan protocol.Update, 256),
		joined:  make(chan struct{}),
		log:     log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime),
	}
}

// Run serves the control socket until Shutdown is commanded (or the parent
// context is canceled). It always removes the socket file on the way out.
func (s *Session) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	if dir := filepath.Dir(s.cfg.SocketPath); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create socket parent dir: %w", err)
		}
	}
	// Single-instance: if an engine already serves this socket, refuse.
	if c, err := net.Dial("unix", s.cfg.SocketPath); err == nil {
		c.Close()
		return fmt.Errorf("another engine is already listening on %s", s.cfg.SocketPath)
	}
	_ = os.Remove(s.cfg.SocketPath)

	ln, err := net.Listen("unix", s.cfg.SocketPath)
	if err != nil {
		return fmt.Errorf("bind control socket: %w", err)
	}
	defer func() {
		ln.Close()
		_ = os.Remove(s.cfg.SocketPath)
	}()

	s.log.Printf("listening on %s (root %s)", s.cfg.SocketPath, s.cfg.Root)

	// Broadcast loop: every update goes to every connected client.
	go func() {
		defer close(s.joined)
		for {
			select {
			case u := <-s.updates:
				s.broadcast(u)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Closing the listener is what actually unblocks Accept on shutdown.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var wg sync.WaitGroup
	// Backend lifecycle: when the backend stops (Shutdown command or
	// failure), tell every client, then tear the session down.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-s.backend.Done():
			s.broadcast(protocol.Update{Update: protocol.UpdExited, Reason: "shutdown"})
			time.Sleep(200 * time.Millisecond) // let client pumps flush
			s.mu.Lock()
			if s.cancel != nil {
				s.cancel()
			}
			s.mu.Unlock()
		case <-ctx.Done():
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				wg.Wait()
				<-s.joined
				return nil
			default:
			}
			s.log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveClient(ctx, conn)
		}()
	}
}

func (s *Session) broadcast(u protocol.Update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn, ch := range s.clients {
		select {
		case ch <- u:
		default: // slow client: drop rather than block the engine
			s.log.Printf("dropping update for slow client %v", conn.RemoteAddr())
		}
	}
}

func (s *Session) addClient(conn net.Conn) chan protocol.Update {
	ch := make(chan protocol.Update, 64)
	s.mu.Lock()
	s.clients[conn] = ch
	s.mu.Unlock()
	return ch
}

func (s *Session) removeClient(conn net.Conn) {
	s.mu.Lock()
	delete(s.clients, conn)
	s.mu.Unlock()
}

func (s *Session) serveClient(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	out := s.addClient(conn)
	defer s.removeClient(conn)

	// Outbound pump.
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for {
			select {
			case u := <-out:
				line, err := protocol.MarshalUpdate(u)
				if err != nil {
					continue
				}
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := conn.Write(append(line, '\n')); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	dec := json.NewDecoder(conn)
	shutdownSeen := false
	for {
		var cmd protocol.Command
		if err := dec.Decode(&cmd); err != nil {
			var netErr net.Error
			if err.Error() == "EOF" || errors.As(err, &netErr) {
				break
			}
			s.log.Printf("invalid command: %v", err)
			continue
		}
		if cmd.Cmd == "" {
			continue
		}
		if s.handleCommand(ctx, cmd) {
			shutdownSeen = true
			break
		}
	}
	<-pumpDone
	_ = shutdownSeen
}

// handleCommand dispatches one command; it returns true when the session
// should stop (Shutdown).
func (s *Session) handleCommand(ctx context.Context, cmd protocol.Command) bool {
	switch cmd.Cmd {
	case protocol.CmdStartLogin:
		go s.backend.StartLogin(ctx, s.updates)
	case protocol.CmdPause:
		_ = s.backend.Pause()
	case protocol.CmdResume:
		_ = s.backend.Resume()
	case protocol.CmdSignOut:
		if err := s.backend.SignOut(ctx); err != nil {
			s.log.Printf("sign out: %v", err)
		}
	case protocol.CmdSetCacheCap:
		cap, _ := cmd.NormalizedCacheCap()
		if err := s.backend.SetCacheCap(cap); err != nil {
			s.log.Printf("set cache cap: %v", err)
		}
	case protocol.CmdSetMode:
		mode := cmd.NormalizedMode()
		if !protocol.ValidMode(mode) {
			s.log.Printf("ignoring invalid SetMode value %q", mode)
			return false
		}
		if err := s.backend.SetMode(mode); err != nil {
			s.log.Printf("set mode: %v", err)
		}
	case protocol.CmdShutdown:
		s.backend.Shutdown() // Run's backend watcher emits Exited + cancels
		return true
	default:
		s.log.Printf("unknown command %q", cmd.Cmd)
	}
	return false
}

// Emit sends an update to all connected clients (used by the backend).
func (s *Session) Emit(u protocol.Update) {
	select {
	case s.updates <- u:
	default:
		s.log.Print("update channel full; dropping update")
	}
}
