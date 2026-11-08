package session

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pds.dev/engine-go/internal/protocol"
)

// stubBackend is a scripted backend for protocol tests.
type stubBackend struct {
	started   chan struct{}
	loginOnce chan struct{}
	signOut   chan struct{}
	pause     chan struct{}
	resume    chan struct{}
	cacheCap  chan *uint64
	setMode   chan string
	shutdown  chan struct{}
	done      chan struct{}
}

func newStubBackend() *stubBackend {
	return &stubBackend{
		started:   make(chan struct{}, 4),
		loginOnce: make(chan struct{}, 4),
		signOut:   make(chan struct{}, 4),
		pause:     make(chan struct{}, 4),
		resume:    make(chan struct{}, 4),
		cacheCap:  make(chan *uint64, 4),
		setMode:   make(chan string, 4),
		shutdown:  make(chan struct{}, 4),
		done:      make(chan struct{}),
	}
}

func (b *stubBackend) StartLogin(ctx context.Context, updates chan<- protocol.Update) {
	updates <- protocol.Update{Update: protocol.UpdLoginStarted}
	updates <- protocol.Update{Update: protocol.UpdLoginUrl, URL: "https://account.example/login", UserCode: "ABCD-EFGH"}
	b.loginOnce <- struct{}{}
}
func (b *stubBackend) SignOut(ctx context.Context) error {
	b.signOut <- struct{}{}
	return nil
}
func (b *stubBackend) Pause() error  { b.pause <- struct{}{}; return nil }
func (b *stubBackend) Resume() error { b.resume <- struct{}{}; return nil }
func (b *stubBackend) SetCacheCap(cap *uint64) error {
	b.cacheCap <- cap
	return nil
}
func (b *stubBackend) SetMode(mode string) error { b.setMode <- mode; return nil }
func (b *stubBackend) Shutdown() {
	b.shutdown <- struct{}{}
	close(b.done)
}
func (b *stubBackend) Done() <-chan struct{} { return b.done }

func waitCh[T any](t *testing.T, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return *new(T)
	}
}

// Full lifecycle over a real Unix socket: connect, command, receive updates,
// shutdown — the same exchange mygo-shell performs.
func TestSessionLifecycle(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "engine.sock")
	root := t.TempDir()
	backend := newStubBackend()
	sess := New(Config{Root: root, SocketPath: socket}, backend)

	served := make(chan error, 1)
	go func() { served <- sess.Run(context.Background()) }()

	// Wait for the socket to appear.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// StartLogin → expect LoginStarted + LoginUrl lines.
	if _, err := conn.Write([]byte(`{"cmd":"StartLogin"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(conn)
	nextUpdate := func() protocol.Update {
		t.Helper()
		if !lines.Scan() {
			t.Fatalf("no more updates: %v", lines.Err())
		}
		var u protocol.Update
		if err := json.Unmarshal(lines.Bytes(), &u); err != nil {
			t.Fatalf("bad update line %q: %v", lines.Text(), err)
		}
		return u
	}
	assertUpdate := func(want string) {
		t.Helper()
		for {
			if got := nextUpdate(); got.Update == want {
				return
			}
		}
	}
	assertUpdate(protocol.UpdLoginStarted)
	assertUpdate(protocol.UpdLoginUrl)
	waitCh(t, backend.loginOnce, "StartLogin")

	// Commands the shell sends that the Rust engine silently drops must work.
	if _, err := conn.Write([]byte(`{"cmd":"SetMode","mode":"mirror"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if mode := waitCh(t, backend.setMode, "SetMode"); mode != "mirror" {
		t.Errorf("SetMode = %q, want mirror", mode)
	}
	capVal := uint64(5 << 30)
	if _, err := conn.Write([]byte(`{"cmd":"SetCacheCap","cap":5368709120}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if got := waitCh(t, backend.cacheCap, "SetCacheCap"); got == nil || *got != capVal {
		t.Errorf("SetCacheCap = %v, want %d", got, capVal)
	}

	if _, err := conn.Write([]byte(`{"cmd":"Pause"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	waitCh(t, backend.pause, "Pause")
	if _, err := conn.Write([]byte(`{"cmd":"Resume"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	waitCh(t, backend.resume, "Resume")

	// Shutdown → Exited line + clean socket removal.
	if _, err := conn.Write([]byte(`{"cmd":"Shutdown"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	waitCh(t, backend.shutdown, "Shutdown")
	assertUpdate(protocol.UpdExited)

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("session.Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop after Shutdown")
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("socket file not cleaned up: %v", err)
	}
}

// A second engine must refuse to start while the first holds the socket.
func TestSingleInstance(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "engine.sock")
	backend := newStubBackend()
	sess := New(Config{Root: t.TempDir(), SocketPath: socket}, backend)
	go sess.Run(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}

	dup := New(Config{Root: t.TempDir(), SocketPath: socket}, newStubBackend())
	if err := dup.Run(context.Background()); err == nil {
		t.Fatal("second session should refuse to start")
	}
}

// Invalid SetMode values are ignored, matching the Rust engine's warn+ignore.
func TestInvalidModeIgnored(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "engine.sock")
	backend := newStubBackend()
	sess := New(Config{Root: t.TempDir(), SocketPath: socket}, backend)
	go sess.Run(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(`{"cmd":"SetMode","mode":"bogus"}` + "\n")); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-backend.setMode:
		t.Errorf("invalid mode reached backend: %q", m)
	case <-time.After(300 * time.Millisecond):
		// expected: ignored
	}
}
