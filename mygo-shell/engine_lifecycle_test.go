package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEngineProcessHelper turns the test binary into a controllable engine.
// The wrapper script made by newFakeEngineApp passes the shell's normal engine
// arguments after --, so no external socket tools or production engine are used.
func TestEngineProcessHelper(t *testing.T) {
	if os.Getenv("PDS_TEST_ENGINE_ACTIVE") != "1" {
		return
	}

	switch os.Getenv("PDS_TEST_ENGINE_MODE") {
	case "early-exit":
		_, _ = fmt.Fprintln(os.Stderr, "sidecar setup failed: missing account key")
		os.Exit(23)
	case "crash-counter":
		f, err := os.OpenFile(os.Getenv("PDS_TEST_ENGINE_COUNT"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(24)
		}
		_, _ = f.WriteString("start\n")
		_ = f.Close()
		_, _ = fmt.Fprintln(os.Stderr, "engine crashed")
		os.Exit(2)
	case "failed-then-exit":
		serveEngineSocket(t, func(conn net.Conn) {
			_, _ = fmt.Fprintln(conn, `{"update":"Failed","error":"sidecar key unlock failed"}`)
			_, _ = fmt.Fprintln(conn, `{"update":"Exited","reason":"backend stopped"}`)
			_, _ = fmt.Fprintln(os.Stderr, "detailed key unlock diagnostic")
		})
		os.Exit(7)
	case "socket":
		serveEngineSocket(t, func(conn net.Conn) {
			_, _ = fmt.Fprintln(conn, `{"update":"EngineStarted"}`)
			scanner := bufio.NewScanner(conn)
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), `"Shutdown"`) {
					return
				}
			}
		})
		os.Exit(0)
	case "run":
		if err := os.WriteFile(os.Getenv("PDS_TEST_ENGINE_READY"), []byte("ready"), 0o600); err != nil {
			os.Exit(25)
		}
		for {
			time.Sleep(time.Second)
		}
	default:
		os.Exit(26)
	}
}

// serveEngineSocket accepts one shell connection and lets fn drive its brief
// protocol exchange. It returns only after the helper's socket is closed.
func serveEngineSocket(t *testing.T, fn func(net.Conn)) {
	t.Helper()
	var socket string
	args := os.Args
	for i, arg := range args {
		if arg == "--socket" && i+1 < len(args) {
			socket = args[i+1]
		}
	}
	if socket == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing --socket")
		os.Exit(27)
	}
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(28)
	}
	defer listener.Close()
	conn, err := listener.Accept()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(29)
	}
	fn(conn)
	_ = conn.Close()
}

func newFakeEngineApp(t *testing.T, mode string) *app {
	t.Helper()
	dir := t.TempDir()
	wrapper := filepath.Join(dir, "fake-engine")
	script := "#!/bin/sh\nexec \"$PDS_TEST_ENGINE_BINARY\" -test.run=^TestEngineProcessHelper$ -- \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake engine: %v", err)
	}
	t.Setenv("PDS_ENGINE_BIN", wrapper)
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("PDS_TEST_ENGINE_BINARY", os.Args[0])
	t.Setenv("PDS_TEST_ENGINE_ACTIVE", "1")
	t.Setenv("PDS_TEST_ENGINE_MODE", mode)
	t.Setenv("PDS_TEST_ENGINE_COUNT", filepath.Join(dir, "starts"))
	t.Setenv("PDS_TEST_ENGINE_READY", filepath.Join(dir, "ready"))

	a := newApp()
	t.Cleanup(a.stopEngine)
	return a
}

func waitForEngineCondition(t *testing.T, a *app, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		ok := condition()
		a.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	t.Fatalf("timed out waiting for %s; phase=%v running=%v fatal=%q retry=%v", description, a.phase, a.engineRunning, a.fatal, a.engineRetryPending)
}

func TestEngineEarlyExitIsReapedAndReportsStderr(t *testing.T) {
	a := newFakeEngineApp(t, "early-exit")
	a.startEngine()

	waitForEngineCondition(t, a, "early exit", func() bool {
		return !a.engineRunning && a.phase == phaseFailed
	})
	a.mu.Lock()
	fatal := a.fatal
	session := a.engine
	retryPending := a.engineRetryPending
	a.mu.Unlock()
	if session != nil {
		t.Error("exited engine session is still attached")
	}
	if !strings.Contains(fatal, "exit status 23") || !strings.Contains(fatal, "missing account key") {
		t.Fatalf("early exit error lacks concrete cause: %q", fatal)
	}
	if strings.Contains(fatal, "Could not connect to engine control socket") {
		t.Fatalf("socket error hid early process exit: %q", fatal)
	}
	if !retryPending {
		t.Error("unexpected early exit did not schedule a retry")
	}
}

func TestEngineFailedUpdateSurvivesProcessExit(t *testing.T) {
	a := newFakeEngineApp(t, "failed-then-exit")
	a.startEngine()

	waitForEngineCondition(t, a, "failed engine exit", func() bool {
		return !a.engineRunning && a.phase == phaseFailed
	})
	a.mu.Lock()
	fatal := a.fatal
	retryPending := a.engineRetryPending
	a.mu.Unlock()
	if !strings.Contains(fatal, "sidecar key unlock failed") {
		t.Fatalf("specific Failed update was overwritten: %q", fatal)
	}
	if !retryPending {
		t.Error("Exited socket update prevented process waiter from scheduling retry")
	}
}

func TestEngineAutomaticallyRestartsUnexpectedExit(t *testing.T) {
	a := newFakeEngineApp(t, "crash-counter")
	a.startEngine()

	waitForEngineCondition(t, a, "automatic restart", func() bool {
		data, err := os.ReadFile(os.Getenv("PDS_TEST_ENGINE_COUNT"))
		return err == nil && bytes.Count(data, []byte("start\n")) >= 2
	})
	a.stopEngine()
	data, err := os.ReadFile(os.Getenv("PDS_TEST_ENGINE_COUNT"))
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(data, []byte("start\n")); got < 2 {
		t.Fatalf("engine starts = %d, want at least 2", got)
	}
}

func TestEngineStopKillsAndReapsWithoutControlSocket(t *testing.T) {
	a := newFakeEngineApp(t, "run")
	a.startEngine()
	waitForEngineCondition(t, a, "helper readiness", func() bool {
		_, err := os.Stat(os.Getenv("PDS_TEST_ENGINE_READY"))
		return err == nil && a.engine != nil
	})

	a.mu.Lock()
	generation := a.engineGeneration
	a.mu.Unlock()
	a.stopEngine()

	a.mu.Lock()
	running := a.engineRunning
	session := a.engine
	pending := a.engineRetryPending
	a.mu.Unlock()
	if running || session != nil || pending {
		t.Fatalf("stop left lifecycle state: running=%v session=%p retry=%v", running, session, pending)
	}
	// Even a previously queued callback for the old generation is inert.
	a.startEngineRetry(generation)
	a.mu.Lock()
	running = a.engineRunning
	a.mu.Unlock()
	if running {
		t.Error("stale retry restarted a stopped engine")
	}
}

func TestEngineGracefulShutdownDoesNotRetry(t *testing.T) {
	a := newFakeEngineApp(t, "socket")
	a.startEngine()
	waitForEngineCondition(t, a, "engine startup", func() bool {
		return a.phase == phaseRunning && a.conn != nil
	})

	a.stopEngine()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.engineRunning || a.engine != nil || a.engineRetryPending {
		t.Fatalf("intentional shutdown scheduled work: running=%v engine=%p retry=%v", a.engineRunning, a.engine, a.engineRetryPending)
	}
}

func TestEngineRetryBackoffAndCap(t *testing.T) {
	for attempt, want := range []string{"1s", "2s", "4s", "8s", "16s"} {
		t.Run(want, func(t *testing.T) {
			a := newApp()
			a.engineGeneration = 1
			a.engineRunning = true
			a.engineRetryAttempt = attempt
			session := &engineSession{generation: 1, done: make(chan struct{}), stderr: &boundedTail{}}
			a.engine = session
			a.engineExited(session)
			defer a.stopEngine()

			a.mu.Lock()
			defer a.mu.Unlock()
			if !a.engineRetryPending || a.engineRetryAttempt != attempt+1 || a.engineRetryIn != want {
				t.Fatalf("retry = pending %v attempt %d delay %q, want true/%d/%q", a.engineRetryPending, a.engineRetryAttempt, a.engineRetryIn, attempt+1, want)
			}
		})
	}

	t.Run("attempt cap", func(t *testing.T) {
		a := newApp()
		a.engineGeneration = 1
		a.engineRunning = true
		a.engineRetryAttempt = engineMaxAutoRestarts
		session := &engineSession{generation: 1, done: make(chan struct{}), stderr: &boundedTail{}}
		a.engine = session
		a.engineExited(session)

		a.mu.Lock()
		defer a.mu.Unlock()
		if a.engineRetryPending || !a.engineRetriesExhausted {
			t.Fatalf("retry cap not exposed: pending=%v exhausted=%v", a.engineRetryPending, a.engineRetriesExhausted)
		}
	})
}

func TestEngineIntentionalTransitionsDoNotRetry(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*app, *engineSession)
	}{
		{"quit", func(a *app, _ *engineSession) { a.quitting = true }},
		{"sign out", func(a *app, _ *engineSession) { a.signOut = signOutInProgress }},
		{"folder change", func(a *app, _ *engineSession) { a.folderChanging = true }},
		{"mode switch", func(a *app, _ *engineSession) { a.modeSwitch = modeSwitchInProgress }},
		{"intentional session", func(_ *app, s *engineSession) { s.intentional = true }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newApp()
			a.engineGeneration = 1
			a.engineRunning = true
			session := &engineSession{generation: 1, done: make(chan struct{}), stderr: &boundedTail{}}
			a.engine = session
			tc.setup(a, session)
			a.engineExited(session)

			a.mu.Lock()
			defer a.mu.Unlock()
			if a.engineRetryPending || a.engineRetriesExhausted {
				t.Fatalf("intentional exit scheduled retry: pending=%v exhausted=%v", a.engineRetryPending, a.engineRetriesExhausted)
			}
		})
	}
}

func TestStaleEngineCallbacksCannotAffectNewSession(t *testing.T) {
	a := newApp()
	a.phase = phaseRunning
	a.engineGeneration = 2
	newSession := &engineSession{generation: 2, done: make(chan struct{}), stderr: &boundedTail{}}
	a.engine = newSession
	a.engineRunning = true
	oldSession := &engineSession{
		generation: 1,
		done:       make(chan struct{}),
		waitErr:    errors.New("old exit"),
		stderr:     &boundedTail{},
	}

	a.engineExited(oldSession)
	a.applyUpdateForGeneration(1, engineUpdate{Update: "Failed", Error: "old failure"})
	a.startEngineRetry(1)

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.engine != newSession || !a.engineRunning || a.phase != phaseRunning || a.fatal != "" {
		t.Fatalf("stale callback changed new session: engine=%p running=%v phase=%v fatal=%q", a.engine, a.engineRunning, a.phase, a.fatal)
	}
}

func TestEngineExitedDoesNotOverwriteFailed(t *testing.T) {
	a := newApp()
	a.phase = phaseRunning
	a.applyUpdate(engineUpdate{Update: "Failed", Error: "concrete engine failure"})
	a.applyUpdate(engineUpdate{Update: "Exited", Reason: "disconnected"})
	if a.fatal != "concrete engine failure" {
		t.Fatalf("fatal = %q, want concrete failure", a.fatal)
	}
}

func TestBoundedStderrTailAndForwarding(t *testing.T) {
	tail := &boundedTail{limit: 8}
	var parent bytes.Buffer
	writer := &engineStderrWriter{parent: &parent, tail: tail}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("defghXYZ")); err != nil {
		t.Fatal(err)
	}
	if got := parent.String(); got != "abcdefghXYZ" {
		t.Fatalf("forwarded stderr = %q", got)
	}
	if got := tail.String(); got != "defghXYZ" {
		t.Fatalf("bounded tail = %q, want %q", got, "defghXYZ")
	}
	if len(tail.data) > tail.limit {
		t.Fatalf("tail length %d exceeds limit %d", len(tail.data), tail.limit)
	}
}
