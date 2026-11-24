package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Engine lifecycle: process supervision, command socket, update stream.

func defaultRoot() string {
	if r := os.Getenv("PDS_SYNC_ROOT"); r != "" {
		return r
	}
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "/tmp"
	}
	return filepath.Join(home, "ProtonDrive")
}

func (a *app) socketPath() string {
	if r := os.Getenv("XDG_RUNTIME_DIR"); r != "" {
		return filepath.Join(r, "pds-engine-headless.sock")
	}
	return filepath.Join(os.TempDir(), "pds-engine-headless.sock")
}

// Engine binary names: the Go engine is the default, the Rust headless
// engine is kept as a fallback for one release. Both speak the same
// socket protocol, so the shell treats them identically.
const (
	goEngineName   = "pds-engine-go"
	rustEngineName = "pds-engine-headless"
)

// engineKind describes which engine a resolved binary is.
const (
	engineKindGo     = "go"
	engineKindRust   = "rust"
	engineKindCustom = "custom"
)

// engineKindForPath classifies a resolved path by binary name.
func engineKindForPath(p string) string {
	switch filepath.Base(p) {
	case goEngineName:
		return engineKindGo
	case rustEngineName:
		return engineKindRust
	}
	return engineKindCustom
}

// engineLabel is the user-facing description used in logs and errors.
func engineLabel(kind string) string {
	switch kind {
	case engineKindGo:
		return "Go engine (" + goEngineName + ")"
	case engineKindRust:
		return "Rust fallback (" + rustEngineName + ")"
	case engineKindCustom:
		return "PDS_ENGINE_BIN override"
	}
	return kind
}

// isFile reports whether p exists and is not a directory.
func isFile(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

// isRepoRoot reports whether dir looks like the workspace root. The original
// Rust-workspace marker (Cargo.toml) is still accepted; the Go module under
// engine-go/ is accepted too so a Go-only checkout still resolves.
func isRepoRoot(dir string) bool {
	if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(dir, "engine-go", "go.mod")); err == nil {
		return true
	}
	return false
}

// engineCandidateDirs builds the search locations for engine binaries,
// nearest first, deduplicated. Order is:
//  1. the shell executable's own dir (packaged side-by-side install),
//  2. for each ancestor that is a repo root (walking up 8 levels):
//     target/debug, target/release, engine-go/, then the root itself —
//     so `go build -o target/debug/pds-engine-go ./engine-go/cmd/pds-engine-go`
//     and `go build ./cmd/pds-engine-go` (in engine-go/) both work.
func engineCandidateDirs(exe string) []string {
	if exe == "" {
		return nil
	}
	exeDir := filepath.Dir(exe)
	var dirs []string
	seen := map[string]struct{}{}
	add := func(d string) {
		if _, ok := seen[d]; ok {
			return
		}
		seen[d] = struct{}{}
		dirs = append(dirs, d)
	}
	add(exeDir)
	base := exeDir
	for i := 0; i < 8 && base != "/" && base != "."; i++ {
		if isRepoRoot(base) {
			add(filepath.Join(base, "target", "debug"))
			add(filepath.Join(base, "target", "release"))
			add(filepath.Join(base, "engine-go"))
			add(base)
		}
		parent := filepath.Dir(base)
		if parent == base {
			break
		}
		base = parent
	}
	return dirs
}

// findEngineBinaryFrom resolves the engine binary given an executable path
// and a PDS_ENGINE_BIN override. PDS_ENGINE_BIN always wins. Otherwise the
// Go engine (pds-engine-go) is preferred globally over the Rust fallback
// (pds-engine-headless): every candidate dir is scanned for the Go binary
// before any Rust fallback is considered. Returns ("", "") when nothing is
// found.
func findEngineBinaryFrom(exe string, envOverride string) (string, string) {
	if envOverride != "" {
		return envOverride, engineKindCustom
	}
	dirs := engineCandidateDirs(exe)
	// Pass 1: Go engine, nearest location first.
	for _, d := range dirs {
		candidate := filepath.Join(d, goEngineName)
		if isFile(candidate) {
			return candidate, engineKindGo
		}
	}
	// Pass 2: Rust fallback (kept for one release).
	for _, d := range dirs {
		candidate := filepath.Join(d, rustEngineName)
		if isFile(candidate) {
			return candidate, engineKindRust
		}
	}
	return "", ""
}

// findEngine resolves the engine for production use and also returns its
// kind, so callers can log which engine was chosen.
func (a *app) findEngine() (string, string) {
	exe, err := os.Executable()
	if err != nil {
		exe = ""
	}
	return findEngineBinaryFrom(exe, os.Getenv("PDS_ENGINE_BIN"))
}

func (a *app) findEngineBinary() string {
	bin, _ := a.findEngine()
	return bin
}

func (a *app) repoRoot(bin string) string {
	if d := os.Getenv("PDS_REPO_ROOT"); d != "" {
		return d
	}
	base := filepath.Dir(bin)
	for i := 0; i < 8 && base != "/" && base != "."; i++ {
		if _, err := os.Stat(filepath.Join(base, "Cargo.toml")); err == nil {
			return base
		}
		base = filepath.Dir(base)
	}
	return ""
}

const (
	engineStderrTailLimit = 16 * 1024
	engineMaxAutoRestarts = 5
	engineRetryBaseDelay  = time.Second
	engineRetryMaxDelay   = 30 * time.Second
)

// engineSession owns one engine process. The wait goroutine is the only code
// that calls Cmd.Wait; stopEngine only waits for done or kills the process.
type engineSession struct {
	generation  uint64
	cmd         *exec.Cmd
	done        chan struct{}
	waitErr     error
	stderr      *boundedTail
	intentional bool
}

// boundedTail retains the last limit bytes written to it.
type boundedTail struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *boundedTail) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if b.limit <= 0 {
		return n, nil
	}
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return n, nil
	}
	if excess := len(b.data) + len(p) - b.limit; excess > 0 {
		copy(b.data, b.data[excess:])
		b.data = b.data[:len(b.data)-excess]
	}
	b.data = append(b.data, p...)
	return n, nil
}

func (b *boundedTail) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.data))
}

type engineStderrWriter struct {
	parent io.Writer
	tail   *boundedTail
}

func (w *engineStderrWriter) Write(p []byte) (int, error) {
	_, _ = w.tail.Write(p)
	if w.parent == nil {
		return len(p), nil
	}
	return w.parent.Write(p)
}

func (a *app) startEngine() {
	a.startEngineGeneration(0, true)
}

func (a *app) startEngineRetry(generation uint64) {
	a.startEngineGeneration(generation, false)
}

// startEngineGeneration starts a manual session or the automatic retry tied
// to generation. A stale retry cannot start a process after a stop or a newer
// manual attempt.
func (a *app) startEngineGeneration(generation uint64, manual bool) {
	a.mu.Lock()
	if a.engineRunning || a.quitting {
		a.mu.Unlock()
		return
	}
	if manual {
		a.engineGeneration++
		generation = a.engineGeneration
		a.engineRetryAttempt = 0
		a.engineRetryPending = false
		a.engineRetriesExhausted = false
	} else {
		if generation != a.engineGeneration || !a.engineRetryPending || !a.canRestartLocked() {
			a.mu.Unlock()
			return
		}
		a.engineRetryPending = false
	}
	a.engineRunning = true
	root := a.root

	bin, kind := a.findEngine()
	if bin == "" {
		a.engineStartFailedLocked("Could not find a sync engine binary (Go engine pds-engine-go, Rust fallback pds-engine-headless). Set PDS_ENGINE_BIN, or build: go build -o target/debug/pds-engine-go ./engine-go/cmd/pds-engine-go (or cargo build pds-engine-headless for the fallback).")
		a.mu.Unlock()
		a.invalidate()
		return
	}
	// Dual-engine rollout: log which engine was chosen so support can
	// tell default (Go) from fallback (Rust) launches apart.
	log.Printf("engine: using %s: %s", engineLabel(kind), bin)

	socket := a.socketPath()
	if _, err := os.Stat(socket); err == nil {
		_ = os.Remove(socket)
	}

	stderr := &boundedTail{limit: engineStderrTailLimit}
	cmd := exec.Command(bin, "--root", root, "--socket", socket)
	cmd.Stdout = os.Stdout
	cmd.Stderr = &engineStderrWriter{parent: os.Stderr, tail: stderr}
	cmd.Dir = a.repoRoot(bin)

	if err := cmd.Start(); err != nil {
		a.engineStartFailedLocked(fmt.Sprintf("Failed to start engine: %v", err))
		a.mu.Unlock()
		a.invalidate()
		return
	}

	session := &engineSession{
		generation: generation,
		cmd:        cmd,
		done:       make(chan struct{}),
		stderr:     stderr,
	}
	a.engine = session
	a.mu.Unlock()

	go a.waitForEngine(session)
	go a.connectEngine(session, socket)
	a.invalidate()
}

func (a *app) engineStartFailedLocked(message string) {
	a.engineRunning = false
	a.engineRetryPending = false
	a.phase = phaseFailed
	a.fatal = message
	a.signOut = signOutNone
	a.modeSwitch = modeSwitchNone
}

// connectEngine watches both the socket and process exit. An early process
// failure is reported by waitForEngine with its real exit error and stderr,
// never replaced by a generic socket error.
func (a *app) connectEngine(session *engineSession, socket string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	var lastErr error
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			a.mu.Lock()
			if a.engine != session || session.generation != a.engineGeneration {
				a.mu.Unlock()
				_ = conn.Close()
				return
			}
			a.conn = conn
			a.mu.Unlock()
			a.readUpdates(conn, session.generation)
			return
		}
		lastErr = err

		select {
		case <-session.done:
			return
		case <-ticker.C:
		case <-timeout.C:
			message := fmt.Sprintf("Could not connect to engine control socket: %v", lastErr)
			if tail := session.stderr.String(); tail != "" {
				message += "\nEngine stderr:\n" + tail
			}
			a.failEngineConnection(session, message)
			return
		}
	}
}

func (a *app) failEngineConnection(session *engineSession, message string) {
	a.mu.Lock()
	current := a.engine == session && session.generation == a.engineGeneration
	if current {
		a.phase = phaseFailed
		a.fatal = message
		a.signOut = signOutNone
		a.modeSwitch = modeSwitchNone
	}
	a.mu.Unlock()
	if current {
		// The process is still running but unusable. Killing it lets the sole
		// wait goroutine reap it and apply the normal retry policy.
		if session.cmd.Process != nil {
			_ = session.cmd.Process.Kill()
		}
		a.invalidate()
	}
}

func (a *app) waitForEngine(session *engineSession) {
	session.waitErr = session.cmd.Wait()
	close(session.done)
	a.engineExited(session)
}

func (a *app) engineExited(session *engineSession) {
	a.mu.Lock()
	if a.engine != session || session.generation != a.engineGeneration {
		a.mu.Unlock()
		return
	}
	conn := a.conn
	a.engine = nil
	a.conn = nil
	a.engineRunning = false

	intentional := a.quitting || session.intentional || a.signOut == signOutInProgress ||
		a.folderChanging || a.modeSwitch == modeSwitchInProgress
	if intentional {
		a.engineRetryPending = false
		a.engineRetriesExhausted = false
		if a.modeSwitch == modeSwitchInProgress {
			a.modeSwitch = modeSwitchNone
			a.phase = phaseStarting
		}
	} else {
		// A Failed update often carries the useful root cause. Do not replace
		// it with the less specific exit notification that follows it.
		if a.phase != phaseFailed || a.fatal == "" {
			a.fatal = engineExitMessage(session)
		}
		a.phase = phaseFailed
		a.scheduleEngineRetryLocked(session.generation)
	}
	a.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	a.invalidate()
}

func engineExitMessage(session *engineSession) string {
	message := "sync engine exited unexpectedly"
	if session.waitErr != nil {
		message = "sync engine exited: " + session.waitErr.Error()
	}
	if tail := session.stderr.String(); tail != "" {
		message += "\nEngine stderr:\n" + tail
	}
	return message
}

func (a *app) canRestartLocked() bool {
	return !a.quitting && a.signOut != signOutInProgress && !a.folderChanging && a.modeSwitch != modeSwitchInProgress
}

func (a *app) scheduleEngineRetryLocked(generation uint64) {
	if !a.canRestartLocked() {
		a.engineRetryPending = false
		return
	}
	if a.engineRetryAttempt >= engineMaxAutoRestarts {
		a.engineRetryPending = false
		a.engineRetriesExhausted = true
		return
	}
	a.engineRetryAttempt++
	delay := engineRetryBaseDelay << (a.engineRetryAttempt - 1)
	if delay > engineRetryMaxDelay {
		delay = engineRetryMaxDelay
	}
	a.engineRetryPending = true
	a.engineRetryIn = delay.String()
	a.engineRetriesExhausted = false
	time.AfterFunc(delay, func() { a.startEngineRetry(generation) })
}

func (a *app) stopEngine() {
	a.mu.Lock()
	// Invalidate first: Shutdown may make the process exit immediately, and
	// its wait goroutine must not schedule an automatic restart.
	a.engineGeneration++
	a.engineRetryPending = false
	a.engineRetriesExhausted = false
	session := a.engine
	conn := a.conn
	if session != nil {
		session.intentional = true
	}
	a.engine = nil
	a.conn = nil
	a.engineRunning = false
	a.mu.Unlock()

	if conn != nil {
		writeCommand(conn, uiCommand{Cmd: "Shutdown"})
		_ = conn.Close()
	}
	if session != nil && session.cmd.Process != nil {
		if conn == nil {
			// There is no control channel on which to request a graceful stop.
			_ = session.cmd.Process.Kill()
		}
		select {
		case <-session.done:
		case <-time.After(5 * time.Second):
			_ = session.cmd.Process.Kill()
			<-session.done
		}
	}
}

func (a *app) sendCommand(cmd uiCommand) {
	a.mu.Lock()
	conn := a.conn
	a.mu.Unlock()
	if conn != nil {
		writeCommand(conn, cmd)
	}
}

func writeCommand(conn net.Conn, cmd uiCommand) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if _, err := conn.Write(payload); err != nil {
		log.Printf("send command failed: %v", err)
	}
}

func (a *app) readUpdates(conn net.Conn, generation uint64) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		var up engineUpdate
		if err := json.Unmarshal(line, &up); err != nil {
			log.Printf("bad update: %v", err)
			continue
		}
		a.applyUpdateForGeneration(generation, up)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("update stream ended: %v", err)
	}
}

func (a *app) applyUpdate(up engineUpdate) {
	a.applyUpdateForGeneration(0, up)
}

func (a *app) applyUpdateForGeneration(generation uint64, up engineUpdate) {
	var loginURL string

	a.mu.Lock()
	if generation != 0 && generation != a.engineGeneration {
		a.mu.Unlock()
		return
	}

	switch up.Update {
	case "AuthRequired":
		a.phase = phaseNeedsLogin
		a.login = loginState{kind: "idle"}
	case "AuthOk":
		a.email = up.Email
	case "LoginStarted":
		a.login = loginState{kind: "waiting"}
	case "LoginUrl":
		a.login = loginState{kind: "url", url: up.URL, userCode: up.UserCode}
		loginURL = up.URL
	case "LoginFailed":
		a.login = loginState{kind: "failed", err: up.Error}
	case "EngineStarted":
		a.phase = phaseRunning
		a.fatal = ""
		a.page = pageOverview
		a.modeSwitch = modeSwitchNone
		a.folderChanging = false
		a.mountError = ""
		a.engineRetryAttempt = 0
		a.engineRetryPending = false
		a.engineRetriesExhausted = false
		a.settingsScroll.Y = 0
	case "LayoutMigrated":
		a.layoutMigrated = true
	case "MountUnavailable":
		a.mountError = up.Error
	case "Status":
		a.status = &engineStatus{
			State:              normalizeEngineState(up.State),
			SyncedNodes:        up.SyncedNodes,
			DirtyNodes:         up.DirtyNodes,
			QueuedTombstones:   up.QueuedTombstones,
			ErroredNodes:       up.ErroredNodes,
			ConflictedNodes:    up.ConflictedNodes,
			Mode:               up.Mode,
			CacheCap:           up.CacheCap,
			CacheUsed:          up.CacheUsed,
			NeedsSignIn:        up.NeedsSignIn,
			SidecarUnavailable: up.SidecarUnavailable,
		}
	case "Activity":
		a.pushActivity(activityItem{
			AtUnix: up.AtUnix,
			Kind:   up.Kind,
			Path:   up.Path,
			Detail: up.Detail,
		})
	case "Inbox":
		a.conflicts = up.Conflicts
		a.errored = up.Errored
	case "SignedOut":
		a.phase = phaseNeedsLogin
		a.fatal = ""
		a.email = ""
		a.login = loginState{kind: "idle"}
		a.status = nil
		a.activity = a.activity[:0]
		a.conflicts = nil
		a.errored = nil
		a.signOut = signOutNone
		a.page = pageOverview
		a.mountError = ""
	case "ModeSwitched":
		a.modeSwitch = modeSwitchInProgress
		a.modeSwitchTo = syncMode(up.Mode)
	case "Failed":
		a.phase = phaseFailed
		a.fatal = up.Error
		a.signOut = signOutNone
		a.modeSwitch = modeSwitchNone
	case "Exited":
		// The process waiter owns session cleanup and restart decisions. This
		// socket update is diagnostic only and may precede Cmd.Wait returning.
		if !a.quitting && a.signOut != signOutInProgress && (a.engine == nil || !a.engine.intentional) {
			if a.modeSwitch == modeSwitchInProgress {
				a.modeSwitch = modeSwitchNone
				a.phase = phaseStarting
			} else if a.engine == nil && (a.phase != phaseFailed || a.fatal == "") {
				a.phase = phaseFailed
				a.fatal = fmt.Sprintf("sync engine stopped: %s", up.Reason)
			}
		}
	}

	// SetToolTip marshals to the main thread, which must stay free to run
	// the UI frame (view()) — calling it with a.mu held deadlocks.
	tooltip := ""
	if a.tray != nil {
		tooltip = fmt.Sprintf("%s — %s", appTitle, a.badge())
	}
	a.mu.Unlock()
	if loginURL != "" {
		if err := openURL(loginURL); err != nil {
			log.Printf("open sign-in page failed: %v", err)
		}
	}
	if a.tray != nil {
		a.tray.SetToolTip(tooltip)
	}
	a.invalidate()
}

func (a *app) pushActivity(item activityItem) {
	a.activity = append([]activityItem{item}, a.activity...)
	if len(a.activity) > 500 {
		a.activity = a.activity[:500]
	}
}

func (a *app) invalidate() {
	if a.win != nil {
		a.win.Invalidate()
	}
}

// normalizeEngineState maps the engine's EngineState Display strings
// ("running", "paused", "guard-paused"; crates/core/src/engine.rs) onto the
// capitalized forms the UI compares against.
func normalizeEngineState(s string) string {
	switch s {
	case "running":
		return "Running"
	case "paused":
		return "Paused"
	case "guard-paused":
		return "GuardPaused"
	}
	return s
}

// ---------------------------------------------------------------------------
// UI: theme
// ---------------------------------------------------------------------------

// Design tokens mirroring crates/app/src/theme.rs, so both shells render
