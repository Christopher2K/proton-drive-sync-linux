package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/egoist/mygo"
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

func (a *app) startEngine() {
	a.mu.Lock()
	if a.engineRunning {
		a.mu.Unlock()
		return
	}
	a.engineRunning = true
	a.mu.Unlock()

	bin, kind := a.findEngine()
	if bin == "" {
		a.applyUpdate(engineUpdate{Update: "Failed", Error: "Could not find a sync engine binary (Go engine pds-engine-go, Rust fallback pds-engine-headless). Set PDS_ENGINE_BIN, or build: go build -o target/debug/pds-engine-go ./engine-go/cmd/pds-engine-go (or cargo build pds-engine-headless for the fallback)."})
		return
	}
	// Dual-engine rollout: log which engine was chosen so support can
	// tell default (Go) from fallback (Rust) launches apart.
	log.Printf("engine: using %s: %s", engineLabel(kind), bin)

	socket := a.socketPath()
	if _, err := os.Stat(socket); err == nil {
		os.Remove(socket)
	}

	cmd := exec.Command(bin, "--root", a.root, "--socket", socket)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = a.repoRoot(bin)

	if err := cmd.Start(); err != nil {
		a.applyUpdate(engineUpdate{Update: "Failed", Error: fmt.Sprintf("Failed to start engine: %v", err)})
		return
	}

	a.mu.Lock()
	a.engineCmd = cmd
	a.mu.Unlock()

	go func() {
		var conn net.Conn
		for i := 0; i < 30; i++ {
			c, err := net.Dial("unix", socket)
			if err == nil {
				conn = c
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if conn == nil {
			a.applyUpdate(engineUpdate{Update: "Failed", Error: "Could not connect to engine control socket"})
			return
		}
		a.mu.Lock()
		a.conn = conn
		a.mu.Unlock()
		a.readUpdates(conn)
	}()
}

func (a *app) stopEngine() {
	a.sendCommand(uiCommand{Cmd: "Shutdown"})

	a.mu.Lock()
	cmd := a.engineCmd
	conn := a.conn
	a.engineCmd = nil
	a.conn = nil
	a.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
	if cmd != nil && cmd.Process != nil {
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}

	a.mu.Lock()
	a.engineRunning = false
	a.mu.Unlock()
}

func (a *app) sendCommand(cmd uiCommand) {
	a.mu.Lock()
	conn := a.conn
	a.mu.Unlock()
	if conn == nil {
		return
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if _, err := conn.Write(payload); err != nil {
		log.Printf("send command failed: %v", err)
	}
}

func (a *app) readUpdates(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		var up engineUpdate
		if err := json.Unmarshal(line, &up); err != nil {
			log.Printf("bad update: %v", err)
			continue
		}
		a.applyUpdate(up)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("update stream ended: %v", err)
	}
	a.applyUpdate(engineUpdate{Update: "Exited", Reason: "disconnected"})
}

func (a *app) applyUpdate(up engineUpdate) {
	a.mu.Lock()

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
		if err := mygo.Shell.OpenExternal(up.URL); err != nil {
			log.Printf("open external failed: %v", err)
		}
	case "LoginFailed":
		a.login = loginState{kind: "failed", err: up.Error}
	case "EngineStarted":
		a.phase = phaseRunning
		a.fatal = ""
		a.page = pageOverview
		a.modeSwitch = modeSwitchNone
		a.mountError = ""
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
		a.engineRunning = false
		a.conn = nil
		a.engineCmd = nil
		if !a.quitting {
			if a.modeSwitch == modeSwitchInProgress {
				a.modeSwitch = modeSwitchNone
				a.phase = phaseStarting
			} else {
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
