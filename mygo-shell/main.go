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
	"sync"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

// app holds the state shown by the native UI. It also manages the
// headless Rust engine process and its JSON-line control socket.
type app struct {
	mu sync.Mutex

	win *mygo.Window

	status        string
	email         string
	engineRunning bool
	loginUrl      string
	userCode      string

	engineCmd *exec.Cmd
	conn      net.Conn
}

// uiCommand is serialized and sent over the Unix socket to the engine.
type uiCommand struct {
	Cmd string `json:"cmd"`
}

// engineUpdate mirrors the JSON-line updates emitted by pds-engine-headless.
type engineUpdate struct {
	Update   string `json:"update"`
	Email    string `json:"email,omitempty"`
	URL      string `json:"url,omitempty"`
	UserCode string `json:"user_code,omitempty"`
	Error    string `json:"error,omitempty"`
	Reason   string `json:"reason,omitempty"`
	State    string `json:"state,omitempty"`
}

func newApp() *app {
	return &app{status: "Idle"}
}

func (a *app) view(c *ui.Context) {
	a.mu.Lock()
	status := a.status
	email := a.email
	loginUrl := a.loginUrl
	userCode := a.userCode
	running := a.engineRunning
	a.mu.Unlock()

	t := c.Theme()
	ui.Column(c).Fill().Padding(24).Gap(16).Children(func() {
		ui.Text(c, "Proton Drive Sync (MyGo experiment)").FontSize(22).Bold()

		ui.Text(c, fmt.Sprintf("Status: %s", status))
		if email != "" {
			ui.Text(c, fmt.Sprintf("Account: %s", email)).TextColor(t.TextMuted)
		}
		if loginUrl != "" {
			ui.Text(c, "Open this URL in your browser to sign in").TextColor(t.TextMuted)
			ui.Text(c, loginUrl).TextColor(t.Accent)
			if userCode != "" {
				ui.Text(c, fmt.Sprintf("Code: %s", userCode)).TextColor(t.TextMuted)
			}
		}

		ui.Row(c).Gap(10).Children(func() {
			if ui.PrimaryButton(c, "Start engine").Clicked() && !running {
				go a.startEngine()
			}
			if ui.Button(c, "Stop engine").Clicked() && running {
				go a.stopEngine()
			}
			if ui.Button(c, "Sign in").Clicked() && running {
				go a.sendCommand("StartLogin")
			}
		})

		ui.Row(c).Gap(10).Children(func() {
			if ui.Button(c, "Pause").Clicked() && running {
				go a.sendCommand("Pause")
			}
			if ui.Button(c, "Resume").Clicked() && running {
				go a.sendCommand("Resume")
			}
		})
	})
}

// syncRoot returns the folder the headless engine will sync.
func (a *app) syncRoot() string {
	if r := os.Getenv("PDS_SYNC_ROOT"); r != "" {
		return r
	}
	return filepath.Join(os.TempDir(), "pds-mygo-root")
}

// socketPath is where the headless engine listens and the UI connects.
func (a *app) socketPath() string {
	if r := os.Getenv("XDG_RUNTIME_DIR"); r != "" {
		return filepath.Join(r, "pds-mygo-engine.sock")
	}
	return filepath.Join(os.TempDir(), "pds-mygo-engine.sock")
}

// findEngineBinary resolves the path to the cargo-built headless engine.
func (a *app) findEngineBinary() string {
	if b := os.Getenv("PDS_ENGINE_BIN"); b != "" {
		return b
	}
	// The MyGo project lives at repo-root/mygo-shell; the cargo target dir
	// is at repo-root/target by default.
	exe, err := os.Executable()
	if err == nil {
		base := filepath.Dir(filepath.Dir(exe))
		candidate := filepath.Join(base, "target", "debug", "pds-engine-headless")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "../target/debug/pds-engine-headless"
}

func (a *app) setStatus(s string) {
	a.mu.Lock()
	a.status = s
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) setEmail(e string) {
	a.mu.Lock()
	a.email = e
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) setLoginUrl(url, code string) {
	a.mu.Lock()
	a.loginUrl = url
	a.userCode = code
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) setEngineRunning(running bool) {
	a.mu.Lock()
	a.engineRunning = running
	if !running {
		a.email = ""
		a.loginUrl = ""
		a.userCode = ""
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) invalidate() {
	if a.win != nil {
		a.win.Invalidate()
	}
}

// repoRoot guesses the repository root from the engine binary path.
func (a *app) repoRoot(bin string) string {
	if d := os.Getenv("PDS_REPO_ROOT"); d != "" {
		return d
	}
	// Walk up from the engine binary looking for a sidecar/ dir.
	base := filepath.Dir(bin)
	for i := 0; i < 6 && base != "/" && base != "."; i++ {
		if _, err := os.Stat(filepath.Join(base, "sidecar")); err == nil {
			return base
		}
		base = filepath.Dir(base)
	}
	return ".."
}

// startEngine launches the headless Rust binary and connects to its socket.
func (a *app) startEngine() {
	a.setStatus("Starting engine...")

	root := a.syncRoot()
	socket := a.socketPath()
	bin := a.findEngineBinary()

	cmd := exec.Command(bin, "--root", root, "--socket", socket)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Run from the repo root so the engine can auto-discover the sidecar dir.
	cmd.Dir = a.repoRoot(bin)

	if err := cmd.Start(); err != nil {
		a.setStatus(fmt.Sprintf("Failed to start engine: %v", err))
		return
	}

	a.mu.Lock()
	a.engineCmd = cmd
	a.mu.Unlock()
	a.setEngineRunning(true)

	// Wait a moment for the socket to be bound, then connect and read updates.
	go func() {
		// Retry connection for up to ~3 seconds.
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
			a.setStatus("Could not connect to engine socket")
			return
		}

		a.mu.Lock()
		a.conn = conn
		a.mu.Unlock()

		a.readUpdates(conn)
	}()
}

// stopEngine sends Shutdown and waits for the process to exit, killing it
// if it does not respond promptly (e.g. it is still in initial sync).
func (a *app) stopEngine() {
	a.sendCommand("Shutdown")

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
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}

	a.setEngineRunning(false)
	a.setStatus("Engine stopped")
}

// sendCommand writes a JSON-line command to the engine socket.
func (a *app) sendCommand(cmd string) {
	a.mu.Lock()
	conn := a.conn
	a.mu.Unlock()

	if conn == nil {
		return
	}
	payload, err := json.Marshal(uiCommand{Cmd: cmd})
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if _, err := conn.Write(payload); err != nil {
		log.Printf("send command failed: %v", err)
	}
}

// readUpdates parses JSON-line updates from the engine and updates the UI.
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
		a.handleUpdate(up)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("update stream ended: %v", err)
	}
	a.setEngineRunning(false)
	a.setStatus("Engine disconnected")
}

func (a *app) handleUpdate(up engineUpdate) {
	switch up.Update {
	case "AuthRequired":
		a.setStatus("Sign-in required")
	case "AuthOk":
		a.setEmail(up.Email)
		a.setStatus(fmt.Sprintf("Signed in as %s", up.Email))
	case "LoginStarted":
		a.setStatus("Waiting for sign-in URL...")
	case "LoginUrl":
		a.setLoginUrl(up.URL, up.UserCode)
		a.setStatus("Open the login URL in your browser")
		if err := mygo.Shell.OpenExternal(up.URL); err != nil {
			log.Printf("open external failed: %v", err)
		}
	case "LoginFailed":
		a.setStatus(fmt.Sprintf("Login failed: %s", up.Error))
	case "EngineStarted":
		a.setStatus("Engine running")
	case "MountUnavailable":
		a.setStatus(fmt.Sprintf("Mount unavailable: %s", up.Error))
	case "Status":
		a.mu.Lock()
		a.status = fmt.Sprintf("Engine state: %s", up.State)
		a.mu.Unlock()
		a.invalidate()
	case "Activity":
		// For the demo, keep the last status; activity stream is logged.
		log.Printf("activity: %s", string(up.Update))
	case "Exited":
		a.setStatus(fmt.Sprintf("Engine exited: %s", up.Reason))
		a.setEngineRunning(false)
	case "Failed":
		a.setStatus(fmt.Sprintf("Engine failed: %s", up.Error))
		a.setEngineRunning(false)
	default:
		log.Printf("update: %s", string(up.Update))
	}
}

func main() {
	a := newApp()

	mygo.App.OnWillQuit(func(e *mygo.QuitEvent) {
		a.stopEngine()
	})

	mygo.App.WhenReady(func() {
		win := mygo.NewWindow(mygo.WindowOptions{
			Title:     "Proton Drive Sync (MyGo experiment)",
			Width:     520,
			Height:    360,
			MinWidth:  360,
			MinHeight: 280,
			StateKey:  "main",
			Content:   ui.View(a.view),
		})
		a.win = win
		if os.Getenv("MYGO_AUTO_START") == "1" {
			go a.startEngine()
		}
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
