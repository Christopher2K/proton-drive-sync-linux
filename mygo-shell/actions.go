package main

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/egoist/mygo"
)

func (a *app) setPage(p page) {
	a.mu.Lock()
	a.page = p
	if p == pageSettings {
		a.settingsScroll.Y = 0
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) setFilter(f activityFilter) {
	a.mu.Lock()
	a.filter = f
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) startLogin() {
	a.mu.Lock()
	a.login = loginState{kind: "waiting"}
	a.mu.Unlock()
	a.invalidate()
	a.sendCommand(uiCommand{Cmd: "StartLogin"})
}

func (a *app) togglePause() {
	a.mu.Lock()
	state := a.engineState()
	a.mu.Unlock()
	switch state {
	case "Running":
		a.sendCommand(uiCommand{Cmd: "Pause"})
	case "Paused", "GuardPaused":
		a.sendCommand(uiCommand{Cmd: "Resume"})
	}
}

func (a *app) openSyncFolder() {
	a.mu.Lock()
	root := a.root
	a.mu.Unlock()
	if err := openFolder(root); err != nil {
		log.Printf("open sync folder failed: %v", err)
	}
}

// openFolder opens a directory in the user's file manager, mirroring the
// Rust app's platform::open_folder (crates/app/src/platform.rs).
//
// mygo.Shell.OpenPath delegates to the desktop's default handler for the
// path's MIME type. On this machine that handler for inode/directory is a
// terminal emulator (kitty-open.desktop), so the button opened a terminal
// instead of a file manager. Like the Rust app:
//   - a FUSE mount point (placeholder-mode sync root) reports
//     inode/mount-point, which no .desktop entry claims, so xdg-open falls
//     through to a browser/terminal fallback — launch a file manager
//     directly;
//   - an ordinary directory honours xdg-open only when the configured
//     inode/directory default actually looks like a file manager.
func openFolder(path string) error {
	if isMountPoint(path) {
		for _, candidate := range []string{
			"nautilus", "dolphin", "thunar", "nemo", "pcmanfm", "caja",
		} {
			if _, err := exec.LookPath(candidate); err == nil {
				return spawnDetached(candidate, path)
			}
		}
		// Last resort, same as the Rust app.
		return spawnDetached("kitty", "+open", path)
	}

	if fileManagerDefault() {
		return spawnDetached("xdg-open", path)
	}
	// Default handler is not a known file manager (e.g. a terminal that
	// registered as inode/directory default); fall back to direct launch.
	for _, candidate := range []string{
		"nautilus", "dolphin", "thunar", "nemo", "pcmanfm", "caja",
	} {
		if _, err := exec.LookPath(candidate); err == nil {
			return spawnDetached(candidate, path)
		}
	}
	return spawnDetached("xdg-open", path)
}

// isMountPoint reports whether path is on a different device than its parent
// (FUSE mounts such as the placeholder-mode sync root).
func isMountPoint(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	parent := filepath.Dir(path)
	if parent == path {
		return false
	}
	pinfo, err := os.Stat(parent)
	if err != nil {
		return false
	}
	return info.Sys().(*syscall.Stat_t).Dev != pinfo.Sys().(*syscall.Stat_t).Dev
}

// knownFileManagerDesktopFiles maps .desktop ids of common file managers to
// their executables.
var knownFileManagerDesktopFiles = map[string]bool{
	"nautilus.desktop": true, "org.gnome.Nautilus.desktop": true,
	"dolphin.desktop": true, "org.kde.dolphin.desktop": true,
	"thunar.desktop": true, "Thunar.desktop": true, "thunar-org.desktop": true,
	"nemo.desktop": true, "pcmanfm.desktop": true, "caja.desktop": true,
}

// fileManagerDefault reports whether the xdg default for inode/directory is
// a known file manager rather than a terminal or editor.
func fileManagerDefault() bool {
	out, err := exec.Command("xdg-mime", "query", "default", "inode/directory").Output()
	if err != nil {
		return false
	}
	return knownFileManagerDesktopFiles[strings.TrimSpace(string(out))]
}

// spawnDetached launches program detached from the UI, mirroring the Rust
// app's spawn_detached.
func spawnDetached(program string, args ...string) error {
	cmd := exec.Command(program, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()
	return nil
}

func (a *app) setCacheCap(cap *uint64) {
	a.sendCommand(uiCommand{Cmd: "SetCacheCap", Cap: cap})
}

func (a *app) setAutostart(enabled bool) {
	err := mygo.App.SetOpenAtLogin(enabled)
	a.mu.Lock()
	if err == nil {
		a.autostartEnabled = enabled
		a.autostartError = ""
	} else {
		a.autostartError = err.Error()
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) openSignOut() {
	a.mu.Lock()
	if a.phase == phaseRunning {
		a.signOut = signOutConfirming
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) cancelSignOut() {
	a.mu.Lock()
	if a.signOut == signOutConfirming {
		a.signOut = signOutNone
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) confirmSignOut() {
	a.mu.Lock()
	if a.signOut == signOutConfirming {
		a.signOut = signOutInProgress
	}
	a.mu.Unlock()
	a.invalidate()
	a.sendCommand(uiCommand{Cmd: "SignOut"})
}

func (a *app) openModeSwitch(to syncMode) {
	a.mu.Lock()
	if a.phase == phaseRunning && a.modeSwitch == modeSwitchNone {
		a.modeSwitch = modeSwitchConfirming
		a.modeSwitchTo = to
	}
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) cancelModeSwitch() {
	a.mu.Lock()
	a.modeSwitch = modeSwitchNone
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) confirmModeSwitch() {
	a.mu.Lock()
	if a.modeSwitch != modeSwitchConfirming {
		a.mu.Unlock()
		return
	}
	to := a.modeSwitchTo
	a.modeSwitch = modeSwitchInProgress
	a.mu.Unlock()
	a.invalidate()
	a.sendCommand(uiCommand{Cmd: "SetMode", Mode: string(to)})
}

func (a *app) quit() {
	a.mu.Lock()
	if a.quitting {
		a.mu.Unlock()
		return
	}
	a.quitting = true
	a.mu.Unlock()
	a.stopEngine()
	mygo.App.Quit()
}

func (a *app) showWindow() {
	if a.win != nil {
		a.win.Show()
		mygo.App.Focus()
	}
}

func (a *app) hideWindow() {
	if a.win != nil {
		a.win.Hide()
	}
}
