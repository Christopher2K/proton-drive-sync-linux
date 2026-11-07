package main

import (
	"log"

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
	if err := mygo.Shell.OpenPath(root); err != nil {
		log.Printf("open sync folder failed: %v", err)
	}
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
