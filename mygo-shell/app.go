package main

import (
	"net"
	"os/exec"
	"sync"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

type app struct {
	mu sync.Mutex

	win  *mygo.Window
	tray *mygo.Tray

	phase phase
	fatal string
	email string
	login loginState

	status *engineStatus

	activity  []activityItem
	conflicts []string
	errored   []erroredItem

	root string

	quitting bool
	signOut  signOutState
	page     page

	layoutMigrated bool
	mountError     string

	modeSwitch   modeSwitchState
	modeSwitchTo syncMode

	autostartEnabled bool
	autostartError   string

	filter activityFilter

	// Scroll states for each page.
	overviewScroll  ui.ScrollState
	activityScroll  ui.ScrollState
	conflictsScroll ui.ScrollState
	settingsScroll  ui.ScrollState

	engineCmd     *exec.Cmd
	conn          net.Conn
	engineRunning bool
}

func newApp() *app {
	return &app{
		phase: phaseStarting,
		page:  pageOverview,
		login: loginState{kind: "idle"},
		root:  defaultRoot(),
	}
}

func (a *app) engineState() string {
	if a.status == nil {
		return ""
	}
	return a.status.State
}

func (a *app) accountLabel() string {
	if a.email != "" {
		return a.email
	}
	return "Not signed in"
}

func (a *app) signInRequired() bool {
	return a.phase == phaseRunning && a.status != nil && a.status.NeedsSignIn
}

func (a *app) sidecarUnavailable() bool {
	return a.phase == phaseRunning && a.status != nil && a.status.SidecarUnavailable
}

func (a *app) attentionCount() int {
	return len(a.conflicts) + len(a.errored)
}

func (a *app) activityCount(f activityFilter) int {
	n := 0
	for _, it := range a.activity {
		if f.matches(it.Kind) {
			n++
		}
	}
	return n
}

func (a *app) filteredActivity(f activityFilter) []activityItem {
	var out []activityItem
	for _, it := range a.activity {
		if f.matches(it.Kind) {
			out = append(out, it)
		}
	}
	return out
}

func (a *app) recentActivity(limit int) []activityItem {
	if len(a.activity) < limit {
		return a.activity
	}
	return a.activity[:limit]
}

func (a *app) isPaused() bool {
	return a.engineState() != "Running"
}

func (a *app) badge() string {
	switch a.phase {
	case phaseStarting:
		return "Starting"
	case phaseNeedsLogin:
		return "Sign-in required"
	case phaseFailed:
		return "Stopped"
	case phaseRunning:
		if a.signInRequired() {
			return "Sign-in required"
		}
		if a.sidecarUnavailable() {
			return "Sidecar lost"
		}
		switch a.engineState() {
		case "Running":
			return "Syncing"
		case "Paused":
			return "Paused"
		case "GuardPaused":
			return "Delete guard"
		default:
			return "Syncing"
		}
	}
	return ""
}

func (a *app) badgeColors() (dot ui.Color, fg ui.Color, bgc ui.Color) {
	theme := appTheme()
	switch a.phase {
	case phaseRunning:
		if a.signInRequired() || a.sidecarUnavailable() {
			return theme.Warning, theme.Warning, warnSoft
		}
		switch a.engineState() {
		case "Running":
			return ok, ok, okSoft
		case "Paused":
			return theme.Warning, theme.Warning, warnSoft
		case "GuardPaused":
			return theme.Danger, theme.Danger, dangerSoft
		}
	}
	return faint, faint, inset
}

func (a *app) pauseLabel() string {
	switch a.engineState() {
	case "Running":
		return "Pause syncing"
	case "Paused":
		return "Resume syncing"
	case "GuardPaused":
		return "Review and resume"
	}
	return "Starting…"
}

func (a *app) currentMode() syncMode {
	if a.status != nil && a.status.Mode == "mirror" {
		return modeMirror
	}
	return modePlaceholders
}

// ---------------------------------------------------------------------------
// Engine control
// ---------------------------------------------------------------------------
