package main

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

//go:embed resources/icon.png
var iconBytes []byte

const (
	appTitle   = "Proton Drive Sync"
	appTagline = "Unofficial Linux client"
	disclosure = "This is a third-party application not officially supported by Proton."
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

type page int

const (
	pageOverview page = iota
	pageActivity
	pageConflicts
	pageSettings
)

func (p page) title() string {
	switch p {
	case pageOverview:
		return "Overview"
	case pageActivity:
		return "Activity"
	case pageConflicts:
		return "Conflicts"
	case pageSettings:
		return "Settings"
	}
	return ""
}

func (p page) subtitle() string {
	switch p {
	case pageOverview:
		return "Your sync status at a glance"
	case pageActivity:
		return "Recent uploads, downloads, and remote changes"
	case pageConflicts:
		return "Items that need your attention"
	case pageSettings:
		return "Account, folders, and application controls"
	}
	return ""
}

type phase int

const (
	phaseStarting phase = iota
	phaseNeedsLogin
	phaseRunning
	phaseFailed
)

type activityFilter int

const (
	filterAll activityFilter = iota
	filterUploads
	filterDownloads
	filterDeletes
	filterConflicts
	filterProblems
)

var allFilters = []activityFilter{filterAll, filterUploads, filterDownloads, filterDeletes, filterConflicts, filterProblems}

func (f activityFilter) label() string {
	switch f {
	case filterAll:
		return "All"
	case filterUploads:
		return "Uploads"
	case filterDownloads:
		return "Downloads"
	case filterDeletes:
		return "Deletes"
	case filterConflicts:
		return "Conflicts"
	case filterProblems:
		return "Problems"
	}
	return ""
}

func (f activityFilter) matches(kind string) bool {
	switch f {
	case filterAll:
		return true
	case filterUploads:
		return kind == "Uploaded"
	case filterDownloads:
		return kind == "Downloaded"
	case filterDeletes:
		return kind == "Trashed"
	case filterConflicts:
		return kind == "Conflict"
	case filterProblems:
		return kind == "UploadErrored" || kind == "DownloadErrored" || kind == "ConnectionError" ||
			kind == "GuardPaused" || kind == "Skipped" || kind == "SignInRequired" || kind == "SidecarUnavailable"
	}
	return false
}

type signOutState int

const (
	signOutNone signOutState = iota
	signOutConfirming
	signOutInProgress
)

type modeSwitchState int

const (
	modeSwitchNone modeSwitchState = iota
	modeSwitchConfirming
	modeSwitchInProgress
)

type loginState struct {
	kind     string // idle, waiting, url, failed
	url      string
	userCode string
	err      string
}

type syncMode string

const (
	modePlaceholders syncMode = "placeholders"
	modeMirror       syncMode = "mirror"
)

func (m syncMode) label() string {
	switch m {
	case modeMirror:
		return "Mirror everything"
	}
	return "On demand (placeholders)"
}

type engineStatus struct {
	State              string  `json:"state"`
	SyncedNodes        int     `json:"synced_nodes"`
	DirtyNodes         int     `json:"dirty_nodes"`
	QueuedTombstones   int     `json:"queued_tombstones"`
	ErroredNodes       int     `json:"errored_nodes"`
	ConflictedNodes    int     `json:"conflicted_nodes"`
	Mode               string  `json:"mode"`
	CacheCap           *uint64 `json:"cache_cap"`
	CacheUsed          uint64  `json:"cache_used"`
	NeedsSignIn        bool    `json:"needs_sign_in"`
	SidecarUnavailable bool    `json:"sidecar_unavailable"`
}

type activityItem struct {
	AtUnix int64  `json:"at_unix"`
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Detail string `json:"detail"`
}

type erroredItem struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

type engineUpdate struct {
	Update string `json:"update"`

	Email    string `json:"email,omitempty"`
	URL      string `json:"url,omitempty"`
	UserCode string `json:"user_code,omitempty"`
	Error    string `json:"error,omitempty"`
	Reason   string `json:"reason,omitempty"`

	State              string  `json:"state,omitempty"`
	SyncedNodes        int     `json:"synced_nodes,omitempty"`
	DirtyNodes         int     `json:"dirty_nodes,omitempty"`
	QueuedTombstones   int     `json:"queued_tombstones,omitempty"`
	ErroredNodes       int     `json:"errored_nodes,omitempty"`
	ConflictedNodes    int     `json:"conflicted_nodes,omitempty"`
	Mode               string  `json:"mode,omitempty"`
	CacheCap           *uint64 `json:"cache_cap,omitempty"`
	CacheUsed          uint64  `json:"cache_used,omitempty"`
	NeedsSignIn        bool    `json:"needs_sign_in,omitempty"`
	SidecarUnavailable bool    `json:"sidecar_unavailable,omitempty"`

	AtUnix int64  `json:"at_unix,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Path   string `json:"path,omitempty"`
	Detail string `json:"detail,omitempty"`

	Conflicts []string      `json:"conflicts,omitempty"`
	Errored   []erroredItem `json:"errored,omitempty"`
}

type uiCommand struct {
	Cmd  string  `json:"cmd"`
	Cap  *uint64 `json:"cap,omitempty"`
	Mode string  `json:"mode,omitempty"`
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

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

func (a *app) findEngineBinary() string {
	if b := os.Getenv("PDS_ENGINE_BIN"); b != "" {
		return b
	}
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	base := filepath.Dir(exe)
	for i := 0; i < 8 && base != "/" && base != "."; i++ {
		if _, err := os.Stat(filepath.Join(base, "Cargo.toml")); err == nil {
			for _, profile := range []string{"debug", "release"} {
				candidate := filepath.Join(base, "target", profile, "pds-engine-headless")
				if _, err := os.Stat(candidate); err == nil {
					return candidate
				}
			}
		}
		base = filepath.Dir(base)
	}
	return ""
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

	bin := a.findEngineBinary()
	if bin == "" {
		a.applyUpdate(engineUpdate{Update: "Failed", Error: "Could not find pds-engine-headless binary. Set PDS_ENGINE_BIN or build the Rust workspace."})
		return
	}

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
	defer a.mu.Unlock()

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
	case "LayoutMigrated":
		a.layoutMigrated = true
	case "MountUnavailable":
		a.mountError = up.Error
	case "Status":
		a.status = &engineStatus{
			State:              up.State,
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

	if a.tray != nil {
		a.tray.SetToolTip(fmt.Sprintf("%s — %s", appTitle, a.badge()))
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

// ---------------------------------------------------------------------------
// UI: theme
// ---------------------------------------------------------------------------

var (
	bg           = ui.RGB(0x10, 0x12, 0x18)
	sidebar      = ui.RGB(0x15, 0x18, 0x21)
	card         = ui.RGB(0x1a, 0x1e, 0x28)
	cardHover    = ui.RGB(0x22, 0x27, 0x33)
	inset        = ui.RGB(0x0d, 0x0f, 0x14)
	border       = ui.RGB(0x27, 0x2d, 0x3a)
	borderStrong = ui.RGB(0x36, 0x40, 0x52)
	text         = ui.RGB(0xe7, 0xea, 0xf0)
	muted        = ui.RGB(0x98, 0xa1, 0xb3)
	faint        = ui.RGB(0x6b, 0x74, 0x86)
	accent       = ui.RGB(0x5b, 0x8d, 0xef)
	accentHover  = ui.RGB(0x6f, 0x9b, 0xf5)
	ok           = ui.RGB(0x44, 0xd1, 0x9a)
	okSoft       = ui.RGB(0x11, 0x32, 0x27)
	warn         = ui.RGB(0xf0, 0xb6, 0x4a)
	warnSoft     = ui.RGB(0x33, 0x28, 0x0e)
	danger       = ui.RGB(0xef, 0x73, 0x73)
	dangerSoft   = ui.RGB(0x36, 0x1a, 0x1f)
)

func appTheme() *ui.Theme {
	return &ui.Theme{
		Dark:           true,
		Background:     bg,
		Surface:        card,
		SurfaceHover:   cardHover,
		SurfacePressed: cardHover,
		Border:         border,
		Text:           text,
		TextMuted:      muted,
		Accent:         accent,
		AccentHover:    accentHover,
		AccentPressed:  accent,
		AccentText:     ui.RGB(0xff, 0xff, 0xff),
		Danger:         danger,
		Warning:        warn,
		Success:        ok,
		Selection:      ui.RGBA(0x5b, 0x8d, 0xef, 0.4),
		Focus:          ui.RGBA(0x6f, 0x9b, 0xf5, 0.6),
		Inverse:        text,
		InverseText:    bg,
		Scrollbar:      ui.RGBA(0xff, 0xff, 0xff, 0.35),
		Radius:         8,
		Spacing:        4,
		FontSize:       14,
	}
}

// ---------------------------------------------------------------------------
// UI: primitives
// ---------------------------------------------------------------------------

func appCard(c *ui.Context, children func()) ui.Element {
	t := c.Theme()
	return ui.Box(c).Background(t.Surface).Border(1, t.Border).Radius(t.Radius).Padding(t.Space(4)).Children(children)
}

func statusDot(c *ui.Context, label string, color ui.Color) ui.Element {
	t := c.Theme()
	size := t.Space(1.75)
	return ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Children(func() {
		ui.Box(c).Width(size).Height(size).Radius(size / 2).Background(color)
		ui.Text(c, label).TextColor(color).FontSize(t.FontSize)
	})
}

func pill(c *ui.Context, txt string, fg, bgc ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Background(bgc).Padding(t.Space(1), t.Space(2)).Radius(999).Children(func() {
		ui.Text(c, txt).TextColor(fg).FontSize(t.FontSize * 0.85).Bold()
	})
}

func statBox(c *ui.Context, label, value string, color ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Grow(1).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
		ui.Text(c, value).FontSize(t.FontSize * 1.75).Bold().TextColor(color)
		ui.Text(c, label).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
	})
}

func sectionTitle(c *ui.Context, title, subtitle string) ui.Element {
	t := c.Theme()
	return ui.Column(c).Gap(t.Space(1)).Children(func() {
		ui.Text(c, title).FontSize(t.FontSize * 1.15).Bold()
		if subtitle != "" {
			ui.Text(c, subtitle).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
		}
	})
}

func emptyState(c *ui.Context, title, body string) ui.Element {
	t := c.Theme()
	return ui.Column(c).Gap(t.Space(2)).AlignItems(ui.Center).Padding(t.Space(12)).Children(func() {
		ui.Text(c, title).FontSize(t.FontSize).Bold()
		ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
	})
}

func banner(c *ui.Context, title, body string, fg, bgc ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Border(1, fg).Background(bgc).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
		ui.Text(c, title).FontSize(t.FontSize).Bold().TextColor(fg)
		ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
	})
}

func fieldRow(c *ui.Context, label, value string) ui.Element {
	t := c.Theme()
	return ui.Row(c).Gap(t.Space(4)).AlignItems(ui.Center).Children(func() {
		ui.Text(c, label).FontSize(t.FontSize).TextColor(t.TextMuted)
		ui.Spacer(c)
		ui.Text(c, value).FontSize(t.FontSize).TextColor(t.Text)
	})
}

func chipButton(c *ui.Context, label string, active bool, onClick func()) ui.Element {
	t := c.Theme()
	el := ui.Button(c, label).FontSize(t.FontSize*0.85).Padding(t.Space(1), t.Space(3)).Radius(t.Radius)
	if active {
		el = el.Background(inset).Border(1, accent).TextColor(accent)
	} else {
		el = el.Background(t.Surface).Border(1, t.Border).TextColor(t.TextMuted)
	}
	if el.Clicked() {
		onClick()
	}
	return el
}

func kindLabel(kind string) string {
	switch kind {
	case "Uploaded":
		return "Uploaded"
	case "Downloaded":
		return "Downloaded"
	case "Trashed":
		return "Trashed"
	case "Conflict":
		return "Conflict"
	case "GuardPaused":
		return "Guard paused"
	case "UploadErrored":
		return "Upload error"
	case "DownloadErrored":
		return "Download error"
	case "ConnectionError":
		return "Connection error"
	case "SignInRequired":
		return "Sign-in required"
	case "SidecarUnavailable":
		return "Sidecar unavailable"
	case "Skipped":
		return "Skipped"
	}
	return kind
}

func kindColor(kind string) ui.Color {
	switch kind {
	case "Uploaded":
		return accent
	case "Downloaded":
		return ok
	case "Trashed":
		return muted
	case "Conflict":
		return warn
	case "GuardPaused", "UploadErrored", "DownloadErrored", "SignInRequired", "SidecarUnavailable":
		return danger
	case "ConnectionError", "Skipped":
		return warn
	}
	return muted
}

func formatTime(unix int64) string {
	t := time.Unix(unix, 0).Local()
	return t.Format("15:04:05")
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ---------------------------------------------------------------------------
// UI: root view
// ---------------------------------------------------------------------------

func (a *app) view(c *ui.Context) {
	c.SetTheme(appTheme())
	t := c.Theme()

	a.mu.Lock()
	ph := a.phase
	a.mu.Unlock()

	ui.Column(c).Fill().Background(t.Background).Children(func() {
		switch ph {
		case phaseStarting:
			a.renderSplash(c)
		case phaseNeedsLogin:
			a.renderLogin(c)
		case phaseFailed:
			a.renderFailure(c)
		case phaseRunning:
			a.renderShell(c)
		}
	})

	a.mu.Lock()
	so := a.signOut
	ms := a.modeSwitch
	a.mu.Unlock()

	if so != signOutNone {
		a.renderSignOutModal(c)
	} else if ms != modeSwitchNone {
		a.renderModeSwitchModal(c)
	}
}

func (a *app) renderSplash(c *ui.Context) {
	t := c.Theme()
	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Gap(t.Space(4)).Children(func() {
		a.brandBlock(c)
		statusDot(c, "Starting the sync engine…", accent)
		ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(faint).AlignItems(ui.Center)
	})
}

func (a *app) renderFailure(c *ui.Context) {
	t := c.Theme()
	a.mu.Lock()
	fatal := a.fatal
	a.mu.Unlock()
	if fatal == "" {
		fatal = "The sync engine stopped unexpectedly."
	}

	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Padding(t.Space(6)).Children(func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(520).Children(func() {
				a.brandBlock(c)
				ui.Text(c, "Sync is stopped").FontSize(t.FontSize * 1.25).Bold().TextColor(danger)
				ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
					ui.Text(c, fatal).FontSize(t.FontSize).TextColor(t.TextMuted)
				})
				ui.Row(c).Gap(t.Space(3)).Children(func() {
					if ui.PrimaryButton(c, "Try again").Clicked() {
						go a.startEngine()
					}
					if ui.Button(c, "Quit").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
						go a.quit()
					}
				})
			})
		})
	})
}

func (a *app) renderLogin(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	login := a.login
	a.mu.Unlock()

	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Padding(t.Space(6)).Children(func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(460).Children(func() {
				a.brandBlock(c)
				ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
					ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
				})

				switch login.kind {
				case "idle":
					ui.Text(c, "Sign in with your Proton account to start syncing. Your browser handles the sign-in — this app never sees your password.").
						FontSize(t.FontSize).TextColor(t.TextMuted)
					if ui.PrimaryButton(c, "Sign in with Proton").Clicked() {
						go a.startLogin()
					}
				case "waiting":
					ui.Text(c, "Opening the Proton sign-in page…").FontSize(t.FontSize)
					ui.Text(c, "If nothing happens, check that a browser is available.").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
				case "url":
					ui.Text(c, "Finish signing in in the browser window that just opened.").FontSize(t.FontSize)
					if login.userCode != "" {
						ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
							ui.Text(c, "Verification code").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							ui.Text(c, login.userCode).FontSize(t.FontSize * 2).Bold()
						})
					}
					ui.Text(c, login.url).FontSize(t.FontSize * 0.85).TextColor(faint).SingleLine()
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Open sign-in page again").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							_ = mygo.Shell.OpenExternal(login.url)
						}
						statusDot(c, "Waiting for sign-in…", accent)
					})
				case "failed":
					ui.Text(c, fmt.Sprintf("Sign-in failed: %s", login.err)).FontSize(t.FontSize).TextColor(danger)
					if ui.PrimaryButton(c, "Try again").Clicked() {
						go a.startLogin()
					}
				}
			})
		})
	})
}

func (a *app) brandBlock(c *ui.Context) ui.Element {
	t := c.Theme()
	return ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
		ui.Box(c).Width(38).Height(38).Radius(10).Background(accent).Children(func() {
			ui.Box(c).Width(17).Height(17).Radius(8.5).Border(2, ui.RGB(0xff, 0xff, 0xff)).Fill()
		})
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, appTitle).FontSize(t.FontSize * 1.25).Bold()
			ui.Text(c, appTagline).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
		})
	})
}

func (a *app) renderShell(c *ui.Context) {
	t := c.Theme()
	ui.Row(c).Fill().Children(func() {
		a.renderSidebar(c)
		ui.Column(c).Grow(1).Children(func() {
			a.renderHeader(c)
			a.renderPage(c)
			ui.Divider(c).Height(1).Background(t.Border)
			a.renderStatusBar(c)
		})
	})
}

func (a *app) renderSidebar(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	badgeText := a.badge()
	account := a.accountLabel()
	a.mu.Unlock()

	ui.Column(c).Width(232).Fill().Background(sidebar).Padding(t.Space(4)).Gap(t.Space(2)).Children(func() {
		a.brandBlock(c)

		ui.Column(c).Gap(t.Space(1)).Grow(1).Padding(0, 0, t.Space(4), 0).Children(func() {
			for _, p := range []page{pageOverview, pageActivity, pageConflicts, pageSettings} {
				a.navItem(c, p)
			}
		})

		appCard(c, func() {
			ui.Column(c).Gap(t.Space(3)).Children(func() {
				dot, _, _ := a.badgeColors()
				statusDot(c, badgeText, dot)
				ui.Text(c, account).FontSize(t.FontSize).Bold().SingleLine()
				pauseLabel := a.pauseLabel()
				if ui.Button(c, pauseLabel).Background(accent).TextColor(ui.RGB(0xff, 0xff, 0xff)).Fill().Clicked() {
					go a.togglePause()
				}
			})
		})

		ui.Row(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, fmt.Sprintf("v%s", appVersion())).FontSize(t.FontSize * 0.85).TextColor(faint)
			ui.Spacer(c)
			ui.Text(c, "Linux").FontSize(t.FontSize * 0.85).TextColor(faint)
		})
	})
}

func (a *app) navItem(c *ui.Context, p page) ui.Element {
	t := c.Theme()

	a.mu.Lock()
	selected := a.page == p
	var badge int
	if p == pageConflicts {
		badge = a.attentionCount()
	}
	a.mu.Unlock()

	el := ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Padding(t.Space(2), t.Space(3)).Radius(t.Radius).
		TextColor(func() ui.Color {
			if selected {
				return t.Text
			}
			return t.TextMuted
		}())
	if selected {
		el = el.Background(cardHover)
	}
	if el.Clicked() {
		a.setPage(p)
	}
	return el.Children(func() {
		ui.Text(c, p.title()).FontSize(t.FontSize)
		ui.Spacer(c)
		if badge > 0 {
			pill(c, strconv.Itoa(badge), warn, warnSoft)
		}
	})
}

func (a *app) renderHeader(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	pageTitle := a.page.title()
	pageSubtitle := a.page.subtitle()
	a.mu.Unlock()

	var statusLabel string
	var statusColor ui.Color
	a.mu.Lock()
	switch a.engineState() {
	case "Running":
		statusLabel, statusColor = "Sync is running", ok
	case "Paused":
		statusLabel, statusColor = "Sync is paused", warn
	case "GuardPaused":
		statusLabel, statusColor = "Paused by delete guard", danger
	default:
		statusLabel, statusColor = "Waiting for engine", faint
	}
	a.mu.Unlock()

	ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Padding(t.Space(4), t.Space(5)).Children(func() {
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, pageTitle).FontSize(t.FontSize * 1.5).Bold()
			ui.Text(c, pageSubtitle).FontSize(t.FontSize).TextColor(t.TextMuted)
		})
		ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
			statusDot(c, statusLabel, statusColor)
			if ui.Button(c, "Open folder").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
				go a.openSyncFolder()
			}
		})
	})
}

func (a *app) renderStatusBar(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	root := a.root
	var counts string
	if a.status != nil {
		counts = fmt.Sprintf("%d synced · %d pending · %d deletes queued", a.status.SyncedNodes, a.status.DirtyNodes, a.status.QueuedTombstones)
	} else {
		counts = "Engine starting…"
	}
	a.mu.Unlock()

	ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Padding(t.Space(3), t.Space(5)).Children(func() {
		ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Children(func() {
			ui.Text(c, "Sync folder").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
			ui.Text(c, root).FontSize(t.FontSize * 0.85).TextColor(t.Text).SingleLine()
		})
		ui.Text(c, counts).FontSize(t.FontSize * 0.85).TextColor(faint)
	})
}

func (a *app) renderPage(c *ui.Context) {
	a.mu.Lock()
	p := a.page
	a.mu.Unlock()
	switch p {
	case pageOverview:
		a.renderOverview(c)
	case pageActivity:
		a.renderActivity(c)
	case pageConflicts:
		a.renderConflicts(c)
	case pageSettings:
		a.renderSettings(c)
	}
}

// ---------------------------------------------------------------------------
// UI: Overview
// ---------------------------------------------------------------------------

func (a *app) renderOverview(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	status := a.status
	dirty := 0
	if status != nil {
		dirty = status.DirtyNodes
	}
	headline := "Waiting for the sync engine"
	if a.phase == phaseRunning {
		switch a.engineState() {
		case "GuardPaused":
			headline = "Syncing paused by the delete guard"
		case "Paused":
			headline = "Sync is paused"
		default:
			if dirty > 0 {
				headline = fmt.Sprintf("Syncing %d pending item%s", dirty, plural(dirty))
			} else {
				headline = "Everything is up to date"
			}
		}
	}
	account := a.accountLabel()
	badgeText := a.badge()
	_, badgeFg, badgeBg := a.badgeColors()
	attention := a.attentionCount()
	recent := a.recentActivity(7)
	a.mu.Unlock()

	ui.Scroll(c).TrackScroll(&a.overviewScroll).Grow(1).Padding(t.Space(7)).Children(func() {
		ui.Column(c).Gap(t.Space(5)).Children(func() {
			if a.engineState() == "GuardPaused" {
				banner(c, "Too many deletes in a short window",
					"Syncing is paused to protect your Drive. Review the pending deletions, then resume to approve them.",
					danger, dangerSoft)
			}

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(4)).Children(func() {
					ui.Row(c).AlignItems(ui.Start).Justify(ui.SpaceBetween).Gap(t.Space(4)).Children(func() {
						ui.Column(c).Gap(t.Space(1)).Children(func() {
							ui.Text(c, headline).FontSize(t.FontSize * 1.75).Bold()
							ui.Text(c, account).FontSize(t.FontSize).TextColor(t.TextMuted)
						})
						pill(c, badgeText, badgeFg, badgeBg)
					})

					if status != nil {
						ui.Row(c).Gap(t.Space(3)).Children(func() {
							statBox(c, "Synced items", strconv.Itoa(status.SyncedNodes), ok)
							statBox(c, "Pending uploads", strconv.Itoa(status.DirtyNodes), func() ui.Color {
								if status.DirtyNodes > 0 {
									return accent
								}
								return t.Text
							}())
							statBox(c, "Deletes queued", strconv.Itoa(status.QueuedTombstones), func() ui.Color {
								if status.QueuedTombstones > 0 {
									return warn
								}
								return t.Text
							}())
							statBox(c, "Needs attention", strconv.Itoa(status.ConflictedNodes+status.ErroredNodes), func() ui.Color {
								if status.ConflictedNodes+status.ErroredNodes > 0 {
									return danger
								}
								return t.Text
							}())
						})
					} else {
						ui.Text(c, "Collecting the first status snapshot…").FontSize(t.FontSize).TextColor(t.TextMuted)
					}
				})
			})

			if attention > 0 {
				appCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
							sectionTitle(c, "Needs attention", "Conflicts and failed uploads")
							if ui.Button(c, "Review").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
								a.setPage(pageConflicts)
							}
						})
						a.mu.Lock()
						conflicts := a.conflicts
						errored := a.errored
						a.mu.Unlock()
						ui.Text(c, fmt.Sprintf("%d conflict%s · %d failed upload%s",
							len(conflicts), plural(len(conflicts)), len(errored), plural(len(errored)))).
							FontSize(t.FontSize).TextColor(t.TextMuted)
					})
				})
			}

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
						sectionTitle(c, "Recent activity", "Newest first")
						if ui.Button(c, "View all").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							a.setPage(pageActivity)
						}
					})
					if len(recent) == 0 {
						emptyState(c, "No activity yet", "Uploads, downloads, deletions, and conflicts will appear here.")
					} else {
						for _, it := range recent {
							a.activityRow(c, it)
						}
					}
				})
			})
		})
	})
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// ---------------------------------------------------------------------------
// UI: Activity
// ---------------------------------------------------------------------------

func (a *app) renderActivity(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	f := a.filter
	a.mu.Unlock()

	rows := a.filteredActivity(f)

	ui.Column(c).Grow(1).MinHeight(0).Padding(t.Space(7)).Gap(t.Space(4)).Children(func() {
		ui.Row(c).Gap(t.Space(2)).Wrap().Children(func() {
			for _, candidate := range allFilters {
				active := candidate == f
				count := a.activityCount(candidate)
				el := ui.Button(c, fmt.Sprintf("%s %d", candidate.label(), count)).
					FontSize(t.FontSize*0.85).Padding(t.Space(1), t.Space(3)).Radius(999)
				if active {
					el = el.Background(accent).TextColor(t.Text)
				} else {
					el = el.Background(t.Surface).TextColor(t.TextMuted)
				}
				if el.Clicked() {
					a.setFilter(candidate)
				}
			}
		})

		appCard(c, func() {
			ui.Column(c).Grow(1).MinHeight(0).Children(func() {
				ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Padding(t.Space(3), t.Space(4)).Children(func() {
					ui.Text(c, fmt.Sprintf("%d %s", len(rows), pluralize(len(rows), "event", "events"))).FontSize(t.FontSize).Bold()
					if ui.Button(c, "Settings").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
						a.setPage(pageSettings)
					}
				})
				if len(rows) == 0 {
					emptyState(c, "Nothing here yet", "Sync activity will show up as files move in either direction.")
				} else {
					ui.Row(c).Gap(t.Space(3)).Padding(t.Space(2), t.Space(4)).Children(func() {
						ui.Text(c, "Time").FontSize(t.FontSize * 0.85).TextColor(faint).Width(64)
						ui.Text(c, "Event").FontSize(t.FontSize * 0.85).TextColor(faint).Width(92)
						ui.Text(c, "Path").FontSize(t.FontSize * 0.85).TextColor(faint).Grow(1)
					})
					ui.Scroll(c).TrackScroll(&a.activityScroll).Grow(1).Children(func() {
						ui.Column(c).Children(func() {
							for _, it := range rows {
								a.activityRow(c, it)
							}
						})
					})
				}
			})
		})
	})
}

func (a *app) activityRow(c *ui.Context, it activityItem) ui.Element {
	t := c.Theme()
	return ui.Column(c).Children(func() {
		ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Padding(t.Space(2), 0).Children(func() {
			ui.Text(c, formatTime(it.AtUnix)).FontSize(t.FontSize).TextColor(faint).Width(64)
			ui.Text(c, kindLabel(it.Kind)).FontSize(t.FontSize).TextColor(kindColor(it.Kind)).Width(92).SingleLine()
			ui.Text(c, it.Path).FontSize(t.FontSize).TextColor(t.Text).Grow(1).SingleLine()
		})
		ui.Divider(c).Height(1).Background(t.Border)
	})
}

func pluralize(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// ---------------------------------------------------------------------------
// UI: Conflicts
// ---------------------------------------------------------------------------

func (a *app) renderConflicts(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	conflicts := append([]string(nil), a.conflicts...)
	errored := append([]erroredItem(nil), a.errored...)
	a.mu.Unlock()

	ui.Scroll(c).TrackScroll(&a.conflictsScroll).Grow(1).Padding(t.Space(7)).Children(func() {
		ui.Column(c).Gap(t.Space(5)).Children(func() {
			if len(conflicts) == 0 && len(errored) == 0 {
				appCard(c, func() {
					emptyState(c, "Everything is in sync", "No conflicts or failed uploads. Keep-both copies and upload errors will show up here.")
				})
			}

			if len(conflicts) > 0 {
				appCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						sectionTitle(c, "Conflicts", "The remote copy changed while a local edit was pending")
						for _, path := range conflicts {
							ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
								ui.Text(c, path).FontSize(t.FontSize).Bold().SingleLine()
								ui.Text(c, "Kept both versions — the local copy will be renamed with a conflict suffix and uploaded separately.").
									FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							})
						}
					})
				})
			}

			if len(errored) > 0 {
				appCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						sectionTitle(c, "Failed uploads", "These files stay local until the next successful retry")
						for _, it := range errored {
							msg := it.Error
							if msg == "" {
								msg = "Upload failed without a reported reason."
							}
							ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
								ui.Text(c, it.Path).FontSize(t.FontSize).Bold().SingleLine()
								ui.Text(c, msg).FontSize(t.FontSize * 0.85).TextColor(danger)
							})
						}
					})
				})
			}
		})
	})
}

// ---------------------------------------------------------------------------
// UI: Settings
// ---------------------------------------------------------------------------

func (a *app) renderSettings(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	account := a.accountLabel()
	currentMode := a.currentMode()
	var cacheCap *uint64
	var cacheUsed uint64
	if a.status != nil {
		cacheCap = a.status.CacheCap
		cacheUsed = a.status.CacheUsed
	}
	switching := a.modeSwitch == modeSwitchInProgress
	root := a.root
	autostart := a.autostartEnabled
	autostartErr := a.autostartError
	mountErr := a.mountError
	a.mu.Unlock()

	ui.Scroll(c).TrackScroll(&a.settingsScroll).Grow(1).Padding(t.Space(7)).Children(func() {
		ui.Column(c).Gap(t.Space(4)).Children(func() {
			if switching {
				banner(c, "Switching sync mode…", "The sync engine is restarting with your new setting. This notice clears when the new mode is active.", accent, inset)
			}
			if mountErr != "" {
				banner(c, "On-demand view unavailable this session",
					fmt.Sprintf("The placeholder filesystem could not be mounted (%s). Files are being materialized in the sync folder instead; the on-demand view returns next time the app starts.", mountErr),
					warn, warnSoft)
			}

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Account", "The Proton account this sync root belongs to")
					fieldRow(c, "Signed in as", account)
				})
			})

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(4)).Children(func() {
					sectionTitle(c, "Sync mode", "")
					for _, m := range []syncMode{modePlaceholders, modeMirror} {
						title := m.label()
						detail := ""
						if m == modePlaceholders {
							detail = "Files stay on Drive until you open them; the local cache is capped and cleaned up automatically. Nothing else downloads."
						} else {
							detail = "Every file in your Drive is downloaded to the sync folder. This uses more disk space but works offline."
						}
						selected := currentMode == m
						el := ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Start).Padding(t.Space(3)).Radius(t.Radius).Border(1, func() ui.Color {
							if selected {
								return accent
							}
							return t.Border
						}()).Background(func() ui.Color {
							if selected {
								return inset
							}
							return t.Surface
						}())
						if el.Clicked() && !selected {
							a.openModeSwitch(m)
						}
						el.Children(func() {
							ui.Column(c).Gap(t.Space(1)).Grow(1).Children(func() {
								ui.Text(c, title).FontSize(t.FontSize).Bold()
								ui.Text(c, detail).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							})
						})
					}
					ui.Text(c, "Switching modes keeps every file and never deletes from Drive.").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
				})
			})

			if currentMode == modePlaceholders {
				appCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
							ui.Text(c, "Local cache").FontSize(t.FontSize).Bold()
							var capText string
							if cacheCap == nil {
								capText = fmt.Sprintf("%s used / unlimited", formatBytes(cacheUsed))
							} else {
								capText = fmt.Sprintf("%s used / %s", formatBytes(cacheUsed), formatBytes(*cacheCap))
							}
							ui.Text(c, capText).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
						})
						ui.Row(c).Gap(t.Space(2)).Wrap().Children(func() {
							presets := []struct {
								cap   *uint64
								label string
							}{
								{ptr(uint64(2 << 30)), "2 GB"},
								{ptr(uint64(5 << 30)), "5 GB"},
								{ptr(uint64(10 << 30)), "10 GB"},
								{ptr(uint64(25 << 30)), "25 GB"},
								{nil, "Unlimited"},
							}
							for _, p := range presets {
								active := (cacheCap == nil && p.cap == nil) || (cacheCap != nil && p.cap != nil && *cacheCap == *p.cap)
								chipButton(c, p.label, active, func(cap *uint64) func() {
									return func() { go a.setCacheCap(cap) }
								}(p.cap))
							}
						})
						ui.Text(c, "Files you open are stored locally up to this size; the least-used are cleaned automatically. Pinned files are never cleaned.").
							FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
					})
				})
			}

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Sync folder", "")
					fieldRow(c, "Location", root)
					if currentMode == modePlaceholders {
						fieldRow(c, "View", "Placeholder view mounted at the sync folder")
					} else {
						fieldRow(c, "View", "Fully materialized folder")
					}
					if ui.Button(c, "Open sync folder").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
						go a.openSyncFolder()
					}
				})
			})

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Startup", "Keep syncing in the background without opening the dashboard")
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Children(func() {
						ui.Column(c).Gap(t.Space(1)).Grow(1).Children(func() {
							ui.Text(c, "Start at login").FontSize(t.FontSize).Bold()
							ui.Text(c, "Launches minimized to the tray when you sign in, so files keep syncing. Close the dashboard at any time to keep the app running in the background.").
								FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
						})
						switchEl := ui.Switch(c, &autostart)
						if switchEl.Changed() {
							go a.setAutostart(autostart)
						}
					})
					if autostartErr != "" {
						ui.Text(c, fmt.Sprintf("Could not update the login entry: %s", autostartErr)).FontSize(t.FontSize * 0.85).TextColor(danger)
					}
				})
			})

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Account actions", "")
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Sign out").Background(danger).TextColor(ui.RGB(0xff, 0xff, 0xff)).Clicked() {
							a.openSignOut()
						}
						if ui.Button(c, "Quit").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							go a.quit()
						}
					})
				})
			})

			appCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "About", "A third-party client built on Proton's official Drive SDK")
					fieldRow(c, "Application", fmt.Sprintf("%s %s", appTitle, appVersion()))
					fieldRow(c, "Platform", "Linux")
					ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
						ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
					})
				})
			})
		})
	})
}

func ptr[T any](v T) *T { return &v }

// ---------------------------------------------------------------------------
// UI: modals
// ---------------------------------------------------------------------------

func modalOverlay(c *ui.Context, content func()) {
	ui.Box(c).Absolute().Top(0).Left(0).Right(0).Bottom(0).Background(ui.RGBA(0x05, 0x06, 0x0a, 0.65)).
		AlignItems(ui.Center).Justify(ui.Center).Children(content)
}

func (a *app) renderSignOutModal(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	account := a.accountLabel()
	inProgress := a.signOut == signOutInProgress
	a.mu.Unlock()

	modalOverlay(c, func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(430).Children(func() {
				ui.Text(c, fmt.Sprintf("Sign out of %s?", account)).FontSize(t.FontSize * 1.25).Bold()
				ui.Text(c, "Sync will stop and the saved session will be deleted. Files on disk are kept, and you can sign back in at any time.").
					FontSize(t.FontSize).TextColor(t.TextMuted)
				if inProgress {
					statusDot(c, "Signing out…", accent)
				} else {
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Sign out").Background(danger).TextColor(ui.RGB(0xff, 0xff, 0xff)).Clicked() {
							go a.confirmSignOut()
						}
						if ui.Button(c, "Cancel").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							a.cancelSignOut()
						}
					})
				}
			})
		})
	})
}

func (a *app) renderModeSwitchModal(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	ms := a.modeSwitch
	to := a.modeSwitchTo
	a.mu.Unlock()

	var title, body string
	switch {
	case ms == modeSwitchInProgress:
		title = "Switching sync mode…"
	case to == modeMirror:
		title = "Download everything to this folder?"
		body = "Switching to mirror materializes every file in your sync folder. Files already on disk are kept; the rest download from Drive once. Your Drive is never deleted."
	case to == modePlaceholders:
		title = "Switch to on-demand placeholders?"
		body = "Files in your sync folder become placeholders at ~0 bytes. Content stays on Drive; opening a file fetches it on demand and the local cache is capped (LRU eviction). Nothing is deleted from Drive."
	}

	modalOverlay(c, func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(460).Children(func() {
				ui.Text(c, title).FontSize(t.FontSize * 1.25).Bold()
				if body != "" {
					ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
				}
				if ms == modeSwitchInProgress {
					statusDot(c, "Switching…", accent)
				} else {
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Switch mode").Background(accent).TextColor(ui.RGB(0xff, 0xff, 0xff)).Clicked() {
							go a.confirmModeSwitch()
						}
						if ui.Button(c, "Cancel").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							a.cancelModeSwitch()
						}
					})
				}
			})
		})
	})
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

func (a *app) setPage(p page) {
	a.mu.Lock()
	a.page = p
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

func appVersion() string {
	v := mygo.App.Version()
	if v == "" {
		return "0.3.0"
	}
	return v
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	a := newApp()

	mygo.App.SetName(appTitle)

	if !mygo.App.RequestSingleInstanceLock() {
		return
	}
	mygo.App.OnSecondInstance(func(args []string, workingDir string) {
		a.showWindow()
	})

	minimized := false
	for _, arg := range os.Args[1:] {
		if arg == "--minimized" {
			minimized = true
		}
	}
	if mygo.App.WasOpenedAtLogin() {
		minimized = true
	}

	mygo.App.OnWillQuit(func(e *mygo.QuitEvent) {
		a.stopEngine()
	})

	mygo.App.WhenReady(func() {
		a.autostartEnabled = mygo.App.OpenAtLogin()

		win := mygo.NewWindow(mygo.WindowOptions{
			Title:     appTitle,
			Width:     1080,
			Height:    720,
			MinWidth:  900,
			MinHeight: 620,
			Hidden:    minimized,
			StateKey:  "main",
			Content:   ui.View(a.view),
		})
		a.win = win

		win.OnClose(func(e *mygo.CloseEvent) {
			e.PreventDefault()
			a.hideWindow()
		})

		tray, err := mygo.NewTray(mygo.TrayOptions{
			Icon:    iconBytes,
			ToolTip: appTitle,
			Menu: mygo.NewMenu([]*mygo.MenuItem{
				{Label: "Open Dashboard", Click: func(*mygo.MenuItem, *mygo.Window) { a.showWindow() }},
				{Label: "Pause/Resume syncing", Click: func(*mygo.MenuItem, *mygo.Window) { a.togglePause() }},
				{Label: "Open Sync Folder", Click: func(*mygo.MenuItem, *mygo.Window) { a.openSyncFolder() }},
				{Label: "Preferences…", Click: func(*mygo.MenuItem, *mygo.Window) { a.setPage(pageSettings); a.showWindow() }},
				mygo.Separator(),
				{Label: fmt.Sprintf("Quit %s", appTitle), Click: func(*mygo.MenuItem, *mygo.Window) { a.quit() }},
			}),
		})
		if err != nil {
			log.Printf("tray: %v", err)
		} else {
			a.tray = tray
		}

		go a.startEngine()
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
