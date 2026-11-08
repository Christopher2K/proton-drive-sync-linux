package main

// Domain types, enums, and shared formatting helpers.

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
