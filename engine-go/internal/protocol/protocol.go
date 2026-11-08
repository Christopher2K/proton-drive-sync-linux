// Package protocol defines the JSON-line control protocol spoken between the
// UI shell (mygo-shell) and the sync engine over a Unix domain socket.
//
// It is byte-compatible with the Rust pds-engine-headless protocol: the same
// command shapes the shell actually sends, and the same update lines the
// shell decodes (see mygo-shell/state.go engineUpdate/uiCommand).
package protocol

import "encoding/json"

// Command names (the "cmd" field of a command line).
const (
	CmdStartLogin  = "StartLogin"
	CmdPause       = "Pause"
	CmdResume      = "Resume"
	CmdSignOut     = "SignOut"
	CmdShutdown    = "Shutdown"
	CmdSetCacheCap = "SetCacheCap"
	CmdSetMode     = "SetMode"
)

// Sync mode values accepted by SetMode.
const (
	ModeMirror       = "mirror"
	ModePlaceholders = "placeholders"
)

// Command is one command line from the shell.
//
// Field note: the Rust engine's UiCommand uses internally-tagged tuple
// variants for SetCacheCap/SetMode, which serde cannot decode from the
// object shape the shell actually sends ({"cmd":"SetMode","mode":...}) —
// those two commands are silently dropped by the Rust engine today. The Go
// engine accepts the shell's shape, and also tolerates the serde indexed
// form ({"cmd":"SetMode","0":...}) for forward compatibility.
type Command struct {
	Cmd  string  `json:"cmd"`
	Cap  *uint64 `json:"cap,omitempty"`
	Mode string  `json:"mode,omitempty"`
}

// UnmarshalJSON also tolerates the serde tuple-variant indexed spelling
// ({"cmd":"SetMode","0":...}) by folding "0" into Mode/Cap.
func (c *Command) UnmarshalJSON(data []byte) error {
	type plain Command
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*c = Command(p)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if v, ok := raw["0"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			if c.Mode == "" {
				c.Mode = s
			}
		} else {
			var n *uint64
			if err := json.Unmarshal(v, &n); err == nil && c.Cap == nil {
				c.Cap = n
			}
		}
	}
	return nil
}

// NormalizedCacheCap returns the effective cap for SetCacheCap, tolerating
// the indexed spelling. ok reports whether the command carries a cap at all.
func (c Command) NormalizedCacheCap() (cap *uint64, ok bool) {
	if c.Cap != nil {
		return c.Cap, true
	}
	return nil, false
}

// NormalizedMode returns the mode for SetMode, tolerating the indexed
// spelling. It does not validate the value.
func (c Command) NormalizedMode() string {
	return c.Mode
}

// ValidMode reports whether m is a known sync mode.
func ValidMode(m string) bool {
	return m == ModeMirror || m == ModePlaceholders
}

// Update names (the "update" field of an update line).
const (
	UpdAuthRequired     = "AuthRequired"
	UpdAuthOk           = "AuthOk"
	UpdLoginStarted     = "LoginStarted"
	UpdLoginUrl         = "LoginUrl"
	UpdLoginFailed      = "LoginFailed"
	UpdEngineStarted    = "EngineStarted"
	UpdLayoutMigrated   = "LayoutMigrated"
	UpdMountUnavailable = "MountUnavailable"
	UpdStatus           = "Status"
	UpdActivity         = "Activity"
	UpdInbox            = "Inbox"
	UpdSignedOut        = "SignedOut"
	UpdModeSwitched     = "ModeSwitched"
	UpdFailed           = "Failed"
	UpdExited           = "Exited"
)

// Update is one update line to the shell. Field names and omission rules
// match the Rust serde serialization (PascalCase variant tag, snake_case
// fields, empty fields omitted as the shell's omitempty decoding expects).
type Update struct {
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
	Errored   []ErroredItem `json:"errored,omitempty"`
}

// ErroredItem is one terminally-errored upload in the inbox.
type ErroredItem struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

// MarshalUpdate serializes an update to one JSON line (no trailing newline).
func MarshalUpdate(u Update) ([]byte, error) {
	return json.Marshal(u)
}
