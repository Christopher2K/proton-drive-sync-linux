// Package sidecar is the Go client for the proton-drive-sync Node.js
// sidecar: JSON-RPC 2.0 over NDJSON (one message per line) on the sidecar's
// stdio, per docs/protocol.md. All Proton Drive traffic goes through the
// sidecar — this package never talks to Proton directly (AGENT.md hard
// rule 3).
//
// Ported from crates/sidecar-client (types.rs, jsonrpc.rs, client.rs,
// spawn.rs, mock.rs); behavior mirrors the Rust client exactly.
package sidecar

import (
	"encoding/json"
	"time"
)

// JSON-RPC error codes beyond the standard ones (protocol.md).
const (
	ErrCodeAuthRequired   int64 = 1001
	ErrCodeSessionExpired int64 = 1002
	ErrCodeRateLimited    int64 = 1003
	ErrCodeIntegrityError int64 = 1004
	ErrCodeConflict       int64 = 1005
	ErrCodeInternal       int64 = 1099
	errCodeConnClosedSent int64 = -32000 // client-side "connection closed" sentinel
)

// NodeKind is the "type" field of a Drive node.
type NodeKind string

const (
	NodeKindFile   NodeKind = "file"
	NodeKindFolder NodeKind = "folder"
)

// Node is a Drive node as returned by drive.* methods.
type Node struct {
	UID              string   `json:"uid"`
	ParentUID        *string  `json:"parentUid"`
	Name             string   `json:"name"`
	Kind             NodeKind `json:"type"`
	Size             *int64   `json:"size"`
	Mtime            *int64   `json:"mtime"` // Unix seconds.
	SHA1             *string  `json:"sha1"`  // Hex SHA-1, files only.
	TreeEventScopeID string   `json:"treeEventScopeId"`
}

// IsFile reports whether the node is a file.
func (n Node) IsFile() bool { return n.Kind == NodeKindFile }

// IsFolder reports whether the node is a folder.
func (n Node) IsFolder() bool { return n.Kind == NodeKindFolder }

// TrashResult is the per-node result of drive.trash.
type TrashResult struct {
	UID   string  `json:"uid"`
	OK    bool    `json:"ok"`
	Error *string `json:"error"`
}

// UploadParams are the params for drive.upload. Exactly one of ParentUID
// (new file) or NodeUID (new revision) must be set.
type UploadParams struct {
	ParentUID *string `json:"parentUid,omitempty"`
	NodeUID   *string `json:"nodeUid,omitempty"`
	Name      string  `json:"name"`
	Path      string  `json:"path"`
	Size      int64   `json:"size"`
	SHA1      string  `json:"sha1"`
	Mtime     int64   `json:"mtime"`
}

// EventKind is the kind of a drive.event notification.
type EventKind string

const (
	EventNodeCreated EventKind = "node_created"
	EventNodeUpdated EventKind = "node_updated"
	EventNodeDeleted EventKind = "node_deleted"
	EventTreeRefresh EventKind = "tree_refresh"
	EventTreeRemove  EventKind = "tree_remove"
	EventFastForward EventKind = "fast_forward"
)

// DriveEvent is the payload of a drive.event notification (F6).
type DriveEvent struct {
	ScopeID       string    `json:"scopeId"`
	EventID       string    `json:"eventId"`
	Kind          EventKind `json:"type"`
	NodeUID       *string   `json:"nodeUid"`
	ParentNodeUID *string   `json:"parentNodeUid"`
	IsTrashed     *bool     `json:"isTrashed"`
}

// Notification is a server→client notification (no id), decoded. The
// concrete types below are the variants; anything unrecognized arrives as
// *OtherNotification so callers can log/forward unknown methods.
type Notification interface{ isNotification() }

// AuthSignInUrlNotification is auth.signInUrl — display/open this URL to
// complete fork login.
type AuthSignInUrlNotification struct {
	URL      string
	UserCode string
}

// TransferProgressNotification is transfer.progress.
type TransferProgressNotification struct {
	TransferID string
	Bytes      int64
	Total      int64
}

// TransferDoneNotification is transfer.done — upload adds node ids.
type TransferDoneNotification struct {
	TransferID      string
	NodeUID         *string
	NodeRevisionUID *string
}

// TransferErrorNotification is transfer.error.
type TransferErrorNotification struct {
	TransferID string
	Code       int64
	Message    string
	Retryable  bool
	// RetryAfter is the server-driven rate-limit hint (retryAfter, code
	// 1003); nil when the server gave no hint.
	RetryAfter *time.Duration
}

// DriveEventNotification is drive.event (F6).
type DriveEventNotification struct {
	Event *DriveEvent
}

// OtherNotification is anything else.
type OtherNotification struct {
	Method string
	Params json.RawMessage
}

func (*AuthSignInUrlNotification) isNotification()    {}
func (*TransferProgressNotification) isNotification() {}
func (*TransferDoneNotification) isNotification()     {}
func (*TransferErrorNotification) isNotification()    {}
func (*DriveEventNotification) isNotification()       {}
func (*OtherNotification) isNotification()            {}
