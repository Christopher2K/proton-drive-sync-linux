package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

// ErrorKind classifies an Error.
type ErrorKind int

// Error kinds.
const (
	// ErrKindRpc is a request-level JSON-RPC error (RPC != nil).
	ErrKindRpc ErrorKind = iota
	// ErrKindMalformed is a response that failed decoding.
	ErrKindMalformed
	// ErrKindIo is a transport failure (write error).
	ErrKindIo
	// ErrKindClosed is the sentinel for a closed client/stream.
	ErrKindClosed
)

// RpcError is a protocol-level error object returned by the server.
type RpcError struct {
	Code    int64
	Message string
	Data    json.RawMessage // may be nil
}

func (e *RpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// Error is what the sidecar client returns: a request-level JSON-RPC error
// (Kind == ErrKindRpc, RPC carries code/message/data) or a transport
// failure. Port of pds_sidecar_client::Error.
type Error struct {
	Kind ErrorKind
	RPC  *RpcError
	Err  error // underlying io error, if any
}

// Error implements error.
func (e *Error) Error() string {
	switch e.Kind {
	case ErrKindRpc:
		return fmt.Sprintf("rpc error %d: %s", e.RPC.Code, e.RPC.Message)
	case ErrKindMalformed:
		return "malformed response: " + e.Err.Error()
	case ErrKindIo:
		return "io error: " + e.Err.Error()
	default:
		return "client closed"
	}
}

// Unwrap exposes the underlying error.
func (e *Error) Unwrap() error { return e.Err }

// Code returns the request-level code, if this is an RPC error.
func (e *Error) Code() (int64, bool) {
	if e.Kind == ErrKindRpc && e.RPC != nil {
		return e.RPC.Code, true
	}
	return 0, false
}

// RetryAfter returns the data.retryAfter hint (seconds), if present.
func (e *Error) RetryAfter() (time.Duration, bool) {
	if e.Kind != ErrKindRpc || e.RPC == nil || len(e.RPC.Data) == 0 {
		return 0, false
	}
	var data struct {
		RetryAfter *float64 `json:"retryAfter"`
	}
	if err := json.Unmarshal(e.RPC.Data, &data); err != nil || data.RetryAfter == nil {
		return 0, false
	}
	return time.Duration(max(0, *data.RetryAfter) * float64(time.Second)), true
}

// ErrClosed is returned when the client/stream is already closed.
var ErrClosed = &Error{Kind: ErrKindClosed}

func malformed(format string, args ...any) *Error {
	return &Error{Kind: ErrKindMalformed, Err: fmt.Errorf(format, args...)}
}

// IsConnectionLost reports whether err means the transport itself is gone
// (process exit, stdout EOF, broken pipe), including the synthetic
// -32000 "connection closed" drain error. Mirrors is_connection_lost.
func IsConnectionLost(err error) bool {
	var se *Error
	if !errors.As(err, &se) {
		return false
	}
	if se.Kind == ErrKindIo || se.Kind == ErrKindClosed {
		return true
	}
	return se.Kind == ErrKindRpc && se.RPC != nil &&
		se.RPC.Code == errCodeConnClosedSent && se.RPC.Message == "connection closed"
}

// IsAuthCode matches the auth codes the sidecar maps 400/401 (and "no
// session") to: the host must re-run auth.loginFork.
func IsAuthCode(code int64) bool {
	return code == ErrCodeAuthRequired || code == ErrCodeSessionExpired
}

// AuthErrorCode extracts the auth code (1001/1002) carried by err, or 0.
func AuthErrorCode(err error) int64 {
	var se *Error
	if errors.As(err, &se) {
		if code, ok := se.Code(); ok && IsAuthCode(code) {
			return code
		}
	}
	return 0
}

// API is the full sidecar surface as an interface, so the engine can run
// against the real child process (SidecarClient) or the in-process mock.
// Port of the Rust SidecarApi trait.
type API interface {
	// Auth
	LoginFork(ctx context.Context) (string, error)
	AuthStatus(ctx context.Context) (AuthStatus, error)
	Logout(ctx context.Context) error

	// Drive
	GetRoot(ctx context.Context) (*Node, error)
	ListChildren(ctx context.Context, parentUID string) ([]Node, error)
	GetNode(ctx context.Context, uid string) (*Node, error)
	CreateFolder(ctx context.Context, parentUID, name string) (*Node, error)
	GetAvailableName(ctx context.Context, parentUID, name string) (string, error)
	RenameNode(ctx context.Context, nodeUID, newName string) (*Node, error)
	MoveNode(ctx context.Context, nodeUID, newParentUID string) (bool, error)
	Trash(ctx context.Context, uids []string) ([]TrashResult, error)
	Restore(ctx context.Context, uids []string) ([]TrashResult, error)
	ListTrashed(ctx context.Context) ([]Node, error)

	// Transfers (completion arrives as notifications)
	Upload(ctx context.Context, params *UploadParams) (string, error)
	Download(ctx context.Context, nodeUID, path string) (string, error)
	TransferPause(ctx context.Context, transferID string) error
	TransferResume(ctx context.Context, transferID string) error
	TransferCancel(ctx context.Context, transferID string) error

	// Events
	EventsStart(ctx context.Context, scopeID string, lastEventID *string) error
	EventsStop(ctx context.Context, scopeID string) error
}

// AuthStatus is the result of auth.status.
type AuthStatus struct {
	LoggedIn bool
	Email    *string
}

// SidecarClient is a client connected to a sidecar (real child process or
// in-process mock): the typed API over a JsonRpcClient.
type SidecarClient struct {
	rpc     *JsonRpcClient
	once    sync.Once
	notifCh <-chan Notification
}

// NewSidecarClient builds the typed client over an arbitrary byte stream.
// The notification channel is consumed exactly once by the engine loop.
func NewSidecarClient(r io.Reader, w io.WriteCloser) (*SidecarClient, <-chan Notification) {
	rpc, notif := NewJsonRpcClient(r, w)
	return &SidecarClient{rpc: rpc, notifCh: notif}, notif
}

// Notifications returns the notification stream (once).
func (c *SidecarClient) Notifications() <-chan Notification { return c.notifCh }

// Close shuts the client down.
func (c *SidecarClient) Close() { c.rpc.Close() }

// Child is a running sidecar child process.
type Child struct {
	cmd *exec.Cmd
}

// Wait blocks until the child exits.
func (ch *Child) Wait() error { return ch.cmd.Wait() }

// Kill terminates the child.
func (ch *Child) Kill() { _ = ch.cmd.Process.Kill() }

// SpawnSidecarProcess spawns a sidecar process with piped stdio (stderr
// inherited: human logs only) and connects a client over it.
func SpawnSidecarProcess(program string, args []string, dir *string) (*SidecarClient, *Child, error) {
	cmd := exec.Command(program, args...)
	if dir != nil {
		cmd.Dir = *dir
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, &Error{Kind: ErrKindIo, Err: err}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, &Error{Kind: ErrKindIo, Err: err}
	}
	cmd.Stderr = nil // inherit
	if err := cmd.Start(); err != nil {
		return nil, nil, &Error{Kind: ErrKindIo, Err: err}
	}
	client, _ := NewSidecarClient(stdout, stdin)
	return client, &Child{cmd: cmd}, nil
}

// ---- typed API methods ----

func resultField(raw json.RawMessage, key string) (json.RawMessage, *Error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, malformed("response is not an object")
	}
	v, ok := m[key]
	if !ok {
		return nil, malformed("missing field `%s` in response", key)
	}
	return v, nil
}

func decodeInto(raw json.RawMessage, v any) *Error {
	if err := json.Unmarshal(raw, v); err != nil {
		return malformed("%v", err)
	}
	return nil
}

// LoginFork runs auth.loginFork and returns the email.
func (c *SidecarClient) LoginFork(ctx context.Context) (string, error) {
	raw, err := c.rpc.CallContext(ctx, "auth.loginFork", map[string]any{})
	if err != nil {
		return "", err
	}
	v, ferr := resultField(raw, "email")
	if ferr != nil {
		return "", ferr
	}
	var email string
	if ferr := decodeInto(v, &email); ferr != nil {
		return "", ferr
	}
	return email, nil
}

// AuthStatus runs auth.status.
func (c *SidecarClient) AuthStatus(ctx context.Context) (AuthStatus, error) {
	raw, err := c.rpc.CallContext(ctx, "auth.status", map[string]any{})
	if err != nil {
		return AuthStatus{}, err
	}
	var st struct {
		LoggedIn bool    `json:"loggedIn"`
		Email    *string `json:"email"`
	}
	if ferr := decodeInto(raw, &st); ferr != nil {
		return AuthStatus{}, ferr
	}
	return AuthStatus{LoggedIn: st.LoggedIn, Email: st.Email}, nil
}

// Logout runs auth.logout.
func (c *SidecarClient) Logout(ctx context.Context) error {
	_, err := c.rpc.CallContext(ctx, "auth.logout", map[string]any{})
	return err
}

// GetRoot runs drive.getRoot.
func (c *SidecarClient) GetRoot(ctx context.Context) (*Node, error) {
	raw, err := c.rpc.Call("drive.getRoot", map[string]any{})
	if err != nil {
		return nil, err
	}
	var n Node
	if ferr := decodeInto(raw, &n); ferr != nil {
		return nil, ferr
	}
	return &n, nil
}

// ListChildren runs drive.listChildren.
func (c *SidecarClient) ListChildren(ctx context.Context, parentUID string) ([]Node, error) {
	raw, err := c.rpc.Call("drive.listChildren", map[string]any{"parentUid": parentUID})
	if err != nil {
		return nil, err
	}
	v, ferr := resultField(raw, "nodes")
	if ferr != nil {
		return nil, ferr
	}
	var nodes []Node
	if ferr := decodeInto(v, &nodes); ferr != nil {
		return nil, ferr
	}
	return nodes, nil
}

// GetNode runs drive.getNode.
func (c *SidecarClient) GetNode(ctx context.Context, uid string) (*Node, error) {
	raw, err := c.rpc.Call("drive.getNode", map[string]any{"uid": uid})
	if err != nil {
		return nil, err
	}
	var n Node
	if ferr := decodeInto(raw, &n); ferr != nil {
		return nil, ferr
	}
	return &n, nil
}

// CreateFolder runs drive.createFolder.
func (c *SidecarClient) CreateFolder(ctx context.Context, parentUID, name string) (*Node, error) {
	raw, err := c.rpc.Call("drive.createFolder", map[string]any{"parentUid": parentUID, "name": name})
	if err != nil {
		return nil, err
	}
	var n Node
	if ferr := decodeInto(raw, &n); ferr != nil {
		return nil, ferr
	}
	return &n, nil
}

// GetAvailableName runs drive.getAvailableName.
func (c *SidecarClient) GetAvailableName(ctx context.Context, parentUID, name string) (string, error) {
	raw, err := c.rpc.Call("drive.getAvailableName", map[string]any{"parentUid": parentUID, "name": name})
	if err != nil {
		return "", err
	}
	v, ferr := resultField(raw, "name")
	if ferr != nil {
		return "", ferr
	}
	var out string
	if ferr := decodeInto(v, &out); ferr != nil {
		return "", ferr
	}
	return out, nil
}

// RenameNode runs drive.rename.
func (c *SidecarClient) RenameNode(ctx context.Context, nodeUID, newName string) (*Node, error) {
	raw, err := c.rpc.Call("drive.rename", map[string]any{"nodeUid": nodeUID, "newName": newName})
	if err != nil {
		return nil, err
	}
	v, ferr := resultField(raw, "node")
	if ferr != nil {
		return nil, ferr
	}
	var n Node
	if ferr := decodeInto(v, &n); ferr != nil {
		return nil, ferr
	}
	return &n, nil
}

// MoveNode runs drive.move.
func (c *SidecarClient) MoveNode(ctx context.Context, nodeUID, newParentUID string) (bool, error) {
	raw, err := c.rpc.Call("drive.move", map[string]any{"nodeUid": nodeUID, "newParentUid": newParentUID})
	if err != nil {
		return false, err
	}
	v, ferr := resultField(raw, "results")
	if ferr != nil {
		return false, ferr
	}
	var results []TrashResult
	if ferr := decodeInto(v, &results); ferr != nil {
		return false, ferr
	}
	return len(results) > 0 && results[0].OK, nil
}

func callUIDs(ctx context.Context, c *SidecarClient, method string, uids []string) ([]TrashResult, error) {
	raw, err := c.rpc.Call(method, map[string]any{"uids": uids})
	if err != nil {
		return nil, err
	}
	v, ferr := resultField(raw, "results")
	if ferr != nil {
		return nil, ferr
	}
	var results []TrashResult
	if ferr := decodeInto(v, &results); ferr != nil {
		return nil, ferr
	}
	return results, nil
}

// Trash runs drive.trash.
func (c *SidecarClient) Trash(ctx context.Context, uids []string) ([]TrashResult, error) {
	return callUIDs(ctx, c, "drive.trash", uids)
}

// Restore runs drive.restore.
func (c *SidecarClient) Restore(ctx context.Context, uids []string) ([]TrashResult, error) {
	return callUIDs(ctx, c, "drive.restore", uids)
}

// ListTrashed runs drive.listTrashed.
func (c *SidecarClient) ListTrashed(ctx context.Context) ([]Node, error) {
	raw, err := c.rpc.Call("drive.listTrashed", map[string]any{})
	if err != nil {
		return nil, err
	}
	v, ferr := resultField(raw, "nodes")
	if ferr != nil {
		return nil, ferr
	}
	var nodes []Node
	if ferr := decodeInto(v, &nodes); ferr != nil {
		return nil, ferr
	}
	return nodes, nil
}

// Upload runs drive.upload.
func (c *SidecarClient) Upload(ctx context.Context, params *UploadParams) (string, error) {
	raw, err := c.rpc.Call("drive.upload", params)
	if err != nil {
		return "", err
	}
	v, ferr := resultField(raw, "transferId")
	if ferr != nil {
		return "", ferr
	}
	var id string
	if ferr := decodeInto(v, &id); ferr != nil {
		return "", ferr
	}
	return id, nil
}

// Download runs drive.download.
func (c *SidecarClient) Download(ctx context.Context, nodeUID, path string) (string, error) {
	raw, err := c.rpc.Call("drive.download", map[string]any{"nodeUid": nodeUID, "path": path})
	if err != nil {
		return "", err
	}
	v, ferr := resultField(raw, "transferId")
	if ferr != nil {
		return "", ferr
	}
	var id string
	if ferr := decodeInto(v, &id); ferr != nil {
		return "", ferr
	}
	return id, nil
}

func transferCall(c *SidecarClient, method, transferID string) error {
	_, err := c.rpc.Call(method, map[string]any{"transferId": transferID})
	return err
}

// TransferPause pauses a transfer.
func (c *SidecarClient) TransferPause(ctx context.Context, transferID string) error {
	return transferCall(c, "transfer.pause", transferID)
}

// TransferResume resumes a transfer.
func (c *SidecarClient) TransferResume(ctx context.Context, transferID string) error {
	return transferCall(c, "transfer.resume", transferID)
}

// TransferCancel cancels a transfer.
func (c *SidecarClient) TransferCancel(ctx context.Context, transferID string) error {
	return transferCall(c, "transfer.cancel", transferID)
}

// EventsStart runs drive.events.start (resuming after lastEventID if given).
func (c *SidecarClient) EventsStart(ctx context.Context, scopeID string, lastEventID *string) error {
	params := map[string]any{"scopeId": scopeID}
	if lastEventID != nil {
		params["lastEventId"] = *lastEventID
	}
	_, err := c.rpc.Call("drive.events.start", params)
	return err
}

// EventsStop runs drive.events.stop.
func (c *SidecarClient) EventsStop(ctx context.Context, scopeID string) error {
	_, err := c.rpc.Call("drive.events.stop", map[string]any{"scopeId": scopeID})
	return err
}

// compile-time checks
var _ API = (*SidecarClient)(nil)

// unused import guards (bufio kept for future scanner use)
var _ = bufio.NewReader
