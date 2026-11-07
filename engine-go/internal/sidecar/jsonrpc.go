package sidecar

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"
)

// RpcError, ErrorKind, Error and helpers live in client.go.

// --- Framing (pure functions, unit-tested) ---

// encodeRequest builds the NDJSON request line for id/method/params.
func encodeRequest(id uint64, method string, params any) (string, error) {
	if params == nil {
		params = struct{}{}
	}
	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodedMessage is one decoded inbound NDJSON line: either a response
// (correlated by id) or a notification (method present, no id).
type decodedMessage struct {
	hasID   bool
	id      uint64
	method  string
	params  json.RawMessage
	result  json.RawMessage
	rpcErr  *RpcError
	unknown bool // neither id nor method: ignore
}

// decodeMessage parses one server line. An unparseable line returns an
// error (the read loop skips it, mirroring the Rust client's warn+skip).
func decodeMessage(line []byte) (decodedMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return decodedMessage{}, err
	}
	var m decodedMessage
	if idRaw, ok := raw["id"]; ok {
		var id float64
		if err := json.Unmarshal(idRaw, &id); err != nil || id < 0 {
			return decodedMessage{}, fmt.Errorf("invalid id")
		}
		m.hasID = true
		m.id = uint64(id)
		if errRaw, ok := raw["error"]; ok && !jsonIsNull(errRaw) {
			rpcErr := &RpcError{Code: -32000, Message: "unknown error"}
			var errObj struct {
				Code    *int64          `json:"code"`
				Message *string         `json:"message"`
				Data    json.RawMessage `json:"data"`
			}
			if json.Unmarshal(errRaw, &errObj) == nil {
				if errObj.Code != nil {
					rpcErr.Code = *errObj.Code
				}
				if errObj.Message != nil {
					rpcErr.Message = *errObj.Message
				}
				rpcErr.Data = errObj.Data
			}
			m.rpcErr = rpcErr
		} else {
			m.result = raw["result"] // nil (JSON null) is preserved like serde's Value::Null
		}
		return m, nil
	}
	if methodRaw, ok := raw["method"]; ok {
		if err := json.Unmarshal(methodRaw, &m.method); err != nil {
			return decodedMessage{}, fmt.Errorf("method is not a string")
		}
		m.params = raw["params"]
		return m, nil
	}
	m.unknown = true
	return m, nil
}

func jsonIsNull(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) == "null"
}

func isKnownEventKind(k EventKind) bool {
	switch k {
	case EventNodeCreated, EventNodeUpdated, EventNodeDeleted,
		EventTreeRefresh, EventTreeRemove, EventFastForward:
		return true
	}
	return false
}

// parseNotification maps a notification method + params onto the decoded
// Notification variants. Missing fields default like the Rust decoder
// ("" / 0 / false / nil).
func parseNotification(method string, params json.RawMessage) Notification {
	var p map[string]any
	if len(params) > 0 && !jsonIsNull(params) {
		_ = json.Unmarshal(params, &p) // best-effort; missing → defaults below
	}
	str := func(k string) string {
		if v, ok := p[k].(string); ok {
			return v
		}
		return ""
	}
	intVal := func(k string) int64 {
		if v, ok := p[k].(float64); ok {
			return int64(v)
		}
		return 0
	}
	optStr := func(k string) *string {
		if v, ok := p[k].(string); ok {
			return &v
		}
		return nil
	}
	optF64 := func(k string) *float64 {
		if v, ok := p[k].(float64); ok {
			return &v
		}
		return nil
	}
	switch method {
	case "auth.signInUrl":
		return &AuthSignInUrlNotification{URL: str("url"), UserCode: str("userCode")}
	case "transfer.progress":
		return &TransferProgressNotification{
			TransferID: str("transferId"),
			Bytes:      intVal("bytes"),
			Total:      intVal("total"),
		}
	case "transfer.done":
		return &TransferDoneNotification{
			TransferID:      str("transferId"),
			NodeUID:         optStr("nodeUid"),
			NodeRevisionUID: optStr("nodeRevisionUid"),
		}
	case "transfer.error":
		retryable := false
		if v, ok := p["retryable"].(bool); ok {
			retryable = v
		}
		n := &TransferErrorNotification{
			TransferID: str("transferId"),
			Code:       intVal("code"),
			Message:    str("message"),
			Retryable:  retryable,
		}
		if ra := optF64("retryAfter"); ra != nil && *ra >= 0 {
			d := time.Duration(*ra * float64(time.Second))
			n.RetryAfter = &d
		}
		return n
	case "drive.event":
		var ev DriveEvent
		// Unknown "type" values must fall back to Other, like serde's
		// enum decode failure on the Rust side.
		if err := json.Unmarshal(params, &ev); err == nil && ev.Kind != "" && isKnownEventKind(ev.Kind) {
			return &DriveEventNotification{Event: &ev}
		}
		return &OtherNotification{Method: method, Params: params}
	default:
		return &OtherNotification{Method: method, Params: params}
	}
}

// --- Async client ---

type callResult struct {
	result json.RawMessage
	err    *RpcError
}

type shared struct {
	mu      sync.Mutex
	pending map[uint64]chan callResult
	nextID  uint64
	// closed is set by the read loop once the stream is gone. Checked
	// under mu so a request cannot slip in after the drain.
	closed  bool
	notifCh chan Notification
}

// JsonRpcClient is an async JSON-RPC 2.0 client over an NDJSON stream.
// Safe for concurrent use; calls are correlated by id.
type JsonRpcClient struct {
	shared    *shared
	writeCh   chan string
	writeDone chan struct{}
	closeOnce sync.Once
}

// NewJsonRpcClient creates a client over an arbitrary byte stream (the
// sidecar's stdout/stdin, an in-memory pipe for tests and the mock).
// It returns the client and a channel delivering decoded server
// notifications. The background goroutines exit when the stream closes
// or Close is called.
func NewJsonRpcClient(r io.Reader, w io.WriteCloser) (*JsonRpcClient, <-chan Notification) {
	sh := &shared{
		pending: make(map[uint64]chan callResult),
		notifCh: make(chan Notification, 256),
	}
	c := &JsonRpcClient{
		shared:    sh,
		writeCh:   make(chan string, 256),
		writeDone: make(chan struct{}),
	}
	go c.writeLoop(w)
	go c.readLoop(r, sh)
	return c, sh.notifCh
}

// Close shuts the client down: the write side is closed (which makes the
// read loop see EOF on stream-shaped transports) and pending requests are
// failed. It is idempotent.
func (c *JsonRpcClient) Close() {
	c.closeOnce.Do(func() { close(c.writeCh) })
}

// Notifications returns the notification channel (mainly for tests; the
// SidecarClient owns it in production use).
func (c *JsonRpcClient) Notifications() <-chan Notification { return c.shared.notifCh }

// Call sends a request and awaits the correlated response.
func (c *JsonRpcClient) Call(method string, params any) (json.RawMessage, error) {
	c.shared.mu.Lock()
	// The read loop is gone: nobody will ever answer this request.
	// Registering it after the EOF drain would hang forever.
	if c.shared.closed {
		c.shared.mu.Unlock()
		return nil, ErrClosed
	}
	id := c.shared.nextID
	c.shared.nextID++
	ch := make(chan callResult, 1)
	c.shared.pending[id] = ch
	c.shared.mu.Unlock()

	line, err := encodeRequest(id, method, params)
	if err != nil {
		c.shared.mu.Lock()
		delete(c.shared.pending, id)
		c.shared.mu.Unlock()
		return nil, malformed("%v", err)
	}

	select {
	case c.writeCh <- line:
	case <-c.writeDone:
		return nil, ErrClosed
	}

	res, ok := <-ch
	if !ok {
		return nil, ErrClosed
	}
	if res.err != nil {
		return nil, &Error{Kind: ErrKindRpc, RPC: res.err}
	}
	return res.result, nil
}

func (c *JsonRpcClient) writeLoop(w io.WriteCloser) {
	defer close(c.writeDone)
	bw := bufio.NewWriter(w)
	for line := range c.writeCh {
		if _, err := bw.WriteString(line + "\n"); err != nil {
			return
		}
		if err := bw.Flush(); err != nil {
			return
		}
	}
	// Client closed: flush and close the write side so the peer (and our
	// own read loop, on stream transports) sees EOF.
	_ = bw.Flush()
	_ = w.Close()
}

func (c *JsonRpcClient) readLoop(r io.Reader, sh *shared) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			break
		}
		trimmed := trimSpace(line)
		if trimmed == "" {
			continue
		}
		msg, derr := decodeMessage([]byte(trimmed))
		if derr != nil {
			// Skip malformed lines, mirroring the Rust client.
			continue
		}
		c.dispatch(msg, sh)
	}
	// Fail every pending request so callers don't hang. Set closed first:
	// a Call either registered before this (and is drained below) or sees
	// the flag under the same lock and fails immediately.
	sh.mu.Lock()
	sh.closed = true
	for id, ch := range sh.pending {
		ch <- callResult{err: &RpcError{
			Code:    errCodeConnClosedSent,
			Message: "connection closed",
		}}
		close(ch)
		delete(sh.pending, id)
	}
	sh.mu.Unlock()
	close(sh.notifCh)
}

func (c *JsonRpcClient) dispatch(msg decodedMessage, sh *shared) {
	switch {
	case msg.hasID:
		sh.mu.Lock()
		ch, ok := sh.pending[msg.id]
		if ok {
			delete(sh.pending, msg.id)
		}
		sh.mu.Unlock()
		if !ok {
			// response for unknown id: ignore
			return
		}
		if msg.rpcErr != nil {
			ch <- callResult{err: msg.rpcErr}
		} else if msg.result == nil {
			ch <- callResult{result: json.RawMessage("null")}
		} else {
			ch <- callResult{result: msg.result}
		}
		close(ch)
	case msg.method != "":
		notif := parseNotification(msg.method, msg.params)
		// Receiver gone (channel closed/full): client is shutting down;
		// drop silently, like tokio's try_send.
		select {
		case sh.notifCh <- notif:
		default:
		}
	}
	// neither id nor method: ignore
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && isSpaceByte(s[start]) {
		start++
	}
	end := len(s)
	for end > start && isSpaceByte(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// itoa is a tiny helper kept for parity with Rust's format! uses.
func itoa(n int) string { return strconv.Itoa(n) }
