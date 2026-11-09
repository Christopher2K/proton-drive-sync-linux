package sidecar

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// jsonrpc_test.go: JSON-RPC framing/correlation tests against the
// in-process Mock and a hand-scripted server for out-of-order/error edge
// cases. Translation of crates/sidecar-client/tests/jsonrpc.rs.

func ctxT(t *testing.T, seconds time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), seconds)
	t.Cleanup(cancel)
	return ctx
}

// call_response_round_trip
func TestCallResponseRoundTrip(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	root, err := client.GetRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root.UID != "root" || !root.IsFolder() {
		t.Fatalf("root = %+v", root)
	}

	children, err := client.ListChildren(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, n := range children {
		names = append(names, n.Name)
	}
	if len(names) != 2 || names[0] != "Documents" || names[1] != "hello.txt" {
		t.Fatalf("children = %v", names)
	}
}

// protocol_error_maps_code
func TestProtocolErrorMapsCode(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	// "Documents" already exists under root → createFolder must fail with 1005.
	_, err := client.CreateFolder(ctx, "root", "Documents")
	if err == nil {
		t.Fatal("expected conflict error")
	}
	code, ok := err.(*Error).Code()
	if !ok || code != ErrCodeConflict {
		t.Fatalf("err = %v, want code %d", err, ErrCodeConflict)
	}
}

// restore_round_trip: trash → list → restore round trip, plus per-node
// error results for unknown uids (drive.restore mirrors drive.trash's
// result shape).
func TestRestoreRoundTrip(t *testing.T) {
	client, handle := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	uid, ok := handle.FindUID("root", "hello.txt")
	if !ok {
		t.Fatal("hello.txt not found")
	}
	results, err := client.Trash(ctx, []string{uid})
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].OK {
		t.Fatalf("trash result %+v", results[0])
	}
	if got := handle.ChildNames("root"); len(got) != 1 || got[0] != "Documents" {
		t.Fatalf("children after trash = %v", got)
	}

	trashed, err := client.ListTrashed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(trashed) != 1 || trashed[0].UID != uid {
		t.Fatalf("trashed = %+v", trashed)
	}

	restore, err := client.Restore(ctx, []string{uid, "no-such"})
	if err != nil {
		t.Fatal(err)
	}
	if len(restore) != 2 {
		t.Fatalf("restore results = %+v", restore)
	}
	if restore[0].UID != uid || !restore[0].OK || restore[0].Error != nil {
		t.Fatalf("restore[0] = %+v", restore[0])
	}
	if restore[1].OK || restore[1].Error == nil {
		t.Fatalf("restore[1] = %+v", restore[1])
	}

	// Restored node is visible as a child again.
	if got := handle.ChildNames("root"); len(got) != 2 {
		t.Fatalf("children after restore = %v", got)
	}
	if trashed, err := client.ListTrashed(ctx); err != nil || len(trashed) != 0 {
		t.Fatalf("trashed after restore = %+v (%v)", trashed, err)
	}
	if got := handle.RestoredUIDs(); len(got) != 2 || got[0] != uid || got[1] != "no-such" {
		t.Fatalf("restored uids = %v", got)
	}
}

// rename_round_trips_and_collides
func TestRenameRoundTripsAndCollides(t *testing.T) {
	client, handle := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	node, err := client.RenameNode(ctx, "node-hello", "hi.txt")
	if err != nil {
		t.Fatal(err)
	}
	if node.Name != "hi.txt" {
		t.Fatalf("node = %+v", node)
	}
	// Colliding with the sibling "Documents" name (as a file, still a name
	// clash in the mock's sibling check) errors with 1005.
	_, err = client.RenameNode(ctx, "node-hello", "Documents")
	code, ok := err.(*Error).Code()
	if !ok || code != ErrCodeConflict {
		t.Fatalf("rename clash err = %v", err)
	}
	// Unknown uid → protocol error.
	if _, err := client.RenameNode(ctx, "nope", "x"); err == nil {
		t.Fatal("expected error for unknown uid")
	}
	renames := handle.Renames()
	if len(renames) == 0 || renames[0][0] != "node-hello" || renames[0][1] != "hi.txt" {
		t.Fatalf("renames = %v", renames)
	}
}

// move_round_trips
func TestMoveRoundTrips(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	// Move hello.txt into Documents (parent uid node-docs).
	ok, err := client.MoveNode(ctx, "node-hello", "node-docs")
	if err != nil || !ok {
		t.Fatalf("move = %v, %v", ok, err)
	}
	children, err := client.ListChildren(ctx, "node-docs")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range children {
		if n.Name == "hello.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("hello.txt not in Documents: %+v", children)
	}
	// Unknown parent fails with ok=false (result-level, not an error).
	ok, err = client.MoveNode(ctx, "node-hello", "nope")
	if err != nil || ok {
		t.Fatalf("move to unknown parent = %v, %v", ok, err)
	}
}

// concurrent_calls_are_correlated
func TestConcurrentCallsAreCorrelated(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)

	var (
		wg                                     sync.WaitGroup
		mu                                     sync.Mutex
		childrenLen, nodeName, loggedInChecked bool
		firstErr                               error
	)
	record := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	wg.Add(3)
	go func() {
		defer wg.Done()
		kids, err := client.ListChildren(ctx, "root")
		record(err)
		mu.Lock()
		childrenLen = err == nil && len(kids) == 2
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		n, err := client.GetNode(ctx, "node-hello")
		record(err)
		mu.Lock()
		nodeName = err == nil && n.Name == "hello.txt"
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		st, err := client.AuthStatus(ctx)
		record(err)
		mu.Lock()
		loggedInChecked = err == nil && st.LoggedIn
		mu.Unlock()
	}()
	wg.Wait()
	if firstErr != nil || !childrenLen || !nodeName || !loggedInChecked {
		t.Fatalf("children=%v node=%v loggedIn=%v err=%v", childrenLen, nodeName, loggedInChecked, firstErr)
	}
}

// notifications_are_dispatched
func TestNotificationsAreDispatched(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)
	rx := client.Notifications()

	// events.start resolves, then a fast_forward drive.event arrives.
	if err := client.EventsStart(ctx, "scope-1", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-rx:
		ev, ok := n.(*DriveEventNotification)
		if !ok {
			t.Fatalf("expected drive.event, got %#v", n)
		}
		if ev.Event.Kind != EventFastForward || ev.Event.ScopeID != "scope-1" {
			t.Fatalf("event = %+v", ev.Event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no drive.event notification")
	}
}

// login_fork_emits_sign_in_url_then_resolves
func TestLoginForkEmitsSignInUrlThenResolves(t *testing.T) {
	client, _ := NewMock().Connect()
	defer client.Close()
	ctx := ctxT(t, 5*time.Second)
	rx := client.Notifications()

	email, err := client.LoginFork(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if email != "mock@proton.me" {
		t.Fatalf("email = %q", email)
	}
	select {
	case n := <-rx:
		su, ok := n.(*AuthSignInUrlNotification)
		if !ok {
			t.Fatalf("expected auth.signInUrl, got %#v", n)
		}
		if su.URL == "" || !contains(su.URL, "account.proton.me") || su.UserCode != "MOCK-CODE" {
			t.Fatalf("signInUrl = %+v", su)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no auth.signInUrl notification")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// out_of_order_responses_are_correlated: hand-scripted server responds to
// requests out of order to prove the client correlates by id, not arrival
// order.
func TestOutOfOrderResponsesAreCorrelated(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	rpc, _ := NewJsonRpcClient(clientConn, clientConn)
	defer rpc.Close()

	go func() {
		defer serverConn.Close()
		// Read two requests, then answer them in reverse id order.
		type pending struct {
			id     uint64
			method string
		}
		var reqs []pending
		dec := json.NewDecoder(serverConn)
		for i := 0; i < 2; i++ {
			var msg struct {
				ID     uint64 `json:"id"`
				Method string `json:"method"`
			}
			if err := dec.Decode(&msg); err != nil {
				return
			}
			reqs = append(reqs, pending{msg.ID, msg.Method})
		}
		reqs[0], reqs[1] = reqs[1], reqs[0]
		for _, r := range reqs {
			resp, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": r.id, "result": map[string]any{"echo": r.method},
			})
			if _, err := serverConn.Write(append(resp, '\n')); err != nil {
				return
			}
		}
		_ = serverConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}()

	// Both calls are in flight concurrently (like tokio::join!), so the
	// server can read two requests before answering either.
	r1c, r2c := make(chan json.RawMessage, 1), make(chan json.RawMessage, 1)
	e1c, e2c := make(chan error, 1), make(chan error, 1)
	go func() { r, err := rpc.Call("first", map[string]any{}); r1c <- r; e1c <- err }()
	go func() { r, err := rpc.Call("second", map[string]any{}); r2c <- r; e2c <- err }()
	r1, r2 := <-r1c, <-r2c
	err1, err2 := <-e1c, <-e2c
	if err1 != nil || err2 != nil {
		t.Fatalf("calls: %v %v", err1, err2)
	}
	var echo1, echo2 struct {
		Echo string `json:"echo"`
	}
	if err := json.Unmarshal(r1, &echo1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(r2, &echo2); err != nil {
		t.Fatal(err)
	}
	if echo1.Echo != "first" || echo2.Echo != "second" {
		t.Fatalf("echoes = %q %q", echo1.Echo, echo2.Echo)
	}
}

// pending_request_fails_when_server_exits (TG-21): a request already in
// flight when the child's stdout closes must fail with a typed error,
// never hang forever waiting for a reply.
func TestPendingRequestFailsWhenServerExits(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	rpc, _ := NewJsonRpcClient(clientConn, clientConn)
	defer rpc.Close()

	// Read the request, then exit without answering (sidecar crash).
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := serverConn.Read(buf); err != nil {
				return
			}
			// Wait for the request line, then close.
			if contains(string(buf), "drive.getRoot") {
				serverConn.Close()
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := rpc.Call("drive.getRoot", map[string]any{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a typed transport error")
		}
		if se, ok := err.(*Error); ok {
			if se.Kind != ErrKindClosed && !(se.Kind == ErrKindRpc && se.RPC != nil &&
				se.RPC.Code == errCodeConnClosedSent) {
				t.Fatalf("unexpected error kind: %v", err)
			}
		} else {
			t.Fatalf("expected *Error, got %T: %v", err, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending request hung after the sidecar exited")
	}

	// A request issued AFTER the read loop drained its pending map must
	// also fail fast: registering it post-drain used to await an answer
	// forever.
	late := make(chan error, 1)
	go func() {
		_, err := rpc.Call("drive.getRoot", map[string]any{})
		late <- err
	}()
	select {
	case err := <-late:
		if err == nil {
			t.Fatal("post-EOF request must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("post-EOF request hung")
	}
}

// closed_connection_fails_calls: server closing the connection must fail
// pending and future calls.
func TestClosedConnectionFailsCalls(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	rpc, _ := NewJsonRpcClient(clientConn, clientConn)
	defer rpc.Close()
	serverConn.Close() // close immediately

	done := make(chan error, 1)
	go func() {
		_, err := rpc.Call("anything", map[string]any{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("call must fail on a closed connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call hung on closed connection")
	}
}

func TestCallContextCancellationForgetsPendingRequest(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	rpc, _ := NewJsonRpcClient(clientConn, clientConn)
	defer rpc.Close()
	defer serverConn.Close()

	requestRead := make(chan struct{})
	go func() {
		var request map[string]any
		if err := json.NewDecoder(serverConn).Decode(&request); err == nil {
			close(requestRead)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rpc.CallContext(ctx, "auth.logout", map[string]any{})
		done <- err
	}()
	select {
	case <-requestRead:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received request")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("CallContext error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallContext ignored cancellation")
	}
	rpc.shared.mu.Lock()
	pending := len(rpc.shared.pending)
	rpc.shared.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending requests after cancellation = %d, want 0", pending)
	}
}

// --- framing unit tests (new, Go-specific: the pure decode helpers) ---

func TestEncodeRequestFraming(t *testing.T) {
	line, err := encodeRequest(7, "drive.getNode", map[string]any{"uid": "u"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["jsonrpc"] != "2.0" || decoded["id"].(float64) != 7 ||
		decoded["method"] != "drive.getNode" {
		t.Fatalf("line = %s", line)
	}
}

func TestDecodeMessageVariants(t *testing.T) {
	t.Run("response with result", func(t *testing.T) {
		m, err := decodeMessage([]byte(`{"jsonrpc":"2.0","id":3,"result":{"a":1}}`))
		if err != nil || !m.hasID || m.id != 3 || m.rpcErr != nil || string(m.result) != `{"a":1}` {
			t.Fatalf("m = %+v err = %v", m, err)
		}
	})
	t.Run("response with error and data", func(t *testing.T) {
		m, err := decodeMessage([]byte(`{"jsonrpc":"2.0","id":4,"error":{"code":1003,"message":"rate limited","data":{"retryAfter":1.5}}}`))
		if err != nil || !m.hasID || m.rpcErr == nil {
			t.Fatalf("m = %+v err = %v", m, err)
		}
		if m.rpcErr.Code != 1003 || m.rpcErr.Message != "rate limited" {
			t.Fatalf("rpcErr = %+v", m.rpcErr)
		}
		// The rate-limit hint (data.retryAfter) decodes via Error.RetryAfter.
		e := &Error{Kind: ErrKindRpc, RPC: m.rpcErr}
		if ra, ok := e.RetryAfter(); !ok || ra != 1500*time.Millisecond {
			t.Fatalf("RetryAfter = %v %v", ra, ok)
		}
	})
	t.Run("notification", func(t *testing.T) {
		m, err := decodeMessage([]byte(`{"jsonrpc":"2.0","method":"transfer.progress","params":{"transferId":"t1","bytes":2,"total":4}}`))
		if err != nil || m.hasID || m.method != "transfer.progress" {
			t.Fatalf("m = %+v err = %v", m, err)
		}
	})
	t.Run("malformed line is an error", func(t *testing.T) {
		if _, err := decodeMessage([]byte(`{not json`)); err == nil {
			t.Fatal("expected decode error")
		}
	})
}

func TestParseNotificationVariants(t *testing.T) {
	t.Run("transfer error retryAfter decodes as duration", func(t *testing.T) {
		n := parseNotification("transfer.error", json.RawMessage(
			`{"transferId":"t9","code":1003,"message":"rate_limited","retryable":true,"retryAfter":2.5}`))
		te, ok := n.(*TransferErrorNotification)
		if !ok {
			t.Fatalf("n = %#v", n)
		}
		if te.Code != 1003 || !te.Retryable || te.RetryAfter == nil ||
			*te.RetryAfter != 2500*time.Millisecond {
			t.Fatalf("te = %+v", te)
		}
	})
	t.Run("unknown method becomes Other", func(t *testing.T) {
		n := parseNotification("some.future.method", json.RawMessage(`{"x":1}`))
		other, ok := n.(*OtherNotification)
		if !ok || other.Method != "some.future.method" {
			t.Fatalf("n = %#v", n)
		}
	})
	t.Run("drive event with unknown kind becomes Other", func(t *testing.T) {
		n := parseNotification("drive.event", json.RawMessage(`{"scopeId":"s","eventId":"e","type":"martian"}`))
		if _, ok := n.(*DriveEventNotification); ok {
			t.Fatalf("unknown event kind should fall back to Other: %#v", n)
		}
	})
}

// pending drain on EOF must fail callers, not hang (write side unused).
func TestReadLoopFailsPendingOnEOF(t *testing.T) {
	pr, pw := io.Pipe()
	// Tee: feed the client's reads from pr; the test server goroutine
	// consumes the request bytes from a second pipe reader.
	serverRead := pr
	rpc, _ := NewJsonRpcClient(serverRead, &nopWriteCloser{})
	defer rpc.Close()
	go func() {
		buf := make([]byte, 4096)
		_, _ = pw.Write(buf) // keep pw referenced; no-op
	}()
	// Close the data source after the request is written: since the test
	// never writes a response, simply EOF the reader side immediately
	// after the request line is produced. Simplest deterministic route:
	// close pw right away — the read loop sees EOF after zero bytes,
	// which still drains pending requests registered before EOF. To
	// exercise "registered then EOF", write the request first.
	go func() {
		// Give the call a moment to register, then EOF.
		time.Sleep(50 * time.Millisecond)
		pw.Close()
	}()
	done := make(chan error, 1)
	go func() {
		_, err := rpc.Call("x", map[string]any{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error on EOF")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call hung after EOF")
	}
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }
