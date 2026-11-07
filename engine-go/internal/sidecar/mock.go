package sidecar

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mock is the deterministic in-process sidecar for tests: it speaks the
// real JSON-RPC line protocol over an in-memory pipe, so the production
// client's framing/correlation paths are exercised. Port of
// crates/sidecar-client/src/mock.rs (mock surface needed by the engine
// test battery).
type Mock struct {
	st *mockState
}

// MockHandle inspects calls and injects remote events while a Mock server
// runs.
type MockHandle struct {
	st *mockState
}

type mockNode struct {
	uid       string
	parentUID *string
	name      string
	kind      string // "file"|"folder"
	size      *int64
	mtime     *int64
	sha1      *string
	content   []byte
	trashed   bool
}

type scriptedFailure struct {
	code       int64
	message    string
	retryable  bool
	retryAfter *time.Duration
}

type heldDownload struct {
	path    string
	content []byte
	written int
	done    string // serialized completion notification
}

type call struct {
	method string
	args   []string
}

type mockState struct {
	mu           sync.Mutex
	nodes        map[string]*mockNode
	nextNode     int64
	nextTransfer int64
	nextEvent    int64
	scopeID      string
	calls        []call
	uploadMtimes []struct {
		name  string
		mtime int64
	}
	uploadSizes []struct {
		name string
		size uint64
	}
	uploadFailures    map[string][]scriptedFailure
	downloadFailures  map[string][]scriptedFailure
	getRootFailures   int
	listChildrenFails int
	offline           bool
	crashNextUpload   bool
	crashed           bool
	downloadHolds     map[string]int
	heldDownloads     map[string]*heldDownload
	outbox            chan string
	eventsStartCalls  int
	downloadDelay     time.Duration
}

// NewMock builds the default mock tree: root `root` containing folder
// `Documents` (empty) and file `hello.txt` ("hello drive").
func NewMock() *Mock {
	nodes := map[string]*mockNode{}
	root := "root"
	nodes[root] = &mockNode{uid: root, name: "", kind: "folder"}
	docs := "node-docs"
	nodes[docs] = &mockNode{uid: docs, parentUID: &root, name: "Documents", kind: "folder", mtime: ip(1700000000)}
	hello := "node-hello"
	content := []byte("hello drive")
	nodes[hello] = &mockNode{
		uid: hello, parentUID: &root, name: "hello.txt", kind: "file",
		size: ip(int64(len(content))), mtime: ip(1700000000),
		sha1: sp(hexSHA1(content)), content: content,
	}
	return &Mock{st: &mockState{
		nodes: nodes, nextNode: 100, scopeID: "scope-1",
		uploadFailures:   map[string][]scriptedFailure{},
		downloadFailures: map[string][]scriptedFailure{},
		downloadHolds:    map[string]int{},
		heldDownloads:    map[string]*heldDownload{},
		outbox:           make(chan string, 1024),
	}}
}

// NewMockWithSeq starts the node/transfer id counters at seq (a fresh mock
// used against a DB written by an earlier mock must not reissue ids).
func NewMockWithSeq(seq int64) *Mock {
	m := NewMock()
	m.st.nextNode = seq
	m.st.nextTransfer = seq
	return m
}

// Handle returns the test handle.
func (m *Mock) Handle() *MockHandle { return &MockHandle{st: m.st} }

// Connect runs the mock server over an in-memory pipe and returns a real
// SidecarClient plus the handle.
func (m *Mock) Connect() (*SidecarClient, *MockHandle) {
	c1, c2 := net.Pipe()
	handle := m.Handle()
	go m.serve(c2)
	client, _ := NewSidecarClient(c1, c1)
	return client, handle
}

// serve runs until the peer closes: one writer goroutine interleaves
// request responses and outbox notifications, mirroring the Rust mock's
// tokio::select! loop, so an event injected while no request is in
// flight is still delivered promptly.
func (m *Mock) serve(conn io.ReadWriteCloser) {
	defer conn.Close()
	type outbound struct {
		msg string
	}
	out := make(chan outbound, 1024)
	// Request reader.
	go func() {
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				close(out)
				return
			}
			if len(strings.TrimSpace(string(line))) == 0 {
				continue
			}
			for _, resp := range m.handleLine(string(line)) {
				out <- outbound{msg: resp}
			}
			m.st.mu.Lock()
			crashed := m.st.crashed
			m.st.mu.Unlock()
			if crashed {
				close(out) // simulated mid-transfer process death
				return
			}
		}
	}()
	// Outbox pump.
	go func() {
		for {
			m.st.mu.Lock()
			ch := m.st.outbox
			m.st.mu.Unlock()
			msg, ok := <-ch
			if !ok {
				return
			}
			out <- outbound{msg: msg}
		}
	}()
	w := bufio.NewWriter(conn)
	for o := range out {
		if _, err := w.WriteString(o.msg + "\n"); err != nil {
			return
		}
		if w.Flush() != nil {
			return
		}
	}
}

func (s *mockState) allocUID() string {
	s.nextNode++
	return fmt.Sprintf("node-%d", s.nextNode)
}

func (s *mockState) allocEventID() string {
	s.nextEvent++
	return fmt.Sprintf("evt-%d", s.nextEvent)
}

func (s *mockState) enqueue(msg string) {
	select {
	case s.outbox <- msg:
	default:
	}
}

func (s *mockState) emitEvent(params map[string]any) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": "drive.event", "params": params,
	})
	s.enqueue(string(b))
}

func (s *mockState) nodeJSON(n *mockNode) map[string]any {
	return map[string]any{
		"uid": n.uid, "parentUid": n.parentUID, "name": n.name, "type": n.kind,
		"size": n.size, "mtime": n.mtime, "sha1": n.sha1,
		"treeEventScopeId": s.scopeID,
	}
}

func (s *mockState) childrenOf(uid string) []*mockNode {
	var v []*mockNode
	for _, n := range s.nodes {
		if !n.trashed && n.parentUID != nil && *n.parentUID == uid {
			v = append(v, n)
		}
	}
	sort.Slice(v, func(i, j int) bool { return v[i].name < v[j].name })
	return v
}

func hexSHA1(b []byte) string {
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:])
}

func ip(v int64) *int64   { return &v }
func sp(v string) *string { return &v }

func okResult(id any, result any) string {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	return string(b)
}

func errResult(id any, code int64, message string) string {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": message},
	})
	return string(b)
}

func notif(method string, params any) string {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	return string(b)
}

func (m *Mock) handleLine(line string) []string {
	var msg struct {
		ID     *json.Number    `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return nil
	}
	var id any
	if msg.ID != nil {
		id = *msg.ID
	}
	var params map[string]any
	_ = json.Unmarshal(msg.Params, &params)
	pstr := func(k string) string {
		if v, ok := params[k].(string); ok {
			return v
		}
		return ""
	}

	m.st.mu.Lock()
	defer m.st.mu.Unlock()

	if m.st.offline {
		return []string{errResult(id, errCodeConnClosedSent, "connection closed")}
	}

	switch msg.Method {
	case "auth.loginFork":
		notifLine := notif("auth.signInUrl", map[string]any{
			"url": "https://account.proton.me/desktop/login?mock=1", "userCode": "MOCK-CODE",
		})
		return []string{notifLine, okResult(id, map[string]any{"email": "mock@proton.me"})}
	case "auth.status":
		return []string{okResult(id, map[string]any{"loggedIn": true, "email": "mock@proton.me"})}
	case "auth.logout":
		return []string{okResult(id, map[string]any{})}
	case "drive.getRoot":
		if m.st.getRootFailures > 0 {
			m.st.getRootFailures--
			return []string{errResult(id, ErrCodeInternal, "transport error: getRoot failed")}
		}
		return []string{okResult(id, m.st.nodeJSON(m.st.nodes["root"]))}
	case "drive.listChildren":
		if m.st.listChildrenFails > 0 {
			m.st.listChildrenFails--
			return []string{errResult(id, ErrCodeInternal, "transport error: listChildren failed")}
		}
		var nodes []map[string]any
		for _, n := range m.st.childrenOf(pstr("parentUid")) {
			nodes = append(nodes, m.st.nodeJSON(n))
		}
		return []string{okResult(id, map[string]any{"nodes": nodes})}
	case "drive.getNode":
		uid := pstr("uid")
		if n := m.st.nodes[uid]; n != nil && !n.trashed {
			return []string{okResult(id, m.st.nodeJSON(n))}
		}
		return []string{errResult(id, -32602, "no such node")}
	case "drive.createFolder":
		parent, name := pstr("parentUid"), pstr("name")
		for _, n := range m.st.childrenOf(parent) {
			if n.name == name {
				return []string{errResult(id, ErrCodeConflict, "name already exists")}
			}
		}
		uid := m.st.allocUID()
		m.st.nodes[uid] = &mockNode{uid: uid, parentUID: &parent, name: name, kind: "folder", mtime: ip(1700000000)}
		return []string{okResult(id, m.st.nodeJSON(m.st.nodes[uid]))}
	case "drive.getAvailableName":
		taken := []string{}
		for _, n := range m.st.childrenOf(pstr("parentUid")) {
			taken = append(taken, n.name)
		}
		return []string{okResult(id, map[string]any{"name": availableName(taken, pstr("name"))})}
	case "drive.rename":
		uid, newName := pstr("nodeUid"), pstr("newName")
		m.st.calls = append(m.st.calls, call{"rename", []string{uid, newName}})
		n := m.st.nodes[uid]
		if n == nil || n.trashed {
			return []string{errResult(id, -32602, "no such node")}
		}
		if n.parentUID != nil {
			for _, other := range m.st.nodes {
				if !other.trashed && other.uid != uid && other.parentUID != nil &&
					*other.parentUID == *n.parentUID && other.name == newName {
					return []string{errResult(id, ErrCodeConflict, "name already exists")}
				}
			}
		}
		n.name = newName
		return []string{okResult(id, map[string]any{"node": m.st.nodeJSON(n)})}
	case "drive.move":
		uid, newParent := pstr("nodeUid"), pstr("newParentUid")
		m.st.calls = append(m.st.calls, call{"move", []string{uid, newParent}})
		n := m.st.nodes[uid]
		if n == nil || n.trashed || m.st.nodes[newParent] == nil {
			return []string{okResult(id, map[string]any{"results": []any{map[string]any{"uid": uid, "ok": false, "error": "no such node"}}})}
		}
		for _, other := range m.st.nodes {
			if !other.trashed && other.uid != uid && other.parentUID != nil &&
				*other.parentUID == newParent && other.name == n.name {
				return []string{okResult(id, map[string]any{"results": []any{map[string]any{"uid": uid, "ok": false, "error": "name already exists"}}})}
			}
		}
		n.parentUID = &newParent
		return []string{okResult(id, map[string]any{"results": []any{map[string]any{"uid": uid, "ok": true}}})}
	case "drive.trash", "drive.restore":
		var uids []string
		if raw, ok := params["uids"].([]any); ok {
			for _, u := range raw {
				if s, ok := u.(string); ok {
					uids = append(uids, s)
				}
			}
		}
		m.st.calls = append(m.st.calls, call{msg.Method[6:], uids})
		trashed := msg.Method == "drive.trash"
		var results []any
		for _, u := range uids {
			if n := m.st.nodes[u]; n != nil {
				n.trashed = trashed
				results = append(results, map[string]any{"uid": u, "ok": true, "error": nil})
			} else {
				results = append(results, map[string]any{"uid": u, "ok": false, "error": "no such node"})
			}
		}
		return []string{okResult(id, map[string]any{"results": results})}
	case "drive.listTrashed":
		var nodes []map[string]any
		for _, n := range m.st.nodes {
			if n.trashed {
				nodes = append(nodes, m.st.nodeJSON(n))
			}
		}
		return []string{okResult(id, map[string]any{"nodes": nodes})}
	case "drive.upload":
		return m.handleUpload(id, params, pstr)
	case "drive.download":
		return m.handleDownload(id, params, pstr)
	case "drive.events.start":
		m.st.eventsStartCalls++
		ff := notif("drive.event", map[string]any{
			"scopeId": m.st.scopeID, "eventId": m.st.allocEventID(), "type": "fast_forward",
		})
		return []string{okResult(id, map[string]any{}), ff}
	case "drive.events.stop":
		return []string{okResult(id, map[string]any{})}
	case "transfer.pause":
		m.st.calls = append(m.st.calls, call{"pause", []string{pstr("transferId")}})
		return []string{okResult(id, map[string]any{})}
	case "transfer.resume":
		tid := pstr("transferId")
		m.st.calls = append(m.st.calls, call{"resume", []string{tid}})
		if held := m.st.heldDownloads[tid]; held != nil {
			delete(m.st.heldDownloads, tid)
			_ = os.WriteFile(held.path, held.content, 0o644)
			m.st.enqueue(held.done)
		}
		return []string{okResult(id, map[string]any{})}
	case "transfer.cancel":
		tid := pstr("transferId")
		m.st.calls = append(m.st.calls, call{"cancel", []string{tid}})
		delete(m.st.heldDownloads, tid)
		return []string{okResult(id, map[string]any{})}
	default:
		return []string{errResult(id, -32601, "method not found")}
	}
}

func (m *Mock) handleUpload(id any, params map[string]any, pstr func(string) string) []string {
	s := m.st
	s.nextTransfer++
	transferID := fmt.Sprintf("transfer-%d", s.nextTransfer)
	name := pstr("name")
	path := pstr("path")
	nodeUID := pstr("nodeUid")
	parentUID := pstr("parentUid")
	mtime := int64(1700000000)
	if v, ok := params["mtime"].(float64); ok {
		mtime = int64(v)
	}
	s.calls = append(s.calls, call{"upload", []string{name}})
	s.uploadMtimes = append(s.uploadMtimes, struct {
		name  string
		mtime int64
	}{name, mtime})

	if s.crashNextUpload {
		s.crashNextUpload = false
		s.crashed = true
		return []string{okResult(id, map[string]any{"transferId": transferID})}
	}

	if nodeUID != "" && len(s.uploadFailures[nodeUID]) > 0 {
		f := s.uploadFailures[nodeUID][0]
		s.uploadFailures[nodeUID] = s.uploadFailures[nodeUID][1:]
		failure := map[string]any{
			"transferId": transferID, "code": f.code, "message": f.message, "retryable": f.retryable,
		}
		if f.retryAfter != nil {
			failure["retryAfter"] = f.retryAfter.Seconds()
		}
		return []string{
			okResult(id, map[string]any{"transferId": transferID}),
			notif("transfer.error", failure),
		}
	}

	content, err := os.ReadFile(path)
	if err != nil {
		content = nil
	}
	size := uint64(len(content))
	sha1 := hexSHA1(content)
	s.uploadSizes = append(s.uploadSizes, struct {
		name string
		size uint64
	}{name, size})

	var uid string
	if nodeUID != "" {
		if n := s.nodes[nodeUID]; n != nil {
			n.size = ip(int64(size))
			n.sha1 = sp(sha1)
			n.mtime = ip(mtime)
			n.content = content
			n.trashed = false
		}
		uid = nodeUID
	} else {
		uid = s.allocUID()
		var p *string
		if parentUID != "" {
			p = &parentUID
		}
		s.nodes[uid] = &mockNode{
			uid: uid, parentUID: p, name: name, kind: "file",
			size: ip(int64(size)), mtime: ip(mtime), sha1: sp(sha1), content: content,
		}
	}
	done := notif("transfer.done", map[string]any{
		"transferId": transferID, "nodeUid": uid, "nodeRevisionUid": uid + "-rev-1",
	})
	return []string{okResult(id, map[string]any{"transferId": transferID}), done}
}

func (m *Mock) handleDownload(id any, params map[string]any, pstr func(string) string) []string {
	s := m.st
	s.nextTransfer++
	nodeUID := pstr("nodeUid")
	path := pstr("path")
	s.calls = append(s.calls, call{"download", []string{nodeUID}})
	transferID := fmt.Sprintf("transfer-%d", s.nextTransfer)
	delay := s.downloadDelay
	if delay > 0 {
		// Deliberately block this test-only server while the download is
		// "in flight"; the state lock is dropped so assertions can still
		// inspect calls.
		s.mu.Unlock()
		time.Sleep(delay)
		s.mu.Lock()
	}

	if len(s.downloadFailures[nodeUID]) > 0 {
		f := s.downloadFailures[nodeUID][0]
		s.downloadFailures[nodeUID] = s.downloadFailures[nodeUID][1:]
		failure := map[string]any{
			"transferId": transferID, "code": f.code, "message": f.message, "retryable": f.retryable,
		}
		if f.retryAfter != nil {
			failure["retryAfter"] = f.retryAfter.Seconds()
		}
		return []string{
			okResult(id, map[string]any{"transferId": transferID}),
			notif("transfer.error", failure),
		}
	}

	var content []byte
	if n := s.nodes[nodeUID]; n != nil {
		content = n.content
	}
	done := notif("transfer.done", map[string]any{"transferId": transferID})
	if holdAfter, ok := s.downloadHolds[nodeUID]; ok {
		delete(s.downloadHolds, nodeUID)
		written := min(holdAfter, len(content))
		_ = os.WriteFile(path, content[:written], 0o644)
		s.heldDownloads[transferID] = &heldDownload{path: path, content: content, written: written, done: done}
		return []string{okResult(id, map[string]any{"transferId": transferID})}
	}
	_ = os.WriteFile(path, content, 0o644)
	s.enqueue(done)
	return []string{okResult(id, map[string]any{"transferId": transferID})}
}

// availableName is the deterministic "make it fit" naming:
// name, name (1).ext, name (2).ext, …
func availableName(taken []string, wanted string) string {
	for _, t := range taken {
		if t == wanted {
			goto collide
		}
	}
	return wanted
collide:
	var stem, ext string
	if i := strings.LastIndex(wanted, "."); i > 0 {
		stem, ext = wanted[:i], wanted[i:]
	} else {
		stem = wanted
	}
	for i := 1; i < 1000; i++ {
		candidate := fmt.Sprintf("%s (%d)%s", stem, i, ext)
		free := true
		for _, t := range taken {
			if t == candidate {
				free = false
				break
			}
		}
		if free {
			return candidate
		}
	}
	return stem + " (x)" + ext
}

// ---- MockHandle test API ----

func (h *MockHandle) withState(f func(s *mockState)) {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	f(h.st)
}

// TrashedUIDs returns the uids passed to drive.trash, in call order.
func (h *MockHandle) TrashedUIDs() []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "trash" {
				out = append(out, c.args...)
			}
		}
	})
	return out
}

// UploadedNames returns the names passed to drive.upload, in call order.
func (h *MockHandle) UploadedNames() []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "upload" {
				out = append(out, c.args...)
			}
		}
	})
	return out
}

// UploadedMtimes returns (name, mtime) of every upload request.
func (h *MockHandle) UploadedMtimes() []struct {
	Name  string
	Mtime int64
} {
	var out []struct {
		Name  string
		Mtime int64
	}
	h.withState(func(s *mockState) {
		for _, e := range s.uploadMtimes {
			out = append(out, struct {
				Name  string
				Mtime int64
			}{e.name, e.mtime})
		}
	})
	return out
}

// UploadedSizes returns (name, bytes actually read) of every upload.
func (h *MockHandle) UploadedSizes() []struct {
	Name string
	Size uint64
} {
	var out []struct {
		Name string
		Size uint64
	}
	h.withState(func(s *mockState) {
		for _, e := range s.uploadSizes {
			out = append(out, struct {
				Name string
				Size uint64
			}{e.name, e.size})
		}
	})
	return out
}

// UploadedMTimesMap maps upload name → last mtime sent.
func (h *MockHandle) UploadedMTimesMap() map[string]int64 {
	out := map[string]int64{}
	h.withState(func(s *mockState) {
		for _, e := range s.uploadMtimes {
			out[e.name] = e.mtime
		}
	})
	return out
}

// DownloadedUIDs returns uids passed to drive.download, in call order.
func (h *MockHandle) DownloadedUIDs() []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "download" {
				out = append(out, c.args...)
			}
		}
	})
	return out
}

// TransferControls returns the transfer control methods sent, in order.
func (h *MockHandle) TransferControls() []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "pause" || c.method == "resume" || c.method == "cancel" {
				out = append(out, c.method)
			}
		}
	})
	return out
}

// CallLog returns the whole (method, args) log in arrival order.
func (h *MockHandle) CallLog() []struct {
	Method string
	Args   []string
} {
	var out []struct {
		Method string
		Args   []string
	}
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			out = append(out, struct {
				Method string
				Args   []string
			}{c.method, c.args})
		}
	})
	return out
}

// UploadAttempts counts drive.upload calls (any outcome) for name.
func (h *MockHandle) UploadAttempts(name string) int {
	n := 0
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "upload" && len(c.args) > 0 && c.args[0] == name {
				n++
			}
		}
	})
	return n
}

// ChildNames returns the current non-trashed children names of a folder.
func (h *MockHandle) ChildNames(parentUID string) []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, n := range s.childrenOf(parentUID) {
			out = append(out, n.name)
		}
	})
	return out
}

// FindUID looks up a node uid by (parent uid, name).
func (h *MockHandle) FindUID(parentUID, name string) (string, bool) {
	var uid string
	found := false
	h.withState(func(s *mockState) {
		for _, n := range s.childrenOf(parentUID) {
			if n.name == name {
				uid, found = n.uid, true
			}
		}
	})
	return uid, found
}

// FileContent returns a remote file's current content.
func (h *MockHandle) FileContent(uid string) ([]byte, bool) {
	var out []byte
	ok := false
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			out, ok = append([]byte(nil), n.content...), true
		}
	})
	return out, ok
}

// FileSHA1 returns a remote file's current content hash.
func (h *MockHandle) FileSHA1(uid string) (string, bool) {
	var out string
	ok := false
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil && n.sha1 != nil {
			out, ok = *n.sha1, true
		}
	})
	return out, ok
}

// FileSize returns a remote file's size.
func (h *MockHandle) FileSize(uid string) (int64, bool) {
	var out int64
	ok := false
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil && n.size != nil {
			out, ok = *n.size, true
		}
	})
	return out, ok
}

// NodeMtime returns a remote node's mtime (Unix seconds).
func (h *MockHandle) NodeMtime(uid string) (int64, bool) {
	var out int64
	ok := false
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil && n.mtime != nil {
			out, ok = *n.mtime, true
		}
	})
	return out, ok
}

func (h *MockHandle) allocAndInsert(parentUID, name, kind string, content []byte) string {
	var uid string
	h.withState(func(s *mockState) {
		uid = s.allocUID()
		n := &mockNode{uid: uid, parentUID: &parentUID, name: name, kind: kind, mtime: ip(1700000000)}
		if kind == "file" {
			n.size = ip(int64(len(content)))
			n.sha1 = sp(hexSHA1(content))
			n.content = content
		}
		s.nodes[uid] = n
	})
	return uid
}

func (h *MockHandle) emitFor(uid string, kind EventKind, extra map[string]any) {
	h.withState(func(s *mockState) {
		params := map[string]any{
			"scopeId": s.scopeID, "eventId": s.allocEventID(), "type": string(kind), "nodeUid": uid,
		}
		for k, v := range extra {
			params[k] = v
		}
		s.emitEvent(params)
	})
}

// RemoteCreateFile injects a remote file and emits node_created.
func (h *MockHandle) RemoteCreateFile(parentUID, name string, content []byte) string {
	uid := h.allocAndInsert(parentUID, name, "file", content)
	h.emitFor(uid, EventNodeCreated, map[string]any{"parentNodeUid": parentUID})
	return uid
}

// RemoteCreateFolder injects a remote folder and emits node_created.
func (h *MockHandle) RemoteCreateFolder(parentUID, name string) string {
	uid := h.allocAndInsert(parentUID, name, "folder", nil)
	h.emitFor(uid, EventNodeCreated, map[string]any{"parentNodeUid": parentUID})
	return uid
}

// RemoteCreateFolderSilent injects a remote folder WITHOUT an event.
func (h *MockHandle) RemoteCreateFolderSilent(parentUID, name string) string {
	return h.allocAndInsert(parentUID, name, "folder", nil)
}

// RemoteUpdateFile changes remote content and emits node_updated.
func (h *MockHandle) RemoteUpdateFile(uid string, content []byte) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.size = ip(int64(len(content)))
			n.sha1 = sp(hexSHA1(content))
			n.content = content
			n.trashed = false
		}
	})
	h.emitFor(uid, EventNodeUpdated, nil)
}

// RemoteUpdateFileSilent changes remote content WITHOUT an event.
func (h *MockHandle) RemoteUpdateFileSilent(uid string, content []byte) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.size = ip(int64(len(content)))
			n.sha1 = sp(hexSHA1(content))
			n.content = content
			n.trashed = false
		}
	})
}

// RemoteRename renames a remote node and emits node_updated.
func (h *MockHandle) RemoteRename(uid, newName string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.name = newName
		}
	})
	h.emitFor(uid, EventNodeUpdated, nil)
}

// RemoteMove moves a remote node and emits node_updated.
func (h *MockHandle) RemoteMove(uid, newParentUID string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.parentUID = &newParentUID
		}
	})
	h.emitFor(uid, EventNodeUpdated, nil)
}

// RemoteMoveSilent moves a remote node WITHOUT an event.
func (h *MockHandle) RemoteMoveSilent(uid, newParentUID string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.parentUID = &newParentUID
		}
	})
}

// RemoteTrash trashes a remote node with the real event shape.
func (h *MockHandle) RemoteTrash(uid string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.trashed = true
		}
	})
	h.emitFor(uid, EventNodeUpdated, map[string]any{"isTrashed": true})
}

// RemoteRestore restores a remote node with the real event shape.
func (h *MockHandle) RemoteRestore(uid string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.trashed = false
		}
	})
	h.emitFor(uid, EventNodeUpdated, map[string]any{"isTrashed": false})
}

// RemoteDelete emits node_deleted and removes the node remotely.
func (h *MockHandle) RemoteDelete(uid string) {
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil {
			n.trashed = true
		}
	})
	h.emitFor(uid, EventNodeDeleted, nil)
}

// InjectUploadFailures scripts failures for the next revision-upload
// attempts of nodeUID: (code, message, retryable).
func (h *MockHandle) InjectUploadFailures(nodeUID string, failures []([]any)) {
	h.InjectUploadFailuresFull(nodeUID, failures)
}

// InjectUploadFailuresFull scripts failures with an optional 4th tuple
// element: the server retryAfter hint in seconds.
func (h *MockHandle) InjectUploadFailuresFull(nodeUID string, failures []([]any)) {
	h.withState(func(s *mockState) {
		var list []scriptedFailure
		for _, f := range failures {
			sf := scriptedFailure{
				code:      toInt64(f[0]),
				message:   toString(f[1]),
				retryable: toBool(f[2]),
			}
			if len(f) > 3 {
				secs := toFloat64(f[3])
				d := time.Duration(secs * float64(time.Second))
				sf.retryAfter = &d
			}
			list = append(list, sf)
		}
		s.uploadFailures[nodeUID] = list
	})
}

// InjectDownloadFailures scripts failures for the next download attempts.
func (h *MockHandle) InjectDownloadFailures(nodeUID string, failures []([]any)) {
	h.withState(func(s *mockState) {
		var list []scriptedFailure
		for _, f := range failures {
			list = append(list, scriptedFailure{
				code: toInt64(f[0]), message: toString(f[1]), retryable: toBool(f[2]),
			})
		}
		s.downloadFailures[nodeUID] = list
	})
}

// FailGetRoot rejects the next n drive.getRoot calls with a transport error.
func (h *MockHandle) FailGetRoot(n int) {
	h.withState(func(s *mockState) { s.getRootFailures = n })
}

// FailListChildren rejects the next n drive.listChildren calls.
func (h *MockHandle) FailListChildren(n int) {
	h.withState(func(s *mockState) { s.listChildrenFails = n })
}

// SetOffline simulates a sidecar outage: every request answers with the
// client-side "connection closed" error until cleared.
func (h *MockHandle) SetOffline(offline bool) {
	h.withState(func(s *mockState) { s.offline = offline })
}

// CrashOnNextUpload makes the transport die during the next upload (respond,
// then close — no transfer.done). One-shot.
func (h *MockHandle) CrashOnNextUpload() {
	h.withState(func(s *mockState) { s.crashNextUpload = true })
}

// HoldNextDownload freezes the next download of nodeUID after n bytes until
// transfer.resume arrives.
func (h *MockHandle) HoldNextDownload(nodeUID string, bytes int) {
	h.withState(func(s *mockState) { s.downloadHolds[nodeUID] = bytes })
}

// HeldDownloads counts downloads currently parked for transfer.resume.
func (h *MockHandle) HeldDownloads() int {
	var n int
	h.withState(func(s *mockState) { n = len(s.heldDownloads) })
	return n
}

// EventsStartCalls counts drive.events.start calls.
func (h *MockHandle) EventsStartCalls() int {
	var n int
	h.withState(func(s *mockState) { n = s.eventsStartCalls })
	return n
}

// UploadBufferedBytes returns the total upload bytes the mock buffered in
// memory (always the full size here; kept for parity with the Rust mock's
// streaming-mode assertions).
func (h *MockHandle) UploadBufferedBytes() uint64 {
	var n uint64
	h.withState(func(s *mockState) {
		for _, e := range s.uploadSizes {
			n += e.size
		}
	})
	return n
}

// RestoredUIDs returns the uids passed to drive.restore, in call order.
func (h *MockHandle) RestoredUIDs() []string {
	var out []string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "restore" {
				out = append(out, c.args...)
			}
		}
	})
	return out
}

// Renames returns (uid, newName) pairs passed to drive.rename, in call order.
func (h *MockHandle) Renames() [][2]string {
	var out [][2]string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "rename" {
				out = append(out, [2]string{c.args[0], c.args[1]})
			}
		}
	})
	return out
}

// Moves returns (uid, newParentUid) pairs passed to drive.move, in call order.
func (h *MockHandle) Moves() [][2]string {
	var out [][2]string
	h.withState(func(s *mockState) {
		for _, c := range s.calls {
			if c.method == "move" {
				out = append(out, [2]string{c.args[0], c.args[1]})
			}
		}
	})
	return out
}

// NodeSHA1 returns a remote node's current content hash.
func (h *MockHandle) NodeSHA1(uid string) (string, bool) {
	var out string
	ok := false
	h.withState(func(s *mockState) {
		if n := s.nodes[uid]; n != nil && n.sha1 != nil {
			out, ok = *n.sha1, true
		}
	})
	return out, ok
}

// NodeSize returns a remote node's size.
func (h *MockHandle) NodeSize(uid string) (int64, bool) {
	return h.FileSize(uid)
}

// InjectDownloadFailuresFull scripts download failures with an optional
// 4th tuple element: the server retryAfter hint in seconds.
func (h *MockHandle) InjectDownloadFailuresFull(nodeUID string, failures []([]any)) {
	h.withState(func(s *mockState) {
		var list []scriptedFailure
		for _, f := range failures {
			sf := scriptedFailure{
				code: toInt64(f[0]), message: toString(f[1]), retryable: toBool(f[2]),
			}
			if len(f) > 3 {
				d := time.Duration(toFloat64(f[3]) * float64(time.Second))
				sf.retryAfter = &d
			}
			list = append(list, sf)
		}
		s.downloadFailures[nodeUID] = list
	})
}

// SetDownloadDelay delays every drive.download response by d (test-only;
// lets a test act while a hydration request is in flight).
func (h *MockHandle) SetDownloadDelay(d time.Duration) {
	h.withState(func(s *mockState) { s.downloadDelay = d })
}

// EmitEvent emits a raw drive event (for custom scenarios).
func (h *MockHandle) EmitEvent(ev *DriveEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	h.withState(func(s *mockState) { s.enqueue(string(b)) })
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func toFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	}
	return 0
}

// compile-time interface checks
var _ API = (*SidecarClient)(nil)

// unused-import guards
var _ = filepath.Join
var _ = strconv.Itoa
