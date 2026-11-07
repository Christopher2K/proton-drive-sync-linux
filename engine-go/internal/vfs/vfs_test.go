//go:build linux

package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// memSnapshot is the test Snapshot (the Go shape of the Rust tests'
// `store.publish_snapshot` helper).
type memSnapshot struct {
	generation uint64
	byPath     map[string]VNode
}

func (s *memSnapshot) Generation() uint64 { return s.generation }
func (s *memSnapshot) Node(rel string) (VNode, bool) {
	n, ok := s.byPath[rel]
	return n, ok
}
func (s *memSnapshot) Children(rel string) []string {
	prefix := ""
	if rel != "" {
		prefix = rel + "/"
	}
	var kids []string
	for r, n := range s.byPath {
		if n.Parent == rel && r != rel {
			kids = append(kids, r)
		}
		_ = prefix
	}
	sort.Strings(kids)
	return kids
}

func vnode(rel string, isDir bool) VNode {
	parent, name := "", rel
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		parent, name = rel[:i], rel[i+1:]
	}
	return VNode{Rel: rel, Parent: parent, Name: name, IsDir: isDir, Size: 1, Mtime: 0, Mode: 0o644, ContentReady: false}
}

// snap builds a snapshot where node i has size i and alternating kind
// (mirrors the Rust `snap` helper: even index = file, odd = dir).
func snap(generation uint64, nodes ...string) *memSnapshot {
	byPath := map[string]VNode{}
	for i, rel := range nodes {
		n := vnode(rel, i%2 == 1)
		n.Size = uint64(i)
		byPath[rel] = n
	}
	return &memSnapshot{generation: generation, byPath: byPath}
}

func TestInosAreStableAcrossRefresh(t *testing.T) {
	tree := NewTree()
	tree.Refresh(snap(1, "a.txt", "dir"))
	aNode, _ := tree.Node("a.txt")
	inoA := aNode.Ino

	tree.Refresh(snap(2, "a.txt", "dir", "dir/b.txt"))
	a, _ := tree.Node("a.txt")
	if a.Ino != inoA {
		t.Errorf("stable across generations: got ino %d, want %d", a.Ino, inoA)
	}
	if _, ok := tree.Node("dir/b.txt"); !ok {
		t.Errorf("dir/b.txt should exist")
	}

	tree.Refresh(snap(3, "dir", "dir/b.txt"))
	if _, ok := tree.Node("a.txt"); ok {
		t.Errorf("vanished nodes are dropped")
	}
	b, _ := tree.Node("dir/b.txt")
	rel, ok := tree.RelOf(b.Ino)
	if !ok || rel != "dir/b.txt" {
		t.Errorf("RelOf(b.ino) = %q, %v; want dir/b.txt", rel, ok)
	}
}

func TestProxyAndChildren(t *testing.T) {
	tree := NewTree()
	tree.Refresh(snap(1, "b.txt", "a.txt"))
	kids, _ := tree.Children(RootIno)
	if len(kids) == 0 || kids[0] != StateProxy {
		t.Errorf(".pds sorts first; got %v", kids)
	}
	found := false
	for _, k := range kids {
		if k == "a.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("a.txt missing from children: %v", kids)
	}
	if rel, ok := tree.ChildRel(RootIno, "a.txt"); !ok || rel != "a.txt" {
		t.Errorf("ChildRel(root, a.txt) = %q, %v", rel, ok)
	}
	if _, ok := tree.ChildRel(RootIno, "../etc"); ok {
		t.Errorf("path traversal name must be rejected")
	}
}

func TestParentInoWalksUp(t *testing.T) {
	tree := NewTree()
	tree.Refresh(snap(1, "dir", "dir/b.txt"))
	b, _ := tree.Node("dir/b.txt")
	dir, _ := tree.Node("dir")
	if got := tree.ParentIno(b.Ino); got != dir.Ino {
		t.Errorf("ParentIno(b) = %d, want %d", got, dir.Ino)
	}
	if got := tree.ParentIno(dir.Ino); got != RootIno {
		t.Errorf("ParentIno(dir) = %d, want root", got)
	}
	if got := tree.ParentIno(RootIno); got != RootIno {
		t.Errorf("ParentIno(root) = %d, want root", got)
	}
}

func TestInoOfOptDistinguishesUnknownFromRoot(t *testing.T) {
	// Regression: a fallback that answered ROOT for unknown paths made
	// rmdir's emptiness check consult the root's children for an
	// overlay-only folder → spurious ENOTEMPTY.
	tree := NewTree()
	tree.Refresh(snap(1, "dir"))
	if ino, ok := tree.InoOfOpt(""); !ok || ino != RootIno {
		t.Errorf("InoOfOpt(\"\") = %d, %v; want root", ino, ok)
	}
	if _, ok := tree.InoOfOpt("dir"); !ok {
		t.Errorf("InoOfOpt(dir) should resolve")
	}
	if _, ok := tree.InoOfOpt("overlay-only"); ok {
		t.Errorf("unknown paths must not resolve to the root inode")
	}
	// Legacy accessor keeps its ROOT fallback (other callers rely on it).
	if got := tree.InoOf("overlay-only"); got != RootIno {
		t.Errorf("InoOf(overlay-only) = %d, want ROOT fallback", got)
	}
}

func TestStableInoForPaths(t *testing.T) {
	a := StableInoFor("some/file.txt")
	if a != StableInoFor("some/file.txt") {
		t.Errorf("path-hash inodes must be deterministic")
	}
	if a == StableInoFor("other/file.txt") {
		t.Errorf("distinct paths should hash apart (collision)")
	}
	if a < OverlayInoBase+16 {
		t.Errorf("overlay inodes must stay above the reserved range: %d", a)
	}
}

// --- mountinfo / occupancy (from mount.rs tests) ---

func TestParsesMountinfoEscapesAndFstype(t *testing.T) {
	line := "36 1 0:32 / /tmp/foo\\040bar rw,nosuid,nodev - fuse.pds pds rw,user_id=1000"
	mountpoint, fstype, ok := ParseMountinfoLine(line)
	if !ok {
		t.Fatalf("valid mountinfo line failed to parse")
	}
	if mountpoint != "/tmp/foo bar" {
		t.Errorf("mountpoint = %q, want /tmp/foo bar", mountpoint)
	}
	if fstype != "fuse.pds" {
		t.Errorf("fstype = %q, want fuse.pds", fstype)
	}
}

func TestOnlyFUSEMountsAreTreatedAsOccupied(t *testing.T) {
	for _, fstype := range []string{"fuse", "fuse.pds", "fuse.sshfs"} {
		if !IsFUSE(fstype) {
			t.Errorf("IsFUSE(%q) = false, want true", fstype)
		}
	}
	for _, fstype := range []string{"ext4", "tmpfs"} {
		if IsFUSE(fstype) {
			t.Errorf("IsFUSE(%q) = true, want false", fstype)
		}
	}
}

func TestOrdinaryDirectoryIsFree(t *testing.T) {
	dir := t.TempDir()
	state := MountpointState(dir, 200*time.Millisecond)
	if state.Kind != MountFree {
		t.Errorf("ordinary dir state = %v, want free", state)
	}
	if state.IsMounted() {
		t.Errorf("ordinary dir must not count as mounted")
	}
	if err := EnsurePlaceholderMountpoint(dir, 200*time.Millisecond); err != nil {
		t.Errorf("EnsurePlaceholderMountpoint: %v", err)
	}
}

func TestServedProbeIsLiveAndProbeErrorsAreClassified(t *testing.T) {
	live := stateFromProbe("/mnt/pds", "fuse.pds", probeResult{kind: probeServed})
	if live.Kind != MountLive {
		t.Errorf("served probe = %v, want live", live)
	}
	err := refusal(live)
	var busy *MountpointError
	if e, isErr := err.(*MountpointError); !isErr || e.Kind != MountLive {
		t.Errorf("refusal(live) = %v, want Busy", err)
	} else {
		busy = e
	}
	if !strings.Contains(busy.Error(), "already mounted") {
		t.Errorf("Busy message should mention 'already mounted': %s", busy.Error())
	}

	t.Run("stale errnos", func(t *testing.T) {
		for _, code := range []syscall.Errno{syscall.ENOTCONN, syscall.ENODEV, syscall.EIO, syscall.ESTALE, syscall.ENOENT} {
			state := stateFromProbe("/mnt/pds", "fuse.pds", probeResult{kind: probeFailed, errno: code})
			if state.Kind != MountStale {
				t.Errorf("errno %v should classify as stale, got %v", code, state)
			}
		}
	})

	timedOut := stateFromProbe("/mnt/pds", "fuse.pds", probeResult{kind: probeTimedOut})
	if timedOut.Kind != MountStale {
		t.Errorf("timed-out probe = %v, want stale", timedOut)
	}
	if err := refusal(timedOut); !strings.Contains(err.Error(), "stale") {
		t.Errorf("Stale message should mention 'stale': %s", err)
	}

	denied := stateFromProbe("/mnt/pds", "fuse.pds", probeResult{kind: probeFailed, errno: syscall.EACCES})
	if denied.Kind != MountUnreachable {
		t.Errorf("EACCES = %v, want unreachable", denied)
	}
	_ = busy
}

// --- snapshot-driven view + write-through (fs-level, no kernel mount) ---

// memStore is the test Store: records callbacks and serves backing files
// from disk when "hydrated".
type memStore struct {
	mu  sync.Mutex
	cur *memSnapshot

	backingRoot string
	hydrated    map[string]bool

	beginHydrate []string
	writes       []string
	mkdirs       []string
	deletes      []string
	renames      [][2]string
	overwrites   [][3]string
	metadata     []struct {
		rel   string
		mtime *int64
		mode  *uint32
	}
	accesses       []string
	openHandles    int
	pendingDeletes map[string]bool
}

func newMemStore(t *testing.T, s *memSnapshot) *memStore {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "backing"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &memStore{cur: s, backingRoot: filepath.Join(root, "backing"), hydrated: map[string]bool{}, pendingDeletes: map[string]bool{}}
}

func (m *memStore) Snapshot() Snapshot { return m.cur }

func (m *memStore) BeginHydrate(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beginHydrate = append(m.beginHydrate, rel)
}

func (m *memStore) BackingPath(rel string) (string, bool) {
	m.mu.Lock()
	ready := m.hydrated[rel]
	m.mu.Unlock()
	if !ready {
		return "", false
	}
	return filepath.Join(m.backingRoot, filepath.FromSlash(rel)), true
}

func (m *memStore) HasPendingDelete(rel string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendingDeletes[rel]
}

func (m *memStore) RecordAccess(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accesses = append(m.accesses, rel)
}
func (m *memStore) RecordWrite(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, rel)
}
func (m *memStore) RecordMkdir(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mkdirs = append(m.mkdirs, rel)
}
func (m *memStore) RecordDelete(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes = append(m.deletes, rel)
	m.pendingDeletes[rel] = true
}
func (m *memStore) RecordRename(oldRel, newRel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renames = append(m.renames, [2]string{oldRel, newRel})
}
func (m *memStore) RecordOverwrite(dst, src, final string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.overwrites = append(m.overwrites, [3]string{dst, src, final})
}
func (m *memStore) RecordMetadata(rel string, mtime *int64, mode *uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metadata = append(m.metadata, struct {
		rel   string
		mtime *int64
		mode  *uint32
	}{rel, mtime, mode})
}
func (m *memStore) Forget(rel string)     {}
func (m *memStore) ForgetTree(rel string) {}
func (m *memStore) AddOpenHandle(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openHandles++
}
func (m *memStore) RemoveOpenHandle(rel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openHandles--
}

// hydrate marks rel hydrated and writes its content bytes.
func (m *memStore) hydrate(t *testing.T, rel, content string) {
	t.Helper()
	abs := filepath.Join(m.backingRoot, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.hydrated[rel] = true
	m.mu.Unlock()
}

func newTestFs(t *testing.T, s *memSnapshot) (*PdsFs, *memStore, string, string) {
	t.Helper()
	root := t.TempDir()
	st := newMemStore(t, s)
	stateDir := filepath.Join(root, ".pds")
	backingDir := filepath.Join(root, ".pds-vfs-backing")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return NewPdsFs(st, stateDir, backingDir), st, stateDir, backingDir
}

func TestGetAttrRootResolves(t *testing.T) {
	// AGENT.md gotcha: the root inode must resolve.
	f, _, _, _ := newTestFs(t, snap(1))
	attr, status := f.GetAttr("", ctx)
	if status != fuseOK {
		t.Fatalf("root GetAttr status = %v", status)
	}
	if attr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		t.Errorf("root must be a directory")
	}
}

func TestGetAttrPlaceholderIsRegularFile(t *testing.T) {
	s := snap(1, "a.txt") // index 0 → file, size 0
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 1234, Mtime: 45, Mode: 0o644}
	f, _, _, _ := newTestFs(t, s)
	attr, status := f.GetAttr("a.txt", ctx)
	if status != fuseOK {
		t.Fatalf("status = %v", status)
	}
	if attr.Mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Errorf("placeholder must stat as a regular file")
	}
	if attr.Size != 1234 {
		t.Errorf("size = %d, want the remote size 1234", attr.Size)
	}
}

func TestGetAttrMissingPathIsEnoent(t *testing.T) {
	f, _, _, _ := newTestFs(t, snap(1))
	if _, status := f.GetAttr("nope.txt", ctx); status != fuseENOENT {
		t.Errorf("missing path status = %v, want ENOENT", status)
	}
}

func TestGetAttrStateProxyServesRealDir(t *testing.T) {
	f, _, stateDir, _ := newTestFs(t, snap(1))
	attr, status := f.GetAttr(StateProxy, ctx)
	if status != fuseOK {
		t.Fatalf("status = %v", status)
	}
	if attr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		t.Errorf(".pds must be a directory")
	}
	if err := os.RemoveAll(stateDir); err != nil {
		t.Fatal(err)
	}
	if _, status := f.GetAttr(StateProxy, ctx); status != fuseENOENT {
		t.Errorf("missing state dir must ENOENT, got %v", status)
	}
}

func TestOpenDirServesSnapshotPlusProxy(t *testing.T) {
	s := snap(1, "a.txt", "dir", "dir/b.txt")
	// fix kinds: dir must be a directory
	s.byPath["dir"] = VNode{Rel: "dir", Parent: "", Name: "dir", IsDir: true, Mode: 0o755}
	s.byPath["dir/b.txt"] = VNode{Rel: "dir/b.txt", Parent: "dir", Name: "b.txt", Size: 10, Mode: 0o644}
	f, _, _, _ := newTestFs(t, s)
	entries, status := f.OpenDir("", ctx)
	if status != fuseOK {
		t.Fatalf("status = %v", status)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, want := range []string{StateProxy, "a.txt", "dir"} {
		if !names[want] {
			t.Errorf("root listing missing %q: %v", want, entries)
		}
	}
	sub, status := f.OpenDir("dir", ctx)
	if status != fuseOK {
		t.Fatalf("dir listing status = %v", status)
	}
	if len(sub) != 1 || sub[0].Name != "b.txt" {
		t.Errorf("dir listing = %v, want [b.txt]", sub)
	}
}

func TestOpenDirOfAFileIsEnotdir(t *testing.T) {
	s := snap(1, "a.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 1, Mode: 0o644}
	f, _, _, _ := newTestFs(t, s)
	if _, status := f.OpenDir("a.txt", ctx); status != fuseStatus(syscall.ENOTDIR) {
		t.Errorf("OpenDir(file) = %v, want ENOTDIR", status)
	}
}

func TestReadPlaceholderHydratesViaCallback(t *testing.T) {
	s := snap(1, "a.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 5, Mode: 0o644}
	f, st, backing, _ := newTestFs(t, s)
	// Seed the backing file behind the store's back, then hydrate on
	// BeginHydrate from a goroutine (the callback the engine would run).
	abs := filepath.Join(backing, "a.txt")
	if err := os.WriteFile(abs, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	go func() {
		// simulate the engine draining the hydrate queue
		st.mu.Lock()
		rel := st.beginHydrate[len(st.beginHydrate)-1]
		st.mu.Unlock()
		st.hydrate(t, rel, "hello")
	}()
	file, status := f.Open("a.txt", syscall.O_RDONLY, ctx)
	if status != fuseOK {
		t.Fatalf("open status = %v", status)
	}
	defer file.Release()
	buf := make([]byte, 5)
	res, status := file.Read(buf, 0)
	if status != fuseOK {
		t.Fatalf("read status = %v", status)
	}
	data, status := res.Bytes(buf)
	if status != fuseOK {
		t.Fatalf("read result status = %v", status)
	}
	if string(data) != "hello" {
		t.Errorf("read = %q, want hello", data)
	}
}

func TestCreateWriteThroughQueuesUploadOnRelease(t *testing.T) {
	s := snap(1, "other.txt")
	s.byPath["other.txt"] = VNode{Rel: "other.txt", Parent: "", Name: "other.txt", Size: 1, Mode: 0o644}
	f, st, _, backing := newTestFs(t, s)
	file, status := f.Create("new.txt", syscall.O_RDWR, 0o644, ctx)
	if status != fuseOK {
		t.Fatalf("create status = %v", status)
	}
	if n, status := file.Write([]byte("body"), 0); status != fuseOK || n != 4 {
		t.Fatalf("write = %d, %v", n, status)
	}
	if status := file.Flush(); status != fuseOK {
		t.Fatalf("flush = %v", status)
	}
	st.mu.Lock()
	writes := len(st.writes)
	st.mu.Unlock()
	if writes != 0 {
		t.Errorf("flush must not upload (double upload); writes = %d", writes)
	}
	file.Release()
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.writes) != 1 || st.writes[0] != "new.txt" {
		t.Errorf("release must queue the upload: %v", st.writes)
	}
	// The backing file carried the content.
	got, err := os.ReadFile(filepath.Join(backing, "new.txt"))
	if err != nil || string(got) != "body" {
		t.Errorf("backing content = %q, %v", got, err)
	}
	// The overlay serves the new file before the snapshot does.
	if _, status := f.GetAttr("new.txt", ctx); status != fuseOK {
		t.Errorf("overlay GetAttr = %v", status)
	}
	entries, _ := f.OpenDir("", ctx)
	found := false
	for _, e := range entries {
		if e.Name == "new.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("overlay file missing from listing: %v", entries)
	}
}

func TestMkdirQueuesRemoteFolder(t *testing.T) {
	f, st, _, _ := newTestFs(t, snap(1))
	if status := f.Mkdir("folder", 0o755, ctx); status != fuseOK {
		t.Fatalf("mkdir = %v", status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.mkdirs) != 1 || st.mkdirs[0] != "folder" {
		t.Errorf("mkdir queue = %v", st.mkdirs)
	}
	if status := f.Mkdir("folder", 0o755, ctx); status != fuseStatus(syscall.EEXIST) {
		t.Errorf("second mkdir = %v, want EEXIST", status)
	}
}

func TestRmdirEmptyOnly(t *testing.T) {
	s := snap(1, "dir", "dir/b.txt")
	s.byPath["dir"] = VNode{Rel: "dir", Parent: "", Name: "dir", IsDir: true, Mode: 0o755}
	s.byPath["dir/b.txt"] = VNode{Rel: "dir/b.txt", Parent: "dir", Name: "b.txt", Size: 1, Mode: 0o644}
	f, _, _, _ := newTestFs(t, s)
	if status := f.Rmdir("dir", ctx); status != fuseStatus(syscall.ENOTEMPTY) {
		t.Errorf("rmdir(non-empty) = %v, want ENOTEMPTY", status)
	}
	if status := f.Rmdir("dir/b.txt", ctx); status != fuseStatus(syscall.ENOTDIR) {
		t.Errorf("rmdir(file) = %v, want ENOTDIR", status)
	}
}

func TestRmdirOverlayDirWithoutSnapshotRow(t *testing.T) {
	// Regression (from tree.rs): an overlay-only folder must not resolve
	// to the root in the emptiness check (spurious ENOTEMPTY).
	f, _, _, _ := newTestFs(t, snap(1))
	if status := f.Mkdir("ephemeral", 0o755, ctx); status != fuseOK {
		t.Fatal(status)
	}
	if status := f.Rmdir("ephemeral", ctx); status != fuseOK {
		t.Errorf("rmdir(overlay-only empty dir) = %v, want OK", status)
	}
}

func TestUnlinkQueuesRemoteDelete(t *testing.T) {
	s := snap(1, "a.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 1, Mode: 0o644}
	f, st, _, backing := newTestFs(t, s)
	if err := os.WriteFile(filepath.Join(backing, "a.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if status := f.Unlink("a.txt", ctx); status != fuseOK {
		t.Fatalf("unlink = %v", status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.deletes) != 1 || st.deletes[0] != "a.txt" {
		t.Errorf("delete queue = %v", st.deletes)
	}
}

func TestUnlinkUnknownPathIsEnoent(t *testing.T) {
	f, _, _, _ := newTestFs(t, snap(1))
	if status := f.Unlink("ghost", ctx); status != fuseENOENT {
		t.Errorf("unlink(ghost) = %v, want ENOENT", status)
	}
}

func TestSpecialFilesAreRefused(t *testing.T) {
	f, _, _, _ := newTestFs(t, snap(1))
	if status := f.Symlink("/target", "link", ctx); status == fuseOK {
		t.Errorf("symlinks must be refused")
	}
	if status := f.Mknod("fifo", syscall.S_IFIFO|0o644, 0, ctx); status == fuseOK {
		t.Errorf("FIFOs must be refused")
	}
	if status := f.Mknod("plain", syscall.S_IFREG|0o644, 0, ctx); status != fuseOK {
		t.Errorf("regular mknod should behave like create: %v", status)
	}
}

func TestRenameFile(t *testing.T) {
	s := snap(1, "a.txt", "b.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 3, Mode: 0o644}
	s.byPath["b.txt"] = VNode{Rel: "b.txt", Parent: "", Name: "b.txt", Size: 9, Mode: 0o644}
	f, st, _, backing := newTestFs(t, s)
	if err := os.WriteFile(filepath.Join(backing, "a.txt"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if status := f.Rename("a.txt", "c.txt", ctx); status != fuseOK {
		t.Fatalf("rename = %v", status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.renames) != 1 || st.renames[0] != [2]string{"a.txt", "c.txt"} {
		t.Errorf("rename queue = %v", st.renames)
	}
	// Backing content moved.
	got, err := os.ReadFile(filepath.Join(backing, "c.txt"))
	if err != nil || string(got) != "abc" {
		t.Errorf("moved backing content = %q, %v", got, err)
	}
	// The old path flips when the engine's next publish re-keys the
	// snapshot rows (same as the Rust port: "the snapshot tree flips on
	// the engine's next publish").
	delete(s.byPath, "a.txt")
	s.byPath["c.txt"] = VNode{Rel: "c.txt", Parent: "", Name: "c.txt", Size: 3, Mode: 0o644}
	s.generation++
	if _, status := f.GetAttr("a.txt", ctx); status != fuseENOENT {
		t.Errorf("old path should be gone after republish: %v", status)
	}
}

func TestRenameFileOverFileQueuesOverwrite(t *testing.T) {
	s := snap(1, "a.txt", "b.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 3, Mode: 0o644}
	s.byPath["b.txt"] = VNode{Rel: "b.txt", Parent: "", Name: "b.txt", Size: 9, Mode: 0o644}
	f, st, _, _ := newTestFs(t, s)
	if status := f.Rename("a.txt", "b.txt", ctx); status != fuseOK {
		t.Fatalf("overwrite rename = %v", status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.overwrites) != 1 || st.overwrites[0] != [3]string{"b.txt", "a.txt", "b.txt"} {
		t.Errorf("overwrite queue = %v", st.overwrites)
	}
}

func TestRenameFileOverDirIsEisdir(t *testing.T) {
	s := snap(1, "a.txt", "dir")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 3, Mode: 0o644}
	s.byPath["dir"] = VNode{Rel: "dir", Parent: "", Name: "dir", IsDir: true, Mode: 0o755}
	f, _, _, _ := newTestFs(t, s)
	if status := f.Rename("a.txt", "dir", ctx); status != fuseStatus(syscall.EISDIR) {
		t.Errorf("file→dir rename = %v, want EISDIR", status)
	}
}

func TestRenameDirIntoOwnSubtreeIsEinval(t *testing.T) {
	s := snap(1, "dir")
	s.byPath["dir"] = VNode{Rel: "dir", Parent: "", Name: "dir", IsDir: true, Mode: 0o755}
	f, _, _, _ := newTestFs(t, s)
	if err := os.MkdirAll(filepath.Join(f.backingDir, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if status := f.Rename("dir", "dir/sub", ctx); status != fuseStatus(syscall.EINVAL) {
		t.Errorf("dir→own subtree rename = %v, want EINVAL", status)
	}
}

func TestChmodPersistsLocalMode(t *testing.T) {
	s := snap(1, "a.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 1, Mode: 0o644}
	f, st, _, _ := newTestFs(t, s)
	if status := f.Chmod("a.txt", 0o755, ctx); status != fuseOK {
		t.Fatalf("chmod = %v", status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.metadata) != 1 || st.metadata[0].mode == nil || *st.metadata[0].mode != 0o755 {
		t.Errorf("metadata queue = %+v", st.metadata)
	}
}

func TestStatfsNeverZero(t *testing.T) {
	f, _, _, _ := newTestFs(t, snap(1))
	out := f.StatFs("")
	if out == nil || out.Blocks == 0 || out.Bsize == 0 {
		t.Errorf("statfs must never answer zeros: %+v", out)
	}
	// Point both dirs at a nonexistent path: synthetic 1 TiB fallback.
	f2, _, _, _ := newTestFs(t, snap(1))
	f2.backingDir = "/nonexistent-pds-backing/under-a-detached/mount"
	f2.stateDir = "/nonexistent-pds-state/under-a-detached/mount"
	// The ancestor walk reaches "/" and answers real numbers; the
	// synthetic 1 TiB arm only fires when nothing answers (detached
	// mount) — verified by construction (never-answer-zeros rule).
	out2 := f2.StatFs("")
	if out2 == nil || out2.Blocks == 0 || out2.Bsize == 0 {
		t.Errorf("statfs must never answer zeros, even via the ancestor walk: %+v", out2)
	}
}

func TestSnapshotWriteROEnforcement(t *testing.T) {
	// Per-file RO enforcement for snapshot-backed files until hydrated
	// RW: a writable open of a non-hydrated placeholder waits bounded,
	// then fails EIO rather than serving an empty write target.
	s := snap(1, "a.txt")
	s.byPath["a.txt"] = VNode{Rel: "a.txt", Parent: "", Name: "a.txt", Size: 4, Mode: 0o644}
	f, _, _, _ := newTestFs(t, s)
	origWait := hydrateWaitOverride
	hydrateWaitOverride = 150 * time.Millisecond
	defer func() { hydrateWaitOverride = origWait }()
	if _, status := f.Open("a.txt", syscall.O_WRONLY, ctx); status != fuseStatus(syscall.EIO) {
		t.Errorf("unhydrated RW open = %v, want bounded-wait EIO", status)
	}
}

func TestFsyncUploadsDirtyHandle(t *testing.T) {
	s := snap(1)
	f, st, _, _ := newTestFs(t, s)
	file, status := f.Create("log.txt", syscall.O_RDWR, 0o644, ctx)
	if status != fuseOK {
		t.Fatal(status)
	}
	defer file.Release()
	if _, status := file.Write([]byte("x"), 0); status != fuseOK {
		t.Fatal(status)
	}
	if status := file.Fsync(0); status != fuseOK {
		t.Fatal(status)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.writes) != 1 {
		t.Errorf("fsync must queue the upload: %v", st.writes)
	}
}

var _ = fmt.Sprintf
