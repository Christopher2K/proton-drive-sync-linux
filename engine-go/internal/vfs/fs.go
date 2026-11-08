//go:build linux

// FUSE filesystem presenting the remote tree with on-demand placeholders
// (Go port of crates/vfs-linux/src/fs.rs, on go-fuse's pathfs layer).
//
// Structure (GetAttr/OpenDir/Lookup via the path bridge) is served from
// the engine's snapshot — no Drive traffic, no local copies. Opening a
// placeholder queues a hydrate request; the first read blocks (bounded)
// until the engine downloads the backing file, then serves bytes from it.
//
// Writes go through to Drive (M3.2): Create makes an ephemeral backing
// file, the final write/Setattr queues an upload through the engine's
// normal machinery (F7, retries, pause), Unlink/Rmdir queue remote
// trashes, Mkdir queues remote folder creation. Ephemeral (not yet
// uploaded) files appear via a write-overlay consulted before the
// snapshot. `.pds` is answered from the real `<root>/.pds` metadata so
// the CLI control socket stays reachable through the mount.

package vfs

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// hydrateWait bounds one hydration wait. Generous: fetching a large file
// on explicit user action beats an EIO, but nothing hangs forever on a
// dead engine.
const hydrateWait = 120 * time.Second

// hydratePoll is the poll slice while waiting.
const hydratePoll = 50 * time.Millisecond

// hydrateWaitOverride lets tests shorten the bounded wait (0 = default).
var hydrateWaitOverride time.Duration

// OverlayInoBase: write-overlay inodes start here (snapshot inos grow
// from 3; a session would need millions of nodes to collide).
const OverlayInoBase uint64 = 1 << 40

// OverlayInoSpan bounds the path-hash overlay inode range.
const OverlayInoSpan uint64 = 1 << 32

// syntheticStatfsBytes: with no statvfs answer (path on a detached mount)
// a sane non-zero 1 TiB synthetic filesystem beats a zero-byte one.
const syntheticStatfsBytes uint64 = 1 << 40

// PdsFs is the placeholder-view filesystem.
type PdsFs struct {
	// Embedding the interface gives us promoted ENOSYS defaults from
	// pathfs.NewDefaultFileSystem() assigned at construction.
	FileSystemDefaults pathfs.FileSystem

	tree *Tree
	st   Store
	// stateDir is the real `<root>/.pds` directory, answered at the
	// `.pds` mount entry.
	stateDir string
	// backingDir is the backing cache root (`<root>/.pds-vfs-backing`) —
	// where ephemeral write-through files are created before upload.
	backingDir string

	mu      sync.Mutex // guards the overlay + pending sizes below
	overlay overlay
	// pendingSizes: local sizes of revision files whose upload has not
	// republished the snapshot yet. Without it, GetAttr would report the
	// old remote size right after close/truncate and the kernel would
	// clip reads at the stale EOF even though the backing file already
	// grew.
	pendingSizes map[string]uint64

	// handleMu guards handles + nextFh: open write handles (fh → handle).
	handleMu sync.Mutex
	handles  map[uint64]*writeHandle
	nextFh   uint64

	uid uint32
	gid uint32

	logger *log.Logger
}

// writeEntry is a file created through the mount, not yet in any snapshot.
type writeEntry struct {
	ino   uint64
	size  uint64
	mtime int64
	mode  uint32
	isDir bool
}

// overlay holds all write-overlay entries: rel → entry. Files stay until
// their engine snapshot row replaces them; dirs until their snapshot row
// appears. inoByRel... rather byIno (ino → rel) is kept in sync so any op
// on an overlay inode resolves.
type overlay struct {
	byRel map[string]writeEntry
	byIno map[uint64]string
}

func (o *overlay) insert(rel string, e writeEntry) {
	o.byIno[e.ino] = rel
	o.byRel[rel] = e
}

// remove drops the entry entirely (unlinked before upload).
func (o *overlay) remove(rel string) {
	if e, ok := o.byRel[rel]; ok {
		delete(o.byRel, rel)
		delete(o.byIno, e.ino)
	}
}

// retire: the snapshot now covers rel — drop it from listings but KEEP
// the ino→rel alias; the kernel may still hold a dentry with the overlay
// inode (TTL window), and ops on it must resolve.
func (o *overlay) retire(rel string) {
	delete(o.byRel, rel)
}

// NewPdsFs builds the filesystem. store must not be nil.
func NewPdsFs(store Store, stateDir, backingDir string) *PdsFs {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	return &PdsFs{
		FileSystemDefaults: pathfs.NewDefaultFileSystem(),
		tree:               NewTree(),
		st:                 store,
		stateDir:           stateDir,
		backingDir:         backingDir,
		overlay:            overlay{byRel: map[string]writeEntry{}, byIno: map[uint64]string{}},
		pendingSizes:       map[string]uint64{},
		handles:            map[uint64]*writeHandle{},
		uid:                uid,
		gid:                gid,
		logger:             log.New(os.Stderr, "vfs: ", log.LstdFlags),
	}
}

// toAbs maps a root-relative path into the backing dir.
func (f *PdsFs) toAbs(rel string) string {
	return filepath.Join(f.backingDir, filepath.FromSlash(rel))
}

// joinRel: parent ("" for root) + name → rel path.
func joinRel(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func (f *PdsFs) nowSecs() int64 { return time.Now().Unix() }

// attrFor builds the fuse.Attr served to the kernel.
func (f *PdsFs) attrFor(ino uint64, isDir bool, size uint64, mtime int64) *fuse.Attr {
	perm := uint32(0o644)
	if isDir {
		perm = 0o755
	}
	mode := f.nodeModeOr(relOr(f, ino), isDir, perm)
	kind := uint32(syscall.S_IFREG)
	if isDir {
		kind = syscall.S_IFDIR
	}
	if mtime < 0 {
		mtime = 0
	}
	return &fuse.Attr{
		Ino:     ino,
		Size:    size,
		Blocks:  (size + 511) / 512,
		Atime:   uint64(mtime),
		Mtime:   uint64(mtime),
		Ctime:   uint64(mtime),
		Mode:    kind | (mode & 0o777),
		Nlink:   1,
		Owner:   fuse.Owner{Uid: f.uid, Gid: f.gid},
		Blksize: 4096,
	}
}

func relOr(f *PdsFs, ino uint64) string {
	rel, ok := f.relOfAny(ino)
	if !ok {
		return ""
	}
	return rel
}

func (f *PdsFs) nodeModeOr(rel string, isDir bool, fallback uint32) uint32 {
	if rel == "" || rel == StateProxy {
		return 0o755
	}
	if e, ok := f.overlayEntry(rel); ok {
		return e.mode
	}
	if n, ok := f.tree.Node(rel); ok {
		return n.Mode
	}
	return fallback
}

// relOfAny: inode → path, consulting the write overlay first (overlay
// inos are not in the tree), then the snapshot tree. "" is the root.
func (f *PdsFs) relOfAny(ino uint64) (string, bool) {
	if ino >= OverlayInoBase {
		f.mu.Lock()
		defer f.mu.Unlock()
		rel, ok := f.overlay.byIno[ino]
		return rel, ok
	}
	return f.tree.RelOf(ino)
}

// overlayEntry returns the metadata for an overlay entry, if rel is one.
func (f *PdsFs) overlayEntry(rel string) (writeEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.overlay.byRel[rel]
	return e, ok
}

// nodeMeta is the tree lookup that also consults the write overlay:
// (isDir, size, mtime, contentReady) for rel.
func (f *PdsFs) nodeMeta(rel string) (bool, uint64, int64, bool, bool) {
	if e, ok := f.overlayEntry(rel); ok {
		return e.isDir, e.size, e.mtime, true, true
	}
	if rel == StateProxy || rel == "" {
		return true, 0, 0, true, true
	}
	n, ok := f.tree.Node(rel)
	if !ok {
		return false, 0, 0, false, false
	}
	size := n.Size
	if pending, ok := f.pendingSize(rel); ok {
		size = pending
	}
	return n.IsDir, size, n.Mtime, n.ContentReady, true
}

// pendingSize is the size of a locally modified revision file that the
// snapshot does not reflect yet. Entries clear themselves once the upload
// publishes the matching size (or the path disappears from the snapshot).
func (f *PdsFs) pendingSize(rel string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	size, ok := f.pendingSizes[rel]
	if !ok {
		return 0, false
	}
	if n, exists := f.tree.Node(rel); !exists || n.Size == size {
		delete(f.pendingSizes, rel)
		return 0, false
	}
	return size, true
}

func (f *PdsFs) setPendingSize(rel string, size uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pendingSizes[rel] = size
}

func (f *PdsFs) clearPendingSize(rel string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pendingSizes, rel)
}

// waitForBacking blocks until the backing file for rel exists or the
// deadline passes.
func (f *PdsFs) waitForBacking(rel string) *os.File {
	// Ephemeral write files are their own backing content.
	if _, isOverlay := f.overlayEntry(rel); isOverlay {
		if h, err := os.Open(f.toAbs(rel)); err == nil {
			return h
		}
	}
	f.st.BeginHydrate(rel)
	wait := hydrateWait
	if hydrateWaitOverride > 0 {
		wait = hydrateWaitOverride
	}
	deadline := time.Now().Add(wait)
	for {
		if path, ok := f.st.BackingPath(rel); ok {
			if file, err := os.Open(path); err == nil {
				return file
			}
		}
		if !time.Now().Before(deadline) {
			f.logger.Printf("hydration wait expired: %s", rel)
			return nil
		}
		time.Sleep(hydratePoll)
	}
}

// openBackingRW opens rel's backing file read-write, waiting (bounded)
// for the engine to hydrate it first. Creates the parent dirs so an
// unlinked-then-recreated path still works.
func (f *PdsFs) openBackingRW(rel string) *os.File {
	abs := f.toAbs(rel)
	wait := hydrateWait
	if hydrateWaitOverride > 0 {
		wait = hydrateWaitOverride
	}
	deadline := time.Now().Add(wait)
	for {
		os.MkdirAll(filepath.Dir(abs), 0o755)
		if fAbs, err := os.OpenFile(abs, os.O_RDWR, 0o644); err == nil {
			return fAbs
		}
		if !time.Now().Before(deadline) {
			f.logger.Printf("revision open: hydration wait expired: %s", rel)
			return nil
		}
		time.Sleep(hydratePoll)
	}
}

// dirIsEmpty: snapshot children + overlay children check (Rmdir and the
// dir-overwrite arm of Rename share it). A folder still only in the
// overlay has no snapshot children by definition.
func (f *PdsFs) dirIsEmpty(rel string) bool {
	prefix := rel + "/"
	// Children with a queued delete are gone on disk; the snapshot keeps
	// serving them only until the engine drains the tombstone and
	// republishes. Counting them would hand an `rm -rf` an ENOTEMPTY
	// milliseconds after its last unlink.
	pendingDelete := func(c string) bool { return f.st.HasPendingDelete(c) }
	if ino, ok := f.tree.InoOfOpt(rel); ok {
		if children, ok := f.tree.Children(ino); ok {
			for _, c := range children {
				if !pendingDelete(c) {
					return false
				}
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.overlay.byRel {
		if strings.HasPrefix(c, prefix) && !pendingDelete(c) {
			return false
		}
	}
	return true
}

// reconcileOverlay drops overlay entries the snapshot now covers (uploaded
// file, mirrored mkdir): the tree's row is authoritative and serving both
// would list the path twice. The ino→rel alias stays so kernel dentries
// still holding the overlay inode keep resolving during the TTL window.
// Cheap: the overlay holds only this session's writes.
func (f *PdsFs) reconcileOverlay() {
	f.mu.Lock()
	defer f.mu.Unlock()
	var covered []string
	for rel := range f.overlay.byRel {
		if _, ok := f.tree.Node(rel); ok {
			covered = append(covered, rel)
		}
	}
	for _, rel := range covered {
		f.overlay.retire(rel)
	}
}

// finalizeWrite: a finished write (Release with dirty data, or Fsync):
// fix up the overlay size and hand the file to the engine for upload.
func (f *PdsFs) finalizeWrite(rel string, size uint64) {
	if rel == "" {
		return
	}
	f.mu.Lock()
	if e, ok := f.overlay.byRel[rel]; ok {
		e.size = size
		e.mtime = f.nowSecs()
		f.overlay.byRel[rel] = e
		f.mu.Unlock()
	} else {
		f.mu.Unlock()
		// Keep getattr/read at the real local size until the upload
		// republishes the snapshot (truncate-grow and append both
		// outrun it).
		f.setPendingSize(rel, size)
	}
	f.st.RecordWrite(rel)
}

// String implements pathfs.FileSystem.
func (f *PdsFs) String() string { return "pds-vfs" }

// SetDebug implements pathfs.FileSystem.
func (f *PdsFs) SetDebug(debug bool) {}

// OnMount primes the tree so the mount is browsable immediately.
func (f *PdsFs) OnMount(nodeFs *pathfs.PathNodeFs) {
	f.tree.Refresh(f.st.Snapshot())
}

// OnUnmount implements pathfs.FileSystem.
func (f *PdsFs) OnUnmount() {
	f.logger.Printf("unmounted")
}

func (f *PdsFs) refresh() {
	f.tree.Refresh(f.st.Snapshot())
	f.reconcileOverlay()
}

// GetAttr is the main entry point through which FUSE discovers files.
// The Ino it returns doubles as the stable path-hash inode for overlay
// entries (go-fuse pathfs hard-links nothing; inos are advisory).
func (f *PdsFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	f.refresh()
	rel := name // pathfs uses "" for the root, like the Rust rel space
	if rel == StateProxy {
		meta, err := osStatNotFollow(f.stateDir)
		if err != nil {
			return nil, fuse.ENOENT
		}
		mtime := meta.ModTime().Unix()
		if mtime < 0 {
			mtime = 0
		}
		return f.attrFor(StateProxyIno, true, 0, mtime), fuse.OK
	}
	isDir, size, mtime, _, ok := f.nodeMeta(rel)
	if !ok {
		return nil, fuse.ENOENT
	}
	attr := f.attrFor(f.inoForAttr(rel, isDir), isDir, size, mtime)
	// A live write handle's backing file is authoritative for size while
	// open (O_APPEND correctness against the stale snapshot size).
	if !isDir {
		if live, ok := f.openWriteSize(rel); ok && live > attr.Size {
			attr.Size = live
		}
	}
	return attr, fuse.OK
}

func (f *PdsFs) inoForAttr(rel string, isDir bool) uint64 {
	if rel == "" {
		return RootIno
	}
	if rel == StateProxy {
		return StateProxyIno
	}
	if e, ok := f.overlayEntry(rel); ok {
		return e.ino
	}
	return f.tree.InoOf(rel)
}

// openWriteSize returns the live size of a file with an open write handle
// (max across handles), if any.
func (f *PdsFs) openWriteSize(rel string) (uint64, bool) {
	f.handleMu.Lock()
	defer f.handleMu.Unlock()
	var best uint64
	found := false
	for _, h := range f.handles {
		if h.rel != rel {
			continue
		}
		if meta, err := h.file.Stat(); err == nil {
			size := uint64(meta.Size())
			if !found || size > best {
				best = size
			}
			found = true
		}
	}
	return best, found
}

// Access: we report 0644/0755 owned by the user; let it pass.
func (f *PdsFs) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	return fuse.OK
}

// OpenDir serves listings from the snapshot tree + write overlay.
func (f *PdsFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	f.refresh()
	ino, ok := f.tree.InoOfOpt(name)
	if !ok {
		if _, isOverlay := f.overlayEntry(name); !isOverlay && name != "" {
			return nil, fuse.ENOENT
		}
		ino = StableInoFor(name)
	}
	children, ok := f.tree.Children(ino)
	if !ok {
		if name != "" {
			if _, isOverlay := f.overlayEntry(name); !isOverlay {
				return nil, fuse.ENOTDIR
			}
		}
		children = nil
	}
	var entries []fuse.DirEntry
	for _, child := range children {
		node, ok := f.tree.Node(child)
		if !ok {
			continue
		}
		// Special files are skipped, not followed (AGENT.md: symlinks
		// and FIFOs never round-trip through the placeholder view).
		name := child[strings.LastIndexByte(child, '/')+1:]
		mode := uint32(fuse.S_IFREG)
		if node.IsDir {
			mode = fuse.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Mode: mode, Name: name, Ino: node.Ino})
	}
	// Write-overlay entries created in this directory (not yet in the
	// snapshot). Prefix check keeps this O(small).
	f.mu.Lock()
	prefix := ""
	if name != "" {
		prefix = name + "/"
	}
	for rel, entry := range f.overlay.byRel {
		if !strings.HasPrefix(rel, prefix) {
			continue
		}
		base := rel[len(prefix):]
		if strings.ContainsRune(base, '/') {
			continue
		}
		mode := uint32(fuse.S_IFREG)
		if entry.isDir {
			mode = fuse.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Mode: mode, Name: base, Ino: entry.ino})
	}
	f.mu.Unlock()
	return entries, fuse.OK
}

// Open: hydrate (bounded) and return a file handle. Snapshot-backed
// files opened for writing: hydration must land first and the handle
// serves the hydrated bytes RW (M3.3 revision writes). Read-only opens of
// snapshot-backed files hold the backing file so the descriptor keeps its
// bytes across rename(2)/unlink(2), like a POSIX fd.
func (f *PdsFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	f.refresh()
	rel := name
	if rel == StateProxy {
		return nil, fuse.EISDIR
	}
	isDir, _, _, contentReady, ok := f.nodeMeta(rel)
	if !ok {
		return nil, fuse.ENOENT
	}
	if isDir {
		return nodefs.NewDefaultFile(), fuse.OK
	}
	writable := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	_, isOverlay := f.overlayEntry(rel)
	if writable && isOverlay {
		// Writable open of an overlay (created-this-session, not yet
		// snapshotted) file: e.g. `cp src dst` re-opened dst with
		// O_WRONLY|O_TRUNC before the upload landed. Serve a real
		// write handle on the backing file.
		truncate := flags&syscall.O_TRUNC != 0
		file, err := os.OpenFile(f.toAbs(rel), os.O_RDWR|os.O_CREATE|(truncateFlag(truncate)), 0o644)
		if err != nil {
			f.logger.Printf("overlay rewrite open failed: %s: %v", rel, err)
			return nil, fuse.EIO
		}
		h := f.newWriteHandle(rel, file, truncate /*dirty*/, true /*overlay*/)
		if truncate {
			f.mu.Lock()
			if e, ok := f.overlay.byRel[rel]; ok {
				e.size = 0
				f.overlay.byRel[rel] = e
			}
			f.mu.Unlock()
		}
		return newWriteFile(h), fuse.OK
	}
	if writable {
		// Revision write (M3.3): the file must be hydrated before the
		// handle can serve bytes.
		if !contentReady {
			f.st.BeginHydrate(rel)
		}
		file := f.openBackingRW(rel)
		if file == nil {
			f.logger.Printf("open: backing file unavailable for writable handle: %s", rel)
			return nil, fuse.EIO
		}
		h := f.newWriteHandle(rel, file, false, false)
		f.st.RecordAccess(rel)
		// Revision handles keep the file evict-protected too.
		f.st.AddOpenHandle(rel)
		return newWriteFile(h), fuse.OK
	}
	// Read open.
	file := f.waitForBacking(rel)
	if file == nil {
		f.logger.Printf("open: backing unavailable for read handle: %s", rel)
		return nil, fuse.EIO
	}
	tracked := !isOverlay
	f.st.RecordAccess(rel)
	// Open files are never evicted; overlay (write-created) files are
	// not tracked (they are never in the hydrated map).
	if tracked {
		f.st.AddOpenHandle(rel)
	}
	h := &readHandle{rel: rel, file: file, ino: f.inoForAttr(rel, false), tracked: tracked, fs: f}
	return newReadFile(h), fuse.OK
}

func truncateFlag(b bool) int {
	if b {
		return syscall.O_TRUNC
	}
	return 0
}

// Create: an ephemeral backing file + overlay entry; the engine uploads
// it on release/fsync.
func (f *PdsFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	f.refresh()
	parent := parentRel(name)
	base := baseName(name)
	if base == "" || containsSlash(base) || base == "." || base == ".." || strings.HasPrefix(base, ".pdsw-") {
		return nil, fuse.EINVAL
	}
	if _, _, _, _, ok := f.nodeMeta(name); ok {
		return nil, fuse.Status(syscall.EEXIST)
	}
	abs := f.toAbs(name)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		f.logger.Printf("backing mkdir failed: %s: %v", name, err)
		return nil, fuse.EIO
	}
	file, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		f.logger.Printf("backing create failed: %s: %v", name, err)
		return nil, fuse.EIO
	}
	_ = parent
	// Preserve the creator's requested mode locally (D2): an executable
	// script created through the mount keeps `+x`.
	perm := uint32(0o644)
	if mode != 0 {
		perm = mode & 0o777
	}
	if err := file.Chmod(os.FileMode(perm)); err != nil {
		f.logger.Printf("backing chmod failed: %s: %v", name, err)
	}
	h := f.newWriteHandle(name, file, false, true)
	ino := StableInoFor(name)
	mtime := f.nowSecs()
	f.mu.Lock()
	f.overlay.insert(name, writeEntry{ino: ino, size: 0, mtime: mtime, mode: perm, isDir: false})
	f.mu.Unlock()
	return newWriteFile(h), fuse.OK
}

// Mkdir: local backing dir entry + queue the remote folder. The folder
// becomes visible via the engine's next snapshot; until then overlay it
// so the user sees their mkdir immediately.
func (f *PdsFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	f.refresh()
	base := baseName(name)
	if base == "" || containsSlash(base) || base == "." || base == ".." {
		return fuse.EINVAL
	}
	if _, _, _, _, ok := f.nodeMeta(name); ok {
		return fuse.Status(syscall.EEXIST)
	}
	if err := os.MkdirAll(f.toAbs(name), 0o755); err != nil {
		f.logger.Printf("backing mkdir failed: %s: %v", name, err)
		return fuse.EIO
	}
	f.st.RecordMkdir(name)
	ino := StableInoFor(name)
	f.mu.Lock()
	f.overlay.insert(name, writeEntry{ino: ino, size: 0, mtime: f.nowSecs(), mode: 0o755, isDir: true})
	f.mu.Unlock()
	return fuse.OK
}

// Unlink: drop local state; the engine mirrors the delete remotely.
// Open fds keep reading the unlinked inode (read handles hold it); their
// writes are discarded so a later recreate is never clobbered.
func (f *PdsFs) Unlink(name string, context *fuse.Context) fuse.Status {
	f.refresh()
	if _, _, _, _, ok := f.nodeMeta(name); !ok {
		return fuse.ENOENT
	}
	f.discardWriteHandles(name)
	f.clearPendingSize(name)
	f.mu.Lock()
	f.overlay.remove(name)
	f.mu.Unlock()
	os.Remove(f.toAbs(name))
	f.st.Forget(name)
	f.st.RecordDelete(name)
	return fuse.OK
}

// Rmdir: POSIX — only empty directories are removable. Snapshot children
// + overlay children both count.
func (f *PdsFs) Rmdir(name string, context *fuse.Context) fuse.Status {
	f.refresh()
	isDir, _, _, _, ok := f.nodeMeta(name)
	if !ok {
		return fuse.ENOENT
	}
	if !isDir {
		return fuse.ENOTDIR
	}
	// A folder still only in the overlay (no snapshot row yet) has no
	// snapshot children by definition — resolving its ino would fall
	// back to ROOT and make the root's listing answer for it (spurious
	// ENOTEMPTY on an empty folder).
	if !f.dirIsEmpty(name) {
		return fuse.Status(syscall.ENOTEMPTY)
	}
	f.mu.Lock()
	f.overlay.remove(name)
	f.mu.Unlock()
	os.Remove(f.toAbs(name))
	f.st.ForgetTree(name)
	f.st.RecordDelete(name)
	return fuse.OK
}

// Rename/move through the mount. Moves the backing content, re-keys the
// write overlay, and queues the remote rename for the engine (which
// re-keys DB rows and calls renameNode/moveNodes — no trash + re-upload,
// so content and revision history survive).
func (f *PdsFs) Rename(oldName, newName string, context *fuse.Context) fuse.Status {
	f.refresh()
	srcIsDir, srcSize, _, _, ok := f.nodeMeta(oldName)
	if !ok {
		return fuse.ENOENT
	}
	if oldName == StateProxy || newName == StateProxy || oldName == "" {
		return fuse.EPERM
	}
	// Renaming a folder into its own subtree is invalid.
	if srcIsDir && strings.HasPrefix(newName, oldName+"/") {
		return fuse.EINVAL
	}
	// Destination handling per rename(2): a file replaces a file, a dir
	// replaces an EMPTY dir, anything else is an error. The replaced
	// destination is mirrored as a remote delete — queued together with
	// the rename (RecordOverwrite), so the engine can never drain the
	// delete without seeing the inbound rename and tear down the content
	// the rename is delivering.
	replaced := false
	if dstIsDir, _, _, _, exists := f.nodeMeta(newName); exists {
		if dstIsDir && !srcIsDir {
			return fuse.EISDIR
		}
		if srcIsDir && !dstIsDir {
			return fuse.ENOTDIR
		}
		if srcIsDir && dstIsDir && !f.dirIsEmpty(newName) {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		replaced = true
		// Fds open on the replaced destination keep the old inode
		// readable, but their writes must not land on the incoming
		// node's path.
		f.discardWriteHandles(newName)
		f.clearPendingSize(newName)
		f.mu.Lock()
		f.overlay.remove(newName)
		f.mu.Unlock()
		abs := f.toAbs(newName)
		if dstIsDir {
			os.Remove(abs)
			f.st.ForgetTree(newName)
		} else {
			os.Remove(abs)
			f.st.Forget(newName)
		}
	}

	// 1. Move the backing content (hydrated or write-created). A pure
	//    placeholder has none — its content arrives by path later.
	from, to := f.toAbs(oldName), f.toAbs(newName)
	if _, err := os.Stat(from); err == nil {
		os.MkdirAll(filepath.Dir(to), 0o755)
		if err := os.Rename(from, to); err != nil {
			f.logger.Printf("backing rename failed: %s -> %s: %v", oldName, newName, err)
			return fuse.EIO
		}
	}

	// 2. Overlay: re-key this entry and any children to the new prefix
	//    (same inos, so kernel dentries stay valid). A snapshot-backed
	//    source gets a fresh overlay entry so the new path serves until
	//    the engine's next snapshot publish re-keys the rows.
	srcMode := f.nodeModeOr(oldName, !srcIsDir, 0o644)
	{
		f.mu.Lock()
		oldPrefix := oldName + "/"
		type move struct {
			rel string
			e   writeEntry
		}
		var moves []move
		for rel, e := range f.overlay.byRel {
			if rel == oldName || (srcIsDir && strings.HasPrefix(rel, oldPrefix)) {
				moves = append(moves, move{rel, e})
			}
		}
		hadSource := false
		for _, m := range moves {
			newPath := newName
			if m.rel != oldName {
				newPath = newName + "/" + m.rel[len(oldName)+1:]
			} else {
				hadSource = true
			}
			f.overlay.remove(m.rel)
			m.e.mtime = f.nowSecs()
			f.overlay.byIno[m.e.ino] = newPath
			f.overlay.byRel[newPath] = m.e
		}
		if !hadSource {
			if _, inTree := f.tree.Node(newName); !inTree {
				f.overlay.insert(newName, writeEntry{
					ino:   StableInoFor(newName),
					size:  srcSize,
					mtime: f.nowSecs(),
					mode:  srcMode,
					isDir: srcIsDir,
				})
			}
		}
		f.mu.Unlock()
	}

	// 3. Queue the remote work (engine re-keys rows, renameNode /
	//    moveNodes remotely, re-registers backing paths). An
	//    overwrite-rename queues the destination delete and the rename
	//    atomically so the engine always sees the pair together.
	f.rekeyHandles(oldName, newName, srcIsDir)
	f.mu.Lock()
	movedSize, hadMoved := f.pendingSizes[oldName]
	delete(f.pendingSizes, oldName)
	f.mu.Unlock()
	if hadMoved {
		f.setPendingSize(newName, movedSize)
	}
	if replaced {
		f.st.RecordOverwrite(newName, oldName, newName)
	} else {
		f.st.RecordRename(oldName, newName)
	}
	return fuse.OK
}

// Truncate: resize the backing file. Overlay files created this session
// are trivially writable; snapshot-backed files truncate in place (M3.3
// revision) — hydration must land first, and the truncation marks the
// content dirty for the next upload.
func (f *PdsFs) Truncate(name string, size uint64, context *fuse.Context) fuse.Status {
	return f.truncateCommon(name, size, nil)
}

func (f *PdsFs) truncateCommon(name string, size uint64, fh *writeHandle) fuse.Status {
	if fh != nil {
		// Truncate through an open fd (ftruncate / deferred O_TRUNC):
		// the fd's inode is authoritative — it stays valid after the path
		// is unlinked or replaced, where the path lookup would miss.
		if err := fh.file.Truncate(int64(size)); err != nil {
			f.logger.Printf("truncate via handle failed: %s: %v", name, err)
			return fuse.EIO
		}
		// One revision for the whole write sequence: defer upload to
		// release/fsync; go-fuse never negotiates ATOMIC_O_TRUNC.
		f.handleMu.Lock()
		fh.dirty = true
		f.handleMu.Unlock()
		return fuse.OK
	}
	isDir, _, _, _, ok := f.nodeMeta(name)
	if !ok {
		return fuse.ENOENT
	}
	if isDir {
		return fuse.EISDIR
	}
	_, isOverlay := f.overlayEntry(name)
	if !isOverlay {
		if _, hydrated := f.st.BackingPath(name); !hydrated {
			// Placeholder content: hydrate first (bounded wait) so the
			// truncate lands on real content, not a missing file.
			file := f.waitForBacking(name)
			if file == nil {
				f.logger.Printf("truncate: backing hydration failed: %s", name)
				return fuse.EIO
			}
			file.Close()
			f.st.RecordAccess(name)
		}
	}
	abs := f.toAbs(name)
	if err := os.Truncate(abs, int64(size)); err != nil {
		f.logger.Printf("truncate failed: %s: %v", name, err)
		return fuse.EIO
	}
	if !isOverlay {
		f.setPendingSize(name, size)
	}
	// Truncate outside a handle (truncate(2)): queue the revision
	// upload directly (re-hash-deduped by the store). Via-handle
	// truncate returned early above (deferred to release/fsync).
	f.st.RecordWrite(name)
	return fuse.OK
}

// Chmod persists the local POSIX mode (D2: `chmod +x` survives
// eviction/rehydration; Drive's node metadata has no POSIX mode).
func (f *PdsFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	f.refresh()
	if _, _, _, _, ok := f.nodeMeta(name); !ok {
		return fuse.ENOENT
	}
	f.mu.Lock()
	if e, ok := f.overlay.byRel[name]; ok {
		e.mode = mode & 0o777
		f.overlay.byRel[name] = e
	}
	f.mu.Unlock()
	abs := f.toAbs(name)
	if info, err := os.Stat(abs); err == nil && !info.IsDir() {
		os.Chmod(abs, os.FileMode(mode&0o777))
	}
	m := mode & 0o777
	f.st.RecordMetadata(name, nil, &m)
	return fuse.OK
}

// Utimens sets the modification time (and atime, ignored for uploads).
func (f *PdsFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) fuse.Status {
	f.refresh()
	if _, _, _, _, ok := f.nodeMeta(name); !ok {
		return fuse.ENOENT
	}
	if mtime != nil {
		secs := mtime.Unix()
		f.mu.Lock()
		if e, ok := f.overlay.byRel[name]; ok {
			e.mtime = secs
			f.overlay.byRel[name] = e
		}
		f.mu.Unlock()
		if abs := f.toAbs(name); fileExists(abs) {
			os.Chtimes(abs, *atimeOrNow(atime), *mtime)
		}
		f.st.RecordMetadata(name, &secs, nil)
	}
	return fuse.OK
}

func atimeOrNow(t *time.Time) *time.Time {
	if t != nil {
		return t
	}
	now := time.Now()
	return &now
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// StatFs reports the backing filesystem's real capacity: placeholder
// sizes are remote sizes, not local usage, so the cache directory is the
// honest answer for `df` and for installers/copy tools that refuse a
// 0-byte filesystem. Never answer zeros: walk to an existing ancestor
// for statvfs; if nothing answers (detached mount), a synthetic 1 TiB
// beats a zero-byte filesystem.
func (f *PdsFs) StatFs(name string) *fuse.StatfsOut {
	var st syscall.Statfs_t
	for dir := f.backingDir; ; dir = filepath.Dir(dir) {
		if err := syscall.Statfs(dir, &st); err == nil {
			out := statfsFromUnix(&st)
			return &out
		}
		if dir == "/" || dir == "." {
			break
		}
	}
	for dir := f.stateDir; ; dir = filepath.Dir(dir) {
		if err := syscall.Statfs(dir, &st); err == nil {
			out := statfsFromUnix(&st)
			return &out
		}
		if dir == "/" || dir == "." {
			break
		}
	}
	const block uint32 = 4096
	blocks := syntheticStatfsBytes / uint64(block)
	return &fuse.StatfsOut{
		Blocks:  blocks,
		Bfree:   blocks,
		Bavail:  blocks,
		Files:   1_000_000,
		Ffree:   1_000_000,
		Bsize:   block,
		NameLen: 255,
		Frsize:  block,
	}
}

func statfsFromUnix(st *syscall.Statfs_t) fuse.StatfsOut {
	frsize := uint64(st.Frsize)
	if frsize == 0 {
		frsize = 1
	}
	bsize := uint64(st.Bsize)
	if bsize == 0 {
		bsize = 1
	}
	blocks := st.Blocks
	if blocks == 0 {
		blocks = 1
	}
	bavail := st.Bavail
	if bavail == 0 {
		bavail = 1
	}
	files := st.Files
	if files == 0 {
		files = 1
	}
	namelen := uint32(255)
	if uint32(st.Namelen) > namelen {
		namelen = uint32(st.Namelen)
	}
	return fuse.StatfsOut{
		Blocks:  blocks,
		Bfree:   st.Bfree,
		Bavail:  bavail,
		Files:   files,
		Ffree:   st.Ffree,
		Bsize:   uint32(bsize),
		NameLen: namelen,
		Frsize:  uint32(frsize),
	}
}

// Symlink/Readlink/Mknod: special files are refused — the placeholder
// view serves only regular files and directories (AGENT.md: symlinks and
// FIFOs are skipped, never followed).
func (f *PdsFs) Symlink(value string, linkName string, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

func (f *PdsFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	return "", fuse.ENOENT
}

func (f *PdsFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	if mode&syscall.S_IFMT == syscall.S_IFREG {
		// Plain mknod of a regular file behaves like create.
		_, status := f.Create(name, 0, mode&0o777, context)
		return status
	}
	return fuse.EPERM
}

// GetXAttr: no extended attributes (kept for parity; listxattr size-form
// gotcha from AGENT.md does not arise — go-fuse answers ENOATTR for us).
func (f *PdsFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return nil, fuse.ENOATTR
}

// ListXAttr: none.
func (f *PdsFs) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	return nil, fuse.ENOATTR
}

// RemoveXAttr: none.
func (f *PdsFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.ENOATTR
}

// SetXAttr: none.
func (f *PdsFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.ENOATTR
}

// Link: hard links are not supported by the Drive model.
func (f *PdsFs) Link(oldName string, newName string, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

// Chown: local-only metadata; accepted and recorded, never uploaded.
func (f *PdsFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	f.refresh()
	if _, _, _, _, ok := f.nodeMeta(name); !ok {
		return fuse.ENOENT
	}
	return fuse.OK
}

func parentRel(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[:i]
	}
	return ""
}

func baseName(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[i+1:]
	}
	return rel
}

// osStatNotFollow stats path with O_PATH|O_NOFOLLOW semantics: special
// files are skipped, not followed (a symlinked or pipe-backed state dir
// entry must not be dereferenced through the mount).
func osStatNotFollow(path string) (os.FileInfo, error) {
	fd, err := syscall.Open(path, 0x200000|syscall.O_NOFOLLOW /* O_PATH */, 0)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return nil, err
	}
	return fileInfoFromStat(&st), nil
}

func fileInfoFromStat(st *syscall.Stat_t) os.FileInfo {
	return statInfo{st: *st}
}

type statInfo struct{ st syscall.Stat_t }

func (s statInfo) Name() string { return "" }
func (s statInfo) Size() int64  { return s.st.Size }
func (s statInfo) Mode() os.FileMode {
	mode := os.FileMode(s.st.Mode & 0o777)
	switch s.st.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	}
	return mode
}
func (s statInfo) ModTime() time.Time { return time.Unix(s.st.Mtim.Sec, s.st.Mtim.Nsec) }
func (s statInfo) IsDir() bool        { return s.st.Mode&syscall.S_IFMT == syscall.S_IFDIR }
func (s statInfo) Sys() any           { return &s.st }
