//go:build linux

package vfs

import (
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
)

// writeHandle is the per-open state of a write handle. overlay marks
// files created through the mount (no snapshot row yet); revision handles
// on snapshot-backed files are open-tracked like read handles.
type writeHandle struct {
	fs      *PdsFs
	rel     string
	file    *os.File
	ino     uint64 // inode at open: stable across rename/unlink for GetAttr
	dirty   bool
	overlay bool
	// discarded: the path was unlinked (or replaced by a rename) while
	// this handle was open — the descriptor still points at the
	// unlinked inode, so its writes must be discarded rather than
	// uploaded to a recycled path.
	discarded bool
}

// readHandle is the per-open state of a read handle. The open file is
// retained so the descriptor keeps serving the inode it was opened on
// even after the path is renamed over or unlinked.
type readHandle struct {
	rel  string
	file *os.File
	ino  uint64 // inode at open: stable across rename/unlink for GetAttr
	// tracked: whether this handle registered with the store's
	// open-handle guard (snapshot-backed files only; overlay files are
	// not evictable).
	tracked bool
	fs      *PdsFs
}

func (f *PdsFs) newWriteHandle(rel string, file *os.File, dirty, overlay bool) *writeHandle {
	ino := f.inoForAttr(rel, false)
	f.handleMu.Lock()
	defer f.handleMu.Unlock()
	f.nextFh++
	h := &writeHandle{fs: f, rel: rel, file: file, ino: ino, dirty: dirty, overlay: overlay}
	f.handles[f.nextFh] = h
	return h
}

func (h *writeHandle) relSnapshot() string {
	h.fs.handleMu.Lock()
	defer h.fs.handleMu.Unlock()
	return h.rel
}

// rekeyHandles: re-key open handles after a rename/move — an fd stays
// valid across rename(2) and its writes belong to the new path on
// release.
func (f *PdsFs) rekeyHandles(oldRel, newRel string, isDir bool) {
	oldPrefix := oldRel + "/"
	rekey := func(rel string) (string, bool) {
		if rel == oldRel {
			return newRel, true
		}
		if isDir && len(rel) > len(oldPrefix) && rel[:len(oldPrefix)] == oldPrefix {
			return newRel + "/" + rel[len(oldPrefix):], true
		}
		return "", false
	}
	f.handleMu.Lock()
	for _, h := range f.handles {
		if nr, ok := rekey(h.rel); ok {
			h.rel = nr
		}
	}
	f.handleMu.Unlock()
}

// discardWriteHandles: a path was unlinked/replaced while handles were
// open — the kernel keeps the old inode readable through those fds, but
// writes to it must never be uploaded to the path's new occupant.
func (f *PdsFs) discardWriteHandles(rel string) {
	f.handleMu.Lock()
	defer f.handleMu.Unlock()
	for _, h := range f.handles {
		if h.rel == rel {
			h.discarded = true
		}
	}
}

// lookupWriteHandle finds the live write handle for rel (if any), for
// setattr-with-fh routing.
func (f *PdsFs) lookupWriteHandle(rel string) *writeHandle {
	f.handleMu.Lock()
	defer f.handleMu.Unlock()
	for _, h := range f.handles {
		if h.rel == rel {
			return h
		}
	}
	return nil
}

// removeWriteHandle removes exactly this handle from the table.
func (f *PdsFs) removeWriteHandle(h *writeHandle) {
	f.handleMu.Lock()
	defer f.handleMu.Unlock()
	for fh, cur := range f.handles {
		if cur == h {
			delete(f.handles, fh)
		}
	}
}

// writeFile adapts a writeHandle to nodefs.File: writes go straight to
// the backing file (write-through), the upload is queued on Flush (final
// close) / Fsync / truncate-with-fh. Flush fires before every close —
// uploading on every flush would make `echo x > f` upload twice, so only
// Release (the last fd) finalizes.
type writeFile struct {
	defaultFile
	h  *writeHandle
	fs *PdsFs
}

func newWriteFile(h *writeHandle) *writeFile {
	return &writeFile{h: h, fs: h.fs}
}

func (w *writeFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := w.h.file.ReadAt(dest, off)
	if n == 0 && err != nil {
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (w *writeFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	n, err := w.h.file.WriteAt(data, off)
	if err != nil && n == 0 {
		return 0, fuse.ToStatus(err)
	}
	rel := w.h.relSnapshot()
	w.fs.handleMu.Lock()
	w.h.dirty = true
	w.fs.handleMu.Unlock()
	// Keep the overlay size current for getattr while open (overlay
	// files only; revision files keep snapshot size until the engine's
	// post-upload snapshot).
	if info, err := w.h.file.Stat(); err == nil {
		size := uint64(info.Size())
		w.fs.mu.Lock()
		if e, ok := w.fs.overlay.byRel[rel]; ok && e.size < size {
			e.size = size
			w.fs.overlay.byRel[rel] = e
		}
		w.fs.mu.Unlock()
	}
	return uint32(n), fuse.OK
}

func (w *writeFile) Flush() fuse.Status {
	w.h.file.Sync()
	return fuse.OK
}

// Release is the final close: sync, upload if dirty and not discarded,
// and drop open-handle protection for revision handles.
func (w *writeFile) Release() {
	h := w.h
	// Capture handle state under handleMu: rekey/discard may race with
	// the final close, so read rel/dirty/discarded/overlay together.
	w.fs.handleMu.Lock()
	rel, dirty, discarded, overlay := h.rel, h.dirty, h.discarded, h.overlay
	w.fs.handleMu.Unlock()
	w.fs.removeWriteHandle(h)
	if dirty && !discarded {
		var size uint64
		if info, err := h.file.Stat(); err == nil {
			size = uint64(info.Size())
		}
		h.file.Sync()
		w.fs.finalizeWrite(rel, size)
	}
	if !overlay {
		// Revision handles are open-tracked; drop the protection.
		w.fs.st.RemoveOpenHandle(rel)
	}
	h.file.Close()
}

func (w *writeFile) Fsync(flags int) fuse.Status {
	w.fs.handleMu.Lock()
	dirty := w.h.dirty && !w.h.discarded
	rel := w.h.rel
	w.fs.handleMu.Unlock()
	w.h.file.Sync()
	var size uint64
	if info, err := w.h.file.Stat(); err == nil {
		size = uint64(info.Size())
	}
	if dirty {
		w.fs.handleMu.Lock()
		w.h.dirty = false
		w.fs.handleMu.Unlock()
		w.fs.finalizeWrite(rel, size)
	}
	return fuse.OK
}

func (w *writeFile) Truncate(size uint64) fuse.Status {
	return w.fs.truncateCommon(w.h.relSnapshot(), size, w.h)
}

func (w *writeFile) GetAttr(out *fuse.Attr) fuse.Status {
	rel := w.h.relSnapshot()
	isDir, _, mtime, _, ok := w.fs.nodeMeta(rel)
	if !ok {
		isDir, mtime = false, w.fs.nowSecs()
	}
	info, err := w.h.file.Stat()
	size := uint64(0)
	if err == nil {
		size = uint64(info.Size())
	}
	// ino is pinned at open: a re-derived ino would follow a recycled
	// path after rename-over/unlink, while POSIX keeps the fd's inode.
	ino := w.h.ino
	if ino == 0 {
		ino = w.fs.inoForAttr(rel, isDir)
	}
	*out = *w.fs.attrFor(ino, isDir, size, mtime)
	return fuse.OK
}

func (w *writeFile) Chmod(perms uint32) fuse.Status {
	if err := w.h.file.Chmod(os.FileMode(perms & 0o777)); err != nil {
		return fuse.ToStatus(err)
	}
	rel := w.h.relSnapshot()
	w.fs.mu.Lock()
	if e, ok := w.fs.overlay.byRel[rel]; ok {
		e.mode = perms & 0o777
		w.fs.overlay.byRel[rel] = e
	}
	w.fs.mu.Unlock()
	return fuse.OK
}

func (w *writeFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.OK
}

func (w *writeFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	if mtime != nil {
		rel := w.h.relSnapshot()
		w.fs.mu.Lock()
		if e, ok := w.fs.overlay.byRel[rel]; ok {
			e.mtime = mtime.Unix()
			w.fs.overlay.byRel[rel] = e
		}
		w.fs.mu.Unlock()
	}
	return fuse.OK
}

// readFile adapts a readHandle to nodefs.File.
type readFile struct {
	defaultFile
	h *readHandle
}

func newReadFile(h *readHandle) *readFile {
	return &readFile{h: h}
}

func (r *readFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	// LRU touch on every read pass (cheap; Open covers the cold case).
	if _, isOverlay := r.h.fs.overlayEntry(r.h.rel); !isOverlay {
		r.h.fs.st.RecordAccess(r.h.rel)
	}
	n, err := r.h.file.ReadAt(dest, off)
	if n == 0 && err != nil && err.Error() != "EOF" {
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (r *readFile) Release() {
	if r.h.tracked {
		r.h.fs.st.RemoveOpenHandle(r.h.rel)
	}
	r.h.file.Close()
}

func (r *readFile) GetAttr(out *fuse.Attr) fuse.Status {
	isDir, _, mtime, _, ok := r.h.fs.nodeMeta(r.h.rel)
	if !ok {
		isDir, mtime = false, r.h.fs.nowSecs()
	}
	info, err := r.h.file.Stat()
	size := uint64(0)
	if err == nil {
		size = uint64(info.Size())
	}
	ino := r.h.ino
	if ino == 0 {
		ino = r.h.fs.inoForAttr(r.h.rel, isDir)
	}
	*out = *r.h.fs.attrFor(ino, isDir, size, mtime)
	return fuse.OK
}

var _ nodefs.File = (*writeFile)(nil)
var _ nodefs.File = (*readFile)(nil)
var _ syscall.Errno = 0

// defaultFile supplies the nodefs.File methods writeFile/readFile do not
// override (locks, Allocate: ENOSYS / OK, mirroring nodefs defaults).
type defaultFile struct{}

func (defaultFile) SetInode(*nodefs.Inode) {}
func (defaultFile) InnerFile() nodefs.File { return nil }
func (d defaultFile) String() string       { return "pdsFile" }
func (defaultFile) GetLk(uint64, *fuse.FileLock, uint32, *fuse.FileLock) fuse.Status {
	return fuse.ENOSYS
}
func (defaultFile) SetLk(uint64, *fuse.FileLock, uint32) fuse.Status { return fuse.ENOSYS }
func (defaultFile) SetLkw(uint64, *fuse.FileLock, uint32) fuse.Status {
	return fuse.ENOSYS
}
func (defaultFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	return fuse.ENOSYS
}

var _ nodefs.File = defaultFile{}

func (defaultFile) Chmod(perms uint32) fuse.Status           { return fuse.ENOSYS }
func (defaultFile) Chown(uid uint32, gid uint32) fuse.Status { return fuse.ENOSYS }
func (defaultFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ENOSYS
}
func (defaultFile) Truncate(size uint64) fuse.Status   { return fuse.ENOSYS }
func (defaultFile) GetAttr(out *fuse.Attr) fuse.Status { return fuse.ENOSYS }
func (defaultFile) Flush() fuse.Status                 { return fuse.OK }
func (defaultFile) Release()                           {}
func (defaultFile) Fsync(flags int) fuse.Status        { return fuse.OK }

func (defaultFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return nil, fuse.ENOSYS
}
func (defaultFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}
