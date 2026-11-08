//go:build linux

package backend

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	storepkg "pds.dev/engine-go/internal/store"
	"pds.dev/engine-go/internal/vfs"
)

// defaultHooks wires the platform FUSE seams. The probe mounts a throwaway
// placeholder filesystem at a temp dir and proves the session answers a
// stat (Rust fuse_available); the mount serves the placeholder view at the
// sync root via vfs.Mount.
func defaultHooks() Hooks {
	return Hooks{
		Preflight: func(root string, timeout time.Duration) error {
			return vfs.EnsurePlaceholderMountpoint(root, timeout)
		},
		Probe: probeFuseAvailable,
		Mount: mountPlaceholderView,
	}
}

var probeSeq atomic.Uint64

func probeFuseAvailable() bool {
	seq := probeSeq.Add(1)
	probe, err := os.MkdirTemp("", fmt.Sprintf("pds-fuse-probe-%d-%d", os.Getpid(), seq))
	if err != nil {
		return false
	}
	defer os.RemoveAll(probe)
	mnt := filepath.Join(probe, "mnt")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		return false
	}
	st := storepkg.New()
	fs := vfs.NewPdsFs(&vfsStoreAdapter{inner: st}, filepath.Join(probe, "state"), filepath.Join(probe, "backing"))
	server, err := vfs.Mount(mnt, fs, false)
	if err != nil {
		return false
	}
	defer func() { _ = server.Unmount() }()
	// Prove the served mount answers a request (the M3.4 freeze: a bare
	// Session::new passes while every access blocks in the kernel).
	type outcome struct{ ok bool }
	done := make(chan outcome, 1)
	go func() {
		_, err := os.Stat(mnt)
		done <- outcome{ok: err == nil}
	}()
	select {
	case o := <-done:
		return o.ok
	case <-time.After(5 * time.Second):
		return false
	}
}

func mountPlaceholderView(st *storepkg.Store, stateDir, backingDir, root string) (func(), error) {
	if st == nil {
		return nil, fmt.Errorf("engine has no VFS store (not in placeholder mode)")
	}
	proxyDir := filepath.Join(stateDir, "mount")
	if err := os.MkdirAll(proxyDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		return nil, err
	}
	fs := vfs.NewPdsFs(&vfsStoreAdapter{inner: st}, proxyDir, backingDir)
	server, err := vfs.Mount(root, fs, false)
	if err != nil {
		return nil, err
	}
	type outcome struct{ ok bool }
	done := make(chan outcome, 1)
	go func() {
		_, err := os.Stat(root)
		done <- outcome{ok: err == nil}
	}()
	select {
	case o := <-done:
		if !o.ok {
			_ = server.Unmount()
			return nil, fmt.Errorf("the placeholder view mounted at %s but does not answer requests", root)
		}
	case <-time.After(5 * time.Second):
		_ = server.Unmount()
		return nil, fmt.Errorf("the placeholder view mounted at %s but does not answer requests", root)
	}
	unmounted := false
	return func() {
		if unmounted {
			return
		}
		unmounted = true
		_ = server.Unmount()
	}, nil
}

// vfsStoreAdapter adapts *store.Store to the vfs.Store interface (the only
// mismatches are BeginHydrate's bool and Snapshot's return type).
type vfsStoreAdapter struct{ inner *storepkg.Store }

func (a *vfsStoreAdapter) Snapshot() vfs.Snapshot {
	return &vfsSnapshotAdapter{snap: a.inner.Snapshot()}
}
func (a *vfsStoreAdapter) BeginHydrate(rel string) { a.inner.BeginHydrate(rel) }
func (a *vfsStoreAdapter) BackingPath(rel string) (string, bool) {
	return a.inner.BackingPath(rel)
}
func (a *vfsStoreAdapter) HasPendingDelete(rel string) bool { return a.inner.HasPendingDelete(rel) }
func (a *vfsStoreAdapter) RecordAccess(rel string)          { a.inner.RecordAccess(rel) }
func (a *vfsStoreAdapter) RecordWrite(rel string)           { a.inner.RecordWrite(rel) }
func (a *vfsStoreAdapter) RecordMkdir(rel string)           { a.inner.RecordMkdir(rel) }
func (a *vfsStoreAdapter) RecordDelete(rel string)          { a.inner.RecordDelete(rel) }
func (a *vfsStoreAdapter) RecordRename(oldRel, newRel string) {
	a.inner.RecordRename(oldRel, newRel)
}
func (a *vfsStoreAdapter) RecordOverwrite(dst, src, final string) {
	a.inner.RecordOverwrite(dst, src, final)
}
func (a *vfsStoreAdapter) RecordMetadata(rel string, mtime *int64, mode *uint32) {
	a.inner.RecordMetadata(rel, mtime, mode)
}
func (a *vfsStoreAdapter) Forget(rel string)     { a.inner.Forget(rel) }
func (a *vfsStoreAdapter) ForgetTree(rel string) { a.inner.ForgetTree(rel) }
func (a *vfsStoreAdapter) AddOpenHandle(rel string) {
	a.inner.AddOpenHandle(rel)
}
func (a *vfsStoreAdapter) RemoveOpenHandle(rel string) { a.inner.RemoveOpenHandle(rel) }

type vfsSnapshotAdapter struct{ snap storepkg.RootSnapshot }

func (s *vfsSnapshotAdapter) Generation() uint64 { return s.snap.Generation }
func (s *vfsSnapshotAdapter) Node(rel string) (vfs.VNode, bool) {
	n, ok := s.snap.ByPath[rel]
	if !ok {
		return vfs.VNode{}, false
	}
	return vfs.VNode{
		Rel: n.Rel, Parent: n.Parent, Name: n.Name, IsDir: n.IsDir,
		Size: n.Size, Mtime: n.Mtime, Mode: n.Mode, ContentReady: n.ContentReady,
	}, true
}
func (s *vfsSnapshotAdapter) Children(rel string) []string {
	return append([]string(nil), s.snap.Children[rel]...)
}
