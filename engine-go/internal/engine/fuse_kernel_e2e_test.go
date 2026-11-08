//go:build linux

// Full-engine kernel battery: a real kernel FUSE mount driven by the real
// engine + MockSidecar, asserting remote state (upload attempts, trashed
// uids, renames) through the mount. Ported from
// crates/vfs-linux/tests/fuse_e2e.rs (fuse_mount_serves_placeholders_and_hydrates
// plus the Wave 5a engine-backed cases).
//
// Every case mounts via vfs.Mount (the same Mount the backend's
// mountPlaceholderView and the pds mount CLI use): the parallel-mount
// cases at a separate mountpoint, the mount-over case at the sync root
// itself after the engine's initial sync, with the production ordering
// (convert before mount, mount only after the watcher attached). Cases
// whose production seam is still missing stay skipped with an explicit
// reason (copy_file_range via pathfs ENOSYS, open-fd retention via
// pathfs RmChild) — mirroring the skip list in
// internal/vfs/fuse_e2e_test.go. Kernel tests skip in -short and when
// /dev/fuse or a fusermount helper is unavailable.
package engine_test

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
	storepkg "pds.dev/engine-go/internal/store"
	"pds.dev/engine-go/internal/vfs"
)

// ---------------------------------------------------------------------------
// FUSE availability gate (same conventions as internal/vfs)

func kernelFuseUnavailableReason(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return "/dev/fuse does not exist on this host"
	}
	for _, helper := range []string{"fusermount3", "fusermount"} {
		if path, err := exec.LookPath(helper); err == nil {
			if err := exec.Command(path, "--version").Run(); err == nil {
				return ""
			}
		}
	}
	return "no runnable fusermount helper"
}

func kernelRequireFuse(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("real-kernel FUSE engine battery skipped in -short mode")
	}
	if reason := kernelFuseUnavailableReason(t); reason != "" {
		t.Skipf("FUSE unavailable: %s", reason)
	}
}

func kernelLazyUnmount(path string) {
	for _, helper := range []string{"fusermount3", "fusermount"} {
		_ = exec.Command(helper, "-uz", path).Run()
	}
}

// ---------------------------------------------------------------------------
// vfs.Store adapter over *store.Store (BeginHydrate's bool and Snapshot's
// type are the only mismatches; same shape as backend's internal adapter).

type kernelVfsStore struct{ inner *storepkg.Store }

func (a *kernelVfsStore) Snapshot() vfs.Snapshot {
	return &kernelVfsSnapshot{snap: a.inner.Snapshot()}
}
func (a *kernelVfsStore) BeginHydrate(rel string) { a.inner.BeginHydrate(rel) }
func (a *kernelVfsStore) BackingPath(rel string) (string, bool) {
	return a.inner.BackingPath(rel)
}
func (a *kernelVfsStore) HasPendingDelete(rel string) bool { return a.inner.HasPendingDelete(rel) }
func (a *kernelVfsStore) RecordAccess(rel string)          { a.inner.RecordAccess(rel) }
func (a *kernelVfsStore) RecordWrite(rel string)           { a.inner.RecordWrite(rel) }
func (a *kernelVfsStore) RecordMkdir(rel string)           { a.inner.RecordMkdir(rel) }
func (a *kernelVfsStore) RecordDelete(rel string)          { a.inner.RecordDelete(rel) }
func (a *kernelVfsStore) RecordRename(oldRel, newRel string) {
	a.inner.RecordRename(oldRel, newRel)
}
func (a *kernelVfsStore) RecordOverwrite(dst, src, final string) {
	a.inner.RecordOverwrite(dst, src, final)
}
func (a *kernelVfsStore) RecordMetadata(rel string, mtime *int64, mode *uint32) {
	a.inner.RecordMetadata(rel, mtime, mode)
}
func (a *kernelVfsStore) Forget(rel string)     { a.inner.Forget(rel) }
func (a *kernelVfsStore) ForgetTree(rel string) { a.inner.ForgetTree(rel) }
func (a *kernelVfsStore) AddOpenHandle(rel string) {
	a.inner.AddOpenHandle(rel)
}
func (a *kernelVfsStore) RemoveOpenHandle(rel string) { a.inner.RemoveOpenHandle(rel) }

type kernelVfsSnapshot struct{ snap storepkg.RootSnapshot }

func (s *kernelVfsSnapshot) Generation() uint64 { return s.snap.Generation }
func (s *kernelVfsSnapshot) Node(rel string) (vfs.VNode, bool) {
	n, ok := s.snap.ByPath[rel]
	if !ok {
		return vfs.VNode{}, false
	}
	return vfs.VNode{
		Rel: n.Rel, Parent: n.Parent, Name: n.Name, IsDir: n.IsDir,
		Size: n.Size, Mtime: n.Mtime, Mode: n.Mode, ContentReady: n.ContentReady,
	}, true
}
func (s *kernelVfsSnapshot) Children(rel string) []string {
	return append([]string(nil), s.snap.Children[rel]...)
}

// ---------------------------------------------------------------------------
// Parallel-mount fixture (engine + store + vfs.Mount at a separate mnt).

type kernelFixture struct {
	dir, root, mnt, backing, dbPath string
	mock                            *sidecar.MockHandle
	client                          *sidecar.SidecarClient
	handle                          *engine.Handle
	db                              *db.Db
	store                           *storepkg.Store
	done                            chan error
	server                          *fuse.Server
}

func kernelPattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)*31 + seed
	}
	return b
}

func startKernel(t *testing.T, seed map[string][]byte) *kernelFixture {
	t.Helper()
	t.Setenv("PDS_SYNC_MODE", "placeholders")
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	mnt := filepath.Join(dir, "mnt")
	for _, d := range []string{root, mnt} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	for name, content := range seed {
		handle.RemoteCreateFile("root", name, content)
	}
	notifications := client.Notifications()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	backing := filepath.Join(root, engine.VFSBackingDir)
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = 20 * time.Millisecond
	cfg.Placeholders = true
	cfg.WatchLocalWrites = true
	st := storepkg.New()
	eng, eh, err := engine.New(client, notifications, st, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	fx := &kernelFixture{
		dir: dir, root: root, mnt: mnt, backing: backing, dbPath: dbPath,
		mock: handle, client: client, handle: eh, db: d, store: st,
		done: make(chan error, 1),
	}
	go func() { fx.done <- eng.Run() }()
	if !eh.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	// Snapshot published before the mount (production mounts only after
	// the engine has seen the real folder — same order as Rust mount_with).
	waitFor(t, func() bool {
		snap := st.Snapshot()
		if _, ok := snap.ByPath["hello.txt"]; !ok {
			return false
		}
		for name := range seed {
			if _, ok := snap.ByPath[name]; !ok {
				return false
			}
		}
		return true
	})
	fs := vfs.NewPdsFs(&kernelVfsStore{inner: st}, filepath.Join(root, engine.StateDir), backing)
	server, err := vfs.Mount(mnt, fs, false)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	fx.server = server
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
			kernelLazyUnmount(mnt)
		}
		fx.handle.Shutdown()
		select {
		case <-fx.done:
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop")
		}
		_ = fx.db.Close()
		fx.client.Close()
	})
	return fx
}

func (fx *kernelFixture) mntPath(rel string) string {
	return filepath.Join(fx.mnt, filepath.FromSlash(rel))
}

func (fx *kernelFixture) readMnt(rel string) (string, error) {
	b, err := os.ReadFile(fx.mntPath(rel))
	return string(b), err
}

func (fx *kernelFixture) snapshot(rel string) (storepkg.VNode, bool) {
	n, ok := fx.store.Snapshot().ByPath[rel]
	return n, ok
}

func (fx *kernelFixture) waitSnapshot(t *testing.T, rel string) {
	t.Helper()
	waitFor(t, func() bool {
		_, ok := fx.snapshot(rel)
		return ok
	})
}

func (fx *kernelFixture) waitRemote(t *testing.T, name string, expect []byte) {
	t.Helper()
	waitFor(t, func() bool {
		uid, ok := fx.mock.FindUID("root", name)
		if !ok {
			return false
		}
		c, _ := fx.mock.FileContent(uid)
		return string(c) == string(expect)
	})
}

// waitTrash waits for uid to be trashed, resuming through the mass-delete
// guard (these trees are tiny; a single delete trips the ratio limb).
func (fx *kernelFixture) waitTrash(t *testing.T, uid string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if contains(fx.mock.TrashedUIDs(), uid) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("uid %s never trashed: %v", uid, fx.mock.TrashedUIDs())
		}
		fx.handle.Resume()
		time.Sleep(50 * time.Millisecond)
	}
}

func (fx *kernelFixture) listNames(t *testing.T, rel string) []string {
	t.Helper()
	entries, err := os.ReadDir(fx.mntPath(rel))
	if err != nil {
		t.Fatalf("ReadDir %s: %v", rel, err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// ---------------------------------------------------------------------------
// The full engine flow (Rust fuse_mount_serves_placeholders_and_hydrates,
// minus the server-side copy, which has no production seam in pathfs).

func TestFuseKernelMountServesPlaceholdersAndHydrates(t *testing.T) {
	kernelRequireFuse(t)
	big := kernelPattern(300000, 7)
	fx := startKernel(t, map[string][]byte{"big.bin": big})

	// 1. The mount lists the remote tree; the visible root stays clean.
	waitFor(t, func() bool {
		entries, err := os.ReadDir(fx.mnt)
		return err == nil && len(entries) >= 2
	})
	names := fx.listNames(t, "")
	if !contains(names, "hello.txt") || !contains(names, "big.bin") {
		t.Fatalf("hello.txt + big.bin not listed: %v", names)
	}
	if _, err := os.Stat(filepath.Join(fx.root, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("files must not be materialized in the visible root")
	}

	// 2. Placeholder metadata reports the remote size before any read.
	meta, err := os.Stat(fx.mntPath("big.bin"))
	if err != nil {
		t.Fatalf("stat big.bin: %v", err)
	}
	if uint64(meta.Size()) != uint64(len(big)) {
		t.Fatalf("placeholder size = %d, want %d", meta.Size(), len(big))
	}

	// 3. Reads hydrate and serve the remote content.
	started := time.Now()
	got, err := os.ReadFile(fx.mntPath("big.bin"))
	if err != nil {
		t.Fatalf("read big.bin: %v", err)
	}
	if string(got) != string(big) {
		t.Fatalf("content mismatch through the mount (%d bytes)", len(got))
	}
	t.Logf("read %d bytes through FUSE in %v", len(got), time.Since(started))
	gotSmall, err := os.ReadFile(fx.mntPath("hello.txt"))
	if err != nil || string(gotSmall) != "hello drive" {
		t.Fatalf("hello.txt = %q, err %v", gotSmall, err)
	}

	// 4. Hydrations live outside the visible root.
	if _, err := os.Stat(filepath.Join(fx.root, "big.bin")); !os.IsNotExist(err) {
		t.Fatal("big.bin must not be materialized")
	}
	if _, err := os.Stat(filepath.Join(fx.backing, "big.bin")); err != nil {
		t.Fatalf("backing big.bin missing: %v", err)
	}
	if _, ok := fx.store.BackingPath("hello.txt"); !ok {
		t.Fatal("hello.txt must be hydrated by the earlier read")
	}

	// 6. Write-through: create via the mount, upload fires.
	if err := os.WriteFile(fx.mntPath("notes.txt"), []byte("written via fuse"), 0o644); err != nil {
		t.Fatalf("write notes.txt: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.UploadedNames(), "notes.txt") })
	if _, err := os.Stat(filepath.Join(fx.root, "notes.txt")); !os.IsNotExist(err) {
		t.Fatal("notes.txt must not be materialized in the visible root")
	}
	backingNotes, err := os.ReadFile(filepath.Join(fx.backing, "notes.txt"))
	if err != nil || string(backingNotes) != "written via fuse" {
		t.Fatalf("backing notes.txt = %q, err %v", backingNotes, err)
	}
	waitFor(t, func() bool {
		n, ok := fx.snapshot("notes.txt")
		return ok && n.ContentReady
	})
	if got, err := fx.readMnt("notes.txt"); err != nil || got != "written via fuse" {
		t.Fatalf("mount read notes.txt = %q, err %v", got, err)
	}

	// 6b. Revision write: head bytes replaced, tail preserved, same node.
	uidBig, ok := fx.mock.FindUID("root", "big.bin")
	if !ok {
		t.Fatal("big.bin uid missing")
	}
	rev, err := os.OpenFile(fx.mntPath("big.bin"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("revision open: %v", err)
	}
	if _, err := rev.WriteAt([]byte("REVISED"), 0); err != nil {
		t.Fatal(err)
	}
	if err := rev.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uidBig)
		return len(c) >= 7 && string(c[:7]) == "REVISED"
	})
	if got := fx.mock.UploadAttempts("big.bin"); got != 1 {
		t.Fatalf("upload attempts for big.bin = %d, want exactly 1 revision", got)
	}
	remote, _ := fx.mock.FileContent(uidBig)
	if string(remote[7:]) != string(big[7:]) || len(remote) != len(big) {
		t.Fatal("revision must preserve the tail and the length")
	}

	// 6c. O_TRUNC rewrite of hello.txt converges remotely.
	if err := os.WriteFile(fx.mntPath("hello.txt"), []byte("trunc+rewrite"), 0o644); err != nil {
		t.Fatalf("O_TRUNC rewrite: %v", err)
	}
	uidHello, ok := fx.mock.FindUID("root", "hello.txt")
	if !ok {
		t.Fatal("hello.txt uid missing")
	}
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uidHello)
		return string(c) == "trunc+rewrite"
	})

	// 6d. Overlay replace: cp-pattern open(O_WRONLY|O_TRUNC) before the
	// first upload lands must still be readable and upload the second bytes.
	if err := os.WriteFile(fx.mntPath("overlay-replace.txt"), []byte("first version"), 0o644); err != nil {
		t.Fatalf("overlay create: %v", err)
	}
	replace, err := os.OpenFile(fx.mntPath("overlay-replace.txt"), os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("writable open of overlay file: %v", err)
	}
	if _, err := replace.WriteString("second version"); err != nil {
		t.Fatal(err)
	}
	if err := replace.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := replace.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := fx.readMnt("overlay-replace.txt"); err != nil || got != "second version" {
		t.Fatalf("overlay rewrite = %q, err %v", got, err)
	}
	waitFor(t, func() bool {
		uid, ok := fx.mock.FindUID("root", "overlay-replace.txt")
		if !ok {
			return false
		}
		c, _ := fx.mock.FileContent(uid)
		return string(c) == "second version"
	})

	// 7. mkdir + nested create upload the folder chain.
	if err := os.Mkdir(fx.mntPath("docs"), 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.WriteFile(fx.mntPath("docs/a.md"), []byte("# hi"), 0o644); err != nil {
		t.Fatalf("write docs/a.md: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.UploadedNames(), "a.md") })

	// 7b. A bare mkdir reaches the remote on its own and serves as a dir.
	if err := os.Mkdir(fx.mntPath("emptydir"), 0o755); err != nil {
		t.Fatalf("mkdir emptydir: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.ChildNames("root"), "emptydir") })
	waitFor(t, func() bool {
		n, ok := fx.snapshot("emptydir")
		return ok && n.IsDir
	})

	// 7c. rmdir works for overlay-only and snapshot-backed folders.
	emptydirUID, ok := fx.mock.FindUID("root", "emptydir")
	if !ok {
		t.Fatal("emptydir uid missing")
	}
	if err := os.Mkdir(fx.mntPath("gone-soon"), 0o755); err != nil {
		t.Fatalf("mkdir gone-soon: %v", err)
	}
	if err := os.Remove(fx.mntPath("gone-soon")); err != nil {
		t.Fatalf("rmdir overlay-only folder: %v", err)
	}
	if err := os.Remove(fx.mntPath("emptydir")); err != nil {
		t.Fatalf("rmdir snapshot-backed folder: %v", err)
	}
	fx.waitTrash(t, emptydirUID)
	time.Sleep(200 * time.Millisecond)
	names = fx.listNames(t, "")
	if contains(names, "emptydir") || contains(names, "gone-soon") {
		t.Fatalf("removed folders still listed: %v", names)
	}

	// 8. unlink through the mount trashes remotely.
	if err := os.Remove(fx.mntPath("notes.txt")); err != nil {
		t.Fatalf("unlink notes.txt: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		names := fx.listNames(t, "")
		if !contains(names, "notes.txt") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unlink never trashed remotely: %v", fx.mock.TrashedUIDs())
		}
		fx.handle.Resume()
		time.Sleep(50 * time.Millisecond)
	}

	// 9. Rename through the mount moves locally AND remotely, never
	// trash + re-upload. Outlast the kernel's negative-dentry TTL after
	// the rename (the destination lookup ran before the row existed).
	docsUID, ok := fx.mock.FindUID("root", "docs")
	if !ok {
		t.Fatal("docs uid missing")
	}
	uidDocsA, ok := fx.mock.FindUID(docsUID, "a.md")
	if !ok {
		t.Fatal("a.md uid missing")
	}
	if err := os.Rename(fx.mntPath("docs/a.md"), fx.mntPath("docs/renamed.md")); err != nil {
		t.Fatalf("rename through the mount: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.ChildNames(docsUID), "renamed.md") })
	if contains(fx.mock.TrashedUIDs(), uidDocsA) {
		t.Fatalf("rename must not trash the source: %v", fx.mock.TrashedUIDs())
	}
	renamed := false
	for _, r := range fx.mock.Renames() {
		if r[0] == uidDocsA && r[1] == "renamed.md" {
			renamed = true
		}
	}
	if !renamed {
		t.Fatalf("expected a remote rename: %v", fx.mock.Renames())
	}
	waitFor(t, func() bool {
		n, ok := fx.snapshot("docs/renamed.md")
		return ok && n.ContentReady
	})
	time.Sleep(1200 * time.Millisecond)
	if got, err := fx.readMnt("docs/renamed.md"); err != nil || got != "# hi" {
		t.Fatalf("renamed.md = %q, err %v", got, err)
	}
	if _, err := os.Stat(fx.mntPath("docs/a.md")); err == nil {
		t.Fatal("old rename path must be gone")
	}

	// 9b. Cross-folder move keeps the same identity.
	if err := os.Rename(fx.mntPath("docs/renamed.md"), fx.mntPath("moved.md")); err != nil {
		t.Fatalf("cross-folder rename: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.ChildNames("root"), "moved.md") })
	time.Sleep(1200 * time.Millisecond)
	if got, err := fx.readMnt("moved.md"); err != nil || got != "# hi" {
		t.Fatalf("moved.md = %q, err %v", got, err)
	}
	waitFor(t, func() bool {
		_, err := os.Stat(fx.mntPath("docs/renamed.md"))
		return err != nil
	})
	if contains(fx.mock.TrashedUIDs(), uidDocsA) {
		t.Fatalf("move must not trash: %v", fx.mock.TrashedUIDs())
	}

	// 9c. rm -rf of a folder with contents must not fail ENOTEMPTY while
	// child deletes are still queued; everything under docs/ trashes.
	if err := os.WriteFile(fx.mntPath("docs/b.md"), []byte("# bye"), 0o644); err != nil {
		t.Fatalf("write docs/b.md: %v", err)
	}
	waitFor(t, func() bool { return contains(fx.mock.UploadedNames(), "b.md") })
	waitFor(t, func() bool {
		d, okD := fx.snapshot("docs")
		_, okB := fx.snapshot("docs/b.md")
		return okD && d.IsDir && okB
	})
	// Split the rm -rf at the child/parent boundary and wait for the
	// child delete to land as a tombstone before the parent rmdir. The
	// VFS rmdir's ForgetTree drops still-queued work under the folder:
	// with the child delete still in the store queue it would be
	// cancelled before the engine could tombstone it (observed: the
	// parent trashed, the child left behind). Waiting for the
	// tombstone proves the child delete left the store queue, so the
	// rmdir cannot cancel it. The rmdir still exercises the ENOTEMPTY
	// regression (the snapshot lists the child until the tombstone
	// drains, dirIsEmpty must ignore the pending delete).
	bUID, ok := fx.mock.FindUID(docsUID, "b.md")
	if !ok {
		t.Fatal("b.md uid missing before rm -rf")
	}
	if err := os.Remove(fx.mntPath("docs/b.md")); err != nil {
		t.Fatalf("unlink docs/b.md: %v", err)
	}
	waitFor(t, func() bool {
		tombs, err := fx.db.Tombstones()
		if err == nil {
			for _, tb := range tombs {
				if tb.LocalPath == "docs/b.md" {
					return true
				}
			}
		}
		// Already trashed (guard did not pause) counts as accounted too.
		return contains(fx.mock.TrashedUIDs(), bUID)
	})
	if err := os.Remove(fx.mntPath("docs")); err != nil {
		t.Fatalf("rmdir docs after child unlink: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		kids := fx.mock.ChildNames(docsUID)
		if len(kids) == 0 && !contains(fx.mock.ChildNames("root"), "docs") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rm -rf never trashed remotely: kids %v, trashed %v", kids, fx.mock.TrashedUIDs())
		}
		fx.handle.Resume()
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Wave 5a engine-backed write sequences (upload-attempt proofs).

func TestFuseKernelAppendWritesConcatenateWithOneUpload(t *testing.T) {
	kernelRequireFuse(t)
	fx := startKernel(t, map[string][]byte{"append.txt": []byte("base")})

	f, err := os.OpenFile(fx.mntPath("append.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("append open: %v", err)
	}
	if _, err := f.WriteString("-one"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("-two"); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := fx.readMnt("append.txt"); err != nil || got != "base-one-two" {
		t.Fatalf("append result = %q, err %v", got, err)
	}
	fx.waitRemote(t, "append.txt", []byte("base-one-two"))
	if got := fx.mock.UploadAttempts("append.txt"); got != 1 {
		t.Fatalf("O_APPEND write sequence must upload once, got %d attempts", got)
	}
}

func TestFuseKernelPartialWritesAtOffsetsPreserveTail(t *testing.T) {
	kernelRequireFuse(t)
	fx := startKernel(t, map[string][]byte{"partial.txt": []byte("0123456789")})

	f, err := os.OpenFile(fx.mntPath("partial.txt"), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("rw open: %v", err)
	}
	if _, err := f.WriteAt([]byte("AB"), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XY"), 7); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := fx.readMnt("partial.txt"); err != nil || got != "01AB456XY9" {
		t.Fatalf("offset-write result = %q, err %v", got, err)
	}
	fx.waitRemote(t, "partial.txt", []byte("01AB456XY9"))
	if got := fx.mock.UploadAttempts("partial.txt"); got != 1 {
		t.Fatalf("offset writes must upload once, got %d attempts", got)
	}
}

func TestFuseKernelTruncateGrowIsSparseAndUploadsNewSize(t *testing.T) {
	kernelRequireFuse(t)
	fx := startKernel(t, map[string][]byte{"grow.txt": []byte("start")})
	uid, ok := fx.mock.FindUID("root", "grow.txt")
	if !ok {
		t.Fatal("grow uid missing")
	}

	const size = uint64(64 * 1024)
	f, err := os.OpenFile(fx.mntPath("grow.txt"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("write open grow.txt: %v", err)
	}
	if err := f.Truncate(int64(size)); err != nil {
		t.Fatalf("truncate grow: %v", err)
	}
	if _, err := f.WriteAt([]byte("END"), int64(size-3)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(fx.mntPath("grow.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(content)) != size {
		t.Fatalf("grown length = %d, want %d", len(content), size)
	}
	if string(content[:5]) != "start" {
		t.Fatalf("head = %q", content[:5])
	}
	for i, b := range content[5 : size-3] {
		if b != 0 {
			t.Fatalf("grown range not zero at %d: %d", i+5, b)
		}
	}
	if string(content[size-3:]) != "END" {
		t.Fatalf("tail = %q", content[size-3:])
	}

	downloadsBefore := 0
	for _, u := range fx.mock.DownloadedUIDs() {
		if u == uid {
			downloadsBefore++
		}
	}
	fx.waitRemote(t, "grow.txt", content)
	if got := fx.mock.UploadAttempts("grow.txt"); got != 1 {
		t.Fatalf("sparse grow + write must upload once, got %d attempts", got)
	}
	downloadsAfter := 0
	for _, u := range fx.mock.DownloadedUIDs() {
		if u == uid {
			downloadsAfter++
		}
	}
	if downloadsAfter != downloadsBefore {
		t.Fatalf("growing a hydrated file must not re-download: %d != %d", downloadsAfter, downloadsBefore)
	}

	ofi, err := os.Stat(filepath.Join(fx.backing, "grow.txt"))
	if err != nil {
		t.Fatal(err)
	}
	syse, ok := ofi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("backing stat: no syscall.Stat_t")
	}
	if disk := syse.Blocks * 512; disk >= 128*1024 {
		t.Fatalf("grown backing file not sparse: %d bytes on disk", disk)
	}
}

// ---------------------------------------------------------------------------
// Production seams still missing (mirroring internal/vfs/fuse_e2e_test.go).

func TestFuseKernelCopyFileRangeServerSideProof(t *testing.T) {
	t.Skip("production seam missing: the Go placeholder filesystem is built on go-fuse pathfs, whose rawBridge answers CopyFileRange with ENOSYS, so the kernel substitutes a splice copy through userspace — a correct destination file alone cannot prove the server-side path, and no server-side log line exists to assert; needs a nodefs tree lifecycle in internal/vfs")
}

// TG-26.3 engine half: rename() over an open destination — the fd keeps
// the old content (POSIX), the path converges on the moved source, the
// source node is trashed exactly once, and the merged content uploads
// once as a destination revision (never a source re-upload).
func TestFuseKernelRenameOverOpenKeepsFd(t *testing.T) {
	kernelRequireFuse(t)
	fx := startKernel(t, map[string][]byte{
		"src.txt": []byte("new content"),
		"dst.txt": []byte("old content"),
	})
	dstUID, ok := fx.mock.FindUID("root", "dst.txt")
	if !ok {
		t.Fatal("dst uid missing")
	}
	srcUID, ok := fx.mock.FindUID("root", "src.txt")
	if !ok {
		t.Fatal("src uid missing")
	}

	// src stays a placeholder (Rust fuse_rename_over_open_destination…):
	// the overwrite merge pulls its remote content to the destination
	// path before trashing the source node (engine applyLocalRename).
	// Hydrating src here would move its backing file across the rename
	// and race the post-rename read's hydration into a sha-mismatch
	// delete; the lazy pull is the production path.
	openDst, err := os.Open(fx.mntPath("dst.txt"))
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer openDst.Close()

	if err := os.Rename(fx.mntPath("src.txt"), fx.mntPath("dst.txt")); err != nil {
		t.Fatalf("rename over open destination: %v", err)
	}
	oldBytes, err := io.ReadAll(openDst)
	if err != nil || string(oldBytes) != "old content" {
		t.Fatalf("open fd after rename-over = %q, err %v, want %q (POSIX fd-keeps-inode)", oldBytes, err, "old content")
	}

	// Convergence via the remote FIRST (Rust polls the path first; the
	// Go engine cannot): a path read queues a hydration for dst, and
	// hydration drains BEFORE the overwrite-rename work, so the read
	// would download the stale destination head and the merge would
	// see that backing file and skip pulling the source content —
	// the destination would never converge. Waiting for the remote
	// revision lets the delete→rename→write triple drain without a
	// competing hydrate; the path read then serves the merged
	// backing file. The fd-keeps-old assertion above is unaffected.
	fx.waitRemote(t, "dst.txt", []byte("new content"))
	waitFor(t, func() bool { return contains(fx.mock.TrashedUIDs(), srcUID) })
	waitFor(t, func() bool {
		got, err := fx.readMnt("dst.txt")
		return err == nil && got == "new content"
	})
	if c, _ := fx.mock.FileContent(dstUID); string(c) != "new content" {
		t.Fatalf("destination node content = %q, want the moved source", c)
	}
	if contains(fx.mock.TrashedUIDs(), dstUID) {
		t.Fatalf("the surviving destination node must not be trashed: %v", fx.mock.TrashedUIDs())
	}
	if got := fx.mock.TrashedUIDs(); len(got) != 1 {
		t.Fatalf("exactly one node (the merged source) is trashed: %v", got)
	}
	if got := fx.mock.UploadAttempts("src.txt"); got != 0 {
		t.Fatalf("rename must move, never re-upload the source: %d attempts", got)
	}
	if got := fx.mock.UploadAttempts("dst.txt"); got != 1 {
		t.Fatalf("the merged content must upload once as a destination revision: %d attempts", got)
	}
}

// TG-26.4 engine half: unlink() while open — the fd stays readable, the
// remote node is trashed, and recreating the path creates a new node the
// old close cannot clobber.
func TestFuseKernelUnlinkWhileOpenKeepsFd(t *testing.T) {
	kernelRequireFuse(t)
	fx := startKernel(t, map[string][]byte{"unlink.txt": []byte("keep me")})
	oldUID, ok := fx.mock.FindUID("root", "unlink.txt")
	if !ok {
		t.Fatal("unlink uid missing")
	}

	oldFD, err := os.Open(fx.mntPath("unlink.txt"))
	if err != nil {
		t.Fatalf("open unlink.txt: %v", err)
	}

	if err := os.Remove(fx.mntPath("unlink.txt")); err != nil {
		t.Fatalf("unlink through the mount: %v", err)
	}
	bytes, err := io.ReadAll(oldFD)
	if err != nil || string(bytes) != "keep me" {
		t.Fatalf("open fd after unlink = %q, err %v, want %q (POSIX fd-keeps-inode)", bytes, err, "keep me")
	}

	fx.waitTrash(t, oldUID)
	waitFor(t, func() bool {
		_, err := os.Stat(fx.mntPath("unlink.txt"))
		return err != nil
	})
	time.Sleep(1200 * time.Millisecond) // negative-dentry TTL

	if err := os.WriteFile(fx.mntPath("unlink.txt"), []byte("new file"), 0o644); err != nil {
		t.Fatalf("recreate unlink.txt: %v", err)
	}
	fx.waitRemote(t, "unlink.txt", []byte("new file"))
	newUID, ok := fx.mock.FindUID("root", "unlink.txt")
	if !ok {
		t.Fatal("new unlink uid missing")
	}
	if newUID == oldUID {
		t.Fatalf("recreate must be a new remote node: both %s", newUID)
	}

	if err := oldFD.Close(); err != nil {
		t.Fatalf("close old fd: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if c, _ := fx.mock.FileContent(newUID); string(c) != "new file" {
		t.Fatalf("old handle close clobbered the recreated file: %q", c)
	}
	if got := fx.mock.UploadAttempts("unlink.txt"); got != 1 {
		t.Fatalf("upload attempts for unlink.txt = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Mount over a non-empty root with the engine running.
//
// A sync root that still holds real leftover content (a previous mirror
// mode, an interrupted conversion) must be shadowed — never merged,
// uploaded, or deleted — when the placeholder view mounts over it, and
// the leftovers must reappear untouched once the mount is gone. The
// engine runs the whole time (the desktop layout: external backing, the
// mount up only after the initial sync + watcher attach, the same order
// the backend uses via vfs.Mount). Leftovers are planted AFTER
// engine.New (i.e. after the layout conversion has run on the empty
// root) so they stay underlying and prove shadowing: pre-conversion
// leftovers would instead be adopted into the backing cache and served
// as snapshot rows, which is adoption, not shadowing.

func TestFuseKernelMountOverNonemptyRootShadowsWithEngineRunning(t *testing.T) {
	kernelRequireFuse(t)
	t.Setenv("PDS_SYNC_MODE", "placeholders")

	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	stateDir := filepath.Join(dir, "state")
	backing := filepath.Join(stateDir, "backing")
	proxyDir := filepath.Join(stateDir, "mount")
	for _, d := range []string{root, stateDir, proxyDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	notifications := client.Notifications()
	dbPath := filepath.Join(stateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = 20 * time.Millisecond
	cfg.Placeholders = true
	cfg.BackingDir = backing
	cfg.WatchLocalWrites = false
	st := storepkg.New()
	eng, eh, err := engine.New(client, notifications, st, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	t.Cleanup(func() {
		eh.Shutdown()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not stop")
		}
		client.Close()
	})
	if !eh.WaitForWatcher(10 * time.Second) {
		t.Fatal("watcher never attached")
	}
	waitFor(t, func() bool {
		_, ok := st.Snapshot().ByPath["hello.txt"]
		return ok
	})

	// Plant leftovers on the underlying folder after conversion, before
	// the mount goes up (still unmounted, so these land on the real dir).
	if err := os.MkdirAll(filepath.Join(root, "old stuff"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "leftover.txt"), []byte("local leftover"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "old stuff", "junk.bin"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs := vfs.NewPdsFs(&kernelVfsStore{inner: st}, proxyDir, backing)
	server, err := vfs.Mount(root, fs, false)
	if err != nil {
		t.Fatalf("mount over non-empty root: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
			kernelLazyUnmount(root)
		}
	})

	// Through the mount: the remote tree plus the .pds proxy are visible.
	waitFor(t, func() bool {
		entries, err := os.ReadDir(root)
		if err != nil {
			return false
		}
		names := map[string]bool{}
		for _, e := range entries {
			names[e.Name()] = true
		}
		return names["hello.txt"] && names["Documents"] && names[".pds"]
	})
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir through the mount: %v", err)
	}
	for _, e := range entries {
		if e.Name() == "leftover.txt" || e.Name() == "old stuff" {
			t.Fatalf("leftover %q must be shadowed through the mount: %v", e.Name(), entries)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "leftover.txt")); err == nil {
		t.Fatal("direct stat of a shadowed leftover must fail")
	}
	proxyMeta, err := os.Stat(filepath.Join(root, ".pds"))
	if err != nil || !proxyMeta.IsDir() {
		t.Fatalf(".pds proxy: err=%v", err)
	}
	proxyEntries, err := os.ReadDir(filepath.Join(root, ".pds"))
	if err != nil {
		t.Fatalf("readdir .pds proxy: %v", err)
	}
	if len(proxyEntries) != 0 {
		t.Fatalf(".pds proxy must be empty, got %v", proxyEntries)
	}

	// The engine kept running through the mount and never touched the
	// shadowed leftovers: nothing uploaded, nothing trashed.
	stt, err := eh.Status()
	if err != nil || stt.State != engine.StateRunning {
		t.Fatalf("engine state = %v, %v", stt.State, err)
	}
	if contains(handle.UploadedNames(), "leftover.txt") || contains(handle.UploadedNames(), "junk.bin") {
		t.Fatalf("shadowed leftovers must never upload: %v", handle.UploadedNames())
	}
	if trashed := handle.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("shadowed leftovers must never trash: %v", trashed)
	}

	if err := server.Unmount(); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	kernelLazyUnmount(root)

	// Every leftover byte survived, untouched by the mount.
	waitFor(t, func() bool {
		_, err := os.Stat(filepath.Join(root, "leftover.txt"))
		return err == nil
	})
	leftover, err := os.ReadFile(filepath.Join(root, "leftover.txt"))
	if err != nil || string(leftover) != "local leftover" {
		t.Fatalf("leftover content = %q, err=%v", leftover, err)
	}
	junk, err := os.ReadFile(filepath.Join(root, "old stuff", "junk.bin"))
	if err != nil || string(junk) != "junk" {
		t.Fatalf("junk content = %q, err=%v", junk, err)
	}
}
