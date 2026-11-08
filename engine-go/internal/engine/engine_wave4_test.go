// Wave 4 of the Rust engine.rs battery port: path-limit edges, corrupt-DB
// startup refusal, environment/transport resilience (TG-19/TG-22), session
// expiry mid-sync (TG-20), sidecar outage recovery, replay dedup, and the
// many-small-files convergence run.
package engine_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/sidecar"
)

// ---------------------------------------------------------------------------
// Wave 2c — path-limit edges
// ---------------------------------------------------------------------------

func TestMaxLengthComponentNameRoundTrips(t *testing.T) {
	fx := start(t)
	// 255 bytes is the Linux NAME_MAX: "x"*251 + ".txt".
	longUpload := strings.Repeat("x", 251) + ".txt"
	if len(longUpload) != 255 {
		t.Fatalf("name length = %d", len(longUpload))
	}
	fx.write(longUpload, []byte("long upload"))
	waitForSynced(t, fx.db, longUpload)
	if !contains(fx.uploadedNames(), longUpload) {
		t.Fatal("255-byte name must reach the remote intact")
	}
	if row, _ := fx.db.NodeByPath(longUpload); row == nil || row.Name != longUpload {
		t.Fatalf("row name = %+v", row)
	}

	// And the download direction.
	longDownload := strings.Repeat("y", 251) + ".bin"
	if len(longDownload) != 255 {
		t.Fatalf("name length = %d", len(longDownload))
	}
	fx.mock.RemoteCreateFile("root", longDownload, []byte("long download"))
	waitForSynced(t, fx.db, longDownload)
	if got := string(fx.read(longDownload)); got != "long download" {
		t.Fatalf("downloaded = %q", got)
	}
	if row, _ := fx.db.NodeByPath(longDownload); row == nil || row.Name != longDownload {
		t.Fatalf("row name = %+v", row)
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

func TestDeepNestingBeyond64LevelsKeepsTheFullPath(t *testing.T) {
	fx := start(t)
	// 70 nested remote folders announced only through one leaf event: the
	// engine must fetch the ancestors and derive the FULL path.
	parentUID := "root"
	var parts []string
	for i := 0; i < 70; i++ {
		name := fmt.Sprintf("d%02d", i)
		parentUID = fx.mock.RemoteCreateFolderSilent(parentUID, name)
		parts = append(parts, name)
	}
	deepRel := strings.Join(parts, "/")
	leafUID := fx.mock.RemoteCreateFile(parentUID, "leaf.txt", []byte("deep bytes"))
	leafRel := deepRel + "/leaf.txt"

	waitForSynced(t, fx.db, leafRel)
	if got := string(fx.read(leafRel)); got != "deep bytes" {
		t.Fatalf("leaf = %q", got)
	}
	row, err := fx.db.NodeByPath(leafRel)
	if err != nil || row == nil {
		t.Fatal("leaf row must use the full 70-level path")
	}
	if row.NodeUID != leafUID || row.LocalPath != leafRel {
		t.Fatalf("leaf row = %+v", row)
	}
	// Every ancestor row also carries its full prefix, not a root anchor.
	for i := range parts {
		prefix := strings.Join(parts[:i+1], "/")
		if r, _ := fx.db.NodeByPath(prefix); r == nil {
			t.Fatalf("missing ancestor row %q", prefix)
		}
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
}

// folderRow mirrors Rust folder_row: a Synced folder row, DB bookkeeping only.
func folderRow(localPath, uid string, parentUID *string) *db.NodeRow {
	mtime := int64(1_700_000_000)
	name := localPath
	if i := strings.LastIndex(localPath, "/"); i >= 0 {
		name = localPath[i+1:]
	}
	return &db.NodeRow{
		LocalPath: localPath,
		NodeUID:   uid,
		ParentUID: parentUID,
		Name:      name,
		Kind:      db.KindFolder,
		Mtime:     &mtime,
		SyncState: db.StateSynced,
	}
}

func TestOverlongPathFailsCleanlyAndSyncContinues(t *testing.T) {
	fx := start(t)
	rec := recordActivity(fx.handle)
	// 70 folders x 60-byte names = a >4096-byte materialized path. The rows
	// exist (as a restart would leave them) but nothing is on disk; the
	// engine must reject the leaf BEFORE creating a partial chain or
	// downloading, and surface a visible skip.
	parentUID := "root"
	var parts []string
	for i := 0; i < 70; i++ {
		name := fmt.Sprintf("d%02d-%s", i, strings.Repeat("x", 56))
		if len(name) != 60 {
			t.Fatalf("name length = %d", len(name))
		}
		uid := fmt.Sprintf("deep-%02d", i)
		rel := name
		if len(parts) > 0 {
			rel = strings.Join(parts, "/") + "/" + name
		}
		parent := "root"
		if i > 0 {
			parent = fmt.Sprintf("deep-%02d", i-1)
		}
		if err := fx.db.UpsertNode(folderRow(rel, uid, &parent)); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, name)
		parentUID = uid
	}
	deepRel := strings.Join(parts, "/")
	leafRel := deepRel + "/big.bin"
	if abs := filepath.Join(fx.root, filepath.FromSlash(leafRel)); len(abs) < 4096 {
		t.Fatalf("fixture path must exceed PATH_MAX, got %d", len(abs))
	}

	leafUID := fx.mock.RemoteCreateFile(parentUID, "big.bin", []byte("too long"))
	skipped := waitForActivity(t, rec, engine.ActivitySkipped, leafRel)
	if !strings.Contains(skipped.Detail, "PATH_MAX") && !strings.Contains(skipped.Detail, "too long") {
		t.Fatalf("detail must explain the path limit, got %q", skipped.Detail)
	}

	// No half-written row, no download attempt, no directory chain.
	if row, _ := fx.db.NodeByPath(leafRel); row != nil {
		t.Fatal("leaf row must not exist")
	}
	if row, _ := fx.db.NodeByUID(leafUID); row != nil {
		t.Fatal("leaf row (by uid) must not exist")
	}
	if contains(fx.mock.DownloadedUIDs(), leafUID) {
		t.Fatal("the engine must not start a doomed transfer")
	}
	if _, err := os.Lstat(filepath.Join(fx.root, parts[0])); !os.IsNotExist(err) {
		t.Fatal("no directory chain may be created")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed: %v", trashed)
	}
	if names := fx.uploadedNames(); len(names) != 0 {
		t.Fatalf("uploaded: %v", names)
	}

	// The rest of the sync continues: a later, normal remote file lands.
	fx.mock.RemoteCreateFile("root", "after-skip.txt", []byte("still syncing"))
	waitFor(t, func() bool { return fx.exists("after-skip.txt") })
	if got := string(fx.read("after-skip.txt")); got != "still syncing" {
		t.Fatalf("after-skip.txt = %q", got)
	}
	if contains(fx.mock.DownloadedUIDs(), leafUID) {
		t.Fatal("no retry storm after the skip")
	}
}

// ---------------------------------------------------------------------------
// Wave 3a — environment / transport resilience (TG-19, TG-22)
// ---------------------------------------------------------------------------

// TG-23: startup refuses a corrupt state DB with an actionable message
// instead of panicking or silently starting from an empty index.
func TestEngineStartupRefusesCorruptStateDBWithActionableError(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath, []byte("not a database, just bytes\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	_, _, err := engine.New(client, client.Notifications(), nil, cfg)
	if err == nil {
		t.Fatal("corrupt DB must refuse startup")
	}
	msg := err.Error()
	if !strings.Contains(msg, "state DB") {
		t.Fatalf("error should name the DB: %v", msg)
	}
	if !strings.Contains(msg, "not a database") && !strings.Contains(msg, "malformed") {
		t.Fatalf("error should name the cause: %v", msg)
	}
	client.Close()
}

// TG-19: a single un-writable download target must not kill the run loop or
// block the rest of the tree. The row stays visible as errored and an
// explicit resume after restoring permissions retries it.
func TestInitialSyncDownloadPermissionDeniedIsSurfacedAndRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(filepath.Join(root, "Documents"), 0o755); err != nil {
		t.Fatal(err)
	}

	mock := sidecar.NewMock()
	client, handle := mock.Connect()
	// Seed a remote file under the locked folder (event predates the stream;
	// initial enumeration is what discovers it).
	handle.RemoteCreateFile("node-docs", "locked.txt", []byte("locked v1"))
	lockDir(t, filepath.Join(root, "Documents"))
	fx := startWithClient(t, root, client, handle, 30*time.Second)

	// Other files keep syncing even though one target is denied.
	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q", got)
	}
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("Documents/locked.txt")
		if row == nil || row.SyncState != db.StateDownloading {
			return false
		}
		e, _ := fx.db.UploadError("Documents/locked.txt")
		return strings.Contains(e, "permission denied")
	})
	st, _ := fx.handle.Status()
	if st.State != engine.StateRunning {
		t.Fatalf("engine must survive a bad target: %v", st.State)
	}

	unlockDir(t, filepath.Join(fx.root, "Documents"))
	fx.handle.Resume()
	waitFor(t, func() bool {
		if b, err := os.ReadFile(filepath.Join(fx.root, "Documents/locked.txt")); err != nil || string(b) != "locked v1" {
			return false
		}
		row, _ := fx.db.NodeByPath("Documents/locked.txt")
		if row == nil || row.SyncState != db.StateSynced {
			return false
		}
		e, _ := fx.db.UploadError("Documents/locked.txt")
		return e == ""
	})
	if st, _ := fx.handle.Status(); st.ErroredNodes != 0 {
		t.Fatalf("errored nodes = %d", st.ErroredNodes)
	}
}

// TG-19: mirror mode with a read-only root. The engine must come up, report
// the failed download, and recover once the root is writable.
func TestNonWritableSyncRootDoesNotStopTheEngine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	// Documents must exist up-front (as in the Rust test): otherwise the
	// locked root turns initial enumeration itself into a fatal startup
	// error instead of a per-file download failure.
	if err := os.MkdirAll(filepath.Join(root, "Documents"), 0o755); err != nil {
		t.Fatal(err)
	}

	mock := sidecar.NewMock()
	client, _ := mock.Connect()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	cfg := engine.DefaultConfig()
	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.RetryBase = 30 * time.Second
	eng, handle, err := engine.New(client, client.Notifications(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	lockDir(t, root)
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()

	waitFor(t, func() bool {
		row, _ := d.NodeByPath("hello.txt")
		if row == nil || row.SyncState != db.StateDownloading {
			return false
		}
		e, _ := d.UploadError("hello.txt")
		return e != ""
	})
	select {
	case <-done:
		t.Fatal("engine exited on a read-only root")
	default:
	}
	if st, _ := handle.Status(); st.State != engine.StateRunning {
		t.Fatalf("state = %v", st.State)
	}
	if _, err := os.Stat(filepath.Join(root, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("hello.txt must not exist yet")
	}

	unlockDir(t, root)
	handle.Resume()
	waitFor(t, func() bool {
		b, err := os.ReadFile(filepath.Join(root, "hello.txt"))
		if err != nil || string(b) != "hello drive" {
			return false
		}
		row, _ := d.NodeByPath("hello.txt")
		return row != nil && row.SyncState == db.StateSynced
	})

	handle.Shutdown()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("engine run returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("engine did not stop")
	}
	client.Close()
}

// TG-19: an in-place remote update whose download target is denied must keep
// the existing local bytes, emit an activity entry, respect backoff, and
// recover on resume after permissions are restored.
func TestFailedDownloadKeepsLocalContentAndReportsActivityFull(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	fx := startWithRetryBase(t, 30*time.Second)
	rec := recordActivity(fx.handle)
	uid := fx.mock.RemoteCreateFile("node-docs", "keep.txt", []byte("v1"))
	waitFor(t, func() bool {
		return fx.exists("Documents/keep.txt") && string(fx.read("Documents/keep.txt")) == "v1"
	})
	waitForSynced(t, fx.db, "Documents/keep.txt")

	downloadsBefore := len(fx.mock.DownloadedUIDs())
	lockDir(t, filepath.Join(fx.root, "Documents"))
	fx.mock.RemoteUpdateFile(uid, []byte("v2"))

	item := waitForActivity(t, rec, engine.ActivityDownloadErrored, "Documents/keep.txt")
	if !strings.Contains(item.Detail, "permission denied") {
		t.Fatalf("detail should name the cause: %q", item.Detail)
	}
	// The failed rename never touches the previous file.
	if got := string(fx.read("Documents/keep.txt")); got != "v1" {
		t.Fatalf("failed download must never truncate existing content: %q", got)
	}
	// 30s retry base: no retry-storm while the target stays unwritable. The
	// unwritable target is rejected before the transfer starts.
	if n := len(fx.mock.DownloadedUIDs()); n != downloadsBefore {
		t.Fatalf("downloads = %d, want %d", n, downloadsBefore)
	}
	if row, _ := fx.db.NodeByPath("Documents/keep.txt"); row == nil || row.SyncState != db.StateDownloading {
		t.Fatalf("row = %+v", row)
	}

	unlockDir(t, filepath.Join(fx.root, "Documents"))
	fx.handle.Resume()
	waitFor(t, func() bool {
		if got := string(fx.read("Documents/keep.txt")); got != "v2" {
			return false
		}
		row, _ := fx.db.NodeByPath("Documents/keep.txt")
		return row != nil && row.SyncState == db.StateSynced
	})
	if e, _ := fx.db.UploadError("Documents/keep.txt"); e != "" {
		t.Fatalf("upload error = %q", e)
	}
}

// TG-19: local ENOSPC reported through the sidecar's transfer.error path.
// Retryable=false on purpose: for a local target error the engine still
// backs off and retries once the condition is cleared (resume), instead of
// dropping the file or clobbering the old copy.
func TestDownloadNoSpaceErrorIsSurfacedAndRecovers(t *testing.T) {
	fx := startWithRetryBase(t, 30*time.Second)
	rec := recordActivity(fx.handle)
	uid := fx.mock.RemoteCreateFile("node-docs", "disk.txt", []byte("old bytes"))
	waitFor(t, func() bool {
		return fx.exists("Documents/disk.txt") && string(fx.read("Documents/disk.txt")) == "old bytes"
	})
	waitForSynced(t, fx.db, "Documents/disk.txt")

	downloadsBefore := len(fx.mock.DownloadedUIDs())
	fx.mock.InjectDownloadFailuresFull(uid, [][]any{
		{int64(sidecar.ErrCodeInternal), "No space left on device", false},
	})
	fx.mock.RemoteUpdateFile(uid, []byte("new bytes"))

	item := waitForActivity(t, rec, engine.ActivityDownloadErrored, "Documents/disk.txt")
	if !strings.Contains(item.Detail, "No space left on device") {
		t.Fatalf("detail should carry the sidecar error: %q", item.Detail)
	}
	if got := string(fx.read("Documents/disk.txt")); got != "old bytes" {
		t.Fatalf("old copy clobbered: %q", got)
	}
	if n := len(fx.mock.DownloadedUIDs()); n != downloadsBefore+1 {
		t.Fatalf("downloads = %d, want %d", n, downloadsBefore+1)
	}

	fx.handle.Resume()
	waitFor(t, func() bool {
		if got := string(fx.read("Documents/disk.txt")); got != "new bytes" {
			return false
		}
		row, _ := fx.db.NodeByPath("Documents/disk.txt")
		return row != nil && row.SyncState == db.StateSynced
	})
}

// TG-19: a queued local edit whose source becomes unreadable must not upload
// an empty/partial revision (which would destroy the remote copy), must
// surface an error, and must heal on a fresh edit after chmod.
func TestUnreadableLocalFileIsErroredWithoutUploadingEmptyContent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	fx := start(t)
	rec := recordActivity(fx.handle)
	path := filepath.Join(fx.root, "secret.txt")
	fx.write("secret.txt", []byte("remote v1"))
	waitFor(t, func() bool { return contains(fx.uploadedNames(), "secret.txt") })
	waitForSynced(t, fx.db, "secret.txt")
	uid, ok := fx.mock.FindUID("root", "secret.txt")
	if !ok {
		t.Fatal("secret.txt uid missing")
	}
	beforeRow, _ := fx.db.NodeByPath("secret.txt")
	beforeSHA := ""
	if beforeRow != nil && beforeRow.SHA1 != nil {
		beforeSHA = *beforeRow.SHA1
	}

	fx.handle.Pause()
	waitForEngineState(t, fx, engine.StatePaused)
	fx.write("secret.txt", []byte("local v2"))
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("secret.txt")
		return row != nil && row.SyncState == db.StateDirty && row.SHA1 != nil && *row.SHA1 != beforeSHA
	})

	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	uploadsBefore := fx.mock.UploadAttempts("secret.txt")
	fx.handle.Resume()

	item := waitForActivity(t, rec, engine.ActivityUploadErrored, "secret.txt")
	if !strings.Contains(item.Detail, "permission denied") {
		t.Fatalf("detail should name the cause: %q", item.Detail)
	}
	if n := fx.mock.UploadAttempts("secret.txt"); n != uploadsBefore {
		t.Fatalf("unreadable source must not reach the uploader: %d attempts", n)
	}
	if c, _ := fx.mock.FileContent(uid); string(c) != "remote v1" {
		t.Fatalf("remote content = %q", c)
	}
	if e, _ := fx.db.UploadError("secret.txt"); e == "" {
		t.Fatal("upload error must be recorded")
	}

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	fx.write("secret.txt", []byte("local v3"))
	waitFor(t, func() bool {
		c, _ := fx.mock.FileContent(uid)
		if string(c) != "local v3" {
			return false
		}
		row, _ := fx.db.NodeByPath("secret.txt")
		return row != nil && row.SyncState == db.StateSynced
	})
	if e, _ := fx.db.UploadError("secret.txt"); e != "" {
		t.Fatalf("upload error = %q", e)
	}
}

// ---------------------------------------------------------------------------
// Wave 3a/5a — replay dedup, session expiry (TG-20), sign-out, sidecar outage
// ---------------------------------------------------------------------------

// TG-22: after a stream reconnect the mock replays the last delivered event
// (the host cursor did not advance past it). Applying it twice must not
// re-download or duplicate the row, and the cursor must keep moving only
// over genuinely applied events.
func TestReplayedEventAfterReconnectDoesNotDuplicateDownloads(t *testing.T) {
	fx := start(t)
	downloadsBefore := len(fx.mock.DownloadedUIDs())
	uid := fx.mock.RemoteCreateFile("root", "reconnect.txt", []byte("v1"))
	waitFor(t, func() bool { return fx.exists("reconnect.txt") && string(fx.read("reconnect.txt")) == "v1" })
	waitForSynced(t, fx.db, "reconnect.txt")
	cursor, _ := fx.db.EventCursor("scope-1")
	if cursor == "" {
		t.Fatal("cursor missing")
	}
	downloadsAfterCreate := len(fx.mock.DownloadedUIDs())
	if downloadsAfterCreate != downloadsBefore+1 {
		t.Fatalf("downloads after create = %d, want %d", downloadsAfterCreate, downloadsBefore+1)
	}

	// Replay the exact event the stream ended on, then drive a marker event
	// through the same FIFO notification channel. Waiting for the marker's
	// cursor guarantees the replay was fully processed before we touch the
	// remote again.
	fx.mock.EmitEvent(&sidecar.DriveEvent{
		ScopeID: "scope-1", EventID: cursor, Kind: sidecar.EventNodeCreated,
		NodeUID: &uid, ParentNodeUID: strPtrT("root"),
	})
	fx.mock.EmitEvent(&sidecar.DriveEvent{
		ScopeID: "scope-1", EventID: "evt-marker", Kind: sidecar.EventFastForward,
	})
	waitFor(t, func() bool {
		c, _ := fx.db.EventCursor("scope-1")
		return c == "evt-marker"
	})
	if n := len(fx.mock.DownloadedUIDs()); n != downloadsAfterCreate {
		t.Fatalf("a replayed event must not download again: %d != %d", n, downloadsAfterCreate)
	}

	fx.mock.RemoteUpdateFile(uid, []byte("v2"))
	waitFor(t, func() bool { return string(fx.read("reconnect.txt")) == "v2" })
	waitForSynced(t, fx.db, "reconnect.txt")

	if n := len(fx.mock.DownloadedUIDs()); n != downloadsAfterCreate+1 {
		t.Fatalf("the later update downloads exactly once: %d != %d", n, downloadsAfterCreate+1)
	}
	var rows []*db.NodeRow
	for _, r := range mustAllNodes(t, fx.db) {
		if r.LocalPath == "reconnect.txt" {
			rows = append(rows, r)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("replay must not duplicate the row: %d rows", len(rows))
	}
	if rows[0].NodeUID != uid {
		t.Fatalf("row uid = %q", rows[0].NodeUID)
	}
	if c, _ := fx.db.EventCursor("scope-1"); c == cursor {
		t.Fatal("cursor must keep moving")
	}
}

// A download rejected with 1002 parks the triggering event (cursor not
// advanced) and raises the same sign-in signal; after re-auth the buffered
// event retries and heals the file.
func TestSessionExpiredDownloadParksEventAndHealsAfterReauthentication(t *testing.T) {
	fx := start(t)
	rec := recordActivity(fx.handle)
	fx.mock.InjectDownloadFailures("node-hello", [][]any{{int64(1002), "session expired", false}})
	fx.mock.RemoteUpdateFile("node-hello", []byte("remote v2"))

	waitFor(t, func() bool {
		st, _ := fx.handle.Status()
		return st.NeedsSignIn
	})
	// Not materialized yet (and the old content was not clobbered).
	if got := string(fx.read("hello.txt")); got != "hello drive" {
		t.Fatalf("hello.txt = %q", got)
	}
	downloads := len(fx.mock.DownloadedUIDs())
	time.Sleep(300 * time.Millisecond)
	if n := len(fx.mock.DownloadedUIDs()); n != downloads {
		t.Fatalf("engine retried a download while signed out: %d != %d", n, downloads)
	}
	if rec.count(engine.ActivitySignInRequired, "") == 0 {
		t.Fatal("expected a sign-in-required activity entry")
	}

	fx.handle.Resume()
	waitFor(t, func() bool { return string(fx.read("hello.txt")) == "remote v2" })
	waitForSynced(t, fx.db, "hello.txt")
	if st, _ := fx.handle.Status(); st.NeedsSignIn {
		t.Fatal("sign-in flag must clear")
	}
}

// Signing out (engine shutdown) with a tombstone and a dirty upload pending
// must keep both in the DB/disk and make no further remote calls. A later
// successful session drains them in the documented order: tombstones (guard)
// before uploads.
func TestSignOutKeepsPendingWorkAndDrainsAfterReauthentication(t *testing.T) {
	fx := start(t)
	seedRemoteSyncedFillers(t, fx, 10)
	fx.handle.Pause()
	waitForEngineState(t, fx, engine.StatePaused)

	fx.write("draft.txt", []byte("pending local work"))
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("draft.txt")
		return row != nil && row.SyncState == db.StateDirty
	})
	if err := os.Remove(filepath.Join(fx.root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, tb := range mustTombstones(t, fx.db) {
			if tb.LocalPath == "hello.txt" {
				return true
			}
		}
		return false
	})

	root, dbPath := fx.root, filepath.Join(fx.root, engine.StateDir, "state.db")
	oldMock := fx.mock
	trashesBefore := len(oldMock.TrashedUIDs())
	uploadsBefore := len(oldMock.UploadedNames())
	fx.shutdown()
	// fx.db was closed by shutdown; re-open on the same file.
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// No session: pending local work survives untouched and nothing reaches
	// the (now stopped) sidecar.
	time.Sleep(300 * time.Millisecond)
	if b, err := os.ReadFile(filepath.Join(root, "draft.txt")); err != nil || string(b) != "pending local work" {
		t.Fatalf("draft.txt = %q (%v)", b, err)
	}
	if row, _ := d.NodeByPath("draft.txt"); row == nil || row.SyncState != db.StateDirty {
		t.Fatalf("draft row = %+v", row)
	}
	if tbs := mustTombstones(t, d); len(tbs) == 0 {
		t.Fatal("hello.txt tombstone must survive sign-out")
	}
	if n := len(oldMock.TrashedUIDs()); n != trashesBefore {
		t.Fatalf("trashes after sign-out = %d, want %d", n, trashesBefore)
	}
	if n := len(oldMock.UploadedNames()); n != uploadsBefore {
		t.Fatalf("uploads after sign-out = %d, want %d", n, uploadsBefore)
	}

	// "Re-login": a fresh sidecar session over the SAME DB. The pending
	// queue must drain: tombstone first, then the upload.
	mock := sidecar.NewMockWithSeq(1000)
	client, mhandle := mock.Connect()
	h, done := runEngine(t, client, root, dbPath, engine.RetryBaseDefault)

	waitFor(t, func() bool { return contains(mhandle.TrashedUIDs(), "node-hello") })
	waitFor(t, func() bool { return contains(mhandle.UploadedNames(), "draft.txt") })
	waitFor(t, func() bool {
		return len(mustTombstones(t, d)) == 0
	})
	waitFor(t, func() bool {
		row, _ := d.NodeByPath("draft.txt")
		return row != nil && row.SyncState == db.StateSynced
	})

	log := mhandle.CallLog()
	trashAt, uploadAt := -1, -1
	for i, c := range log {
		if c.Method == "trash" && contains(c.Args, "node-hello") && trashAt < 0 {
			trashAt = i
		}
		if c.Method == "upload" && contains(c.Args, "draft.txt") && uploadAt < 0 {
			uploadAt = i
		}
	}
	if trashAt < 0 || uploadAt < 0 || trashAt > uploadAt {
		t.Fatalf("pending deletes must drain before uploads: trash@%d upload@%d", trashAt, uploadAt)
	}
	if b, err := os.ReadFile(filepath.Join(root, "draft.txt")); err != nil || string(b) != "pending local work" {
		t.Fatalf("draft.txt = %q (%v)", b, err)
	}
	stopEngine(t, h, done, client)
}

// A sidecar outage while calls are being made is visible in status/activity;
// pending tombstone + upload work stays intact, and the engine probes again
// and drains the queues once calls succeed (mock "restarted").
func TestSidecarOutageIsVisibleAndRecoversWhenCallsSucceedAgain(t *testing.T) {
	fx := start(t)
	rec := recordActivity(fx.handle)
	seedRemoteSyncedFillers(t, fx, 10)
	fx.handle.Pause()
	waitForEngineState(t, fx, engine.StatePaused)

	fx.write("deferred.txt", []byte("waiting for sidecar"))
	waitFor(t, func() bool {
		row, _ := fx.db.NodeByPath("deferred.txt")
		return row != nil && row.SyncState == db.StateDirty
	})
	if err := os.Remove(filepath.Join(fx.root, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, tb := range mustTombstones(t, fx.db) {
			if tb.LocalPath == "hello.txt" {
				return true
			}
		}
		return false
	})

	callsBefore := len(fx.mock.CallLog())
	fx.mock.SetOffline(true)
	fx.handle.Resume()

	waitFor(t, func() bool {
		st, _ := fx.handle.Status()
		return st.SidecarUnavailable
	})
	// Both queues survive; the failed calls were never dispatched remotely.
	found := false
	for _, tb := range mustTombstones(t, fx.db) {
		if tb.LocalPath == "hello.txt" {
			found = true
		}
	}
	if !found {
		t.Fatal("tombstone must survive the outage")
	}
	if trashed := fx.mock.TrashedUIDs(); len(trashed) != 0 {
		t.Fatalf("trashed during outage: %v", trashed)
	}
	if contains(fx.uploadedNames(), "deferred.txt") {
		t.Fatal("deferred.txt uploaded during outage")
	}
	waitFor(t, func() bool {
		return rec.count(engine.ActivitySidecarUnavailable, "") > 0
	})

	// The probe is delayed (no tight loop while the outage persists).
	time.Sleep(300 * time.Millisecond)
	if n := len(fx.mock.CallLog()); n != callsBefore {
		t.Fatalf("probe loop during outage: %d calls, before %d", n, callsBefore)
	}

	// "Restart": calls answer again; the next probe drains tombstone→upload.
	fx.mock.SetOffline(false)
	waitFor(t, func() bool { return contains(fx.mock.TrashedUIDs(), "node-hello") })
	waitFor(t, func() bool { return contains(fx.uploadedNames(), "deferred.txt") })
	waitForSynced(t, fx.db, "deferred.txt")
	st, _ := fx.handle.Status()
	if st.SidecarUnavailable {
		t.Fatal("outage flag must clear")
	}
	if st.QueuedTombstones != 0 {
		t.Fatalf("queued tombstones = %d", st.QueuedTombstones)
	}

	log := fx.mock.CallLog()
	trashAt, uploadAt := -1, -1
	for i, c := range log {
		if c.Method == "trash" && contains(c.Args, "node-hello") && trashAt < 0 {
			trashAt = i
		}
		if c.Method == "upload" && contains(c.Args, "deferred.txt") && uploadAt < 0 {
			uploadAt = i
		}
	}
	if trashAt < 0 || uploadAt < 0 || trashAt > uploadAt {
		t.Fatalf("recovery order wrong: trash@%d upload@%d", trashAt, uploadAt)
	}
}

func mustAllNodes(t *testing.T, d *db.Db) []*db.NodeRow {
	t.Helper()
	rows, err := d.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// ---------------------------------------------------------------------------
// Wave 6 — many small files (1500 variant; the 5000 run stays manual)
// ---------------------------------------------------------------------------

func TestManySmallFilesConvergeWithoutDuplicateUploads1500(t *testing.T) {
	if testing.Short() {
		t.Skip("1500-file convergence run is too slow for -short")
	}
	manySmallFiles(t, 1500)
}

func manySmallFiles(t *testing.T, count int) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "sync")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("tiny-%04d.txt", i)
		if err := os.WriteFile(filepath.Join(root, name), []byte(fmt.Sprintf("content %d", i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mock := sidecar.NewMock()
	client, mhandle := mock.Connect()
	dbPath := filepath.Join(root, engine.StateDir, "state.db")
	h, done := runEngine(t, client, root, dbPath, engine.RetryBaseDefault)
	defer func() { stopEngine(t, h, done, client) }()

	waitFor(t, func() bool { return len(mhandle.ChildNames("root")) >= count+2 }) // + Documents/hello.txt
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	waitFor(t, func() bool {
		rows := mustAllNodes(t, d)
		n, synced := 0, 0
		for _, r := range rows {
			if strings.HasPrefix(r.LocalPath, "tiny-") && r.IsFile() {
				n++
				if r.SyncState == db.StateSynced {
					synced++
				}
			}
		}
		return n == count && synced == count
	})

	names := mhandle.UploadedNames()
	if len(names) != count {
		t.Fatalf("exactly one upload per local file (no duplicates): %d", len(names))
	}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("tiny-%04d.txt", i)
		if n := mhandle.UploadAttempts(name); n != 1 {
			t.Fatalf("duplicate upload for %s: %d attempts", name, n)
		}
	}
	rows := mustAllNodes(t, d)
	tiny := 0
	for _, r := range rows {
		if strings.HasPrefix(r.LocalPath, "tiny-") && r.IsFile() {
			tiny++
			if r.NodeUID == "" || strings.HasPrefix(r.NodeUID, "pending:") {
				t.Fatalf("every local file received a real remote uid: %q", r.NodeUID)
			}
		}
	}
	if tiny != count {
		t.Fatalf("one DB row per file, no duplicates: %d", tiny)
	}
}
