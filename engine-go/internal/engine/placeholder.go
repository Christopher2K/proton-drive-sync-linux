// Placeholder-mode engine paths (SPEC F5, Phase 2 step 5): VFS snapshot
// publication, on-demand hydration, LRU eviction, pins, and write-through
// uploads. Port of the placeholder branches of crates/core/src/engine.rs
// (hydrate_one / download_to_backing / upload_write_one /
// write_upload_row / maybe_evict / pin_file / publish_vfs_snapshot plus
// the store seams drained from the main loop and pump_work).
//
// All functions run on the engine goroutine (the sole DB writer). Store
// wake-ups arrive as synthetic "store" commands on cmdCh (see
// startStoreForwarder): event-driven, no polling. In mirror mode e.store
// is nil and none of this runs.
package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/sidecar"
	storepkg "pds.dev/engine-go/internal/store"
)

// pinRequest is a queued pin command awaiting its (synchronous) hydration.
type pinRequest struct {
	rel   string
	reply chan<- error
	pin   bool
}

// errBackingVanished marks an upload whose backing file vanished while
// its write was queued/in flight (the rename/delete path owns it now).
var errBackingVanished = errors.New("backing file vanished")

// ---- Handle API (M3.2) -------------------------------------------------

// Pin keeps rel's content in the backing cache (download if missing).
// Fails on unknown paths and in materialized (mirror) mode.
func (h *Handle) Pin(rel string) error {
	reply := make(chan error, 1)
	h.cmdCh <- Command{Kind: "pin", Rel: rel, Reply: reply}
	select {
	case err := <-reply:
		return err
	case <-time.After(30 * time.Second):
		return errors.New("engine not answering")
	}
}

// Unpin drops a pin set via Pin. The file stays hydrated until the
// eviction pass picks it.
func (h *Handle) Unpin(rel string) error {
	reply := make(chan error, 1)
	h.cmdCh <- Command{Kind: "unpin", Rel: rel, Reply: reply}
	select {
	case err := <-reply:
		return err
	case <-time.After(30 * time.Second):
		return errors.New("engine not answering")
	}
}

// ---- store wake-ups ------------------------------------------------------

// startStoreForwarder forwards store Notify wake-ups to the engine's
// command channel as synthetic "store" commands. Notify.Wait is not
// otherwise selectable, so this bridges the VFS goroutines to the engine
// loop without polling. done is closed when Run returns; the forwarder
// exits on the next wake-up after that (one goroutine per engine run).
func (e *Engine) startStoreForwarder(done <-chan struct{}) {
	concrete, ok := e.store.(*storepkg.Store)
	if !ok || concrete == nil || concrete.Notify == nil {
		return
	}
	notify := concrete.Notify
	go func() {
		for {
			notify.Wait()
			select {
			case <-done:
				return
			default:
			}
			select {
			case e.cmdCh <- Command{Kind: "store"}:
			case <-done:
				return
			default:
				// Buffer full: a wake is already pending (coalesced).
			}
		}
	}()
}

// ---- VFS snapshot (M3) -----------------------------------------------------

// publishVfsSnapshot publishes the current DB state as the VFS tree
// snapshot. Files with a backing file on disk are flagged content_ready.
func (e *Engine) publishVfsSnapshot() error {
	rows, err := e.db.AllNodes()
	if err != nil {
		return err
	}
	byPath := make(map[string]storepkg.VNode, len(rows))
	for _, row := range rows {
		name := row.LocalPath
		parent := ""
		if i := strings.LastIndex(row.LocalPath, "/"); i >= 0 {
			name = row.LocalPath[i+1:]
			parent = row.LocalPath[:i]
		}
		var size uint64
		if row.Size != nil && *row.Size > 0 {
			size = uint64(*row.Size)
		}
		var mtime int64
		if row.Mtime != nil {
			mtime = *row.Mtime
		}
		modeVal := uint32(0o755)
		if row.IsFile() {
			modeVal = 0o644
			if row.Mode != nil {
				modeVal = *row.Mode & 0o777
			}
		}
		byPath[row.LocalPath] = storepkg.VNode{
			Rel:          row.LocalPath,
			Parent:       parent,
			Name:         name,
			IsDir:        !row.IsFile(),
			Size:         size,
			Mtime:        mtime,
			Mode:         modeVal,
			ContentReady: isRegularFile(toAbs(e.backingDir, row.LocalPath)),
		}
	}
	e.store.PublishSnapshot(byPath)
	return nil
}

// applyMetadataUpdate applies a VFS setattr metadata change to the row and
// snapshot. Timestamp-only changes do NOT queue an upload: the content
// hash is unchanged, and a metadata-only revision would be remote churn.
func (e *Engine) applyMetadataUpdate(rel string, mtime *int64, mode *uint32) {
	row, err := e.db.NodeByPath(rel)
	if err != nil || row == nil {
		return
	}
	if mtime != nil {
		row.Mtime = mtime
	}
	if mode != nil {
		m := *mode & 0o777
		row.Mode = &m
	}
	if err := e.db.UpsertNode(row); err != nil {
		e.log.Printf("%s: metadata update failed: %v", rel, err)
		return
	}
	if err := e.publishVfsSnapshot(); err != nil {
		e.log.Printf("%s: post-metadata VFS snapshot failed: %v", rel, err)
	}
}

// ---- hydration (M3.1) ----------------------------------------------------

// hydrateOne downloads one requested placeholder to the backing dir,
// registers the backing file, and refreshes the snapshot. Failures just
// unblock the waiting reader (it retries with EIO on the next open).
func (e *Engine) hydrateOne(rel string) {
	row, err := e.db.NodeByPath(rel)
	if err != nil || row == nil {
		e.store.FinishHydrate(rel, false)
		return
	}
	if row.SyncState == db.StateSynced || row.SyncState == db.StateDownloading {
		if err := e.db.SetSyncState(rel, db.StateDownloading); err != nil {
			e.log.Printf("%s: failed to mark hydration in progress: %v", rel, err)
		}
	}
	// Hydration only owns the download direction here: verify the bytes
	// before marking synced (M4 soak lesson — never trust a state flag
	// alone). Only Synced/Downloading rows carry a remote hash contract;
	// Dirty rows keep their local content.
	verify := row.SyncState == db.StateSynced || row.SyncState == db.StateDownloading
	if err := e.downloadToBacking(row.NodeUID, rel, row.Mtime, row.Mode); err != nil {
		e.log.Printf("%s: hydration failed: %v", rel, err)
		if !e.shuttingDown {
			e.recordDownloadFailure(rel, err)
		}
		e.store.FinishHydrate(rel, true)
		return
	}
	if verify && row.SHA1 != nil {
		abs := toAbs(e.backingDir, rel)
		sha, _, herr := sha1File(abs)
		if herr != nil {
			e.log.Printf("%s: hydration failed: %v", rel, herr)
			_ = os.Remove(abs)
			if !e.shuttingDown {
				e.recordDownloadFailure(rel, herr)
			}
			e.store.FinishHydrate(rel, true)
			return
		}
		if sha != *row.SHA1 {
			merr := fmt.Errorf("hydrated %s: sha1 %s != remote %s", rel, sha, *row.SHA1)
			e.log.Printf("%s: %v", rel, merr)
			_ = os.Remove(abs)
			if !e.shuttingDown {
				e.recordDownloadFailure(rel, merr)
			}
			e.store.FinishHydrate(rel, true)
			return
		}
	}
	e.store.SetBacking(rel, toAbs(e.backingDir, rel))
	// Never rewrite a row with pending local changes (Dirty/Uploading).
	if current, cerr := e.db.NodeByPath(rel); cerr == nil && current != nil &&
		(current.SyncState == db.StateDownloading || current.SyncState == db.StateSynced) {
		if err := e.db.SetSyncState(rel, db.StateSynced); err != nil {
			e.log.Printf("%s: failed to mark hydration complete: %v", rel, err)
		}
		if err := e.clearUploadBlocks(rel); err != nil {
			e.log.Printf("%s: failed to clear hydration error: %v", rel, err)
		}
		delete(e.retryAt, rel)
	}
	if err := e.publishVfsSnapshot(); err != nil {
		e.log.Printf("%s: post-hydrate snapshot failed: %v", rel, err)
	}
	e.log.Printf("hydrated on demand: %s", rel)
	e.store.FinishHydrate(rel, true)
}

// downloadToBacking downloads a node into the backing dir (placeholder
// mode). Same atomic tmp+rename pattern as tryDownloadFile, different
// target. Never the mount root.
func (e *Engine) downloadToBacking(uid, rel string, mtime *int64, mode *uint32) error {
	if reason, bad := pathLimitReason(e.backingDir, rel); bad {
		e.reportUnmaterializable(rel, reason)
		return fmt.Errorf("%s", reason)
	}
	abs := toAbs(e.backingDir, rel)
	if isRegularFile(abs) {
		return nil // already hydrated (lost wake-up)
	}
	if i := strings.LastIndex(abs, "/"); i >= 0 {
		if err := os.MkdirAll(abs[:i], 0o755); err != nil {
			return err
		}
	}
	tmp := toAbs(e.backingDir, transferTempRel(rel))
	// Surface an unwritable backing dir before the transfer; this is
	// also the temp the transfer writes.
	if f, err := os.Create(tmp); err != nil {
		return fmt.Errorf("prepare hydration target %s: %w", abs, err)
	} else {
		f.Close()
	}
	_, failure := e.runTransfer("download", rel, func() (string, error) {
		ctx, cancel := ctxBG()
		defer cancel()
		return e.sidecar.Download(ctx, uid, tmp)
	})
	if failure != nil {
		_ = os.Remove(tmp)
		if failure.aborted {
			if !e.shuttingDown && sidecar.IsConnectionLost(failure.err) {
				e.noteSidecarFailure(failure.err.Error())
			}
			return failure.err
		}
		if sidecar.IsAuthCode(failure.code) {
			e.noteAuthRequired(rel, failure.code, failure.message)
			return fmt.Errorf("sign-in required while downloading `%s`: %s", rel, failure.message)
		}
		return fmt.Errorf("hydrate `%s`: %s", rel, failure.message)
	}
	if err := os.Rename(tmp, abs); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install hydrated file %s: %w", abs, err)
	}
	applyFileMetadata(abs, mtime, mode)
	return nil
}

// ---- eviction (M3.2) --------------------------------------------------------

// maybeEvict dehydrates the least-recently-used files while hydrated
// content exceeds the cap. Never touches pinned, dirty/un-uploaded,
// open, or uploading files, nor the just-hydrated one. Unlimited = skip.
func (e *Engine) maybeEvict(protect string) {
	if e.evictionCap == nil {
		return
	}
	cap := *e.evictionCap
	rows, err := e.db.AllNodes()
	if err != nil {
		return
	}
	byPath := make(map[string]*db.NodeRow, len(rows))
	for _, r := range rows {
		byPath[r.LocalPath] = r
	}
	var total uint64
	for _, age := range e.store.HydratedByAge() {
		if row, ok := byPath[age.Rel]; ok && row.Size != nil && *row.Size > 0 {
			total += uint64(*row.Size)
		}
	}
	if total <= cap {
		return
	}
	for _, age := range e.store.HydratedByAge() {
		if total <= cap {
			break
		}
		rel := age.Rel
		if rel == protect {
			continue
		}
		row, ok := byPath[rel]
		if !ok {
			// No row (stale backing entry after a delete): drop the file.
			_ = os.Remove(toAbs(e.backingDir, rel))
			e.store.ClearBacking(rel)
			continue
		}
		if row.Pinned || row.SyncState != db.StateSynced ||
			e.store.IsOpen(rel) || e.store.IsUploading(rel) {
			continue
		}
		var size uint64
		if row.Size != nil && *row.Size > 0 {
			size = uint64(*row.Size)
		}
		abs := toAbs(e.backingDir, rel)
		if err := os.Remove(abs); err == nil {
			e.store.ClearBacking(rel)
			if total >= size {
				total -= size
			} else {
				total = 0
			}
			e.log.Printf("dehydrated (LRU eviction): %s (%d bytes)", rel, size)
		}
	}
	// Snapshot so content_ready flips back to placeholder immediately.
	if err := e.publishVfsSnapshot(); err != nil {
		e.log.Printf("post-evict VFS snapshot failed: %v", err)
	}
}

// ---- pins (M3.2) -------------------------------------------------------------

// pinFile marks the row (and rows under it, for folders) pinned and
// hydrates whatever is missing. Directories pin recursively.
func (e *Engine) pinFile(rel string) error {
	if !e.placeholders {
		return errors.New("pins apply to placeholder (mount) mode")
	}
	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("unknown path `%s`", rel)
	}
	var targets []string
	if row.IsFile() {
		targets = []string{rel}
	} else {
		children, err := e.db.NodesUnder(likeUnder(rel))
		if err != nil {
			return err
		}
		for _, child := range children {
			if child.IsFile() {
				targets = append(targets, child.LocalPath)
			}
		}
		targets = append(targets, rel)
	}
	for _, t := range targets {
		if err := e.db.SetPinned(t, true); err != nil {
			return err
		}
	}
	// Hydrate whatever is missing (sequential; sidecar transfers are
	// serialized per engine anyway). A folder itself has no content.
	for _, t := range targets {
		if t == rel && !row.IsFile() {
			continue
		}
		if _, ok := e.store.BackingPath(t); !ok {
			e.hydrateOne(t)
		}
	}
	e.log.Printf("pinned: %s (%d files)", rel, len(targets))
	return nil
}

// unpinFile drops the flag; content stays until the eviction pass picks it.
func (e *Engine) unpinFile(rel string) error {
	if !e.placeholders {
		return errors.New("pins apply to placeholder (mount) mode")
	}
	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("unknown path `%s`", rel)
	}
	if row.IsFile() {
		if err := e.db.SetPinned(rel, false); err != nil {
			return err
		}
	} else {
		if err := e.db.SetPinnedTree(rel, false); err != nil {
			return err
		}
	}
	e.log.Printf("unpinned: %s", rel)
	return nil
}

// ---- write-through (M3.2/M3.3) ----------------------------------------------

// uploadWriteOne drains one write-queue entry: upload the backing content
// through the standard machinery (F7 precondition, retries, pause
// buffering). Failures stay dirty and re-queue via the store.
func (e *Engine) uploadWriteOne(rel string) {
	// Parent folders first: the VFS queues mkdirs before the writes
	// inside them, but a crash could leave an orphan write queued.
	for {
		dir, ok := e.store.NextMkdirRequest()
		if !ok {
			break
		}
		if _, err := e.ensureRemoteFolder(dir); err != nil {
			e.log.Printf("%s: write mkdir failed: %v", dir, err)
		}
	}
	result := e.writeUploadRow(rel)
	if result != nil {
		e.log.Printf("%s: write upload failed: %v", rel, result)
	}
	vanished := result != nil && isVanishedError(result)
	if vanished {
		first, ok := e.vanishedWrites[rel]
		if !ok {
			e.vanishedWrites[rel] = time.Now()
			first = e.vanishedWrites[rel]
		}
		if time.Since(first) < WriteVanishGrace {
			e.store.FinishWrite(rel, false)
			return
		}
		delete(e.vanishedWrites, rel)
	}
	e.store.FinishWrite(rel, result == nil || vanished)
	if err := e.publishVfsSnapshot(); err != nil {
		e.log.Printf("post-write VFS snapshot failed: %v", err)
	}
}

// isVanishedError reports whether err means the backing file vanished
// (as opposed to a real local failure like unreadable permissions).
func isVanishedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errBackingVanished) {
		return true
	}
	return os.IsNotExist(err)
}

// writeUploadRow creates/updates the DB row for a write-through file and
// uploads it. Mirrors upsertLocalFile but sources content from the
// backing dir and never renames on collision (the VFS file stays where
// the user put it; the collision path uploads under an available name).
func (e *Engine) writeUploadRow(rel string) error {
	abs := toAbs(e.backingDir, rel)
	// Unreadable backing content is a local environment failure, not a
	// vanished file: surface it and drop the queue entry (a fresh write
	// heals it), never upload empty bytes.
	if f, err := os.Open(abs); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %v", errBackingVanished, err)
		}
		return e.recordLocalUploadFailure(rel, fmt.Errorf("read local file %s: %w", abs, err))
	} else {
		f.Close()
	}
	meta, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("%w: %v", errBackingVanished, err)
	}
	sha1hex, size, err := sha1File(abs)
	if err != nil {
		return err
	}
	mtime := meta.ModTime().Unix()
	mtimeCopy := mtime
	modeVal := uint32(meta.Mode().Perm())
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}

	var parentUID *string
	var nodeUID *string
	var syncedSHA1 *string

	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row != nil {
		if row.SyncState == db.StateSynced && row.SHA1 != nil && *row.SHA1 == sha1hex {
			// Content identical to the synced revision: the write wins
			// over any queued delete for this node; a chmod may still
			// have changed the local mode.
			e.cancelStaleTombstone(rel, row.NodeUID)
			if row.Mode == nil || *row.Mode != modeVal {
				m := modeVal
				row.Mode = &m
				if err := e.db.UpsertNode(row); err != nil {
					return err
				}
			}
			return nil
		}
		if row.ParentUID != nil {
			parentUID = row.ParentUID
		} else if strings.HasPrefix(row.NodeUID, "pending:") {
			parentRel := ""
			hasParent := false
			if i := strings.LastIndex(rel, "/"); i >= 0 {
				parentRel = rel[:i]
				hasParent = true
			}
			if hasParent {
				uid, uerr := e.ensureRemoteFolder(parentRel)
				if uerr != nil {
					return uerr
				}
				parentUID = &uid
			} else if uid, ok := e.dbRootUID(); ok {
				parentUID = &uid
			}
		}
		if !strings.HasPrefix(row.NodeUID, "pending:") {
			u := row.NodeUID
			nodeUID = &u
		}
		syncedSHA1 = row.SyncedSHA1
	} else {
		// New file: ensure the folder chain exists remotely, then check
		// for a remote sibling collision (F7) — without moving the file.
		parentRel := ""
		hasParent := false
		if i := strings.LastIndex(rel, "/"); i >= 0 {
			parentRel = rel[:i]
			hasParent = true
		}
		if hasParent {
			uid, uerr := e.ensureRemoteFolder(parentRel)
			if uerr != nil {
				return uerr
			}
			parentUID = &uid
		} else if uid, ok := e.dbRootUID(); ok {
			parentUID = &uid
		}
		if parentUID == nil {
			return errors.New("no root uid")
		}
		remoteName := name
		ctx, cancel := ctxBG()
		available, aerr := e.sidecar.GetAvailableName(ctx, *parentUID, name)
		cancel()
		if aerr == nil && available != name {
			e.log.Printf("%s: remote name collision on write-through (renamed to %s)", rel, available)
			remoteName = available
		}
		sz := int64(size)
		sh := sha1hex
		mt := mtimeCopy
		md := modeVal
		if err := e.db.UpsertNode(&db.NodeRow{
			LocalPath:  rel,
			NodeUID:    "pending:" + rel,
			ParentUID:  parentUID,
			Name:       remoteName,
			Kind:       db.KindFile,
			Size:       &sz,
			SHA1:       &sh,
			Mtime:      &mt,
			Mode:       &md,
			SyncState:  db.StateDirty,
			Pinned:     false,
			SyncedSHA1: nil,
		}); err != nil {
			return err
		}
		// Tuple (None, None, None): re-read below must not clobber the
		// parent/name stored by this upsert.
		nodeUID = nil
		parentUID = nil
		syncedSHA1 = nil
	}

	// Re-read: for a brand-new file the upsert above already resolved the
	// parent uid (and possibly a collision-renamed name).
	current, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	uid := current.NodeUID
	if nodeUID != nil {
		uid = *nodeUID
	}
	puid := current.ParentUID
	if parentUID != nil {
		puid = parentUID
	}
	sz := int64(size)
	sh := sha1hex
	mt := mtimeCopy
	md := modeVal
	if err := e.db.UpsertNode(&db.NodeRow{
		LocalPath:  rel,
		NodeUID:    uid,
		ParentUID:  puid,
		Name:       current.Name,
		Kind:       db.KindFile,
		Size:       &sz,
		SHA1:       &sh,
		Mtime:      &mt,
		Mode:       &md,
		SyncState:  db.StateDirty,
		Pinned:     current.Pinned,
		SyncedSHA1: syncedSHA1,
	}); err != nil {
		return err
	}
	_ = e.db.ClearUploadFailures(rel)
	delete(e.retryAt, rel)
	delete(e.vanishedWrites, rel)

	if e.state != StateRunning {
		e.log.Printf("%s: paused: write-through upload buffered", rel)
		return nil
	}
	if err := e.uploadOne(rel); err != nil {
		return err
	}
	// A revision just landed on an existing remote node; a same-node
	// tombstone is stale (see cancelStaleTombstone).
	if nodeUID != nil {
		e.cancelStaleTombstone(rel, *nodeUID)
	}
	return nil
}

// ---- store drain (main loop) -------------------------------------------------

// drainStoreWork drains one unit of VFS work in Rust main-loop priority
// order: pins → metadata → hydration → mkdir → work (delete → rename →
// write). Returns true when one unit was handled (the caller loops).
func (e *Engine) drainStoreWork() (bool, error) {
	if e.store == nil {
		return false, nil
	}
	// Pins queue downloads too; drain them first so a just-pinned file
	// hydrates before unrelated requests.
	if len(e.pendingPins) > 0 {
		req := e.pendingPins[0]
		e.pendingPins = e.pendingPins[1:]
		var result error
		if req.pin {
			result = e.pinFile(req.rel)
		}
		if req.reply != nil {
			req.reply <- result
		}
		return true, nil
	}
	// Metadata-only changes (setattr mtime/mode): row + snapshot, never
	// the remote (hash unchanged).
	if rel, mtime, mode, ok := e.store.NextMetadataRequest(); ok {
		e.applyMetadataUpdate(rel, mtime, mode)
		return true, nil
	}
	// Hydration: download on demand, then the idle-path eviction pass.
	// The fresh file itself is protected from eviction.
	if rel, ok := e.store.NextHydrateRequest(); ok {
		e.hydrateOne(rel)
		e.maybeEvict(rel)
		// A cap change landed mid-hydration; apply it now, without the
		// just-hydrated-file protection.
		if e.evictionRecheck {
			e.evictionRecheck = false
			e.maybeEvict("")
		}
		return true, nil
	}
	// Bare mkdirs have no write-queue entry to drag them out.
	if dir, ok := e.store.NextMkdirRequest(); ok {
		if _, err := e.ensureRemoteFolder(dir); err != nil {
			e.log.Printf("%s: queued mkdir failed; re-queueing: %v", dir, err)
			e.store.RequeueMkdir(dir)
		} else if err := e.publishVfsSnapshot(); err != nil {
			e.log.Printf("post-mkdir VFS snapshot failed: %v", err)
		}
		return true, nil
	}
	// Deletes before renames and writes: an overwrite-rename queues the
	// destination delete plus a rename onto the now-free path. NextWorkRequest
	// resolves the priority atomically so the VFS triple cannot split.
	if req, ok := e.store.NextWorkRequest(); ok {
		switch req.Kind {
		case storepkg.WorkDelete:
			if err := e.handleLocalDelete(req.Rel); err != nil {
				e.log.Printf("%s: write-through delete failed: %v", req.Rel, err)
			}
			if err := e.publishVfsSnapshot(); err != nil {
				e.log.Printf("post-delete VFS snapshot failed: %v", err)
			}
		case storepkg.WorkRename:
			if err := e.applyLocalRename(req.Rel, req.NewRel); err != nil {
				e.log.Printf("%s -> %s: write-through rename failed: %v", req.Rel, req.NewRel, err)
				// A name conflict re-queues a few times then drops:
				// renames drain before pump_work, so retrying forever
				// would starve the tombstone drain that frees the name.
				permanent := isRenameNameConflict(err)
				if !e.store.FinishRename(req.Rel, req.NewRel, false, permanent) {
					e.log.Printf("%s -> %s: write-through rename dropped after repeated name conflicts; the remote keeps the source name", req.Rel, req.NewRel)
				}
			} else {
				e.store.FinishRename(req.Rel, req.NewRel, true, false)
			}
			if err := e.publishVfsSnapshot(); err != nil {
				e.log.Printf("post-rename VFS snapshot failed: %v", err)
			}
		case storepkg.WorkWrite:
			e.uploadWriteOne(req.Rel)
		}
		return true, nil
	}
	return false, nil
}

// isRenameNameConflict reports whether err is the "destination name
// already exists remotely" rename class (retry cannot heal it alone).
func isRenameNameConflict(err error) bool {
	code, ok := sidecarErrCode(err)
	if !ok {
		return false
	}
	if code == sidecar.ErrCodeConflict {
		return true
	}
	return code == sidecar.ErrCodeInternal &&
		strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// dirSize is the recursive on-disk size of dir (regular files only).
// Missing dir → 0. Used for the backing-cache usage figure in Status.
func dirSize(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total uint64
	for _, entry := range entries {
		path := dir + string(os.PathSeparator) + entry.Name()
		if entry.IsDir() {
			total += dirSize(path)
			continue
		}
		fi, serr := os.Stat(path)
		if serr == nil && fi.Mode().IsRegular() {
			total += uint64(fi.Size())
		}
	}
	return total
}
