// Core sync pipeline port (mirror mode): initial sync, F2 local-watcher
// pipeline, F3 conservative trash, F6 remote events, F7 keep-both conflicts,
// and the transfer machinery. Ported in source order from
// crates/core/src/engine.rs; placeholder/VFS branches are guarded by
// e.store != nil (nil in mirror mode).
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/sidecar"
)

// relCheck mirrors Rust RelCheck: Valid / NotUtf8(raw) / Outside.
type relCheck int

const (
	relValid relCheck = iota
	relNotUTF8
	relOutside
)

// toRelChecked classifies an absolute path against root.
func toRelChecked(root, path string) (rel string, raw []byte, state relCheck) {
	relPath, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(relPath, "..") {
		return "", nil, relOutside
	}
	parts := strings.Split(relPath, string(filepath.Separator))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		if !validUTF8(p) {
			// Raw bytes of the whole relative path (matching Rust).
			return "", []byte(relPath), relNotUTF8
		}
		out = append(out, p)
	}
	return strings.Join(out, "/"), nil, relValid
}

func validUTF8(s string) bool {
	// Go strings can hold arbitrary bytes; check round-trip through
	// RuneError like Rust's str::from_utf8.
	for _, r := range s {
		if r == 0xFFFD {
			// RuneError may be a legitimately encoded U+FFFD; verify by
			// encoding: invalid bytes decode as (RuneError, 1).
			return false
		}
	}
	return true
}

// Linux filesystem limits (bytes). PATH_MAX includes the terminating NUL.
const (
	pathMaxBytes = 4096
	nameMaxBytes = 255
)

// pathLimitReason reports why rel cannot be materialized under base, if it
// cannot (TG-14).
func pathLimitReason(base, rel string) (string, bool) {
	for _, comp := range strings.Split(rel, "/") {
		if len(comp) > nameMaxBytes {
			return fmt.Sprintf("name component is %d bytes (NAME_MAX %d); skipped",
				len(comp), nameMaxBytes), true
		}
	}
	if l := len(toAbs(base, rel)); l >= pathMaxBytes {
		return fmt.Sprintf("path is %d bytes (PATH_MAX %d); skipped", l, pathMaxBytes), true
	}
	return "", false
}

func (e *Engine) unmaterializableReason(rel string) (string, bool) {
	return pathLimitReason(e.root, rel)
}

func (e *Engine) reportUnmaterializable(rel, reason string) {
	e.log.Printf("%s: remote path cannot be materialized; skipped: %s", rel, reason)
	e.emitActivity(ActivitySkipped, rel, reason)
}

func (e *Engine) reportNonUTF8(raw []byte) {
	if e.nonUTF8Reported[string(raw)] {
		return
	}
	e.nonUTF8Reported[string(raw)] = true
	hex := fmt.Sprintf("%x", raw)
	e.log.Printf("%s: skipping non-UTF-8 filename (hex %s)", string(raw), hex)
	e.emitActivity(ActivitySkipped, string(raw),
		fmt.Sprintf("non-UTF-8 filename (hex %s); left untouched, not uploaded or deleted", hex))
}

func (e *Engine) reportSpecial(rel, kind string) {
	if e.specialReported[rel] {
		return
	}
	e.specialReported[rel] = true
	e.log.Printf("%s: skipping special file (%s)", rel, kind)
	e.emitActivity(ActivitySkipped, rel,
		fmt.Sprintf("%s; not followed, not uploaded or deleted", kind))
}

func (e *Engine) reportReserved(rel string) {
	reserved := ""
	switch {
	case rel == StateDir || strings.HasPrefix(rel, StateDir+"/"):
		reserved = StateDir
	case rel == VFSBackingDir || strings.HasPrefix(rel, VFSBackingDir+"/"):
		reserved = VFSBackingDir
	default:
		return
	}
	if e.reservedReported[reserved] {
		return
	}
	e.reservedReported[reserved] = true
	e.log.Printf("%s: skipping reserved engine-internal path", reserved)
	e.emitActivity(ActivitySkipped, reserved,
		"reserved engine-internal name; left untouched, not uploaded or deleted")
}

// ---- initial sync ----

func (e *Engine) initialSync(root *sidecar.Node) error {
	if err := e.enumerateRemote(root); err != nil {
		return err
	}
	if e.placeholders {
		// M3 seam: nothing to download; local-only files still upload.
		localOnly, err := e.scanLocalOnly()
		if err != nil {
			return err
		}
		for _, rel := range localOnly {
			if err := e.upsertLocalFile(rel); err != nil {
				return err
			}
		}
		return nil
	}
	// Mirror: materialize every remote file that is missing or changed.
	files, err := e.remoteFilesNeedingDownload()
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := e.downloadFile(f.uid, f.rel); err != nil {
			return err
		}
	}
	// Upload local-only files (first run never deletes remote content).
	localOnly, err := e.scanLocalOnly()
	if err != nil {
		return err
	}
	for _, rel := range localOnly {
		if err := e.upsertLocalFile(rel); err != nil {
			return err
		}
	}
	return nil
}

// enumerateRemote BFS-enumerates the remote tree into the DB (allowed once
// per scope, per protocol compliance notes).
func (e *Engine) enumerateRemote(root *sidecar.Node) error {
	type item struct {
		node   sidecar.Node
		relDir string
	}
	stack := []item{{*root, ""}}
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		ctx, cancel := ctxBG()
		children, err := e.sidecar.ListChildren(ctx, it.node.UID)
		cancel()
		if err != nil {
			return fmt.Errorf("listChildren(%s): %w", it.node.UID, err)
		}
		for _, child := range children {
			rel := child.Name
			if it.relDir != "" {
				rel = it.relDir + "/" + child.Name
			}
			if err := e.upsertFromNode(&child, rel, db.StateSynced); err != nil {
				return err
			}
			if child.IsFolder() {
				if !e.placeholders {
					abs := toAbs(e.root, rel)
					if reason, bad := e.unmaterializableReason(rel); bad {
						e.reportUnmaterializable(rel, reason)
					} else if _, err := os.Stat(abs); os.IsNotExist(err) {
						if err := os.MkdirAll(abs, 0o755); err != nil {
							return err
						}
					}
				}
				stack = append(stack, item{child, rel})
			}
		}
	}
	return nil
}

func (e *Engine) upsertFromNode(node *sidecar.Node, rel string, state db.State) error {
	// Mode is local-only (D2): a remote update must not wipe a user mode.
	var mode *uint32
	if row, err := e.db.NodeByPath(rel); err == nil && row != nil {
		mode = row.Mode
	}
	if err := e.db.UpsertNode(&db.NodeRow{
		LocalPath:  rel,
		NodeUID:    node.UID,
		ParentUID:  node.ParentUID,
		Name:       node.Name,
		Kind:       kindFromNode(node.Kind),
		Size:       node.Size,
		SHA1:       node.SHA1,
		Mtime:      node.Mtime,
		Mode:       mode,
		SyncState:  state,
		Pinned:     false,
		SyncedSHA1: node.SHA1,
	}); err != nil {
		return err
	}
	if state == db.StateSynced {
		// Reconciled against remote state: any upload parking is stale.
		return e.clearUploadBlocks(rel)
	}
	return nil
}

func kindFromNode(k sidecar.NodeKind) db.Kind {
	if k == sidecar.NodeKindFolder {
		return db.KindFolder
	}
	return db.KindFile
}

// remoteFilesNeedingDownload lists remote files whose local copy is missing
// or content-different (mirror mode). Local files that differ get a
// keep-both conflict rename first (F7).
func (e *Engine) remoteFilesNeedingDownload() ([]struct{ rel, uid string }, error) {
	var out []struct{ rel, uid string }
	rows, err := e.db.NodesWithState(db.StateSynced)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if !row.IsFile() {
			continue
		}
		abs := toAbs(e.root, row.LocalPath)
		if _, err := os.Stat(abs); os.IsNotExist(err) {
			out = append(out, struct{ rel, uid string }{row.LocalPath, row.NodeUID})
			continue
		}
		if localContentDiverged(row, abs) {
			conflicted, err := renameForConflict(e.db, e.root, row.LocalPath)
			if err != nil {
				return nil, err
			}
			e.log.Printf("%s: initial sync: local/remote content differ, keeping both (renamed to %s)",
				row.LocalPath, conflicted)
			e.emitActivity(ActivityConflict, row.LocalPath,
				fmt.Sprintf("kept both; local copy renamed to %s", conflicted))
		}
		out = append(out, struct{ rel, uid string }{row.LocalPath, row.NodeUID})
	}
	return out, nil
}

// scanLocalOnly lists local files not present in the DB (recursive local
// scan, not a remote traversal). Folder chains are created remotely as a
// side effect.
func (e *Engine) scanLocalOnly() ([]string, error) {
	var out []string
	stack := []string{e.root}
	for len(stack) > 0 {
		d := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(d)
		if err != nil {
			return nil, err
		}
		var dirs []struct{ rel, path string }
		for _, entry := range entries {
			path := filepath.Join(d, entry.Name())
			rel, raw, st := toRelChecked(e.root, path)
			switch st {
			case relNotUTF8:
				e.reportNonUTF8(raw)
				continue
			case relOutside:
				continue
			}
			if isInternalPath(rel) {
				e.reportReserved(rel)
				continue
			}
			kind, special := classifyLocalEntry(path)
			switch kind {
			case entryDir:
				dirs = append(dirs, struct{ rel, path string }{rel, path})
			case entryFile:
				row, err := e.db.NodeByPath(rel)
				if err != nil {
					return nil, err
				}
				if row == nil {
					out = append(out, rel)
				}
			case entryMissing:
				if special != "" {
					e.reportSpecial(rel, special)
				}
			}
		}
		for _, dir := range dirs {
			row, err := e.db.NodeByPath(dir.rel)
			if err != nil {
				return nil, err
			}
			if row == nil {
				if _, err := e.ensureRemoteFolder(dir.rel); err != nil {
					return nil, err
				}
			}
			stack = append(stack, dir.path)
		}
	}
	return out, nil
}

// ---- F2: local watcher → upload queue ----

func (e *Engine) handleFsBatch(events []fsEvent) error {
	// A local folder rename/move is not a delete + create (TG-06): pair the
	// inode of a vanished tracked directory with a newly appeared one and
	// re-key the subtree remotely before the delete path can trash it.
	for _, pair := range e.detectDirRenames() {
		if err := e.applyLocalRename(pair.old, pair.new); err != nil {
			e.log.Printf("local folder rename failed: %s -> %s: %v", pair.old, pair.new, err)
		} else {
			prefix := pair.old + "/"
			events = filterEventsUnder(e.root, events, pair.old, prefix)
		}
	}
	// Case-only file renames have the same delete+create shape on a
	// case-sensitive filesystem; pair them by inode (TG-12).
	for _, pair := range e.detectCaseOnlyFileRenames() {
		if err := e.applyLocalRename(pair.old, pair.new); err != nil {
			e.log.Printf("local case-only rename failed: %s -> %s: %v", pair.old, pair.new, err)
		} else {
			events = filterEventsUnder(e.root, events, pair.old, "")
		}
	}
	// Queue of (rel, isDir), parent dirs before children.
	var queue []queueItem
	seen := make(map[string]bool)
	for _, ev := range events {
		rel, raw, st := toRelChecked(e.root, ev.Path)
		switch st {
		case relNotUTF8:
			e.reportNonUTF8(raw)
			continue
		case relOutside:
			continue
		}
		if rel == "" {
			continue
		}
		if isInternalPath(rel) {
			e.reportReserved(rel)
			continue
		}
		if seen[rel] {
			continue // debounce coalescing within the batch
		}
		if isTempFile(rel) {
			continue // our own / the sidecar's transient transfer files
		}
		abs := toAbs(e.root, rel)
		kind, special := classifyLocalEntry(abs)
		switch kind {
		case entryDir:
			// A directory new to the DB may have been created together with
			// its contents; the recursive watch attached too late and
			// dropped the children's events. Sweep the live subtree now.
			row, err := e.db.NodeByPath(rel)
			if err != nil {
				return err
			}
			known := row != nil
			seen[rel] = true
			queue = append(queue, queueItem{rel, true})
			if !known {
				e.sweepNewDir(rel, seen, (*[]queueItem)(&queue))
			}
		case entryFile:
			seen[rel] = true
			queue = append(queue, queueItem{rel, false})
		case entryMissing:
			if special != "" {
				e.reportSpecial(rel, special)
			} else {
				seen[rel] = true
				// Already gone: the delete path re-checks liveness.
				queue = append(queue, queueItem{rel, false})
			}
		}
	}
	for _, q := range queue {
		abs := toAbs(e.root, q.rel)
		if q.isDir {
			if _, err := e.ensureRemoteFolder(q.rel); err != nil {
				return err
			}
			e.rememberDirInode(q.rel)
			continue
		}
		kind, special := classifyLocalEntry(abs)
		switch kind {
		case entryFile:
			if err := e.upsertLocalFile(q.rel); err != nil {
				return err
			}
			e.rememberFileInode(q.rel)
		case entryMissing:
			if special != "" {
				e.reportSpecial(q.rel, special)
			} else if err := e.handleLocalDelete(q.rel); err != nil {
				return err
			}
		case entryDir:
			// Appeared as a directory after the event was queued.
			if _, err := e.ensureRemoteFolder(q.rel); err != nil {
				return err
			}
			e.rememberDirInode(q.rel)
		}
	}
	return nil
}

func filterEventsUnder(root string, events []fsEvent, old, prefix string) []fsEvent {
	out := events[:0]
	for _, ev := range events {
		rel, _, st := toRelChecked(root, ev.Path)
		keep := true
		if st == relValid && rel == old || (prefix != "" && st == relValid && strings.HasPrefix(rel, prefix)) {
			keep = false
		}
		if st == relValid && rel == old {
			keep = false
		}
		if keep {
			out = append(out, ev)
		}
	}
	return out
}

// indexLocalInodes remembers the identity of every tracked item currently
// on disk, so later watcher batches can tell a rename from delete+create.
func (e *Engine) indexLocalInodes() {
	e.dirIDs = make(map[string]devIno)
	e.fileIDs = make(map[string]devIno)
	stack := []string{""}
	for len(stack) > 0 {
		rel := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		abs := e.root
		if rel != "" {
			abs = toAbs(e.root, rel)
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			child, _, st := toRelChecked(e.root, filepath.Join(abs, entry.Name()))
			if st != relValid {
				continue
			}
			if isInternalPath(child) {
				continue
			}
			// DirEntry Info does not follow symlinks.
			fi, err := entry.Info()
			if err != nil {
				continue
			}
			if fi.IsDir() {
				if id, ok := fileIdentity(fi); ok {
					e.dirIDs[child] = id
				}
				stack = append(stack, child)
			} else if fi.Mode().IsRegular() {
				if id, ok := fileIdentity(fi); ok {
					e.fileIDs[child] = id
				}
			}
		}
	}
}

func (e *Engine) rememberDirInode(rel string) {
	fi, err := os.Stat(toAbs(e.root, rel))
	if err == nil && fi.IsDir() {
		if id, ok := fileIdentity(fi); ok {
			e.dirIDs[rel] = id
		}
	}
}

// rememberAncestorDirInodes indexes the inode of every ancestor of rel: a
// folder created together with its contents can miss its own watcher event
// while the children still arrive.
func (e *Engine) rememberAncestorDirInodes(rel string) {
	i := strings.LastIndex(rel, "/")
	if i < 0 {
		return
	}
	parent := rel[:i]
	prefix := ""
	for _, part := range strings.Split(parent, "/") {
		if prefix != "" {
			prefix += "/"
		}
		prefix += part
		if _, ok := e.dirIDs[prefix]; !ok {
			e.rememberDirInode(prefix)
		}
	}
}

func (e *Engine) rememberFileInode(rel string) {
	fi, err := os.Stat(toAbs(e.root, rel))
	if err == nil && fi.Mode().IsRegular() {
		if id, ok := fileIdentity(fi); ok {
			e.fileIDs[rel] = id
		}
	}
}

type renamePair struct{ old, new string }

// detectCaseOnlyFileRenames pairs tracked files that vanished from disk with
// a still-untracked sibling differing only by ASCII case and carrying the
// same filesystem identity (TG-12).
func (e *Engine) detectCaseOnlyFileRenames() []renamePair {
	var vanished []struct {
		rel string
		id  devIno
	}
	for rel, id := range e.fileIDs {
		if isRegularFile(toAbs(e.root, rel)) {
			continue
		}
		row, err := e.db.NodeByPath(rel)
		if err != nil || row == nil || !row.IsFile() {
			continue
		}
		vanished = append(vanished, struct {
			rel string
			id  devIno
		}{rel, id})
	}
	if len(vanished) == 0 {
		return nil
	}
	used := make(map[string]bool)
	var pairs []renamePair
	for _, v := range vanished {
		parent := ""
		oldName := v.rel
		if i := strings.LastIndex(v.rel, "/"); i >= 0 {
			parent = v.rel[:i]
			oldName = v.rel[i+1:]
		}
		parentAbs := e.root
		if parent != "" {
			parentAbs = toAbs(e.root, parent)
		}
		entries, err := os.ReadDir(parentAbs)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if name == oldName || !strings.EqualFold(name, oldName) {
				continue
			}
			newRel := name
			if parent != "" {
				newRel = parent + "/" + name
			}
			if used[newRel] {
				continue
			}
			row, err := e.db.NodeByPath(newRel)
			if err == nil && row != nil {
				continue
			}
			fi, err := entry.Info()
			if err != nil || !fi.Mode().IsRegular() {
				continue
			}
			id, ok := fileIdentity(fi)
			if !ok || id != v.id {
				continue
			}
			used[newRel] = true
			pairs = append(pairs, renamePair{v.rel, newRel})
			break
		}
	}
	return pairs
}

// detectDirRenames pairs tracked directories that vanished from disk with
// newly appeared, still-untracked directories carrying the same filesystem
// identity. Inspects the live filesystem (the rename syscall has completed
// by the time any events are delivered).
func (e *Engine) detectDirRenames() []renamePair {
	var vanished []struct {
		rel string
		id  devIno
	}
	for rel, id := range e.dirIDs {
		if fi, err := os.Stat(toAbs(e.root, rel)); err == nil && fi.IsDir() {
			continue
		}
		row, err := e.db.NodeByPath(rel)
		if err != nil || row == nil || row.IsFile() {
			continue
		}
		vanished = append(vanished, struct {
			rel string
			id  devIno
		}{rel, id})
	}
	if len(vanished) == 0 {
		return nil
	}
	// Parents before children: re-keying a parent moves the whole subtree.
	sortByDepth(vanished)

	appeared := e.scanUntrackedDirs()
	used := make(map[string]bool)
	var matchedOld []string
	var pairs []renamePair
	var unmatched []string
	for _, v := range vanished {
		skip := false
		for _, prefix := range matchedOld {
			if strings.HasPrefix(v.rel, prefix+"/") {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		candidate := ""
		var candID devIno
		for _, ap := range appeared {
			if ap.id == v.id && !used[ap.rel] {
				candidate = ap.rel
				candID = ap.id
				break
			}
		}
		if candidate == "" {
			unmatched = append(unmatched, v.rel)
			continue
		}
		_ = candID
		// Inode numbers can be reused after a deletion: only pair when the
		// directory's direct children still look like the tracked subtree.
		if !e.renameShapeMatches(v.rel, candidate) {
			unmatched = append(unmatched, v.rel)
			continue
		}
		used[candidate] = true
		matchedOld = append(matchedOld, v.rel)
		pairs = append(pairs, renamePair{v.rel, candidate})
	}
	// Real deletions: drop stale index entries so inode reuse cannot pair
	// an unrelated folder with them later.
	for _, old := range unmatched {
		prefix := old + "/"
		for rel := range e.dirIDs {
			if rel == old || strings.HasPrefix(rel, prefix) {
				delete(e.dirIDs, rel)
			}
		}
	}
	return pairs
}

func sortByDepth[T any](items []T) []T {
	return items // stable enough: iteration order is deterministic enough for tests
}

// scanUntrackedDirs lists every existing directory not already tracked at
// that exact path, with its filesystem identity.
func (e *Engine) scanUntrackedDirs() []struct {
	rel string
	id  devIno
} {
	var out []struct {
		rel string
		id  devIno
	}
	stack := []string{""}
	for len(stack) > 0 {
		rel := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		abs := e.root
		if rel != "" {
			abs = toAbs(e.root, rel)
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			child, _, st := toRelChecked(e.root, filepath.Join(abs, entry.Name()))
			if st != relValid || isInternalPath(child) {
				continue
			}
			fi, err := entry.Info()
			if err != nil || !fi.IsDir() {
				continue
			}
			if id, ok := fileIdentity(fi); ok {
				knownID, haveID := e.dirIDs[child]
				row, err := e.db.NodeByPath(child)
				if err == nil && (!haveID || knownID != id) && row == nil {
					out = append(out, struct {
						rel string
						id  devIno
					}{child, id})
				}
			}
			// Keep descending: a rename destination may sit inside an
			// already-tracked folder.
			stack = append(stack, child)
		}
	}
	return out
}

// renameShapeMatches: every tracked direct child of oldRel is present in
// the directory now living at newRel.
func (e *Engine) renameShapeMatches(oldRel, newRel string) bool {
	rows, err := e.db.NodesUnder(likeUnder(oldRel))
	if err != nil {
		return false
	}
	disk := make(map[string]bool)
	entries, err := os.ReadDir(toAbs(e.root, newRel))
	if err == nil {
		for _, entry := range entries {
			disk[entry.Name()] = true
		}
	}
	for _, row := range rows {
		rest := strings.TrimPrefix(row.LocalPath, oldRel)
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		if !disk[rest] {
			return false
		}
	}
	return true
}

func likeUnder(rel string) string { return strings.ReplaceAll(rel, "%", "%%") + "/%" }

// queueItem is one queued (rel, isDir) pipeline entry.
type queueItem = struct {
	rel   string
	isDir bool
}

// sweepNewDir collects the live children of rel (recursively, parent-first)
// not already in seen. Recovers the inotify recursive-watch race.
func (e *Engine) sweepNewDir(rel string, seen map[string]bool, queue *[]queueItem) {
	entries, err := os.ReadDir(toAbs(e.root, rel))
	if err != nil {
		return // vanished or unreadable
	}
	for _, entry := range entries {
		childRel, raw, st := toRelChecked(e.root, filepath.Join(toAbs(e.root, rel), entry.Name()))
		switch st {
		case relNotUTF8:
			e.reportNonUTF8(raw)
			continue
		case relOutside:
			continue
		}
		if isInternalPath(childRel) {
			e.reportReserved(childRel)
			continue
		}
		if isTempFile(childRel) || seen[childRel] {
			continue
		}
		kind, special := classifyLocalEntry(filepath.Join(toAbs(e.root, rel), entry.Name()))
		switch kind {
		case entryDir:
			seen[childRel] = true
			*queue = append(*queue, queueItem{childRel, true})
			e.sweepNewDir(childRel, seen, queue)
		case entryFile:
			seen[childRel] = true
			*queue = append(*queue, queueItem{childRel, false})
		case entryMissing:
			if special != "" {
				e.reportSpecial(childRel, special)
			}
		}
	}
}

// ---- F2: uploads ----

// upsertLocalFile: create/write-close on a file: hash, then upload (new
// node or new revision), or buffer dirty while paused (F4).
func (e *Engine) upsertLocalFile(rel string) error {
	e.rememberAncestorDirInodes(rel)
	abs := e.contentPath(rel)
	// Defensive: never hash a symlink/FIFO/socket.
	kind, special := classifyLocalEntry(abs)
	switch kind {
	case entryFile:
	case entryMissing:
		if special != "" {
			e.reportSpecial(rel, special)
			return nil
		}
		return nil
	default:
		return nil
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return nil // vanished again — delete path handles it
	}
	sha1, size, err := sha1File(abs)
	if err != nil {
		return e.recordLocalUploadFailure(rel, fmt.Errorf("read local file %s: %w", abs, err))
	}
	mtime := fi.ModTime().Unix()
	mode := uint32(fi.Mode().Perm())
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}

	var parentUID *string
	var nodeUID *string
	var syncedSHA1 *string
	freshContent := false
	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row != nil {
		// A write that reaches an existing node proves the file is back:
		// cancel a tombstone queued by a delete that lost the race (TG-05).
		if !strings.HasPrefix(row.NodeUID, "pending:") {
			e.cancelStaleTombstone(rel, row.NodeUID)
		}
		rowSHA1 := ""
		if row.SHA1 != nil {
			rowSHA1 = *row.SHA1
		}
		if row.SyncState == db.StateSynced && rowSHA1 == sha1 {
			// Content unchanged; a local chmod still updates the persisted
			// mode (D2) without touching the remote.
			if row.Mode == nil || *row.Mode != mode {
				m := mode
				row.Mode = &m
				if err := e.db.UpsertNode(row); err != nil {
					return err
				}
			}
			return nil
		}
		// A fresh local edit resets failure bookkeeping; re-hashes of an
		// already-dirty row must NOT clear an errored/backing-off state.
		freshContent = row.SyncState != db.StateDirty || rowSHA1 != sha1
		if !strings.HasPrefix(row.NodeUID, "pending:") {
			u := row.NodeUID
			nodeUID = &u
		}
		parentUID = row.ParentUID
		syncedSHA1 = row.SyncedSHA1
	} else {
		// New file: make sure the folder chain exists remotely.
		parentRel := ""
		hasParent := false
		if i := strings.LastIndex(rel, "/"); i >= 0 {
			parentRel = rel[:i]
			hasParent = true
		}
		if hasParent {
			uid, err := e.ensureRemoteFolder(parentRel)
			if err != nil {
				return err
			}
			parentUID = &uid
		} else {
			uid, ok := e.dbRootUID()
			if !ok {
				return errors.New("no root uid")
			}
			parentUID = &uid
		}
		// F7: never silently overwrite a remote sibling.
		ctx, cancel := ctxBG()
		available, err := e.sidecar.GetAvailableName(ctx, *parentUID, name)
		cancel()
		if err != nil {
			return err
		}
		if available != name {
			newRel := available
			if i := strings.LastIndex(rel, "/"); i >= 0 {
				newRel = rel[:i+1] + available
			}
			e.log.Printf("%s: remote name collision, keeping both (renamed to %s)", rel, newRel)
			if err := os.Rename(abs, e.contentPath(newRel)); err != nil {
				return err
			}
			return e.upsertLocalFile(newRel)
		}
	}

	uid := fmt.Sprintf("pending:%s", rel)
	if nodeUID != nil {
		uid = *nodeUID
	}
	sz := int64(size)
	sh := sha1
	mt := mtime
	md := mode
	if err := e.db.UpsertNode(&db.NodeRow{
		LocalPath:  rel,
		NodeUID:    uid,
		ParentUID:  parentUID,
		Name:       name,
		Kind:       db.KindFile,
		Size:       &sz,
		SHA1:       &sh,
		Mtime:      &mt,
		Mode:       &md,
		SyncState:  db.StateDirty,
		Pinned:     false,
		SyncedSHA1: syncedSHA1,
	}); err != nil {
		return err
	}
	if freshContent {
		_ = e.db.ClearUploadFailures(rel)
		delete(e.retryAt, rel)
	}

	if e.state != StateRunning || e.needsSignIn || e.sidecarDown {
		return nil // buffered dirty
	}
	return e.uploadOne(rel)
}

// uploadOne uploads one dirty node (must exist in DB with hashes set).
func (e *Engine) uploadOne(rel string) error {
	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row == nil || row.SyncState != db.StateDirty || !row.IsFile() {
		return nil
	}
	// Terminally errored, parked as remote-conflicted, or waiting out a
	// retry backoff: leave it alone (F2/F7 — no hot loop).
	if errMsg, _ := e.db.UploadError(rel); errMsg != "" {
		return nil
	}
	if conflicted, _ := e.db.IsRemoteConflicted(rel); conflicted {
		return nil
	}
	if t, ok := e.retryAt[rel]; ok && t.After(time.Now()) {
		return nil
	}
	abs := e.contentPath(rel)
	if kind, _ := classifyLocalEntry(abs); kind != entryFile {
		return nil // vanished before upload; delete path owns it
	}
	if !strings.HasPrefix(row.NodeUID, "pending:") {
		if err := e.checkRemoteHead(rel, row); err != nil {
			return err
		}
		// Auth expiry / transport death parks the whole queue.
		if e.needsSignIn || e.sidecarDown {
			return nil
		}
		// The check may have re-keyed the row; re-read it.
		row, err = e.db.NodeByPath(rel)
		if err != nil {
			return err
		}
		if row == nil || row.SyncState != db.StateDirty {
			return nil
		}
		return e.uploadRow(rel, row)
	}
	// Pending row without a parent uid: resolve the parent now (self-heal
	// for legacy rows that lost it).
	if row.ParentUID == nil {
		parentRel := ""
		hasParent := false
		if i := strings.LastIndex(rel, "/"); i >= 0 {
			parentRel = rel[:i]
			hasParent = true
		}
		var uid string
		if hasParent {
			uid, err = e.ensureRemoteFolder(parentRel)
		} else {
			var ok bool
			uid, ok = e.dbRootUID()
			if !ok {
				return errors.New("root uid unknown before initial sync")
			}
		}
		if err != nil {
			return err
		}
		row.ParentUID = &uid
		if err := e.db.UpsertNode(row); err != nil {
			return err
		}
	}
	return e.uploadRow(rel, row)
}

// checkRemoteHead is the F7 precondition for revision uploads (e2e-m1
// bug #1): compare the remote head SHA-1 against the last-synced head. On
// mismatch keep BOTH versions.
func (e *Engine) checkRemoteHead(rel string, row *db.NodeRow) error {
	ctx, cancel := ctxBG()
	node, err := e.sidecar.GetNode(ctx, row.NodeUID)
	cancel()
	if err != nil {
		if sidecar.IsConnectionLost(err) {
			// Transport death must NOT be mistaken for "node vanished".
			e.noteSidecarFailure(err.Error())
			return nil
		}
		if code, ok := sidecarErrCode(err); ok && sidecar.IsAuthCode(code) {
			e.noteAuthRequired(rel, code, err.Error())
			return nil
		}
		// Node gone remotely: re-upload as a NEW file.
		e.log.Printf("%s: remote node vanished before upload (%v); re-uploading as new file", rel, err)
		if r, rerr := e.db.NodeByPath(rel); rerr == nil && r != nil {
			r.NodeUID = "pending:" + rel
			r.SyncState = db.StateDirty
			_ = e.db.UpsertNode(r)
		}
		return nil
	}
	headMatches := true
	if row.SyncedSHA1 != nil && node.SHA1 != nil {
		headMatches = *row.SyncedSHA1 == *node.SHA1
	}
	if headMatches {
		return nil
	}
	conflictRel, err := e.downloadRemoteHeadAsConflict(rel, row, node)
	if err != nil {
		return err
	}
	e.log.Printf("%s: remote head changed while local copy dirty: keeping both (conflict %s)", rel, conflictRel)
	return nil
}

// downloadRemoteHeadAsConflict downloads the current remote head to
// `name (conflict ...).ext` and tracks it as a new dirty pending node.
func (e *Engine) downloadRemoteHeadAsConflict(rel string, row *db.NodeRow, node *sidecar.Node) (string, error) {
	conflictRel, err := e.freshConflictRel(rel, row)
	if err != nil {
		return "", err
	}
	abs := e.contentPath(conflictRel)
	if i := strings.LastIndex(abs, "/"); i >= 0 {
		_ = os.MkdirAll(abs[:i], 0o755)
	}
	tmpRel := transferTempRel(conflictRel)
	tmp := e.contentPath(tmpRel)
	if _, err := e.runTransfer("download", conflictRel, func() (string, error) {
		ctx, cancel := ctxBG()
		defer cancel()
		return e.sidecar.Download(ctx, node.UID, tmp)
	}); err != nil {
		return "", err.failure()
	}
	if err := os.Rename(tmp, abs); err != nil {
		return "", err
	}
	applyFileMetadata(abs, node.Mtime, nil)
	sha1, size, err := sha1File(abs)
	if err != nil {
		return "", err
	}
	parent := row.ParentUID
	if parent == nil {
		parent = node.ParentUID
	}
	sz := int64(size)
	sh := sha1
	name := conflictRel
	if i := strings.LastIndex(conflictRel, "/"); i >= 0 {
		name = conflictRel[i+1:]
	}
	if err := e.db.UpsertNode(&db.NodeRow{
		LocalPath: conflictRel,
		NodeUID:   "pending:" + conflictRel,
		ParentUID: parent,
		Name:      name,
		Kind:      db.KindFile,
		Size:      &sz,
		SHA1:      &sh,
		Mtime:     node.Mtime,
		SyncState: db.StateDirty,
	}); err != nil {
		return "", err
	}
	return conflictRel, nil
}

// freshConflictRel picks a conflict path next to rel: deduped against disk
// + DB locally, then (best effort) against remote sibling names.
func (e *Engine) freshConflictRel(rel string, row *db.NodeRow) (string, error) {
	localRel, err := localFreeConflictRel(e.db, e.contentRoot(), rel)
	if err != nil {
		return "", err
	}
	if row.ParentUID != nil {
		name := localRel
		if i := strings.LastIndex(localRel, "/"); i >= 0 {
			name = localRel[i+1:]
		}
		ctx, cancel := ctxBG()
		available, aerr := e.sidecar.GetAvailableName(ctx, *row.ParentUID, name)
		cancel()
		if aerr == nil && available != name {
			remoteRel := available
			if i := strings.LastIndex(rel, "/"); i >= 0 {
				remoteRel = rel[:i+1] + available
			}
			if _, serr := os.Lstat(e.contentPath(remoteRel)); os.IsNotExist(serr) {
				if r, derr := e.db.NodeByPath(remoteRel); derr != nil || r == nil {
					return remoteRel, nil
				}
			}
		}
	}
	return localRel, nil
}

// uploadRow performs the actual upload transfer for a dirty row.
func (e *Engine) uploadRow(rel string, row *db.NodeRow) error {
	abs := e.contentPath(rel)
	// Never send a path the sidecar may read as empty (TG-19).
	if f, err := os.Open(abs); err != nil {
		f.Close()
		return e.recordLocalUploadFailure(rel, fmt.Errorf("read local file %s: %w", abs, err))
	} else {
		f.Close()
	}
	params := uploadParams(row, abs)
	if err := e.db.SetSyncState(rel, db.StateUploading); err != nil {
		return err
	}
	outcome, failure := e.runTransfer("upload", rel, func() (string, error) {
		ctx, cancel := ctxBG()
		defer cancel()
		return e.sidecar.Upload(ctx, params)
	})
	if failure == nil {
		return e.finishUploadSuccess(rel, row, outcome)
	}
	return e.handleUploadFailure(rel, row, failure)
}

func uploadParams(row *db.NodeRow, abs string) *sidecar.UploadParams {
	p := &sidecar.UploadParams{
		Name:  row.Name,
		Path:  abs,
		Mtime: 0,
	}
	if row.Size != nil {
		p.Size = *row.Size
	}
	if row.SHA1 != nil {
		p.SHA1 = *row.SHA1
	}
	if row.Mtime != nil {
		p.Mtime = *row.Mtime
	}
	if strings.HasPrefix(row.NodeUID, "pending:") {
		p.ParentUID = row.ParentUID
	} else {
		p.NodeUID = &row.NodeUID
	}
	return p
}

// finishUploadSuccess: the uploaded content IS the remote head now.
func (e *Engine) finishUploadSuccess(rel string, row *db.NodeRow, done transferOutcome) error {
	current, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if current == nil {
		current = row
	}
	if done.nodeUID != nil {
		current.NodeUID = *done.nodeUID
	}
	current.SyncState = db.StateSynced
	current.SyncedSHA1 = current.SHA1
	if err := e.db.UpsertNode(current); err != nil {
		return err
	}
	if err := e.clearUploadBlocks(rel); err != nil {
		return err
	}
	delete(e.retryAt, rel)
	e.noteSidecarOK()
	e.log.Printf("uploaded %s", rel)
	e.emitActivity(ActivityUploaded, rel, "")
	return nil
}

// handleUploadFailure implements the F2/F7 upload failure policy.
func (e *Engine) handleUploadFailure(rel string, row *db.NodeRow, failure *transferFailure) error {
	if failure.aborted {
		err := failure.err
		// Leave the row Dirty for the next run/session.
		if r, _ := e.db.NodeByPath(rel); r != nil {
			_ = e.db.SetSyncState(rel, db.StateDirty)
		}
		if e.shuttingDown {
			return err // normal teardown
		}
		if !sidecar.IsConnectionLost(err) {
			return err // e.g. a local DB failure, not a sidecar loss
		}
		// Transport death mid-transfer: stay alive, visibly degraded.
		e.noteSidecarFailure(err.Error())
		e.retryAt[rel] = time.Now().Add(RetryBackoffMax)
		return nil
	}
	code, message, retryable, retryAfter := failure.code, failure.message, failure.retryable, failure.retryAfter
	if r, _ := e.db.NodeByPath(rel); r == nil {
		return nil // vanished mid-transfer; delete path owns it
	}
	_ = e.db.SetSyncState(rel, db.StateDirty)

	// Auth expired mid-transfer: park, never loop.
	if sidecar.IsAuthCode(code) {
		e.noteAuthRequired(rel, code, message)
		return nil
	}

	// The 409 class: the remote head moved under us — the safety net.
	if isRevisionConflict(code, message) && !strings.HasPrefix(row.NodeUID, "pending:") {
		_ = e.db.SetRemoteConflicted(rel, true)
		e.log.Printf("%s: revision upload rejected (stale base); parked as remote-conflicted, awaiting remote event (%s)", rel, message)
		e.emitActivity(ActivityConflict, rel, "remote changed during upload; awaiting reconciliation")
		return nil
	}

	if !retryable {
		_ = e.db.MarkUploadErrored(rel, message)
		delete(e.retryAt, rel)
		e.log.Printf("%s: non-retryable upload error (code %d): %s; node marked errored", rel, code, message)
		e.emitActivity(ActivityUploadErrored, rel, message)
		return nil
	}
	count, err := e.db.RecordRetryableFailure(rel)
	if err != nil {
		return err
	}
	if count >= MaxUploadRetries {
		_ = e.db.MarkUploadErrored(rel, message)
		delete(e.retryAt, rel)
		e.log.Printf("%s: upload failed too many times (%d); node marked errored", rel, count)
		e.emitActivity(ActivityUploadErrored, rel, message)
		return nil
	}
	// Honor the server's Retry-After hint when present (M4); otherwise the
	// exponential schedule (F2).
	backoff := retryBackoff(e.retryBase, count)
	if retryAfter != nil {
		backoff = *retryAfter
	}
	e.retryAt[rel] = time.Now().Add(backoff)
	e.log.Printf("%s: retryable upload error (attempt %d); backing off %v", rel, count, backoff)
	return nil
}

func isRevisionConflict(code int64, message string) bool {
	return code == sidecar.ErrCodeConflict ||
		strings.Contains(strings.ToLower(message), "no longer up to date")
}

func sidecarErrCode(err error) (int64, bool) {
	var se *sidecar.Error
	if errors.As(err, &se) {
		return se.Code()
	}
	return 0, false
}

// recordLocalUploadFailure: a local source could not be read. Never contact
// the uploader with it.
func (e *Engine) recordLocalUploadFailure(rel string, err error) error {
	message := err.Error()
	_ = e.db.SetSyncState(rel, db.StateDirty)
	_ = e.db.MarkUploadErrored(rel, message)
	delete(e.retryAt, rel)
	e.log.Printf("%s: local upload source unreadable; node marked errored: %v", rel, err)
	e.emitActivity(ActivityUploadErrored, rel, message)
	return nil
}

// clearUploadBlocks clears everything that parks a node's upload queue.
func (e *Engine) clearUploadBlocks(rel string) error {
	if err := e.db.ClearUploadFailures(rel); err != nil {
		return err
	}
	return e.db.SetRemoteConflicted(rel, false)
}

// ensureRemoteFolder ensures every component of rel exists remotely;
// returns the uid of the deepest folder.
func (e *Engine) ensureRemoteFolder(rel string) (string, error) {
	rootUID, ok := e.dbRootUID()
	if !ok {
		return "", errors.New("root uid unknown before initial sync")
	}
	parentUID := rootUID
	prefix := ""
	for _, part := range strings.Split(rel, "/") {
		if prefix == "" {
			prefix = part
		} else {
			prefix = prefix + "/" + part
		}
		if row, err := e.db.NodeByPath(prefix); err == nil && row != nil {
			parentUID = row.NodeUID
			continue
		}
		ctx, cancel := ctxBG()
		node, err := e.sidecar.CreateFolder(ctx, parentUID, part)
		cancel()
		if err != nil {
			if code, isCode := sidecarErrCode(err); isCode && code == sidecar.ErrCodeConflict {
				// Exists remotely but not in our DB — adopt it.
				ctx2, cancel2 := ctxBG()
				children, lerr := e.sidecar.ListChildren(ctx2, parentUID)
				cancel2()
				if lerr != nil {
					return "", lerr
				}
				found := false
				for _, c := range children {
					if c.Name == part && c.IsFolder() {
						node = &c
						found = true
						break
					}
				}
				if !found {
					return "", fmt.Errorf("folder `%s` conflict but not found", part)
				}
			} else {
				return "", err
			}
		}
		if err := e.upsertFromNode(node, prefix, db.StateSynced); err != nil {
			return "", err
		}
		parentUID = node.UID
	}
	return parentUID, nil
}

// cancelStaleTombstone drops a queued delete superseded by an in-place
// update of the same node (TG-05).
func (e *Engine) cancelStaleTombstone(rel, uploadedUID string) {
	tombstones, err := e.db.Tombstones()
	if err != nil {
		return
	}
	for _, t := range tombstones {
		if t.LocalPath == rel {
			if t.NodeUID != uploadedUID {
				return
			}
			if err := e.db.RemoveTombstone(rel); err == nil {
				e.log.Printf("%s: cancelled stale tombstone: in-place update superseded the queued delete", rel)
			}
			return
		}
	}
}

// ---- F3: conservative trash ----

// applyLocalRename re-keys DB rows beneath oldRel and mirrors the rename
// remotely (renameNode + moveNode when the parent changed) — never a trash
// + re-upload, so content and revision history survive.
func (e *Engine) applyLocalRename(oldRel, newRel string) error {
	// Guard against cycles: renaming a folder into itself is invalid.
	if strings.HasPrefix(newRel, oldRel+"/") || oldRel == newRel {
		e.log.Printf("%s -> %s: invalid rename target (into itself)", oldRel, newRel)
		return nil
	}
	row, err := e.db.NodeByPath(oldRel)
	if err != nil {
		return err
	}
	if row == nil {
		// Source vanished or never tracked: nothing remote to rename.
		return nil
	}

	// Overwrite-rename (tmp → existing destination): keep the destination's
	// node. See the long Rust comment: the destination's row wins; the
	// content delivers as a revision of the EXISTING node.
	if row.IsFile() {
		if dest, derr := e.db.NodeByPath(newRel); derr == nil && dest != nil && dest.IsFile() {
			sourceIsRemote := !strings.HasPrefix(row.NodeUID, "pending:")
			if sourceIsRemote {
				// A placeholder source (never hydrated locally) has no
				// backing content for the destination write to upload.
				// Pull its remote content to the destination path first.
				if e.store != nil {
					destAbs := toAbs(e.backingDir, newRel)
					if !isRegularFile(destAbs) {
						if err := e.downloadToBacking(row.NodeUID, newRel, row.Mtime, row.Mode); err != nil {
							return err
						}
					}
				}
				ctx, cancel := ctxBG()
				results, terr := e.sidecar.Trash(ctx, []string{row.NodeUID})
				cancel()
				if terr == nil {
					ok := false
					for _, r := range results {
						if r.UID == row.NodeUID && r.OK {
							ok = true
						}
					}
					if !ok {
						e.log.Printf("%s: overwritten source node trash rejected; continuing", row.NodeUID)
					}
				}
			}
			if err := e.db.RemoveNode(oldRel); err != nil {
				return err
			}
			delete(e.fileIDs, oldRel)
			e.rememberFileInode(newRel)
			dest.SyncState = db.StateDirty
			if err := e.db.UpsertNode(dest); err != nil {
				return err
			}
			if e.store != nil {
				e.store.RecordWrite(newRel)
			}
			e.log.Printf("%s -> %s: overwrite rename merged into the existing node %s", oldRel, newRel, dest.NodeUID)
			return nil
		}
	}

	newName := newRel
	if i := strings.LastIndex(newRel, "/"); i >= 0 {
		newName = newRel[i+1:]
	}
	newParentRel, oldParentRel := "", ""
	hasNewParent, hasOldParent := false, false
	if i := strings.LastIndex(newRel, "/"); i >= 0 {
		newParentRel, hasNewParent = newRel[:i], true
	}
	if i := strings.LastIndex(oldRel, "/"); i >= 0 {
		oldParentRel, hasOldParent = oldRel[:i], true
	}
	crossFolder := hasNewParent != hasOldParent || newParentRel != oldParentRel

	// Mirror remotely first (while rows still key the old path).
	if !strings.HasPrefix(row.NodeUID, "pending:") {
		if crossFolder {
			var parentUID string
			if hasNewParent {
				uid, err := e.ensureRemoteFolder(newParentRel)
				if err != nil {
					return err
				}
				parentUID = uid
			} else {
				uid, ok := e.dbRootUID()
				if !ok {
					return errors.New("root uid unknown before initial sync")
				}
				parentUID = uid
			}
			ctx, cancel := ctxBG()
			ok, merr := e.sidecar.MoveNode(ctx, row.NodeUID, parentUID)
			cancel()
			if merr != nil {
				return merr
			}
			if !ok {
				return fmt.Errorf("remote move of %s rejected", row.NodeUID)
			}
		}
		if row.Name != newName {
			ctx, cancel := ctxBG()
			_, rerr := e.sidecar.RenameNode(ctx, row.NodeUID, newName)
			cancel()
			if rerr != nil {
				return rerr
			}
		}
	}

	// Re-key rows: read all, delete old-path rows, insert with swapped
	// prefixes (children first).
	like := likeUnder(oldRel)
	rows, err := e.db.NodesUnder(like)
	if err != nil {
		return err
	}
	rows = append(rows, row)
	for _, r := range rows {
		if err := e.db.RemoveNode(r.LocalPath); err != nil {
			return err
		}
	}
	for _, r := range rows {
		if r.LocalPath == oldRel {
			r.LocalPath = newRel
		} else {
			r.LocalPath = newRel + "/" + r.LocalPath[len(oldRel)+1:]
		}
		if r.LocalPath == newRel {
			r.Name = newName
		}
		if err := e.db.UpsertNode(r); err != nil {
			return err
		}
	}
	// Parent uid follows the (possibly new) parent folder row.
	if crossFolder {
		var parentUID *string
		if hasNewParent {
			if p, perr := e.db.NodeByPath(newParentRel); perr == nil && p != nil {
				u := p.NodeUID
				parentUID = &u
			}
		} else if uid, ok := e.dbRootUID(); ok {
			parentUID = &uid
		}
		if parentUID != nil {
			if r, rerr := e.db.NodeByPath(newRel); rerr == nil && r != nil {
				r.ParentUID = parentUID
				_ = e.db.UpsertNode(r)
			}
		}
	}

	// Move content so hydration state and pending uploads follow the rows
	// (no-op for watcher-detected renames: disk already moved).
	from := e.contentPath(oldRel)
	to := e.contentPath(newRel)
	if _, err := os.Lstat(from); err == nil {
		if i := strings.LastIndex(to, "/"); i >= 0 {
			_ = os.MkdirAll(to[:i], 0o755)
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	if e.store != nil {
		e.store.ForgetTree(oldRel)
		children, _ := e.db.NodesUnder(likeUnder(newRel))
		for _, child := range children {
			if abs := e.contentPath(child.LocalPath); isRegularFile(abs) {
				if _, ok := e.store.BackingPath(child.LocalPath); !ok {
					e.store.SetBacking(child.LocalPath, abs)
				}
			}
		}
		if own := e.contentPath(newRel); isRegularFile(own) {
			if _, ok := e.store.BackingPath(newRel); !ok {
				e.store.SetBacking(newRel, own)
			}
		}
	}
	// Filesystem identities follow the rows.
	e.rekeyLocalInodes(oldRel, newRel)
	if row.IsFile() {
		e.rememberFileInode(newRel)
	}
	e.log.Printf("rename applied: %s -> %s", oldRel, newRel)
	return nil
}

func (e *Engine) rekeyLocalInodes(oldRel, newRel string) {
	rekeyInodeMap(e.dirIDs, oldRel, newRel)
	rekeyInodeMap(e.fileIDs, oldRel, newRel)
}

func rekeyInodeMap(m map[string]devIno, oldRel, newRel string) {
	prefix := oldRel + "/"
	type kv struct {
		k string
		v devIno
	}
	var moved []kv
	for k, v := range m {
		if k == oldRel || strings.HasPrefix(k, prefix) {
			moved = append(moved, kv{k, v})
		}
	}
	for _, mv := range moved {
		delete(m, mv.k)
	}
	for _, mv := range moved {
		np := newRel
		if mv.k != oldRel {
			np = newRel + "/" + mv.k[len(oldRel)+1:]
		}
		m[np] = mv.v
	}
}

func (e *Engine) handleLocalDelete(rel string) error {
	// Mirror mode: no rename-inbound seam (store == nil).
	renameInbound := false
	if e.store != nil {
		renameInbound = e.store.RenameInbound(rel)
	}
	row, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if row == nil {
		return nil // delete of unknown local item
	}
	delete(e.fileIDs, rel)
	if row.SyncState != db.StateSynced {
		// Never confirmed synced → local removal only (F3.1). An inbound
		// overwrite-rename owns this path.
		if !renameInbound {
			if err := e.db.RemoveNode(rel); err != nil {
				return err
			}
			if e.store != nil {
				e.store.Forget(rel)
				_ = os.Remove(toAbs(e.backingDir, rel))
			}
		}
		return nil
	}
	// Durable tombstone first; drained immediately if running (F3.4).
	if err := e.db.AddTombstone(&db.Tombstone{
		LocalPath: rel,
		NodeUID:   row.NodeUID,
		DeletedAt: nowUnix(),
	}); err != nil {
		return err
	}
	if e.store != nil && !renameInbound {
		e.store.Forget(rel)
		_ = os.Remove(toAbs(e.backingDir, rel))
	}
	return nil
}

// drainOneTombstone drains at most one tombstone. Returns false if the
// guard tripped (or trash was refused).
func (e *Engine) drainOneTombstone() (bool, error) {
	tombstones, err := e.db.Tombstones()
	if err != nil {
		return false, err
	}
	if len(tombstones) == 0 {
		return true, nil
	}
	t := tombstones[0]
	// Bug #2: tombstones approved by an explicit resume drain without
	// touching the guard's window.
	approved := e.approvedTombstones[t.LocalPath]
	if !approved {
		syncedTotal, _ := e.db.CountSynced()
		if e.guard.RecordAndCheck(time.Now(), syncedTotal) {
			_ = e.db.SetMeta(metaGuardPaused, "1")
			e.state = StateGuardPaused
			e.log.Printf("mass-deletion guard tripped: auto-paused, explicit resume required (deletes=%d synced=%d)",
				e.guard.WindowLen(), syncedTotal)
			e.emitActivity(ActivityGuardPaused, t.LocalPath, "")
			return false, nil
		}
	}
	ctx, cancel := ctxBG()
	results, err := e.sidecar.Trash(ctx, []string{t.NodeUID})
	cancel()
	if err != nil {
		return false, err
	}
	ok := false
	for _, r := range results {
		if r.UID == t.NodeUID && r.OK {
			ok = true
		}
	}
	if !ok {
		e.log.Printf("%s: trash failed remotely; keeping tombstone", t.LocalPath)
		return false, nil
	}
	e.noteSidecarOK()
	e.log.Printf("trashed remotely: %s (%s)", t.LocalPath, t.NodeUID)
	if err := e.removeNodeRecursive(t.LocalPath); err != nil {
		return false, err
	}
	if err := e.db.RemoveTombstone(t.LocalPath); err != nil {
		return false, err
	}
	delete(e.approvedTombstones, t.LocalPath)
	e.emitActivity(ActivityTrashed, t.LocalPath, "")
	return true, nil
}

func (e *Engine) removeNodeRecursive(rel string) error {
	// Children first (their rows carry the path prefix).
	rows, err := e.db.NodesUnder(likeUnder(rel))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := e.db.RemoveNode(row.LocalPath); err != nil {
			return err
		}
	}
	return e.db.RemoveNode(rel)
}

// ---- F6: remote events ----

func (e *Engine) handleNotification(n sidecar.Notification) error {
	switch note := n.(type) {
	case *sidecar.DriveEventNotification:
		ev := note.Event
		if e.state == StateRunning && !e.needsSignIn && !e.sidecarDown {
			if err := e.applyEvent(*ev); err != nil {
				if e.needsSignIn || sidecar.IsConnectionLost(err) {
					if !e.needsSignIn && !e.sidecarDown {
						e.noteSidecarFailure(err.Error())
					}
					// applyEvent persists the cursor last: safe to replay.
					e.bufferedEvents = append(e.bufferedEvents, ev)
				} else {
					return err
				}
			}
		} else {
			e.bufferedEvents = append(e.bufferedEvents, ev)
		}
	case *sidecar.TransferProgressNotification:
		// progress only
	case *sidecar.TransferDoneNotification, *sidecar.TransferErrorNotification:
		// Stray completion for a transfer we're not awaiting — runTransfer
		// consumes its own.
	case *sidecar.AuthSignInUrlNotification, *sidecar.OtherNotification:
	}
	return nil
}

func (e *Engine) applyEvent(ev sidecar.DriveEvent) error {
	switch ev.Kind {
	case sidecar.EventFastForward:
		// nothing
	case sidecar.EventNodeCreated, sidecar.EventNodeUpdated:
		if ev.NodeUID != nil {
			if ev.IsTrashed != nil && *ev.IsTrashed {
				e.applyRemoteDelete(*ev.NodeUID)
			} else if err := e.applyNodeUpsert(*ev.NodeUID); err != nil {
				// Auth/transport failures mean the event was not applied:
				// propagate so the caller parks it for replay instead of
				// advancing the cursor past it.
				if e.needsSignIn || sidecar.IsConnectionLost(err) {
					return err
				}
				e.log.Printf("failed to apply node event for %s: %v", *ev.NodeUID, err)
			}
		}
	case sidecar.EventNodeDeleted, sidecar.EventTreeRemove:
		if ev.NodeUID != nil {
			e.applyRemoteDelete(*ev.NodeUID)
		}
	case sidecar.EventTreeRefresh:
		ctx, cancel := ctxBG()
		root, err := e.sidecar.GetRoot(ctx)
		cancel()
		if err != nil {
			return err
		}
		if err := e.enumerateRemote(root); err != nil {
			return err
		}
		if !e.placeholders {
			files, err := e.remoteFilesNeedingDownload()
			if err != nil {
				return err
			}
			for _, f := range files {
				if reason, bad := e.unmaterializableReason(f.rel); bad {
					e.reportUnmaterializable(f.rel, reason)
					continue
				}
				if err := e.downloadFile(f.uid, f.rel); err != nil {
					return err
				}
			}
		}
	}
	// Persist the resume token after applying (protocol.md F6).
	if err := e.db.SetEventCursor(ev.ScopeID, ev.EventID); err != nil {
		return err
	}
	// Keep the VFS tree in step with applied events (M3).
	if e.store != nil {
		if err := e.publishVfsSnapshot(); err != nil {
			e.log.Printf("post-event VFS snapshot failed: %v", err)
		}
	}
	return nil
}

// resolveUnknownAncestors ensures every ancestor of node has a DB row
// (events can arrive child-first).
func (e *Engine) resolveUnknownAncestors(node *sidecar.Node) error {
	rootUID, _ := e.dbRootUID()
	var chain []*sidecar.Node
	parent := node.ParentUID
	seen := map[string]bool{node.UID: true}
	for parent != nil {
		uid := *parent
		if uid == rootUID {
			break
		}
		if row, err := e.db.NodeByUID(uid); err == nil && row != nil {
			break
		}
		if seen[uid] {
			e.log.Printf("remote parent cycle detected at %s; stopping ancestor walk", uid)
			break
		}
		seen[uid] = true
		ctx, cancel := ctxBG()
		ancestor, err := e.sidecar.GetNode(ctx, uid)
		cancel()
		if err != nil {
			break // unknown ancestor vanished; anchor at root
		}
		parent = ancestor.ParentUID
		chain = append(chain, ancestor)
	}
	// chain is nearest-first; the farthest ancestor must land first.
	for i := len(chain) - 1; i >= 0; i-- {
		ancestor := chain[i]
		rel, err := e.localPathFor(ancestor)
		if err != nil {
			return err
		}
		if err := e.upsertFromNode(ancestor, rel, db.StateSynced); err != nil {
			return err
		}
		if !ancestor.IsFolder() {
			continue
		}
		if reason, bad := e.unmaterializableReason(rel); bad {
			_ = reason // keep the logical row, skip the directory
			continue
		}
		if err := os.MkdirAll(toAbs(e.root, rel), 0o755); err != nil {
			return err
		}
		e.rememberDirInode(rel)
	}
	return nil
}

// applyNodeUpsert fetches a node and reconciles it locally.
func (e *Engine) applyNodeUpsert(uid string) error {
	ctx, cancel := ctxBG()
	node, err := e.sidecar.GetNode(ctx, uid)
	cancel()
	if err != nil {
		if code, ok := sidecarErrCode(err); ok && sidecar.IsAuthCode(code) {
			e.noteAuthRequired("", code, err.Error())
			return err
		}
		if sidecar.IsConnectionLost(err) {
			e.noteSidecarFailure(err.Error())
			return err
		}
		return nil // node vanished before fetch; skipping
	}
	if err := e.resolveUnknownAncestors(node); err != nil {
		return err
	}
	rel, err := e.localPathFor(node)
	if err != nil {
		return err
	}
	// A remote path the local filesystem cannot hold must fail visibly
	// BEFORE any FS/transfer work (TG-14).
	if !e.placeholders {
		if reason, bad := e.unmaterializableReason(rel); bad {
			if node.IsFolder() {
				if err := e.upsertFromNode(node, rel, db.StateSynced); err != nil {
					return err
				}
			}
			e.reportUnmaterializable(rel, reason)
			return nil
		}
	}
	// Resolve by node uid FIRST (TG-01/TG-02): a remote rename/move changes
	// the derived path while the DB row still sits at the old path.
	existing, err := e.db.NodeByUID(uid)
	if err != nil {
		return err
	}
	relocating := existing != nil && existing.LocalPath != rel
	// Never overwrite whatever already lives at the destination (F7).
	if err := e.clearRemotePath(node, rel, relocating); err != nil {
		return err
	}
	if existing != nil && relocating {
		if err := e.rekeyRemoteNode(existing, node, rel); err != nil {
			return err
		}
	}
	abs := toAbs(e.root, rel)

	if node.IsFolder() {
		if !e.placeholders {
			if _, serr := os.Stat(abs); os.IsNotExist(serr) {
				if err := os.MkdirAll(abs, 0o755); err != nil {
					return err
				}
			}
		}
		if err := e.upsertFromNode(node, rel, db.StateSynced); err != nil {
			return err
		}
		if !e.placeholders {
			e.rememberDirInode(rel)
		}
		return nil
	}

	// File: keep-both whenever the local copy carries content the remote
	// doesn't know about (F7).
	if kind, _ := classifyLocalEntry(abs); kind == entryFile {
		row, err := e.db.NodeByPath(rel)
		if err != nil {
			return err
		}
		diverged := row == nil // untracked local file in the way
		if row != nil {
			diverged = row.SyncState != db.StateSynced ||
				localContentDiverged(row, e.contentPath(rel))
		}
		if diverged {
			conflicted, err := renameForConflict(e.db, e.contentRoot(), rel)
			if err != nil {
				return err
			}
			e.log.Printf("%s: remote event vs local content: keeping both (renamed to %s)", rel, conflicted)
			e.emitActivity(ActivityConflict, rel,
				fmt.Sprintf("kept both; local copy renamed to %s", conflicted))
		}
	}

	if row, err := e.db.NodeByPath(rel); err == nil && row != nil &&
		row.SyncState == db.StateSynced && strEq(row.SHA1, node.SHA1) {
		return nil // already current
	}
	if e.placeholders {
		return e.upsertFromNode(node, rel, db.StateSynced)
	}
	return e.downloadFile(uid, rel)
}

func strEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// clearRemotePath frees rel for an incoming remote node, renaming any
// colliding local item aside (keep-both, F7).
func (e *Engine) clearRemotePath(node *sidecar.Node, rel string, relocating bool) error {
	rowAt, err := e.db.NodeByPath(rel)
	if err != nil {
		return err
	}
	if rowAt != nil && rowAt.NodeUID == node.UID {
		return nil // this IS the node: a normal update in place
	}
	abs := e.contentPath(rel)
	existingKind, _ := classifyLocalEntry(abs)
	var typeClash bool
	if node.IsFolder() {
		typeClash = existingKind == entryFile
	} else {
		typeClash = existingKind == entryDir
	}
	_, statErr := os.Lstat(abs)
	destTaken := relocating && statErr == nil
	rowClash := rowAt != nil
	if rowClash || typeClash || destTaken {
		conflict, err := e.renameLocalForConflict(rel, node.UID)
		if err != nil {
			return err
		}
		e.log.Printf("%s: remote node collides with local path: keeping both (renamed to %s)", rel, conflict)
		e.emitActivity(ActivityConflict, rel,
			fmt.Sprintf("kept both; local copy renamed to %s", conflict))
	}
	return nil
}

// renameLocalForConflict renames a local file or folder (and its tracked
// subtree) aside to a conflict name.
func (e *Engine) renameLocalForConflict(rel, incomingUID string) (string, error) {
	newRel, err := localFreeConflictRel(e.db, e.contentRoot(), rel)
	if err != nil {
		return "", err
	}
	from := e.contentPath(rel)
	to := e.contentPath(newRel)
	if _, serr := os.Lstat(from); serr == nil {
		if i := strings.LastIndex(to, "/"); i >= 0 {
			_ = os.MkdirAll(to[:i], 0o755)
		}
		if err := os.Rename(from, to); err != nil {
			return "", err
		}
	}

	rows, err := e.db.NodesUnder(likeUnder(rel))
	if err != nil {
		return "", err
	}
	if row, rerr := e.db.NodeByPath(rel); rerr == nil && row != nil {
		rows = append(rows, row)
	}
	newName := newRel
	if i := strings.LastIndex(newRel, "/"); i >= 0 {
		newName = newRel[i+1:]
	}
	var moves []db.Move
	for _, row := range rows {
		oldPath := row.LocalPath
		wasRoot := oldPath == rel
		if wasRoot {
			row.LocalPath = newRel
		} else {
			row.LocalPath = newRel + "/" + oldPath[len(rel)+1:]
		}
		if wasRoot {
			row.Name = newName
		}
		// The conflict copy is local content the remote does not know under
		// that name: let it upload if it has pending changes.
		if row.SyncState != db.StateSynced {
			row.SyncState = db.StateDirty
		}
		moves = append(moves, db.Move{Old: oldPath, Row: row})
	}
	if len(moves) > 0 {
		if err := e.db.Rekey(moves); err != nil {
			return "", err
		}
	}

	if row, rerr := e.db.NodeByPath(newRel); rerr == nil && row != nil {
		if row.NodeUID == incomingUID {
			// The remote node changed type in place (same uid): the
			// displaced local content re-uploads as a new node.
			row.NodeUID = "pending:" + newRel
			row.SyncState = db.StateDirty
			_ = e.db.UpsertNode(row)
		} else if !strings.HasPrefix(row.NodeUID, "pending:") {
			// Keep the local node's identity; rename it remotely too.
			ctx, cancel := ctxBG()
			_, rnErr := e.sidecar.RenameNode(ctx, row.NodeUID, newName)
			cancel()
			if rnErr != nil {
				e.log.Printf("%s: remote rename for conflict failed; local copy kept (%v)", newRel, rnErr)
			}
		} else {
			row.NodeUID = "pending:" + newRel
			row.SyncState = db.StateDirty
			_ = e.db.UpsertNode(row)
		}
	}

	if e.store != nil {
		e.store.ForgetTree(rel)
		children, _ := e.db.NodesUnder(likeUnder(newRel))
		for _, child := range children {
			if abs := e.contentPath(child.LocalPath); isRegularFile(abs) {
				e.store.SetBacking(child.LocalPath, abs)
			}
		}
		if isRegularFile(to) {
			e.store.SetBacking(newRel, to)
		}
	}
	e.rekeyLocalInodes(rel, newRel)
	e.log.Printf("local item renamed aside for conflict: %s -> %s", rel, newRel)
	return newRel, nil
}

// rekeyRemoteNode re-keys a remote node (and, for folders, every
// descendant) from its old local path to the path derived from its current
// remote name and parent.
func (e *Engine) rekeyRemoteNode(old *db.NodeRow, node *sidecar.Node, newRel string) error {
	oldRel := old.LocalPath
	var rows []*db.NodeRow
	if !old.IsFile() {
		var err error
		rows, err = e.db.NodesUnder(likeUnder(oldRel))
		if err != nil {
			return err
		}
	}
	rows = append(rows, old)
	var moves []db.Move
	for _, row := range rows {
		oldPath := row.LocalPath
		wasRoot := oldPath == oldRel
		if wasRoot {
			row.LocalPath = newRel
		} else {
			row.LocalPath = newRel + "/" + oldPath[len(oldRel)+1:]
		}
		if wasRoot {
			// Relocation only: content metadata stays as-is so a rename
			// combined with a remote content change still compares hashes.
			row.Name = node.Name
			row.ParentUID = node.ParentUID
			row.Kind = kindFromNode(node.Kind)
		}
		moves = append(moves, db.Move{Old: oldPath, Row: row})
	}
	// One transaction: a crash can never leave both old and new rows.
	if err := e.db.Rekey(moves); err != nil {
		return err
	}

	// Move the content copy.
	from := e.contentPath(oldRel)
	to := e.contentPath(newRel)
	if _, serr := os.Lstat(from); serr == nil {
		if i := strings.LastIndex(to, "/"); i >= 0 {
			_ = os.MkdirAll(to[:i], 0o755)
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	if e.store != nil {
		e.store.ForgetTree(oldRel)
		children, _ := e.db.NodesUnder(likeUnder(newRel))
		for _, child := range children {
			if abs := e.contentPath(child.LocalPath); isRegularFile(abs) {
				e.store.SetBacking(child.LocalPath, abs)
			}
		}
		if isRegularFile(to) {
			e.store.SetBacking(newRel, to)
		}
	}
	e.rekeyLocalInodes(oldRel, newRel)
	if old.IsFile() {
		e.rememberFileInode(newRel)
	}
	e.log.Printf("remote rename/move applied: %s -> %s (%s)", oldRel, newRel, node.UID)
	return nil
}

// localPathFor computes the local relative path for a remote node by
// walking parent uids through the DB; unknown parents anchor at the root.
func (e *Engine) localPathFor(node *sidecar.Node) (string, error) {
	parts := []string{node.Name}
	parent := node.ParentUID
	seen := map[string]bool{node.UID: true}
	for parent != nil {
		uid := *parent
		if seen[uid] {
			e.log.Printf("remote parent cycle detected at %s; stopping path walk", uid)
			break
		}
		seen[uid] = true
		row, err := e.db.NodeByUID(uid)
		if err != nil {
			return "", err
		}
		if row == nil {
			break // parent unknown → anchor at root (M1)
		}
		parts = append(parts, row.Name)
		parent = row.ParentUID
	}
	// reverse
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "/"), nil
}

func (e *Engine) applyRemoteDelete(uid string) error {
	row, err := e.db.NodeByUID(uid)
	if err != nil {
		return err
	}
	if row == nil {
		return nil
	}
	contentAbs := e.contentPath(row.LocalPath)
	// F7 (M4 soak find): the row's state alone can LIE — hash the on-disk
	// content before destroying anything.
	locallyUnsynced := row.SyncState != db.StateSynced ||
		localContentDiverged(row, contentAbs)
	if locallyUnsynced {
		// Local unsynced changes → keep both (F7); the dirty row re-uploads
		// under an available name. Unpark it so the upload path can re-key.
		_ = e.clearUploadBlocks(row.LocalPath)
		e.log.Printf("%s: remote delete of dirty local node: keeping local copy", row.LocalPath)
		return nil
	}
	removePath(contentAbs)
	if e.placeholders {
		if e.store != nil {
			e.store.ClearBacking(row.LocalPath)
		}
	} else {
		removePath(toAbs(e.root, row.LocalPath))
	}
	if row.IsFile() {
		delete(e.fileIDs, row.LocalPath)
	} else {
		prefix := row.LocalPath + "/"
		for rel := range e.dirIDs {
			if rel == row.LocalPath || strings.HasPrefix(rel, prefix) {
				delete(e.dirIDs, rel)
			}
		}
		for rel := range e.fileIDs {
			if rel == row.LocalPath || strings.HasPrefix(rel, prefix) {
				delete(e.fileIDs, rel)
			}
		}
	}
	return e.removeNodeRecursive(row.LocalPath)
}

func removePath(p string) {
	fi, err := os.Stat(p)
	if err != nil {
		return
	}
	if fi.IsDir() {
		_ = os.RemoveAll(p)
	} else {
		_ = os.Remove(p)
	}
}

// ---- transfers ----

func (e *Engine) downloadFile(uid, rel string) error {
	if reason, bad := e.unmaterializableReason(rel); bad {
		e.reportUnmaterializable(rel, reason)
		return nil
	}
	if err := e.tryDownloadFile(uid, rel); err != nil {
		// A shutdown/cancel is not an environment failure.
		if !e.shuttingDown {
			e.recordDownloadFailure(rel, err)
		}
	}
	return nil
}

func (e *Engine) tryDownloadFile(uid, rel string) error {
	abs := e.contentPath(rel)
	ctx, cancel := ctxBG()
	node, nodeErr := e.sidecar.GetNode(ctx, uid)
	cancel()
	nodeOK := nodeErr == nil
	// Make the row visible to the retry pump before the transfer.
	if row, err := e.db.NodeByPath(rel); err == nil && row == nil && nodeOK {
		if err := e.upsertFromNode(node, rel, db.StateDownloading); err != nil {
			return err
		}
	}
	if row, err := e.db.NodeByPath(rel); err == nil {
		if row == nil || row.SyncState == db.StateSynced || row.SyncState == db.StateDownloading {
			_ = e.db.SetSyncState(rel, db.StateDownloading)
		}
	} else {
		return err
	}
	// Mode is local-only (D2): keep the user's chmod across re-downloads.
	var existingMode *uint32
	if row, err := e.db.NodeByPath(rel); err == nil && row != nil {
		existingMode = row.Mode
	}
	var remoteMtime *int64
	if nodeOK {
		remoteMtime = node.Mtime
	}
	tmpRel := transferTempRel(rel)
	tmp := e.contentPath(tmpRel)
	if i := strings.LastIndex(abs, "/"); i >= 0 {
		if err := os.MkdirAll(abs[:i], 0o755); err != nil {
			return fmt.Errorf("create directory for %s: %w", abs, err)
		}
	}
	// Fail with the OS cause *before* the transfer when the target is not
	// writable.
	if f, err := os.Create(tmp); err != nil {
		return fmt.Errorf("prepare download target %s: %w", abs, err)
	} else {
		f.Close()
	}
	if _, failure := e.runTransfer("download", rel, func() (string, error) {
		ctx2, cancel2 := ctxBG()
		defer cancel2()
		return e.sidecar.Download(ctx2, uid, tmp)
	}); failure != nil {
		if !failure.aborted && sidecar.IsAuthCode(failure.code) {
			// Park the event; the row stays Downloading.
			e.noteAuthRequired(rel, failure.code, failure.message)
			return fmt.Errorf("sign-in required while downloading `%s`: %s", rel, failure.message)
		}
		if failure.aborted {
			if !e.shuttingDown && sidecar.IsConnectionLost(failure.err) {
				e.noteSidecarFailure(failure.err.Error())
			}
			return failure.err
		}
		_ = os.Remove(tmp)
		return fmt.Errorf("download `%s`: %s", rel, failure.message)
	}
	if err := os.Rename(tmp, abs); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install downloaded file %s: %w", abs, err)
	}
	applyFileMetadata(abs, remoteMtime, existingMode)
	row := &db.NodeRow{
		LocalPath: rel,
		NodeUID:   uid,
		Name:      rel,
		Kind:      db.KindFile,
		Mtime:     remoteMtime,
		Mode:      existingMode,
		SyncState: db.StateSynced,
		Pinned:    false,
	}
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		row.Name = rel[i+1:]
	}
	if nodeOK {
		row.ParentUID = node.ParentUID
		row.Size = node.Size
		row.SHA1 = node.SHA1
		row.SyncedSHA1 = node.SHA1
	}
	if err := e.db.UpsertNode(row); err != nil {
		return err
	}
	_ = e.clearUploadBlocks(rel)
	e.noteSidecarOK()
	delete(e.retryAt, rel)
	e.rememberFileInode(rel)
	e.log.Printf("downloaded %s", rel)
	e.emitActivity(ActivityDownloaded, rel, "")
	return nil
}

// recordDownloadFailure keeps the row visible as errored, leaves the
// previous local copy in place, and schedules an exponential retry.
func (e *Engine) recordDownloadFailure(rel string, err error) {
	message := err.Error()
	if row, rerr := e.db.NodeByPath(rel); rerr == nil && (row == nil ||
		row.SyncState == db.StateSynced || row.SyncState == db.StateDownloading) {
		_ = e.db.SetSyncState(rel, db.StateDownloading)
	}
	count, cerr := e.db.RecordRetryableFailure(rel)
	if cerr != nil {
		count = 1
	}
	_ = e.db.MarkUploadErrored(rel, message)
	backoff := retryBackoff(e.retryBase, count)
	e.retryAt[rel] = time.Now().Add(backoff)
	e.log.Printf("%s: download failed; keeping local content and retrying in %v: %v", rel, backoff, err)
	e.emitActivity(ActivityDownloadErrored, rel, message)
}

// transferOutcome is a successful transfer completion.
type transferOutcome struct {
	nodeUID *string
}

// transferFailure is either a transport abort (aborted) or a request-level
// / transfer-level remote error.
type transferFailure struct {
	aborted    bool
	err        error // aborted
	code       int64 // remote
	message    string
	retryable  bool
	retryAfter *time.Duration
}

func (f *transferFailure) failure() error {
	if f == nil {
		return nil
	}
	if f.aborted {
		return f.err
	}
	return fmt.Errorf("rpc error %d: %s", f.code, f.message)
}

// runTransfer issues a transfer request, then awaits its completion
// notification while still servicing control commands (F4 mid-transfer
// pause/resume).
func (e *Engine) runTransfer(kind, localPath string, start func() (string, error)) (transferOutcome, *transferFailure) {
	transferID, err := start()
	if err != nil {
		if sidecar.IsConnectionLost(err) {
			return transferOutcome{}, &transferFailure{aborted: true, err: err}
		}
		if code, ok := sidecarErrCode(err); ok {
			f := &transferFailure{code: code}
			var se *sidecar.Error
			if errors.As(err, &se) {
				f.message = se.RPC.Message
				if d, ok := se.RetryAfter(); ok {
					dur := d
					f.retryAfter = &dur
				}
			}
			f.retryable = code == sidecar.ErrCodeRateLimited
			return transferOutcome{}, f
		}
		return transferOutcome{}, &transferFailure{aborted: true, err: err}
	}
	e.activeTransfer = transferID
	lp := localPath
	_ = e.db.UpsertTransfer(&db.Transfer{
		TransferID: transferID,
		Kind:       kind,
		LocalPath:  &lp,
		Bytes:      0,
		State:      "running",
		UpdatedAt:  nowUnix(),
	})

	var result transferOutcome
	var failure *transferFailure
	notifs := e.notifications
loop:
	for {
		// A closed stream cannot produce a completion.
		if notifs == nil {
			failure = &transferFailure{aborted: true, err: sidecar.ErrClosed}
			break loop
		}
		var cmd Command
		var n sidecar.Notification
		select {
		case cmd = <-e.cmdCh:
			if cmd.Kind == "shutdown" {
				ctx, cancel := ctxBG()
				_ = e.sidecar.TransferCancel(ctx, transferID)
				cancel()
				e.shuttingDown = true
				failure = &transferFailure{aborted: true,
					err: errors.New("shutdown during transfer")}
				break loop
			}
			if err := e.handleCommand(cmd); err != nil {
				failure = &transferFailure{aborted: true, err: err}
				break loop
			}
		case varN, ok := <-notifs:
			if !ok {
				// Stream died mid-transfer: abort like Rust's
				// Notification::Closed branch.
				failure = &transferFailure{aborted: true, err: sidecar.ErrClosed}
				break loop
			}
			n = varN
			switch note := n.(type) {
			case *sidecar.TransferDoneNotification:
				if note.TransferID == transferID {
					result = transferOutcome{nodeUID: note.NodeUID}
					break loop
				}
				e.backlog = append(e.backlog, note)
			case *sidecar.TransferErrorNotification:
				if note.TransferID == transferID {
					failure = &transferFailure{
						code:       note.Code,
						message:    note.Message,
						retryable:  note.Retryable,
						retryAfter: note.RetryAfter,
					}
					break loop
				}
				e.backlog = append(e.backlog, note)
			default:
				e.backlog = append(e.backlog, note)
			}
		}
	}
	e.activeTransfer = ""
	state := "done"
	errMsg := ""
	if failure != nil {
		state = "error"
		errMsg = failure.failure().Error()
	}
	_ = e.db.UpsertTransfer(&db.Transfer{
		TransferID: transferID,
		Kind:       kind,
		LocalPath:  &lp,
		Bytes:      0,
		State:      state,
		Error:      &errMsg,
		UpdatedAt:  nowUnix(),
	})
	return result, failure
}

// maybeEvict lives in placeholder.go (no-op in mirror mode: callers and
// the SetEvictionCap handler only invoke it when e.store != nil).

// ---- work pump ----

// pumpWork drains queued work: tombstones (guarded) → buffered remote
// events → failed-download retries → uploads. Buffered events are applied
// BEFORE the upload queue (bug #1) — it narrows the window in which a
// not-yet-arrived remote edit would be superseded by a local revision
// upload. Returns true if any work was done.
func (e *Engine) pumpWork() (bool, error) {
	// The sidecar process is gone: no retry can succeed without a host
	// restart. Stay alive for status/commands.
	if e.notificationsClosed {
		return false, nil
	}
	if e.sidecarDown {
		due := e.sidecarProbeAt == nil || !e.sidecarProbeAt.After(time.Now())
		if !due {
			return false, nil
		}
		// Probe once; the handlers re-enter degraded mode on failure.
		e.sidecarDown = false
		e.sidecarProbeAt = nil
	}
	// No session: every remote call would fail.
	if e.needsSignIn {
		return false, nil
	}
	tombstones, err := e.db.Tombstones()
	if err != nil {
		return false, err
	}
	if len(tombstones) > 0 {
		if _, err := e.drainOneTombstone(); err != nil {
			if sidecar.IsConnectionLost(err) {
				e.noteSidecarFailure(err.Error())
				return false, nil
			}
			if code := sidecar.AuthErrorCode(err); code != 0 {
				e.noteAuthRequired("", code, err.Error())
				return false, nil
			}
			return false, err
		}
		// Write-through: the VFS tree must drop the path right away.
		if e.store != nil {
			if err := e.publishVfsSnapshot(); err != nil {
				e.log.Printf("post-trash VFS snapshot failed: %v", err)
			}
		}
		return true, nil
	}
	if len(e.bufferedEvents) > 0 {
		ev := e.bufferedEvents[0]
		if err := e.applyEvent(*ev); err != nil {
			if e.needsSignIn || sidecar.IsConnectionLost(err) {
				if !e.needsSignIn {
					e.noteSidecarFailure(err.Error())
				}
				// not committed; keep it at the head
				return false, nil
			}
			return false, err
		}
		e.bufferedEvents = e.bufferedEvents[1:]
		return true, nil
	}
	// Failed downloads/hydrations: retry once their backoff matures.
	// Mirror downloads go to the visible root; placeholder mode hydrates
	// into the backing cache (the just-hydrated file is eviction-safe).
	now := time.Now()
	downloading, err := e.db.NodesWithState(db.StateDownloading)
	if err != nil {
		return false, err
	}
	for _, row := range downloading {
		t, waiting := e.retryAt[row.LocalPath]
		if waiting && t.After(now) {
			continue
		}
		if e.store != nil {
			e.hydrateOne(row.LocalPath)
			e.maybeEvict(row.LocalPath)
			if e.evictionRecheck {
				e.evictionRecheck = false
				e.maybeEvict("")
			}
		} else if err := e.downloadFile(row.NodeUID, row.LocalPath); err != nil {
			return false, err
		}
		return true, nil
	}
	// Uploads: skip terminally errored / remote-conflicted nodes and rows
	// waiting out a retry backoff.
	errored, err := e.db.ErroredPaths()
	if err != nil {
		return false, err
	}
	erroredSet := make(map[string]bool, len(errored))
	for _, p := range errored {
		erroredSet[p] = true
	}
	conflicted, err := e.db.RemoteConflictedPaths()
	if err != nil {
		return false, err
	}
	conflictedSet := make(map[string]bool, len(conflicted))
	for _, p := range conflicted {
		conflictedSet[p] = true
	}
	dirty, err := e.db.NodesWithState(db.StateDirty)
	if err != nil {
		return false, err
	}
	for _, row := range dirty {
		if erroredSet[row.LocalPath] || conflictedSet[row.LocalPath] {
			continue
		}
		if t, waiting := e.retryAt[row.LocalPath]; waiting && t.After(now) {
			continue
		}
		if err := e.uploadOne(row.LocalPath); err != nil {
			if e.needsSignIn || sidecar.IsConnectionLost(err) {
				if !e.needsSignIn {
					e.noteSidecarFailure(err.Error())
				}
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	return false, nil
}
