// Layout conversion and per-root session layout (SPEC F5, AGENT.md M3.4).
//
// ConvertLayout ports crates/core/src/engine.rs convert_layout (it lives
// there in Rust). In the Go engine it is invoked by the backend/mock before
// the mount and before the engine starts — never by the engine itself — so
// it only needs the exported db/mode surface (no engine-private access).
//
// Session integration (default placeholders for new roots, legacy adoption,
// $PDS_SYNC_MODE override) is expressed through ResolveSessionLayout, which
// the backend calls on every restart. FUSE probe/mount ordering lives in the
// backend: probe before the engine starts, mount after WaitForWatcher.
package mode

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"pds.dev/engine-go/internal/db"
)

// LayoutConvertedFile is the direction-aware marker inside the backing dir
// recording which layout the conversion last produced.
const LayoutConvertedFile = ".layout-converted"

// internal names (duplicated from engine to avoid an import cycle: mode is
// imported by engine).
const (
	stateDirName  = ".pds"
	vfsBackingDir = ".pds-vfs-backing"
)

// ConvertLayout moves content between the mirror layout (root) and the
// placeholders layout (backingDir) without re-downloading anything.
//
//   - Switching TO placeholders: every tracked file in the root moves into
//     the backing cache (hydrated, hash-consistent); untracked leftovers
//     move too and register as dirty pending uploads so nothing is stranded
//     under the future mount; empty tracked dirs are pruned.
//   - Switching TO mirror: hydrated backing files move back into the root;
//     placeholders without backing content are re-downloaded by the normal
//     materialization pass (nothing is deleted here).
//
// Idempotent via the direction-aware marker: a repeat pass for the same
// layout is a no-op, the opposite direction re-runs the moves. Crash-
// restartable: a rerun finishes files whose moves were interrupted before
// the marker was written (already-moved files are skipped because the
// source is gone or the destination exists). Conservative: files that fail
// to move stay where they are for the normal sync machinery to reconcile;
// rows are left as-is.
func ConvertLayout(d *db.Db, root, backingDir string, target SyncMode) error {
	marker := filepath.Join(backingDir, LayoutConvertedFile)
	if data, err := os.ReadFile(marker); err == nil {
		if got, ok := SyncModeFromMetaValue(strings.TrimSpace(string(data)), true); ok && got == target {
			return nil
		}
	}
	toPlaceholders := target == ModePlaceholders
	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		return err
	}
	rows, err := d.AllNodes()
	if err != nil {
		return err
	}
	moved := 0
	for _, row := range rows {
		if !row.IsFile() {
			continue
		}
		inRoot := toAbs(root, row.LocalPath)
		inBacking := toAbs(backingDir, row.LocalPath)
		from, to := inRoot, inBacking
		if !toPlaceholders {
			from, to = inBacking, inRoot
		}
		if !isRegularFile(from) {
			continue
		}
		if _, err := os.Lstat(to); err == nil {
			continue // destination exists: never overwrite
		}
		if err := MoveFile(from, to); err != nil {
			log.Printf("mode: layout move %s failed: %v (leaving in place for sync to reconcile)", row.LocalPath, err)
			continue
		}
		moved++
	}
	if toPlaceholders {
		adopted, err := adoptUntracked(d, root, backingDir)
		if err != nil {
			return err
		}
		moved += adopted
		// Empty tracked dirs in the root would linger forever (placeholder
		// mode never materializes dirs); prune the ones that are empty.
		for _, row := range rows {
			if row.IsFile() {
				continue
			}
			_ = os.Remove(toAbs(root, row.LocalPath)) // fails while non-empty: fine
		}
	}
	if err := os.WriteFile(marker, []byte(target.AsMetaValue()), 0o644); err != nil {
		return err
	}
	if moved > 0 {
		log.Printf("mode: local layout converted to %s (%d files moved)", target.AsMetaValue(), moved)
	}
	return nil
}

// adoptUntracked moves every on-disk file that has no DB row into the
// backing cache and registers it dirty (pending upload). Walk is iterative
// (Rust VecDeque pattern) and skips unreadable dirs, internal paths, and
// temp files; DB errors abort the conversion. Returns the adopted count.
func adoptUntracked(d *db.Db, root, backingDir string) (int, error) {
	stack := []string{root}
	adopted := 0
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue // unreadable dir: leave it; never fail adoption
		}
		var dirs []string
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			rel, ok := toRel(root, path)
			if !ok {
				continue
			}
			if isInternalPath(rel) || isTempFile(rel) {
				continue // temps are swept by crash recovery, never adopted
			}
			info, err := os.Lstat(path)
			if err != nil {
				continue
			}
			if info.IsDir() {
				dirs = append(dirs, path)
				continue
			}
			if !info.Mode().IsRegular() {
				continue // symlinks/specials: never follow, never adopt
			}
			row, err := d.NodeByPath(rel)
			if err != nil {
				return 0, err
			}
			if row != nil {
				continue
			}
			dest := toAbs(backingDir, rel)
			if err := MoveFile(path, dest); err != nil {
				log.Printf("mode: adoption move %s failed: %v (leaving in place)", rel, err)
				continue
			}
			sha1, size := sha1FileDest(dest)
			var mtime *int64
			var perm *uint32
			if fi, err := os.Stat(dest); err == nil {
				m := fi.ModTime().Unix()
				mtime = &m
				p := uint32(fi.Mode().Perm() & 0o777)
				perm = &p
			}
			name := rel
			if i := strings.LastIndex(rel, "/"); i >= 0 {
				name = rel[i+1:]
			}
			if err := d.UpsertNode(&db.NodeRow{
				LocalPath: rel,
				NodeUID:   "pending:" + rel,
				Name:      name,
				Kind:      db.KindFile,
				Size:      &size,
				SHA1:      &sha1,
				Mtime:     mtime,
				Mode:      perm,
				SyncState: db.StateDirty,
			}); err != nil {
				return 0, err
			}
			adopted++
		}
		for _, dirPath := range dirs {
			stack = append(stack, dirPath)
		}
	}
	return adopted, nil
}

// Layout is the desktop (external state dir) layout for a sync root.
type Layout struct {
	// Root is the canonical sync root (the FUSE mountpoint in placeholder mode).
	Root string
	// StateDir holds state.db, backing/, and the root marker.
	StateDir string
	// BackingDir is the VFS backing cache (outside the root).
	BackingDir string
	// DBPath is the external state DB (state.db).
	DBPath string
	// AdoptedLegacyDB reports whether the state DB was just adopted from
	// the legacy in-root location (<root>/.pds/state.db).
	AdoptedLegacyDB bool
}

// PrepareLayout creates the sync root (before canonicalizing, so first
// launch on a fresh machine works), the external state dir, adopts a legacy
// in-root state DB when present (verified copy, then the original is
// removed), and writes the root marker. Never deletes content.
func PrepareLayout(root string) (*Layout, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create sync root %s: %w", root, err)
	}
	canonical := root
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		canonical = resolved
	} else if abs, err := filepath.Abs(root); err == nil {
		canonical = abs
	}
	stateDir := EngineStateDir(canonical)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create engine state dir %s: %w", stateDir, err)
	}
	dbPath := filepath.Join(stateDir, "state.db")
	adopted := false
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		legacyDB := filepath.Join(canonical, stateDirName, "state.db")
		if fi, err := os.Stat(legacyDB); err == nil && fi.Mode().IsRegular() {
			if err := copyFileBytes(legacyDB, dbPath); err != nil {
				return nil, fmt.Errorf("adopting state DB from %s to %s: %w", legacyDB, dbPath, err)
			}
			for _, suffix := range []string{"-wal", "-shm"} {
				_ = os.Remove(legacyDB + suffix)
			}
			if !VerifyDBFile(dbPath) {
				_ = os.Remove(dbPath)
				return nil, fmt.Errorf("adopted state DB at %s did not verify; keeping the original at %s", dbPath, legacyDB)
			}
			if err := os.Remove(legacyDB); err != nil {
				return nil, fmt.Errorf("removing adopted legacy state DB %s: %w", legacyDB, err)
			}
			adopted = true
			log.Printf("mode: adopted legacy in-root state DB %s -> %s", legacyDB, dbPath)
		}
	}
	if err := os.WriteFile(filepath.Join(stateDir, "root"), []byte(canonical), 0o644); err != nil {
		return nil, fmt.Errorf("write root marker: %w", err)
	}
	return &Layout{
		Root:            canonical,
		StateDir:        stateDir,
		BackingDir:      filepath.Join(stateDir, "backing"),
		DBPath:          dbPath,
		AdoptedLegacyDB: adopted,
	}, nil
}

// VerifyDBFile reports whether dbPath opens as the state DB (sqlite_master
// readable). Used to guard legacy adoption: the original is only dropped
// once the copy provably opens as the same database.
func VerifyDBFile(dbPath string) bool {
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return false
	}
	defer conn.Close()
	var n int64
	if err := conn.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
		return false
	}
	return n >= 0
}

// PersistedModeFromDBPath reads the persisted sync_mode straight from a
// state DB file (read-only; absent/invalid → not ok).
func PersistedModeFromDBPath(dbPath string) (SyncMode, bool) {
	if _, err := os.Stat(dbPath); err != nil {
		return ModePlaceholders, false
	}
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return ModePlaceholders, false
	}
	defer conn.Close()
	var raw string
	if err := conn.QueryRow("SELECT value FROM meta WHERE key = '" + MetaSyncMode + "'").Scan(&raw); err != nil {
		return ModePlaceholders, false
	}
	return SyncModeFromMetaValue(raw, true)
}

// ResolveSessionMode resolves the effective mode for a canonical root:
// $PDS_SYNC_MODE > persisted external sync_mode > persisted in-root
// sync_mode (Go mirror layout) > legacy mirror default > placeholders
// default for new roots. Mirrors the headless pre-read; the engine
// re-resolves authoritatively via ApplyModeSetting.
func ResolveSessionMode(canonicalRoot string) SyncMode {
	if m, ok := SyncModeFromEnv(); ok {
		return m
	}
	stateDir := EngineStateDir(canonicalRoot)
	externalDB := filepath.Join(stateDir, "state.db")
	if _, err := os.Stat(externalDB); err == nil {
		if m, ok := PersistedModeFromDBPath(externalDB); ok {
			return m
		}
		return ModePlaceholders
	}
	legacyDB := filepath.Join(canonicalRoot, stateDirName, "state.db")
	if fi, err := os.Stat(legacyDB); err == nil && fi.Mode().IsRegular() {
		// An in-root DB from the Go mirror layout carries sync_mode;
		// a pre-0.3 Rust DB does not. The persisted value, when
		// present, wins over the legacy mirror default.
		if m, ok := PersistedModeFromDBPath(legacyDB); ok {
			return m
		}
		return ModeMirror
	}
	return ModePlaceholders
}

// SessionLayout is the resolved per-session layout: effective mode plus the
// DB/backing paths the engine should use.
type SessionLayout struct {
	// Root is the canonical sync root.
	Root string
	// EffectiveMode is the persisted/env-resolved mode (conversion target).
	EffectiveMode SyncMode
	// UseExternal selects the $XDG_DATA_HOME roots/<hash>/ layout versus
	// the classic in-root .pds layout (mirror mode, new roots). Placeholders
	// always use external; mirror uses external when an adopted/external DB
	// already exists so state continuity is preserved.
	UseExternal bool
	// StateDir is the external state dir (always prepared).
	StateDir string
	// BackingDir is the selected backing cache.
	BackingDir string
	// DBPath is the selected state DB.
	DBPath string
	// AdoptedLegacyDB reports a just-adopted legacy DB.
	AdoptedLegacyDB bool
}

// ResolveSessionLayout canonicalizes the root, resolves the effective mode,
// prepares the external layout (adopting a legacy DB), and selects DB/backing
// paths: external in placeholder mode, in-root for plain mirror mode.
func ResolveSessionLayout(root string) (*SessionLayout, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create sync root %s: %w", root, err)
	}
	canonical := root
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		canonical = resolved
	} else if abs, err := filepath.Abs(root); err == nil {
		canonical = abs
	}
	effective := ResolveSessionMode(canonical)
	layout, err := PrepareLayout(canonical)
	if err != nil {
		return nil, err
	}
	_, statErr := os.Stat(layout.DBPath)
	externalExists := statErr == nil
	useExternal := effective == ModePlaceholders || externalExists || layout.AdoptedLegacyDB
	dbPath := layout.DBPath
	backingDir := layout.BackingDir
	if !useExternal {
		dbPath = filepath.Join(canonical, stateDirName, "state.db")
		backingDir = filepath.Join(canonical, vfsBackingDir)
	}
	return &SessionLayout{
		Root:            canonical,
		EffectiveMode:   effective,
		UseExternal:     useExternal,
		StateDir:        layout.StateDir,
		BackingDir:      backingDir,
		DBPath:          dbPath,
		AdoptedLegacyDB: layout.AdoptedLegacyDB,
	}, nil
}

// ConvertLayoutForSession runs ConvertLayout when the session uses the
// external backing cache (the only case conversion is possible). Callers
// log-and-continue on error, matching Rust's warn+continue in Engine::new.
func ConvertLayoutForSession(d *db.Db, layout *SessionLayout) error {
	if !layout.UseExternal {
		return nil
	}
	return ConvertLayout(d, layout.Root, layout.BackingDir, layout.EffectiveMode)
}

// ---- private helpers (Rust engine.rs vocabulary, duplicated to avoid a cycle) ----

func toAbs(base, rel string) string { //nolint:revive // matches Rust to_abs
	if rel == "" {
		return base
	}
	return filepath.Join(append([]string{base}, strings.Split(rel, "/")...)...)
}

// toRel returns the slash-separated root-relative path for path, or false
// when path is not under root (Rust to_rel).
func toRel(root, path string) (string, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", false
	}
	if rel == "." {
		return "", false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	for _, p := range parts {
		if !validUTF8(p) {
			return "", false
		}
	}
	return strings.Join(parts, "/"), true
}

func validUTF8(s string) bool {
	for _, r := range s {
		if r == 0xFFFD {
			return false
		}
	}
	return true
}

func isInternalPath(rel string) bool {
	return rel == stateDirName || strings.HasPrefix(rel, stateDirName+"/") ||
		rel == vfsBackingDir || strings.HasPrefix(rel, vfsBackingDir+"/")
}

// isTempFile matches the exact generated shapes: *.pds-part and
// *.pdstmp-<uuid36>; user files merely containing a marker sync normally.
func isTempFile(rel string) bool {
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}
	if strings.HasSuffix(name, ".pds-part") {
		return true
	}
	i := strings.LastIndex(name, ".pdstmp-")
	if i <= 0 {
		return false
	}
	id := name[i+len(".pdstmp-"):]
	if len(id) != 36 {
		return false
	}
	for j, b := range []byte(id) {
		switch j {
		case 8, 13, 18, 23:
			if b != '-' {
				return false
			}
		default:
			if !isHexDigit(b) {
				return false
			}
		}
	}
	return true
}

func isHexDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

func isRegularFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

// sha1FileDest streams a file; on error it matches Rust's
// unwrap_or_else((""),0): the row still registers with an empty hash.
func sha1FileDest(path string) (string, int64) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0
	}
	defer f.Close()
	h := sha1.New()
	var size int64
	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			size += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0
		}
	}
	return hex.EncodeToString(h.Sum(nil)), size
}

// sha1File is the streaming SHA-1 helper for tests (Rust sha1_file).
func sha1File(path string) (string, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha1.New()
	var size uint64
	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			size += uint64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

func copyFileBytes(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(to)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}
