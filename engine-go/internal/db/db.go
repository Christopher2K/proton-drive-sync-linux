// Package db is the SQLite state DB (SPEC.md §3): node map, tombstones,
// transfers, event cursor, meta key/value. Opened per sync root; the schema
// is byte-compatible with the Rust engine's state.db (crates/core/src/db.rs)
// so either engine can open the same file.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// Kind mirrors pds_sidecar_client::NodeKind.
type Kind string

// Node kinds as stored in the `kind` column.
const (
	KindFile   Kind = "file"
	KindFolder Kind = "folder"
)

// State is the sync state of a node row (SPEC F2/F3), as stored in the
// `sync_state` column.
type State string

// Sync states.
const (
	StateSynced      State = "synced"
	StateDirty       State = "dirty"
	StateUploading   State = "uploading"
	StateDownloading State = "downloading"
)

// NodeRow is a row of the `nodes` table.
type NodeRow struct {
	// LocalPath is root-relative, '/'-separated ("" unused; root itself is
	// not stored).
	LocalPath string
	NodeUID   string
	ParentUID *string
	Name      string
	Kind      Kind
	Size      *int64
	SHA1      *string
	Mtime     *int64
	// Mode is the local POSIX permission bits (0o777 mask), persisted so
	// `chmod +x` survives eviction/rehydration. Local-only: never uploaded
	// (Drive's node metadata has no POSIX mode).
	Mode      *uint32
	SyncState State
	Pinned    bool
	// SyncedSHA1 is the remote head SHA-1 as of the last completed sync in
	// either direction. It stays put while SHA1 tracks local content on
	// dirty rows, so revision uploads can precondition on the remote head
	// (F7, e2e-m1 bug #1).
	SyncedSHA1 *string
}

// IsFile reports whether the row is a file.
func (r *NodeRow) IsFile() bool { return r.Kind == KindFile }

// Tombstone is a row of the `tombstones` table: an offline/guard-blocked
// delete to drain.
type Tombstone struct {
	LocalPath string
	NodeUID   string
	DeletedAt int64
}

const schema = `
CREATE TABLE IF NOT EXISTS nodes (
    local_path TEXT PRIMARY KEY,
    node_uid   TEXT NOT NULL UNIQUE,
    parent_uid TEXT,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('file','folder')),
    size       INTEGER,
    sha1       TEXT,
    mtime      INTEGER,
    mode       INTEGER,
    sync_state TEXT NOT NULL CHECK (sync_state IN ('synced','dirty','uploading','downloading')),
    pinned     INTEGER NOT NULL DEFAULT 0,
    synced_sha1 TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    remote_conflicted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tombstones (
    local_path TEXT PRIMARY KEY,
    node_uid   TEXT NOT NULL,
    deleted_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    kind        TEXT NOT NULL CHECK (kind IN ('upload','download')),
    local_path  TEXT,
    node_uid    TEXT,
    bytes       INTEGER NOT NULL DEFAULT 0,
    total       INTEGER,
    state       TEXT NOT NULL,
    error       TEXT,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS event_cursor (
    scope_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// Db is the state database; safe for concurrent use (a mutex serializes
// access, mirroring the Rust engine's blocking-call-behind-mutex design).
type Db struct {
	mu   sync.Mutex
	conn *sql.DB
}

// Open opens (creating if needed) the state DB at path, in WAL mode.
func Open(path string) (*Db, error) {
	if dir := filepath.Dir(path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Single connection: WAL + one writer keeps modernc sqlite happy and
	// matches the mutex-serialized Rust design.
	conn.SetMaxOpenConns(1)
	if _, err := conn.Exec("PRAGMA journal_mode=WAL"); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Exec(schema); err != nil {
		conn.Close()
		return nil, err
	}
	d := &Db{conn: conn}
	if err := d.migrate(); err != nil {
		conn.Close()
		return nil, err
	}
	return d, nil
}

// OpenMemory opens an in-memory DB (tests).
func OpenMemory() (*Db, error) {
	conn, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)
	if _, err := conn.Exec(schema); err != nil {
		conn.Close()
		return nil, err
	}
	return &Db{conn: conn}, nil
}

// Close closes the underlying connection.
func (d *Db) Close() error { return d.conn.Close() }

// migrate applies additive migrations for DBs created by older builds
// (same list and order as db.rs).
func (d *Db) migrate() error {
	rows, err := d.conn.Query("PRAGMA table_info(nodes)")
	if err != nil {
		return err
	}
	cols := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull int
		var dfltValue any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			rows.Close()
			return err
		}
		cols[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	migrations := []struct{ name, ddl string }{
		{"synced_sha1", "ALTER TABLE nodes ADD COLUMN synced_sha1 TEXT"},
		{"retry_count", "ALTER TABLE nodes ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"},
		{"last_error", "ALTER TABLE nodes ADD COLUMN last_error TEXT"},
		{"remote_conflicted", "ALTER TABLE nodes ADD COLUMN remote_conflicted INTEGER NOT NULL DEFAULT 0"},
		{"mode", "ALTER TABLE nodes ADD COLUMN mode INTEGER"},
	}
	for _, m := range migrations {
		if !cols[m.name] {
			if _, err := d.conn.Exec(m.ddl); err != nil {
				return fmt.Errorf("migrate %s: %w", m.name, err)
			}
		}
	}
	return nil
}

const nodeCols = `local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned,synced_sha1,mode`

func scanNode(scan func(dest ...any) error) (*NodeRow, error) {
	var r NodeRow
	var kind, state string
	var pinned int
	var parentUID sql.NullString
	if err := scan(&r.LocalPath, &r.NodeUID, &parentUID, &r.Name, &kind,
		&r.Size, &r.SHA1, &r.Mtime, &state, &pinned, &r.SyncedSHA1, &r.Mode); err != nil {
		return nil, err
	}
	if parentUID.Valid {
		v := parentUID.String
		r.ParentUID = &v
	}
	if kind == "file" {
		r.Kind = KindFile
	} else {
		r.Kind = KindFolder
	}
	switch state {
	case "synced":
		r.SyncState = StateSynced
	case "dirty":
		r.SyncState = StateDirty
	case "uploading":
		r.SyncState = StateUploading
	case "downloading":
		r.SyncState = StateDownloading
	default:
		return nil, fmt.Errorf("bad sync_state %q", state)
	}
	r.Pinned = pinned != 0
	return &r, nil
}

func nullable[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// ---- nodes ----

// UpsertNode inserts or updates a node row keyed by local_path (the
// retry/conflict bookkeeping columns are deliberately not written, so row
// rewrites never clobber them; same contract as db.rs).
func (d *Db) UpsertNode(row *NodeRow) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(`INSERT INTO nodes (local_path, node_uid, parent_uid, name, kind, size, sha1, mtime, sync_state, pinned, synced_sha1, mode)
	     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
	     ON CONFLICT(local_path) DO UPDATE SET
	       node_uid=excluded.node_uid, parent_uid=excluded.parent_uid, name=excluded.name,
	       kind=excluded.kind, size=excluded.size, sha1=excluded.sha1, mtime=excluded.mtime,
	       sync_state=excluded.sync_state, pinned=excluded.pinned, synced_sha1=excluded.synced_sha1,
	       mode=excluded.mode`,
		row.LocalPath, row.NodeUID, nullable(row.ParentUID), row.Name, string(row.Kind),
		nullable(row.Size), nullable(row.SHA1), nullable(row.Mtime),
		string(row.SyncState), row.Pinned, nullable(row.SyncedSHA1), nullable(row.Mode))
	return err
}

func (d *Db) nodeWhere(where string, arg any) (*NodeRow, error) {
	row := d.conn.QueryRow(
		"SELECT "+nodeCols+" FROM nodes WHERE "+where, arg)
	r, err := scanNode(row.Scan)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return r, err
}

// NodeByPath looks a node row up by root-relative path.
func (d *Db) NodeByPath(localPath string) (*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeWhere("local_path=?", localPath)
}

// NodeByUID looks a node row up by remote node UID.
func (d *Db) NodeByUID(nodeUID string) (*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeWhere("node_uid=?", nodeUID)
}

// RemoveNode deletes a node row.
func (d *Db) RemoveNode(localPath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("DELETE FROM nodes WHERE local_path=?", localPath)
	return err
}

// Move is one entry of a Rekey batch.
type Move struct {
	Old string
	Row *NodeRow
}

// Rekey performs an all-or-nothing re-key of node rows (see RekeyNodes).
func (d *Db) Rekey(moves []Move) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, m := range moves {
		if _, err := tx.Exec("DELETE FROM nodes WHERE local_path=?", m.Old); err != nil {
			return err
		}
	}
	for _, m := range moves {
		_, err := tx.Exec(`INSERT INTO nodes (local_path, node_uid, parent_uid, name, kind, size, sha1, mtime, sync_state, pinned, synced_sha1, mode)
		     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			m.Row.LocalPath, m.Row.NodeUID, nullable(m.Row.ParentUID), m.Row.Name, string(m.Row.Kind),
			nullable(m.Row.Size), nullable(m.Row.SHA1), nullable(m.Row.Mtime),
			string(m.Row.SyncState), m.Row.Pinned, nullable(m.Row.SyncedSHA1), nullable(m.Row.Mode))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetSyncState updates the sync state of one row.
func (d *Db) SetSyncState(localPath string, state State) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("UPDATE nodes SET sync_state=? WHERE local_path=?", string(state), localPath)
	return err
}

// SetPinned sets the pin flag for one row (M3.2 eviction protection).
func (d *Db) SetPinned(localPath string, pinned bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("UPDATE nodes SET pinned=? WHERE local_path=?", pinned, localPath)
	return err
}

// SetPinnedTree sets the pin flag for rel and every path under it
// (folder pins).
func (d *Db) SetPinnedTree(rel string, pinned bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(
		"UPDATE nodes SET pinned=? WHERE local_path=? OR local_path LIKE ? || '/%'",
		pinned, rel, rel)
	return err
}

func (d *Db) nodeQuery(query string, args ...any) ([]*NodeRow, error) {
	rows, err := d.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*NodeRow
	for rows.Next() {
		r, err := scanNode(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// NodesWithState returns all rows in the given state, ordered by path.
func (d *Db) NodesWithState(state State) ([]*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeQuery("SELECT "+nodeCols+" FROM nodes WHERE sync_state=? ORDER BY local_path", string(state))
}

// CountSynced counts rows in the synced state.
func (d *Db) CountSynced() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var n int
	err := d.conn.QueryRow("SELECT COUNT(*) FROM nodes WHERE sync_state='synced'").Scan(&n)
	return n, err
}

// ---- upload failure bookkeeping (F2 retry policy) ----
// These columns are deliberately NOT part of NodeRow/UpsertNode, so row
// rewrites (dirty marks, sync completions) never clobber them; the engine
// clears them explicitly on success or fresh local edits.

// RecordRetryableFailure increments the consecutive-failure counter and
// returns the new count.
func (d *Db) RecordRetryableFailure(localPath string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.conn.Exec(
		"UPDATE nodes SET retry_count = retry_count + 1 WHERE local_path=?", localPath); err != nil {
		return 0, err
	}
	var n int64
	err := d.conn.QueryRow(
		"SELECT retry_count FROM nodes WHERE local_path=?", localPath).Scan(&n)
	return n, err
}

// MarkUploadErrored marks the node terminally errored (surfaced in status;
// never requeued until a fresh local edit clears it).
func (d *Db) MarkUploadErrored(localPath, errMsg string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("UPDATE nodes SET last_error=? WHERE local_path=?", errMsg, localPath)
	return err
}

// UploadError returns the terminal error recorded for a path, if any.
func (d *Db) UploadError(localPath string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var e sql.NullString
	err := d.conn.QueryRow(
		"SELECT last_error FROM nodes WHERE local_path=?", localPath).Scan(&e)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !e.Valid {
		return "", nil
	}
	return e.String, nil
}

// ErroredPaths returns the paths of all terminally errored nodes.
func (d *Db) ErroredPaths() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows, err := d.conn.Query("SELECT local_path FROM nodes WHERE last_error IS NOT NULL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountErrored counts terminally errored nodes.
func (d *Db) CountErrored() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var n int
	err := d.conn.QueryRow("SELECT COUNT(*) FROM nodes WHERE last_error IS NOT NULL").Scan(&n)
	return n, err
}

// ClearUploadFailures resets failure state (successful upload, or a fresh
// local edit).
func (d *Db) ClearUploadFailures(localPath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(
		"UPDATE nodes SET retry_count=0, last_error=NULL WHERE local_path=?", localPath)
	return err
}

// ---- remote-conflict parking (F7) ----
// Set when a revision upload was rejected 409 (stale base): the remote head
// moved but our only view of it may be the SDK's stale cache, so
// reconciliation is deferred to the next matching remote event, which runs
// the standard event-application keep-both and clears the flag.

// SetRemoteConflicted sets or clears the conflict-parking flag.
func (d *Db) SetRemoteConflicted(localPath string, on bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("UPDATE nodes SET remote_conflicted=? WHERE local_path=?", on, localPath)
	return err
}

// IsRemoteConflicted reports the conflict-parking flag (false for unknown
// paths, like db.rs).
func (d *Db) IsRemoteConflicted(localPath string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var v sql.NullInt64
	err := d.conn.QueryRow(
		"SELECT remote_conflicted FROM nodes WHERE local_path=?", localPath).Scan(&v)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v.Int64 != 0, nil
}

// RemoteConflictedPaths returns all parked-conflict paths.
func (d *Db) RemoteConflictedPaths() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows, err := d.conn.Query("SELECT local_path FROM nodes WHERE remote_conflicted != 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountRemoteConflicted counts parked-conflict rows.
func (d *Db) CountRemoteConflicted() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var n int
	err := d.conn.QueryRow("SELECT COUNT(*) FROM nodes WHERE remote_conflicted != 0").Scan(&n)
	return n, err
}

// ChildrenOfUID returns direct children rows of a node uid (remote tree
// walk).
func (d *Db) ChildrenOfUID(nodeUID string) ([]*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeQuery(
		"SELECT "+nodeCols+" FROM nodes WHERE parent_uid=? ORDER BY local_path", nodeUID)
}

// NodesUnder returns all rows whose path matches a SQL LIKE pattern
// (subtree removal).
func (d *Db) NodesUnder(likePattern string) ([]*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeQuery(
		"SELECT "+nodeCols+" FROM nodes WHERE local_path LIKE ? ORDER BY local_path", likePattern)
}

// AllNodes returns every row, unordered (VFS snapshot publication).
func (d *Db) AllNodes() ([]*NodeRow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodeQuery("SELECT " + nodeCols + " FROM nodes")
}

// ---- tombstones ----

// AddTombstone records a pending delete (INSERT OR REPLACE, like db.rs).
func (d *Db) AddTombstone(tb *Tombstone) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(
		"INSERT OR REPLACE INTO tombstones (local_path, node_uid, deleted_at) VALUES (?,?,?)",
		tb.LocalPath, tb.NodeUID, tb.DeletedAt)
	return err
}

// Tombstones lists pending deletes oldest-first.
func (d *Db) Tombstones() ([]*Tombstone, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows, err := d.conn.Query(
		"SELECT local_path, node_uid, deleted_at FROM tombstones ORDER BY deleted_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Tombstone
	for rows.Next() {
		var tb Tombstone
		if err := rows.Scan(&tb.LocalPath, &tb.NodeUID, &tb.DeletedAt); err != nil {
			return nil, err
		}
		out = append(out, &tb)
	}
	return out, rows.Err()
}

// RemoveTombstone deletes a tombstone row.
func (d *Db) RemoveTombstone(localPath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("DELETE FROM tombstones WHERE local_path=?", localPath)
	return err
}

// ---- transfers ----

// Transfer is one row of the `transfers` table.
type Transfer struct {
	TransferID string
	Kind       string // "upload" | "download"
	LocalPath  *string
	NodeUID    *string
	Bytes      int64
	Total      *int64
	State      string
	Error      *string
	UpdatedAt  int64
}

// UpsertTransfer inserts or updates a transfer row keyed by transfer_id.
func (d *Db) UpsertTransfer(tr *Transfer) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(`INSERT INTO transfers (transfer_id,kind,local_path,node_uid,bytes,total,state,error,updated_at)
	     VALUES (?,?,?,?,?,?,?,?,?)
	     ON CONFLICT(transfer_id) DO UPDATE SET
	       bytes=excluded.bytes, state=excluded.state, error=excluded.error, updated_at=excluded.updated_at`,
		tr.TransferID, tr.Kind, nullable(tr.LocalPath), nullable(tr.NodeUID),
		tr.Bytes, nullable(tr.Total), tr.State, nullable(tr.Error), tr.UpdatedAt)
	return err
}

// ---- event cursor ----

// SetEventCursor persists the event-stream resume token for a scope.
func (d *Db) SetEventCursor(scopeID, eventID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec(
		"INSERT OR REPLACE INTO event_cursor (scope_id, event_id) VALUES (?,?)", scopeID, eventID)
	return err
}

// EventCursor returns the persisted resume token, or "" if none.
func (d *Db) EventCursor(scopeID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var id string
	err := d.conn.QueryRow(
		"SELECT event_id FROM event_cursor WHERE scope_id=?", scopeID).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

// ---- meta ----

// SetMeta persists a meta key/value.
func (d *Db) SetMeta(key, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", key, value)
	return err
}

// Meta returns a meta value, or "" if unset.
func (d *Db) Meta(key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var v string
	err := d.conn.QueryRow("SELECT value FROM meta WHERE key=?", key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// RemoveMeta deletes a meta key.
func (d *Db) RemoveMeta(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Exec("DELETE FROM meta WHERE key=?", key)
	return err
}

// helper for callers building LIKE patterns (Rust: `rel || '/%'`).
//
//nolint:unused // referenced by engine port (kept next to SetPinnedTree semantics)
func likeUnder(rel string) string { return strings.ReplaceAll(rel, "%", "%%") + "/%" }
