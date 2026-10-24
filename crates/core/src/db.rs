//! SQLite state DB (SPEC.md §3): node map, tombstones, transfers, event
//! cursor, meta key/value. Opened per sync root; blocking `rusqlite` calls
//! behind a mutex are fine at M1 volumes.

use std::path::Path;
use std::sync::{Arc, Mutex};

use pds_sidecar_client::NodeKind;
use rusqlite::{Connection, OptionalExtension, params};

/// Sync state of a node row (SPEC F2/F3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Local and remote agree; deletes may propagate (F3).
    Synced,
    /// Local change not yet uploaded.
    Dirty,
    /// Upload transfer in flight.
    Uploading,
    /// Download transfer in flight.
    Downloading,
}

impl SyncState {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncState::Synced => "synced",
            SyncState::Dirty => "dirty",
            SyncState::Uploading => "uploading",
            SyncState::Downloading => "downloading",
        }
    }
    fn from_str(s: &str) -> rusqlite::Result<Self> {
        match s {
            "synced" => Ok(SyncState::Synced),
            "dirty" => Ok(SyncState::Dirty),
            "uploading" => Ok(SyncState::Uploading),
            "downloading" => Ok(SyncState::Downloading),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("bad sync_state `{other}`").into(),
            )),
        }
    }
}

/// A row of the `nodes` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    /// Root-relative path, '/'-separated ("" unused; root itself is not stored).
    pub local_path: String,
    pub node_uid: String,
    pub parent_uid: Option<String>,
    pub name: String,
    pub kind: NodeKind,
    pub size: Option<i64>,
    pub sha1: Option<String>,
    pub mtime: Option<i64>,
    pub sync_state: SyncState,
    pub pinned: bool,
}

impl NodeRow {
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
}

/// A row of the `tombstones` table: an offline/guard-blocked delete to drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    pub local_path: String,
    pub node_uid: String,
    pub deleted_at: i64,
}

#[derive(Debug, Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS nodes (
    local_path TEXT PRIMARY KEY,
    node_uid   TEXT NOT NULL UNIQUE,
    parent_uid TEXT,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('file','folder')),
    size       INTEGER,
    sha1       TEXT,
    mtime      INTEGER,
    sync_state TEXT NOT NULL CHECK (sync_state IN ('synced','dirty','uploading','downloading')),
    pinned     INTEGER NOT NULL DEFAULT 0
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
";

impl Db {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// In-memory DB for tests.
    pub fn open_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().expect("db mutex poisoned")
    }

    // ---- nodes ----

    pub fn upsert_node(&self, row: &NodeRow) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT INTO nodes (local_path, node_uid, parent_uid, name, kind, size, sha1, mtime, sync_state, pinned)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(local_path) DO UPDATE SET
               node_uid=excluded.node_uid, parent_uid=excluded.parent_uid, name=excluded.name,
               kind=excluded.kind, size=excluded.size, sha1=excluded.sha1, mtime=excluded.mtime,
               sync_state=excluded.sync_state, pinned=excluded.pinned",
            params![
                row.local_path,
                row.node_uid,
                row.parent_uid,
                row.name,
                match row.kind { NodeKind::File => "file", NodeKind::Folder => "folder" },
                row.size,
                row.sha1,
                row.mtime,
                row.sync_state.as_str(),
                row.pinned as i64,
            ],
        )?;
        Ok(())
    }

    fn row_from(r: &rusqlite::Row<'_>) -> rusqlite::Result<NodeRow> {
        let kind: String = r.get(4)?;
        let state: String = r.get(8)?;
        Ok(NodeRow {
            local_path: r.get(0)?,
            node_uid: r.get(1)?,
            parent_uid: r.get(2)?,
            name: r.get(3)?,
            kind: if kind == "file" {
                NodeKind::File
            } else {
                NodeKind::Folder
            },
            size: r.get(5)?,
            sha1: r.get(6)?,
            mtime: r.get(7)?,
            sync_state: SyncState::from_str(&state)?,
            pinned: r.get::<_, i64>(9)? != 0,
        })
    }

    pub fn node_by_path(&self, local_path: &str) -> rusqlite::Result<Option<NodeRow>> {
        self.lock()
            .query_row(
                "SELECT local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned
                 FROM nodes WHERE local_path=?1",
                params![local_path],
                Self::row_from,
            )
            .optional()
    }

    pub fn node_by_uid(&self, node_uid: &str) -> rusqlite::Result<Option<NodeRow>> {
        self.lock()
            .query_row(
                "SELECT local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned
                 FROM nodes WHERE node_uid=?1",
                params![node_uid],
                Self::row_from,
            )
            .optional()
    }

    pub fn remove_node(&self, local_path: &str) -> rusqlite::Result<()> {
        self.lock()
            .execute("DELETE FROM nodes WHERE local_path=?1", params![local_path])?;
        Ok(())
    }

    pub fn set_sync_state(&self, local_path: &str, state: SyncState) -> rusqlite::Result<()> {
        self.lock().execute(
            "UPDATE nodes SET sync_state=?2 WHERE local_path=?1",
            params![local_path, state.as_str()],
        )?;
        Ok(())
    }

    pub fn nodes_with_state(&self, state: SyncState) -> rusqlite::Result<Vec<NodeRow>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned
             FROM nodes WHERE sync_state=?1 ORDER BY local_path",
        )?;
        let rows = stmt
            .query_map(params![state.as_str()], Self::row_from)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn count_synced(&self) -> rusqlite::Result<usize> {
        let n: i64 = self.lock().query_row(
            "SELECT COUNT(*) FROM nodes WHERE sync_state='synced'",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// Direct children rows of a node uid (remote tree walk).
    pub fn children_of_uid(&self, node_uid: &str) -> rusqlite::Result<Vec<NodeRow>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned
             FROM nodes WHERE parent_uid=?1 ORDER BY local_path",
        )?;
        let rows = stmt
            .query_map(params![node_uid], Self::row_from)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// All rows whose path matches a SQL LIKE pattern (subtree removal).
    pub fn nodes_under(&self, like_pattern: &str) -> rusqlite::Result<Vec<NodeRow>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned
             FROM nodes WHERE local_path LIKE ?1 ORDER BY local_path",
        )?;
        let rows = stmt
            .query_map(params![like_pattern], Self::row_from)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ---- tombstones ----

    pub fn add_tombstone(&self, t: &Tombstone) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT OR REPLACE INTO tombstones (local_path, node_uid, deleted_at) VALUES (?1,?2,?3)",
            params![t.local_path, t.node_uid, t.deleted_at],
        )?;
        Ok(())
    }

    pub fn tombstones(&self) -> rusqlite::Result<Vec<Tombstone>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT local_path, node_uid, deleted_at FROM tombstones ORDER BY deleted_at",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(Tombstone {
                    local_path: r.get(0)?,
                    node_uid: r.get(1)?,
                    deleted_at: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn remove_tombstone(&self, local_path: &str) -> rusqlite::Result<()> {
        self.lock().execute(
            "DELETE FROM tombstones WHERE local_path=?1",
            params![local_path],
        )?;
        Ok(())
    }

    // ---- transfers ----

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_transfer(
        &self,
        transfer_id: &str,
        kind: &str,
        local_path: Option<&str>,
        node_uid: Option<&str>,
        bytes: i64,
        total: Option<i64>,
        state: &str,
        error: Option<&str>,
        updated_at: i64,
    ) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT INTO transfers (transfer_id,kind,local_path,node_uid,bytes,total,state,error,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(transfer_id) DO UPDATE SET
               bytes=excluded.bytes, state=excluded.state, error=excluded.error, updated_at=excluded.updated_at",
            params![transfer_id, kind, local_path, node_uid, bytes, total, state, error, updated_at],
        )?;
        Ok(())
    }

    // ---- event cursor ----

    pub fn set_event_cursor(&self, scope_id: &str, event_id: &str) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT OR REPLACE INTO event_cursor (scope_id, event_id) VALUES (?1,?2)",
            params![scope_id, event_id],
        )?;
        Ok(())
    }

    pub fn event_cursor(&self, scope_id: &str) -> rusqlite::Result<Option<String>> {
        self.lock()
            .query_row(
                "SELECT event_id FROM event_cursor WHERE scope_id=?1",
                params![scope_id],
                |r| r.get(0),
            )
            .optional()
    }

    // ---- meta ----

    pub fn set_meta(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1,?2)",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn meta(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.lock()
            .query_row("SELECT value FROM meta WHERE key=?1", params![key], |r| {
                r.get(0)
            })
            .optional()
    }
}
