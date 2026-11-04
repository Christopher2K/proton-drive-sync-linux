//! State DB round-trip tests (no sidecar involved).

use pds_core::db::{Db, NodeRow, SyncState, Tombstone};
use pds_sidecar_client::NodeKind;

fn row(rel: &str, uid: &str, state: SyncState) -> NodeRow {
    NodeRow {
        local_path: rel.to_string(),
        node_uid: uid.to_string(),
        parent_uid: Some("root".into()),
        name: rel.rsplit('/').next().unwrap_or(rel).to_string(),
        kind: if rel.ends_with('/') {
            NodeKind::Folder
        } else {
            NodeKind::File
        },
        size: Some(42),
        sha1: Some("deadbeef".into()),
        mtime: Some(1_700_000_000),
        mode: None,
        sync_state: state,
        pinned: false,
        synced_sha1: Some("deadbeef".into()),
    }
}

#[test]
fn node_round_trip_by_path_and_uid() {
    let db = Db::open_memory().unwrap();
    db.upsert_node(&row("docs/a.txt", "uid-1", SyncState::Synced))
        .unwrap();

    let by_path = db.node_by_path("docs/a.txt").unwrap().unwrap();
    let by_uid = db.node_by_uid("uid-1").unwrap().unwrap();
    assert_eq!(by_path, by_uid);
    assert_eq!(by_path.sync_state, SyncState::Synced);
    assert!(by_path.is_file());
    assert_eq!(by_path.size, Some(42));

    assert!(db.node_by_path("nope").unwrap().is_none());
    assert!(db.node_by_uid("nope").unwrap().is_none());
}

#[test]
fn upsert_updates_existing_path() {
    let db = Db::open_memory().unwrap();
    db.upsert_node(&row("a.txt", "uid-1", SyncState::Dirty))
        .unwrap();
    let mut updated = row("a.txt", "uid-1", SyncState::Synced);
    updated.sha1 = Some("cafe".into());
    db.upsert_node(&updated).unwrap();
    let got = db.node_by_path("a.txt").unwrap().unwrap();
    assert_eq!(got.sync_state, SyncState::Synced);
    assert_eq!(got.sha1.as_deref(), Some("cafe"));
}

#[test]
fn sync_state_transitions_and_queries() {
    let db = Db::open_memory().unwrap();
    db.upsert_node(&row("a.txt", "uid-1", SyncState::Synced))
        .unwrap();
    db.upsert_node(&row("b.txt", "uid-2", SyncState::Dirty))
        .unwrap();

    assert_eq!(db.count_synced().unwrap(), 1);
    assert_eq!(db.nodes_with_state(SyncState::Dirty).unwrap().len(), 1);

    db.set_sync_state("b.txt", SyncState::Synced).unwrap();
    assert_eq!(db.count_synced().unwrap(), 2);
    assert!(db.nodes_with_state(SyncState::Dirty).unwrap().is_empty());
}

#[test]
fn nodes_under_matches_subtree() {
    let db = Db::open_memory().unwrap();
    db.upsert_node(&row("docs", "uid-d", SyncState::Synced))
        .unwrap();
    db.upsert_node(&row("docs/a.txt", "uid-1", SyncState::Synced))
        .unwrap();
    db.upsert_node(&row("docs/sub/b.txt", "uid-2", SyncState::Synced))
        .unwrap();
    db.upsert_node(&row("other.txt", "uid-3", SyncState::Synced))
        .unwrap();

    let under = db.nodes_under("docs/%").unwrap();
    let paths: Vec<_> = under.iter().map(|r| r.local_path.as_str()).collect();
    assert_eq!(paths, ["docs/a.txt", "docs/sub/b.txt"]);
}

#[test]
fn tombstone_round_trip_ordered_by_time() {
    let db = Db::open_memory().unwrap();
    db.add_tombstone(&Tombstone {
        local_path: "b.txt".into(),
        node_uid: "u2".into(),
        deleted_at: 20,
    })
    .unwrap();
    db.add_tombstone(&Tombstone {
        local_path: "a.txt".into(),
        node_uid: "u1".into(),
        deleted_at: 10,
    })
    .unwrap();
    let ts = db.tombstones().unwrap();
    assert_eq!(ts[0].local_path, "a.txt");
    assert_eq!(ts[1].local_path, "b.txt");

    db.remove_tombstone("a.txt").unwrap();
    assert_eq!(db.tombstones().unwrap().len(), 1);
}

#[test]
fn cursor_and_meta_round_trip() {
    let db = Db::open_memory().unwrap();
    assert!(db.event_cursor("scope-1").unwrap().is_none());
    db.set_event_cursor("scope-1", "evt-1").unwrap();
    db.set_event_cursor("scope-1", "evt-2").unwrap();
    assert_eq!(
        db.event_cursor("scope-1").unwrap().as_deref(),
        Some("evt-2")
    );

    assert!(db.meta("k").unwrap().is_none());
    db.set_meta("k", "v").unwrap();
    db.set_meta("k", "v2").unwrap();
    assert_eq!(db.meta("k").unwrap().as_deref(), Some("v2"));
}

#[test]
fn transfers_upsert() {
    let db = Db::open_memory().unwrap();
    db.upsert_transfer(
        "t-1",
        "upload",
        Some("a.txt"),
        None,
        0,
        Some(100),
        "running",
        None,
        1,
    )
    .unwrap();
    db.upsert_transfer(
        "t-1",
        "upload",
        Some("a.txt"),
        None,
        100,
        Some(100),
        "done",
        None,
        2,
    )
    .unwrap();
    // No panic + no constraint violation is the assertion; transfers table is
    // a bookkeeping log in M1.
}

#[test]
fn file_backed_db_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    {
        let db = Db::open(&path).unwrap();
        db.upsert_node(&row("a.txt", "uid-1", SyncState::Synced))
            .unwrap();
        db.set_event_cursor("scope-1", "evt-9").unwrap();
    }
    let db = Db::open(&path).unwrap();
    assert!(db.node_by_path("a.txt").unwrap().is_some());
    assert_eq!(
        db.event_cursor("scope-1").unwrap().as_deref(),
        Some("evt-9")
    );
}

/// Columns present in the pre-#39 `nodes` table (mtime was already there;
/// `mode` is the Wave 2a addition). Used to build a realistic old DB.
const PRE_39_NODES: &str = "
CREATE TABLE nodes (
    local_path TEXT PRIMARY KEY,
    node_uid   TEXT NOT NULL UNIQUE,
    parent_uid TEXT,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('file','folder')),
    size       INTEGER,
    sha1       TEXT,
    mtime      INTEGER,
    sync_state TEXT NOT NULL CHECK (sync_state IN ('synced','dirty','uploading','downloading')),
    pinned     INTEGER NOT NULL DEFAULT 0,
    synced_sha1 TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    remote_conflicted INTEGER NOT NULL DEFAULT 0
);
";

fn column_names(path: &std::path::Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare("PRAGMA table_info(nodes)").unwrap();
    stmt.query_map([], |r| r.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn pre_39_schema_gets_mode_added_and_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(PRE_39_NODES).unwrap();
        conn.execute_batch(
            "CREATE TABLE event_cursor (scope_id TEXT PRIMARY KEY, event_id TEXT NOT NULL);
             CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes
               (local_path,node_uid,parent_uid,name,kind,size,sha1,mtime,sync_state,pinned,synced_sha1)
             VALUES ('docs/a.txt','uid-old','root','a.txt','file',42,'deadbeef',1700000000,'synced',1,'deadbeef')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO event_cursor (scope_id,event_id) VALUES ('scope-1','evt-old')",
            [],
        )
        .unwrap();
    }

    assert!(!column_names(&path).iter().any(|c| c == "mode"));
    let db = Db::open(&path).unwrap();
    let got = db.node_by_path("docs/a.txt").unwrap().unwrap();
    assert_eq!(got.node_uid, "uid-old");
    assert_eq!(got.size, Some(42));
    assert_eq!(got.mode, None, "old rows have no local mode recorded");
    assert!(got.pinned);
    assert_eq!(
        db.event_cursor("scope-1").unwrap().as_deref(),
        Some("evt-old")
    );
    assert!(column_names(&path).iter().any(|c| c == "mode"));

    // Opening again is idempotent: no duplicate column / no data loss.
    drop(db);
    let db = Db::open(&path).unwrap();
    assert_eq!(db.node_by_path("docs/a.txt").unwrap().unwrap().node_uid, "uid-old");
    let modes = column_names(&path).iter().filter(|c| *c == "mode").count();
    assert_eq!(modes, 1, "migration must not add the column twice");
}

#[test]
fn oldest_schema_migrates_every_additive_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    {
        // M1-era schema: none of the later additive node columns exist.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE nodes (
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
            INSERT INTO nodes (local_path,node_uid,name,kind,sync_state)
              VALUES ('old.txt','uid-old','old.txt','file','synced');",
        )
        .unwrap();
    }

    let db = Db::open(&path).unwrap();
    let got = db.node_by_path("old.txt").unwrap().unwrap();
    assert_eq!(got.node_uid, "uid-old");
    assert_eq!(got.synced_sha1, None);
    assert_eq!(got.mode, None);
    drop(db);

    let db = Db::open(&path).unwrap();
    let cols = column_names(&path);
    for c in [
        "synced_sha1",
        "retry_count",
        "last_error",
        "remote_conflicted",
        "mode",
    ] {
        assert_eq!(
            cols.iter().filter(|col| *col == c).count(),
            1,
            "column {c} added exactly once: {cols:?}"
        );
    }
    assert_eq!(db.node_by_path("old.txt").unwrap().unwrap().node_uid, "uid-old");
}

#[test]
fn corrupt_db_is_refused_not_panicked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    std::fs::write(&path, b"this is definitely not a sqlite database\n").unwrap();
    let err = Db::open(&path).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not a database") || msg.contains("malformed"),
        "expected an actionable corruption error, got {err:?}"
    );
    // The bad file is left untouched: refusing lets the user act on it.
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"this is definitely not a sqlite database\n"
    );
}

#[test]
fn truncated_db_is_refused_not_panicked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    {
        let db = Db::open(&path).unwrap();
        db.upsert_node(&row("a.txt", "uid-1", SyncState::Synced))
            .unwrap();
    }
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() > 100);
    std::fs::write(&path, &bytes[..100]).unwrap();
    let err = Db::open(&path).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not a database") || msg.contains("malformed"),
        "expected an actionable corruption error, got {err:?}"
    );
}

/// An ungraceful stop leaves committed transactions in the `-wal` file. The
/// next open must replay them (no clean checkpoint happened).
#[test]
fn wal_recovery_replays_committed_rows_after_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let db = Db::open(&path).unwrap();
    for i in 0..200 {
        db.upsert_node(&row(
            &format!("docs/file-{i}.txt"),
            &format!("uid-{i}"),
            SyncState::Synced,
        ))
        .unwrap();
    }
    db.set_event_cursor("scope-1", "evt-crash").unwrap();

    // Copy the DB while the connection is still open: this is the file set a
    // crashed process leaves behind (no checkpoint on close).
    let crash_dir = dir.path().join("crashed");
    std::fs::create_dir_all(&crash_dir).unwrap();
    let copy = crash_dir.join("state.db");
    std::fs::copy(&path, &copy).unwrap();
    let wal = dir.path().join("state.db-wal");
    assert!(
        wal.is_file(),
        "test needs an uncheckpointed WAL; committed writes must live there"
    );
    std::fs::copy(&wal, crash_dir.join("state.db-wal")).unwrap();
    let shm = dir.path().join("state.db-shm");
    if shm.is_file() {
        std::fs::copy(&shm, crash_dir.join("state.db-shm")).unwrap();
    }
    drop(db); // clean close of the ORIGINAL only; the copy simulates a crash

    let recovered = Db::open(&copy).unwrap();
    assert_eq!(recovered.count_synced().unwrap(), 200);
    assert_eq!(
        recovered.event_cursor("scope-1").unwrap().as_deref(),
        Some("evt-crash")
    );
    assert_eq!(
        recovered.node_by_path("docs/file-199.txt").unwrap().unwrap().node_uid,
        "uid-199"
    );
}

