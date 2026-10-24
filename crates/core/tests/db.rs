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
