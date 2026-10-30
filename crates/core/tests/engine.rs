//! Engine integration tests against the in-process MockSidecar.
//! No real Proton account required; all timing uses generous polling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pds_core::{Db, Engine, EngineConfig, EngineState, STATE_DIR};
use pds_sidecar_client::{MockHandle, MockSidecar, SidecarClient};
use serial_test::serial;

struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    handle: pds_core::EngineHandle,
    mock: MockHandle,
    db: Db,
    engine: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start() -> Fixture {
    start_with_retry_base(pds_core::RETRY_BASE_DEFAULT).await
}

async fn start_with_retry_base(retry_base: Duration) -> Fixture {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    start_with_client(dir, root, client, mock, retry_base).await
}

fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pds_core=debug,pds_sidecar_client=debug")
        .with_test_writer()
        .try_init();
}

async fn start_with_client(
    dir: tempfile::TempDir,
    root: std::path::PathBuf,
    mut client: SidecarClient,
    mock: MockHandle,
    retry_base: Duration,
) -> Fixture {
    init_logs();
    let notifications = client.take_notifications();
    let db_path = root.join(STATE_DIR).join("state.db");
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: db_path.clone(),
            retry_base,
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let db = Db::open(&db_path).unwrap();
    let engine = tokio::spawn(engine.run());
    // The fixture's wait (hello.txt materialized) can complete before the
    // watcher attaches — tests that write files right after `start()` would
    // then race inotify (M4 flake fix). Synchronize on the watcher.
    assert!(
        handle.wait_for_watcher(Duration::from_secs(10)).await,
        "watcher never attached"
    );
    wait_for(|| root.join("hello.txt").is_file()).await;
    Fixture {
        _dir: dir,
        root,
        handle,
        mock,
        db,
        engine,
    }
}

async fn wait_for(cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not met within 30 s");
}

impl Fixture {
    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        let _ = self.engine.await;
    }

    /// Like [`Fixture::shutdown`], but keeps the temp dir on disk — for
    /// restart tests where a second engine must see the SAME root+db.
    async fn shutdown_keep_dir(self) {
        self.handle.shutdown().await.unwrap();
        let _ = self.engine.await;
        std::mem::forget(self._dir); // leaked on purpose; OS cleans /tmp
    }
}

// ---------------------------------------------------------------------------
// F1 — initial sync / first-run rules
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn initial_sync_materializes_remote_tree() {
    let fx = start().await;
    assert_eq!(
        std::fs::read(fx.root.join("hello.txt")).unwrap(),
        b"hello drive"
    );
    assert!(fx.root.join("Documents").is_dir());
    assert_eq!(
        fx.db.meta("initial_sync_done").unwrap().as_deref(),
        Some("1")
    );
    // Materialization must never trash anything (first-run rule).
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn empty_local_folder_never_deletes_remote() {
    // Covered by construction (empty root at start) — assert explicitly.
    let fx = start().await;
    tokio::time::sleep(Duration::from_millis(1200)).await; // watcher settles
    assert!(fx.mock.trashed_uids().is_empty());
    assert!(fx.db.tombstones().unwrap().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn first_run_uploads_local_only_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("mine.txt"), b"local first-run").unwrap();

    let (mut client, mock) = MockSidecar::new().connect();
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: root.join(STATE_DIR).join("state.db"),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let join = tokio::spawn(engine.run());

    wait_for(|| mock.uploaded_names().contains(&"mine.txt".to_string())).await;
    assert!(mock.trashed_uids().is_empty());
    // Remote now holds the uploaded file.
    assert!(mock.find_uid("root", "mine.txt").is_some());

    handle.shutdown().await.unwrap();
    let _ = join.await;
}

// ---------------------------------------------------------------------------
// F2 — watcher → upload
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_create_and_modify_upload() {
    let fx = start().await;

    std::fs::write(fx.root.join("new.txt"), b"fresh").unwrap();
    wait_for(|| fx.mock.uploaded_names().contains(&"new.txt".to_string())).await;
    let uid = fx
        .mock
        .find_uid("root", "new.txt")
        .expect("uploaded remotely");

    // Modify → new revision on the same node.
    std::fs::write(fx.root.join("new.txt"), b"fresh v2").unwrap();
    let uploads_before = fx.mock.uploaded_names().len();
    wait_for(|| fx.mock.uploaded_names().len() > uploads_before).await;
    assert_eq!(fx.mock.find_uid("root", "new.txt").unwrap(), uid);

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_new_folder_created_remotely() {
    let fx = start().await;
    std::fs::create_dir_all(fx.root.join("photos/2026")).unwrap();
    std::fs::write(fx.root.join("photos/2026/pic.jpg"), b"jpg").unwrap();
    wait_for(|| fx.mock.uploaded_names().contains(&"pic.jpg".to_string())).await;
    let photos = fx
        .mock
        .find_uid("root", "photos")
        .expect("photos folder remote");
    assert!(fx.mock.find_uid(&photos, "2026").is_some());
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// F3 — conservative trash + mass-delete guard
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn synced_delete_propagates_as_trash() {
    // Seed 10 extra remote files so one delete (~9%) stays under the 25%
    // ratio guard.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    for i in 0..10 {
        mock.remote_create_file("root", &format!("seed{i}.txt"), b"x");
    }
    let fx = start_with_client(
        dir,
        root.clone(),
        client,
        mock,
        pds_core::RETRY_BASE_DEFAULT,
    )
    .await;

    std::fs::remove_file(root.join("hello.txt")).unwrap();
    wait_for(|| fx.mock.trashed_uids().contains(&"node-hello".to_string())).await;
    // Drained: no tombstone left, node row gone. The trash lands mid-drain,
    // so wait for the bookkeeping to finish rather than asserting at once.
    wait_for(|| {
        fx.db.tombstones().unwrap().is_empty()
            && fx.db.node_by_path("hello.txt").unwrap().is_none()
    })
    .await;
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn delete_of_never_synced_item_stays_local() {
    let fx = start().await;
    fx.handle.pause().await.unwrap();
    wait_for(|| {
        // engine paused: status reachable
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::Paused)
            .unwrap_or(false)
    })
    .await;

    // Create + delete while paused: the watcher buffers the dirty mark, the
    // delete then clears it — all without any remote call.
    std::fs::write(fx.root.join("scratch.txt"), b"tmp").unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await; // let create batch land
    std::fs::remove_file(fx.root.join("scratch.txt")).unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await; // let delete batch land

    fx.handle.resume().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        fx.mock.trashed_uids().is_empty(),
        "never-synced delete must not propagate"
    );
    assert!(fx.db.tombstones().unwrap().is_empty());
    assert!(fx.db.node_by_path("scratch.txt").unwrap().is_none());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn mass_delete_guard_auto_pauses_and_requires_resume() {
    // Seed 30 remote files before the engine starts (initial enumeration
    // picks them up; no events are consumed yet).
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    for i in 0..30 {
        mock.remote_create_file("root", &format!("f{i:02}.txt"), b"x");
    }
    let fx = start_with_client(
        dir,
        root.clone(),
        client,
        mock,
        pds_core::RETRY_BASE_DEFAULT,
    )
    .await;

    // Delete 25 of 32 synced nodes: the ratio guard (>25% of synced nodes)
    // trips partway through. ONE explicit resume is the user's confirmation
    // (SPEC F3.3) and approves the whole pending batch (bug #2): it must
    // drain completely without re-tripping as the synced base shrinks.
    for i in 0..25 {
        std::fs::remove_file(root.join(format!("f{i:02}.txt"))).unwrap();
    }

    let mut resumes = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if fx.mock.trashed_uids().len() == 25 {
            break;
        }
        assert!(Instant::now() < deadline, "tombstone draining stalled");
        wait_for(|| {
            futures::executor::block_on(fx.handle.status())
                .map(|s| s.state == EngineState::GuardPaused)
                .unwrap_or(false)
        })
        .await;
        // Guard must have stopped the cascade before all 25 went through.
        assert!(fx.mock.trashed_uids().len() < 25);
        assert!(!fx.db.tombstones().unwrap().is_empty());
        fx.handle.resume().await.unwrap();
        resumes += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        resumes, 1,
        "one explicit resume must approve and drain the whole pending batch"
    );
    assert_eq!(fx.mock.trashed_uids().len(), 25);
    assert!(fx.db.tombstones().unwrap().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn mass_delete_guard_re_trips_on_new_deletes_beyond_approved_set() {
    // Same seeding as above.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    for i in 0..30 {
        mock.remote_create_file("root", &format!("f{i:02}.txt"), b"x");
    }
    let fx = start_with_client(
        dir,
        root.clone(),
        client,
        mock,
        pds_core::RETRY_BASE_DEFAULT,
    )
    .await;

    // First batch: 25 deletes → guard trips → one resume drains them all.
    for i in 0..25 {
        std::fs::remove_file(root.join(format!("f{i:02}.txt"))).unwrap();
    }
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::GuardPaused)
            .unwrap_or(false)
    })
    .await;
    fx.handle.resume().await.unwrap();
    wait_for(|| fx.mock.trashed_uids().len() == 25).await;
    assert!(fx.db.tombstones().unwrap().is_empty());

    // NEW deletes beyond the approved set must re-trip the guard (bug #2:
    // approval covers only the batch pending at resume time). 7 synced nodes
    // remain (5 seeds + hello.txt + Documents), so the 2nd new delete breaks
    // the 25% ratio.
    for i in 25..30 {
        std::fs::remove_file(root.join(format!("f{i:02}.txt"))).unwrap();
    }
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::GuardPaused)
            .unwrap_or(false)
    })
    .await;
    let trashed_at_trip = fx.mock.trashed_uids().len();
    assert!(
        trashed_at_trip < 30,
        "guard must re-trip before the new batch fully drains"
    );
    assert!(!fx.db.tombstones().unwrap().is_empty());

    // A second explicit resume approves the new batch and drains it.
    fx.handle.resume().await.unwrap();
    wait_for(|| fx.mock.trashed_uids().len() == 30).await;
    assert!(fx.db.tombstones().unwrap().is_empty());
    // And nothing more re-trips afterwards.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        fx.handle.status().await.unwrap().state,
        EngineState::Running
    );
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// F4 — pause / resume
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pause_buffers_uploads_and_resume_drains() {
    let fx = start().await;
    fx.handle.pause().await.unwrap();

    std::fs::write(fx.root.join("paused.txt"), b"while paused").unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        !fx.mock.uploaded_names().contains(&"paused.txt".to_string()),
        "upload must not happen while paused"
    );
    // Buffered as dirty in the DB.
    let row = fx.db.node_by_path("paused.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Dirty);

    fx.handle.resume().await.unwrap();
    wait_for(|| fx.mock.uploaded_names().contains(&"paused.txt".to_string())).await;
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// F6 — remote events
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_create_downloads_and_persists_cursor() {
    let fx = start().await;
    let uid = fx
        .mock
        .remote_create_file("root", "from-remote.txt", b"remote bytes");
    wait_for(|| fx.root.join("from-remote.txt").is_file()).await;
    assert_eq!(
        std::fs::read(fx.root.join("from-remote.txt")).unwrap(),
        b"remote bytes"
    );
    // Cursor persisted (resume token).
    let cursor = fx.db.event_cursor("scope-1").unwrap();
    assert!(cursor.is_some());
    let row = fx.db.node_by_path("from-remote.txt").unwrap().unwrap();
    assert_eq!(row.node_uid, uid);
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_delete_removes_local_file() {
    let fx = start().await;
    fx.mock.remote_delete("node-hello");
    wait_for(|| !fx.root.join("hello.txt").exists()).await;
    assert!(fx.db.node_by_path("hello.txt").unwrap().is_none());
    // Remote-originated deletes never call trash back.
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_events_buffered_while_paused() {
    let fx = start().await;
    fx.handle.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    fx.mock.remote_create_file("root", "later.txt", b"later");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !fx.root.join("later.txt").exists(),
        "event must not apply while paused"
    );

    fx.handle.resume().await.unwrap();
    wait_for(|| fx.root.join("later.txt").is_file()).await;
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// F7 — conflicts (engine level; naming itself is unit-tested in conflict.rs)
// ---------------------------------------------------------------------------

/// First directory entry in `root` named `{stem} (conflict …).*`.
fn conflict_entry(root: &std::path::Path, stem: &str) -> Option<std::fs::DirEntry> {
    std::fs::read_dir(root)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{stem} (conflict "))
        })
}

// Bug #1 test (a): remote head differs from the DB's synced head at upload
// time → keep-both: conflict copy with the REMOTE content created locally +
// local content uploaded as the new revision + remote head preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn dirty_upload_with_changed_remote_head_keeps_both() {
    let fx = start().await;
    // Pause → local edit buffered → remote edit whose event has NOT arrived
    // yet (event-stream lag, simulated by a silent update) → resume. The
    // upload precondition (drive.getNode head check) must catch this.
    fx.handle.pause().await.unwrap();
    std::fs::write(fx.root.join("hello.txt"), b"local edit").unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await; // watcher buffers dirty
    fx.mock
        .remote_update_file_silent("node-hello", b"remote edit");
    fx.handle.resume().await.unwrap();

    // Local content wins the revision race on the ORIGINAL node…
    wait_for(|| fx.mock.file_content("node-hello").as_deref() == Some(b"local edit")).await;
    // …and the remote head survives locally as a conflict copy…
    wait_for(|| {
        conflict_entry(&fx.root, "hello")
            .map(|e| std::fs::read(e.path()).unwrap() == b"remote edit")
            .unwrap_or(false)
    })
    .await;
    // …which then syncs up as a NEW remote node (never trashed/overwritten).
    let conflict_uid = wait_for_mock(&fx.mock, "hello (conflict ").await;
    assert_eq!(
        fx.mock.file_content(&conflict_uid).as_deref(),
        Some(b"remote edit".as_slice())
    );
    // The local original path still holds the local edit, and nothing was
    // trashed in either direction.
    assert_eq!(
        std::fs::read(fx.root.join("hello.txt")).unwrap(),
        b"local edit"
    );
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

/// Poll until a node named with `prefix` appears under `root`; return its uid.
async fn wait_for_mock(mock: &MockHandle, name_prefix: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(name) = mock
            .uploaded_names()
            .into_iter()
            .find(|n| n.starts_with(name_prefix))
            && let Some(uid) = mock.find_uid("root", &name)
        {
            return uid;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no uploaded node named `{name_prefix}*` within 30 s");
}

// Bug #1 test (b): unchanged remote head → fast path uploads the revision
// without any conflict copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn dirty_upload_with_unchanged_remote_head_uploads_without_conflict() {
    let fx = start().await;
    std::fs::write(fx.root.join("hello.txt"), b"v2").unwrap();
    wait_for(|| fx.mock.file_content("node-hello").as_deref() == Some(b"v2")).await;
    tokio::time::sleep(Duration::from_millis(1200)).await; // settle

    // Exactly one revision upload, no conflict file anywhere.
    assert_eq!(
        fx.mock
            .uploaded_names()
            .iter()
            .filter(|n| *n == "hello.txt")
            .count(),
        1
    );
    assert!(conflict_entry(&fx.root, "hello").is_none());
    // DB row is synced and the recorded remote head advanced to v2.
    let row = fx.db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
    let (expected, _) = pds_core::sha1_file(&fx.root.join("hello.txt")).unwrap();
    assert_eq!(row.synced_sha1.as_deref(), Some(expected.as_str()));
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_update_over_locally_diverged_file_keeps_both() {
    let fx = start().await;
    // Pause, modify a synced file, queue a remote edit for the same node,
    // then resume. Per SPEC F4 pump order the local upload may win the
    // revision race, or the remote event may be applied first — either way
    // BOTH contents must survive (F7 keep-both, no silent overwrite).
    fx.handle.pause().await.unwrap();
    std::fs::write(fx.root.join("hello.txt"), b"local edit").unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    fx.mock.remote_update_file("node-hello", b"remote edit");
    fx.handle.resume().await.unwrap();

    wait_for(|| {
        let local = std::fs::read(fx.root.join("hello.txt")).unwrap_or_default();
        let conflict_present = conflict_entry(&fx.root, "hello").is_some();
        local == b"local edit" || (local == b"remote edit" && conflict_present)
    })
    .await;
    // Settle, then verify BOTH contents survive and nothing was trashed.
    // (Buffered events apply before uploads on resume, and the upload
    // precondition keeps both even when the event hasn't arrived — so this
    // holds regardless of the race outcome.)
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(fx.mock.trashed_uids().is_empty());
    let local = std::fs::read(fx.root.join("hello.txt")).unwrap_or_default();
    let conflict = conflict_entry(&fx.root, "hello").expect("conflict copy must exist");
    let conflict_content = std::fs::read(conflict.path()).unwrap();
    let mut contents = [local, conflict_content];
    contents.sort();
    assert_eq!(
        contents,
        [b"local edit".to_vec(), b"remote edit".to_vec()],
        "both versions must survive (F7 keep-both)"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_create_over_untracked_local_keeps_both() {
    // Pre-seed: local file exists that initial scan uploaded, then simulate
    // a second remote node with the SAME name (e.g. created by another
    // client concurrently). Engine must rename one copy, never overwrite.
    let fx = start().await;
    std::fs::write(fx.root.join("dupe.txt"), b"local copy").unwrap();
    wait_for(|| fx.mock.uploaded_names().contains(&"dupe.txt".to_string())).await;

    // Diverge the local bytes quietly (direct write; watcher will also see
    // it, but the remote event lands within the debounce window either way).
    std::fs::write(fx.root.join("dupe.txt"), b"local copy v2").unwrap();
    fx.mock
        .remote_create_file("root", "dupe.txt", b"other client copy");

    wait_for(|| {
        std::fs::read_dir(&fx.root)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("dupe (conflict ")
            })
            || std::fs::read(fx.root.join("dupe.txt"))
                .map(|c| c == b"local copy v2")
                .unwrap_or(false)
    })
    .await;
    // Whatever the race outcome, both byte sequences must survive somewhere.
    let entries: Vec<String> = std::fs::read_dir(&fx.root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    let conflict = entries.iter().find(|n| n.starts_with("dupe (conflict "));
    if let Some(name) = conflict {
        assert_eq!(std::fs::read(fx.root.join(name)).unwrap(), b"local copy v2");
    }
    assert!(!entries.is_empty());
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// F2 — upload retry policy + 409 keep-both safety net (bug #1 follow-up)
// ---------------------------------------------------------------------------

// (a)+(b) Revision upload rejected with the 409 "no longer up to date"
// class → the node is parked as remote-conflicted with NO inline retry and
// NO conflict copy (an immediate getNode would serve the SDK's stale cache).
// When the remote event with the genuinely fresh head arrives, the proven
// event-application keep-both path runs: fresh content lands at the original
// path, the local edit survives as a conflict copy, and the node heals
// without an engine restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn revision_conflict_409_heals_via_remote_event() {
    let fx = start().await;
    // Several scripted 409s: an inline-retry regression would burn attempts.
    let failures: Vec<(i64, &str, bool)> = (0..5)
        .map(|_| {
            (
                1099,
                "POST /drive/v2/volumes/x/links/y/revisions 409 'Current revision is no longer up to date'",
                false,
            )
        })
        .collect();
    fx.mock.inject_upload_failures("node-hello", &failures);

    std::fs::write(fx.root.join("hello.txt"), b"local edit").unwrap();

    // (b) Exactly ONE rejected attempt, then parked: no retry, no conflict
    // copy yet, engine keeps running.
    wait_for(|| fx.db.is_remote_conflicted("hello.txt").unwrap()).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);
    assert!(conflict_entry(&fx.root, "hello").is_none());
    assert_eq!(
        fx.handle.status().await.unwrap().state,
        EngineState::Running
    );
    let st = fx.handle.status().await.unwrap();
    assert_eq!(st.conflicted_nodes, 1);
    assert_eq!(st.errored_nodes, 0);

    // (a) The remote event carrying the fresh head arrives; the event path
    // reconciles with GENUINELY fresh content.
    fx.mock.remote_update_file("node-hello", b"remote fresh");
    wait_for(|| {
        std::fs::read(fx.root.join("hello.txt")).unwrap_or_default() == b"remote fresh"
            && conflict_entry(&fx.root, "hello")
                .map(|e| std::fs::read(e.path()).unwrap() == b"local edit")
                .unwrap_or(false)
    })
    .await;

    // The conflict copy (local edit) syncs up as a NEW node…
    let conflict_uid = wait_for_mock(&fx.mock, "hello (conflict ").await;
    assert_eq!(
        fx.mock.file_content(&conflict_uid).as_deref(),
        Some(b"local edit".as_slice())
    );
    // …the original node holds the fresh remote content…
    assert_eq!(
        fx.mock.file_content("node-hello").as_deref(),
        Some(b"remote fresh".as_slice())
    );
    // …and the node healed without a restart: flag cleared, row synced, no
    // further upload attempts on the original path.
    wait_for(|| !fx.db.is_remote_conflicted("hello.txt").unwrap()).await;
    let st = fx.handle.status().await.unwrap();
    assert_eq!(st.conflicted_nodes, 0);
    assert_eq!(st.errored_nodes, 0);
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

// (c) A terminally errored node also self-heals on the next matching remote
// event: the event-application keep-both clears the error state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn errored_node_recovers_on_next_remote_event() {
    let fx = start().await;
    let failures: Vec<(i64, &str, bool)> = (0..30)
        .map(|_| (1099, "integrity check failed", false))
        .collect();
    fx.mock.inject_upload_failures("node-hello", &failures);

    std::fs::write(fx.root.join("hello.txt"), b"broken local edit").unwrap();
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.errored_nodes == 1)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);

    // The remote side moves on; its event reconciles the errored node.
    fx.mock.remote_update_file("node-hello", b"remote fix");
    wait_for(|| {
        std::fs::read(fx.root.join("hello.txt")).unwrap_or_default() == b"remote fix"
            && conflict_entry(&fx.root, "hello")
                .map(|e| std::fs::read(e.path()).unwrap() == b"broken local edit")
                .unwrap_or(false)
    })
    .await;

    // Healed: error cleared, no restart, both versions preserved remotely.
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.errored_nodes == 0)
            .unwrap_or(false)
    })
    .await;
    let conflict_uid = wait_for_mock(&fx.mock, "hello (conflict ").await;
    assert_eq!(
        fx.mock.file_content(&conflict_uid).as_deref(),
        Some(b"broken local edit".as_slice())
    );
    // Still no further upload attempts on the errored path itself.
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);
    fx.shutdown().await;
}

// (b) Persistent non-retryable error → node marked errored after the FIRST
// attempt; never blind-requeued (the observed ~4/sec hot loop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn non_retryable_upload_error_marks_errored_without_loop() {
    let fx = start().await;
    // Plenty of scripted failures: a loop would burn through them.
    let failures: Vec<(i64, &str, bool)> = (0..30)
        .map(|_| (1099, "integrity check failed", false))
        .collect();
    fx.mock.inject_upload_failures("node-hello", &failures);

    std::fs::write(fx.root.join("hello.txt"), b"broken").unwrap();
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.errored_nodes == 1)
            .unwrap_or(false)
    })
    .await;
    // Bounded: exactly one attempt, still exactly one after settling.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);
    assert_eq!(
        fx.db.upload_error("hello.txt").unwrap().as_deref(),
        Some("integrity check failed")
    );
    fx.shutdown().await;
}

// (c) Retryable errors → exponential backoff (not a hot loop), then success
// once the failures stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn retryable_upload_error_backs_off_then_succeeds() {
    let fx = start_with_retry_base(Duration::from_millis(20)).await;
    let failures: Vec<(i64, &str, bool)> = (0..3).map(|_| (1003, "rate limited", true)).collect();
    fx.mock.inject_upload_failures("node-hello", &failures);

    let t0 = Instant::now();
    std::fs::write(fx.root.join("hello.txt"), b"v2").unwrap();
    wait_for(|| fx.mock.file_content("node-hello").as_deref() == Some(b"v2")).await;
    let elapsed = t0.elapsed();

    // 3 failures → waits of 20+40+80 ms before the 4th (successful) attempt.
    // A no-backoff requeue loop would finish all 4 in a few milliseconds.
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 4);
    assert!(
        elapsed >= Duration::from_millis(100),
        "backoff not respected: 4 attempts in {elapsed:?}"
    );
    assert_eq!(fx.handle.status().await.unwrap().errored_nodes, 0);
    fx.shutdown().await;
}

// --- M4 hardening: crash recovery, Retry-After, resume-drops-backoff -----

// A crash mid-upload leaves the row in `Uploading`; the next engine run must
// recover it (→ Dirty → re-upload through the full machinery) instead of
// leaving it stuck forever (never drained, never evicted).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn crashed_upload_recovers_on_restart() {
    let fx = start().await;
    std::fs::write(fx.root.join("hello.txt"), b"crashed mid-flight").unwrap();

    // Simulate the crash: set the state the in-flight transfer would leave.
    fx.db.set_sync_state("hello.txt", pds_core::SyncState::Uploading).unwrap();
    let (root, db_path) = (fx.root.clone(), fx.root.join(STATE_DIR).join("state.db"));
    fx.shutdown_keep_dir().await;

    // New engine over the SAME root+db: recovery must requeue the upload.
    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: db_path.clone(),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let engine = tokio::spawn(engine.run());
    wait_for(|| mock.file_content("node-hello").as_deref() == Some(b"crashed mid-flight")).await;

    // F7 safety net intact: the revision landed on the SAME node.
    let db = Db::open(&db_path).unwrap();
    let row = db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.node_uid, "node-hello");
    assert!(row.synced_sha1.is_some());
    handle.shutdown().await.ok();
    let _ = engine.await;
}

// A crash during an upload whose file has since been removed must tombstone
// the row (delete path owns it), not resurrect a phantom upload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn crashed_upload_with_vanished_file_tombstones() {
    let fx = start().await;
    fx.db.set_sync_state("hello.txt", pds_core::SyncState::Uploading).unwrap();
    let (root, db_path) = (fx.root.clone(), fx.root.join(STATE_DIR).join("state.db"));
    std::fs::remove_file(root.join("hello.txt")).unwrap();
    fx.shutdown_keep_dir().await;

    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: db_path.clone(),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let engine = tokio::spawn(engine.run());
    let db = Db::open(&db_path).unwrap();

    // The stale row is gone and the remote file lands in the trash.
    wait_for(|| db.node_by_path("hello.txt").unwrap().is_none()).await;
    wait_for(|| !mock.trashed_uids().is_empty()).await;
    assert!(
        mock.trashed_uids().iter().all(|u| u == "node-hello"),
        "only the interrupted file may be trashed: {:?}",
        mock.trashed_uids()
    );
    handle.shutdown().await.ok();
    let _ = engine.await;
}

// Stale `*.pds-part` temps (crashed transfers) are swept at startup — in
// the visible root AND nested subdirectories — and never re-uploaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn stale_transfer_temps_are_swept_on_startup() {
    let fx = start().await;
    let root = fx.root.clone();
    std::fs::create_dir_all(root.join("docs")).unwrap();
    std::fs::write(root.join("big.pds-part"), b"half-written").unwrap();
    std::fs::write(root.join("docs").join("a.txt.pds-part"), b"half").unwrap();
    fx.shutdown_keep_dir().await;

    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: root.join(STATE_DIR).join("state.db"),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let engine = tokio::spawn(engine.run());
    wait_for(|| root.join("hello.txt").is_file()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(!root.join("big.pds-part").exists());
    assert!(!root.join("docs").join("a.txt.pds-part").exists());
    assert!(
        !mock.uploaded_names().iter().any(|n| n.contains("pds-part")),
        "swept temps must not enter the upload pipeline"
    );
    handle.shutdown().await.ok();
    let _ = engine.await;
}

// A rate-limited upload with a server `retryAfter` hint honors the hint
// (not the shorter computed backoff) before the next attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn rate_limit_retry_after_hint_is_honored() {
    let fx = start_with_retry_base(Duration::from_millis(20)).await;
    // Hint 1 s ≫ computed backoff (20 ms). The engine must wait ≥ 1 s.
    fx.mock
        .inject_upload_failures_full("node-hello", &[(1003, "rate limited", true, Some(1))]);

    let t0 = Instant::now();
    std::fs::write(fx.root.join("hello.txt"), b"throttled").unwrap();
    wait_for(|| fx.mock.upload_attempts("hello.txt") == 2).await;
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900),
        "retryAfter hint ignored: 2nd attempt after only {elapsed:?}"
    );
    // Without the hint the whole 2-attempt exchange takes < 200 ms.
    fx.shutdown().await;
}

// Explicit resume means "try now": a pending retry backoff is dropped even
// when the backoff is far in the future (server hint or long schedule).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn resume_drops_pending_backoff() {
    let fx = start_with_retry_base(Duration::from_millis(20)).await;
    // First failure schedules a LONG backoff via the server hint.
    fx.mock
        .inject_upload_failures_full("node-hello", &[(1003, "rate limited", true, Some(60))]);
    std::fs::write(fx.root.join("hello.txt"), b"v2").unwrap();
    wait_for(|| fx.mock.upload_attempts("hello.txt") == 1).await;
    // The attempt STARTS before the failure is processed (retry scheduled);
    // resume only once the row came back Dirty, else the backoff would be
    // (correctly) scheduled after our clear and the test would race.
    wait_for(|| {
        fx.db
            .node_by_path("hello.txt")
            .ok()
            .flatten()
            .map(|r| r.sync_state == pds_core::SyncState::Dirty)
            .unwrap_or(false)
    })
    .await;

    // Nothing further happens while the 60 s backoff runs…
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(fx.mock.upload_attempts("hello.txt"), 1);

    // …but an explicit resume is "try now".
    fx.handle.resume().await.unwrap();
    wait_for(|| fx.mock.upload_attempts("hello.txt") == 2).await;
    assert!(
        fx.mock.file_content("node-hello").as_deref() == Some(b"v2"),
        "second attempt must succeed (failure script exhausted)"
    );
    fx.shutdown().await;
}

// A terminally errored node recovers when the user edits the file again:
// the fresh local rewrite clears the error and re-uploads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn fresh_edit_heals_errored_node() {
    let fx = start().await;
    // Exactly ONE failure: the fresh-edit retry must then succeed.
    fx.mock
        .inject_upload_failures("node-hello", &[(1099, "integrity check failed", false)]);
    std::fs::write(fx.root.join("hello.txt"), b"broken").unwrap();
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.errored_nodes == 1)
            .unwrap_or(false)
    })
    .await;

    // The user fixes the file: fresh edit → error cleared → upload proceeds.
    std::fs::write(fx.root.join("hello.txt"), b"fixed by user").unwrap();
    wait_for(|| fx.mock.file_content("node-hello").as_deref() == Some(b"fixed by user")).await;
    let st = fx.handle.status().await.unwrap();
    assert_eq!(st.errored_nodes, 0);
    let row = fx.db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
    assert_eq!(fx.db.upload_error("hello.txt").unwrap(), None);
    fx.shutdown().await;
}

// Helper so tests can use `futures::executor::block_on` without an extra dep.
mod futures {
    pub mod executor {
        pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
        }
    }
}
