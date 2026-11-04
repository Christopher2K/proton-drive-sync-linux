//! Engine integration tests against the in-process MockSidecar.
//! No real Proton account required; all timing uses generous polling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pds_core::{ActivityKind, Db, Engine, EngineConfig, EngineState, STATE_DIR};
use pds_sidecar_client::{DriveEvent, EventKind, MockHandle, MockSidecar, SidecarClient};
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
            ..Default::default()
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

/// Wait until the node's upload bookkeeping is fully settled: `Synced` with
/// no recorded upload error. The mock applies the uploaded bytes BEFORE the
/// engine's success handler writes that bookkeeping, so waiting only on
/// `MockHandle::file_content` can observe the content while the row is still
/// Dirty/errored (CI flake, 2026-11-04).
async fn wait_for_synced(db: &Db, rel: &str) {
    wait_for(|| {
        let Ok(Some(row)) = db.node_by_path(rel) else {
            return false;
        };
        row.sync_state == pds_core::SyncState::Synced
            && db.upload_error(rel).is_ok_and(|error| error.is_none())
    })
    .await;
}

/// Wait until every one of `total` local deletes is accounted for — already
/// trashed remotely, or still pending as a tombstone.
///
/// The watcher delivers removes in debounced batches, so a burst of deletes
/// can reach the engine as several batches under load. A guard-paused engine
/// keeps recording them but drains nothing, so "the pending batch" is only
/// final once nothing is in flight between the watcher and the DB. Without
/// this wait, a resume can approve a partial batch and the late-arriving
/// tombstones legitimately re-trip the guard (CI flake, 2026-10-31).
async fn wait_for_deletes_accounted(fx: &Fixture, total: usize) {
    wait_for(|| {
        fx.db
            .tombstones()
            .map(|pending| pending.len() + fx.mock.trashed_uids().len() == total)
            .unwrap_or(false)
    })
    .await;
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
            ..Default::default()
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
    //
    // The removals arrive in two watcher batches (separated by the ~500 ms
    // debounce) on purpose: that is the CI flake of 2026-10-31 — the guard
    // paused on batch one and re-tripped on batch-two tombstones that landed
    // after the resume. Keep the split so the multi-batch path stays covered.
    for i in 0..12 {
        std::fs::remove_file(root.join(format!("f{i:02}.txt"))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for i in 12..25 {
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
        // The removes may have arrived as several watcher batches; the guard
        // paused after the first one. Wait until all 25 are known (pending
        // tombstones + already trashed) so this resume covers the whole batch.
        wait_for_deletes_accounted(&fx, 25).await;
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
    // Same batching caveat as above: wait until all 25 deletes are accounted
    // for (pending or trashed) so the resume approves the whole batch.
    wait_for_deletes_accounted(&fx, 25).await;
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
    // Late watcher batches would re-trip again; absorb them before resuming.
    wait_for_deletes_accounted(&fx, 30).await;

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
            ..Default::default()
        },
    )
    .unwrap();
    let engine = tokio::spawn(engine.run());
    wait_for(|| mock.file_content("node-hello").as_deref() == Some(b"crashed mid-flight")).await;

    // F7 safety net intact: the revision landed on the SAME node.
    let db = Db::open(&db_path).unwrap();
    wait_for_synced(&db, "hello.txt").await;
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
            ..Default::default()
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
            ..Default::default()
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
    wait_for_synced(&fx.db, "hello.txt").await;
    assert_eq!(
        fx.mock.file_content("node-hello").as_deref(),
        Some(b"fixed by user".as_slice())
    );
    let st = fx.handle.status().await.unwrap();
    assert_eq!(st.errored_nodes, 0);
    let row = fx.db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
    assert_eq!(fx.db.upload_error("hello.txt").unwrap(), None);
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// Remote renames / moves (TG-01, TG-02) and file <-> folder collisions
// (TG-03), plus non-UTF-8 local names (TG-04).
// ---------------------------------------------------------------------------

/// First entry in `dir` whose name starts with `prefix` (conflict copies).
fn entry_with_prefix(dir: &std::path::Path, prefix: &str) -> Option<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(prefix))
        })
}

/// Drain the activity receiver and return every item collected so far.
fn drain_activity(rx: &mut tokio::sync::broadcast::Receiver<pds_core::ActivityItem>) -> Vec<pds_core::ActivityItem> {
    let mut out = Vec::new();
    while let Ok(item) = rx.try_recv() {
        out.push(item);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_file_rename_rekeys_row_and_preserves_content() {
    let fx = start().await;
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_rename("node-hello", "renamed.txt");

    // The row is resolved by uid and re-keyed; the old path is gone.
    wait_for(|| {
        fx.db
            .node_by_path("renamed.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.node_uid == "node-hello")
            && fx.db.node_by_path("hello.txt").ok().flatten().is_none()
    })
    .await;
    assert_eq!(
        std::fs::read(fx.root.join("renamed.txt")).unwrap(),
        b"hello drive"
    );
    assert!(!fx.root.join("hello.txt").exists());
    // Hydrated content moved with the row: no re-download, no upload, no trash.
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);
    assert_eq!(
        fx.db
            .all_nodes()
            .unwrap()
            .iter()
            .filter(|r| r.node_uid == "node-hello")
            .count(),
        1,
        "no duplicate rows for the renamed node"
    );
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_file_move_updates_parent_and_materializes_destination() {
    let fx = start().await;
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_move("node-hello", "node-docs");

    wait_for(|| {
        fx.db
            .node_by_path("Documents/hello.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.node_uid == "node-hello")
            && fx.db.node_by_path("hello.txt").ok().flatten().is_none()
    })
    .await;
    let row = fx.db.node_by_path("Documents/hello.txt").unwrap().unwrap();
    assert_eq!(row.parent_uid.as_deref(), Some("node-docs"));
    assert_eq!(
        std::fs::read(fx.root.join("Documents/hello.txt")).unwrap(),
        b"hello drive"
    );
    assert!(!fx.root.join("hello.txt").exists());
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_folder_rename_rekeys_subtree_and_preserves_nested_content() {
    let fx = start().await;
    fx.mock
        .remote_create_file("node-docs", "nested.txt", b"nested bytes");
    wait_for(|| fx.root.join("Documents/nested.txt").is_file()).await;
    let nested_uid = fx.mock.find_uid("node-docs", "nested.txt").unwrap();
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_rename("node-docs", "Docs");

    wait_for(|| {
        fx.db
            .node_by_path("Docs/nested.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.node_uid == nested_uid)
            && fx.db.node_by_path("Documents/nested.txt").ok().flatten().is_none()
    })
    .await;
    assert_eq!(
        fx.db.node_by_path("Docs").unwrap().unwrap().node_uid,
        "node-docs"
    );
    assert_eq!(
        std::fs::read(fx.root.join("Docs/nested.txt")).unwrap(),
        b"nested bytes"
    );
    assert!(!fx.root.join("Documents").exists());
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);
    assert_eq!(
        fx.db
            .node_by_path("Docs/nested.txt")
            .unwrap()
            .unwrap()
            .parent_uid
            .as_deref(),
        Some("node-docs")
    );
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_folder_move_rekeys_subtree_and_back_to_root() {
    let fx = start().await;
    fx.mock
        .remote_create_file("node-docs", "nested.txt", b"nested bytes");
    wait_for(|| fx.root.join("Documents/nested.txt").is_file()).await;
    std::fs::create_dir(fx.root.join("Archive")).unwrap();
    wait_for(|| fx.mock.find_uid("root", "Archive").is_some()).await;
    let archive = fx.mock.find_uid("root", "Archive").unwrap();
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_move("node-docs", &archive);
    wait_for(|| {
        fx.root.join("Archive/Documents/nested.txt").is_file()
            && fx.db
                .node_by_path("Archive/Documents/nested.txt")
                .ok()
                .flatten()
                .is_some()
    })
    .await;
    assert!(fx.db.node_by_path("Documents").unwrap().is_none());
    assert_eq!(
        std::fs::read(fx.root.join("Archive/Documents/nested.txt")).unwrap(),
        b"nested bytes"
    );
    assert_eq!(
        fx.db
            .node_by_path("Archive/Documents")
            .unwrap()
            .unwrap()
            .parent_uid
            .as_deref(),
        Some(archive.as_str())
    );

    // Moving the folder back to the root rebuilds the parent chain.
    fx.mock.remote_move("node-docs", "root");
    wait_for(|| {
        fx.root.join("Documents/nested.txt").is_file()
            && fx.db
                .node_by_path("Documents/nested.txt")
                .ok()
                .flatten()
                .is_some()
    })
    .await;
    assert!(fx.db.node_by_path("Archive/Documents").unwrap().is_none());
    assert_eq!(
        std::fs::read(fx.root.join("Documents/nested.txt")).unwrap(),
        b"nested bytes"
    );
    assert_eq!(
        fx.db
            .node_by_path("Documents")
            .unwrap()
            .unwrap()
            .parent_uid
            .as_deref(),
        Some("root")
    );
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_folder_over_local_file_keeps_both() {
    let fx = start().await;
    fx.handle.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::write(fx.root.join("clash"), b"local file").unwrap();
    // The watcher indexes it while paused (buffered as a dirty upload).
    wait_for(|| fx.db.node_by_path("clash").ok().flatten().is_some()).await;
    let mut activity = fx.handle.subscribe_activity();

    fx.mock.remote_create_folder("root", "clash");
    fx.handle.resume().await.unwrap();

    wait_for(|| {
        fx.db
            .node_by_path("clash")
            .ok()
            .flatten()
            .is_some_and(|r| !r.is_file())
            && fx.root.join("clash").is_dir()
            && entry_with_prefix(&fx.root, "clash (conflict ")
                .is_some_and(|p| std::fs::read(&p).ok().as_deref() == Some(b"local file".as_slice()))
    })
    .await;
    assert!(fx.mock.trashed_uids().is_empty());
    let items = drain_activity(&mut activity);
    assert!(
        items.iter().any(|i| i.kind == ActivityKind::Conflict && i.path == "clash"),
        "expected a conflict activity entry, got {items:?}"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_file_over_local_folder_keeps_both_with_children() {
    let fx = start().await;
    fx.handle.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::create_dir(fx.root.join("clashdir")).unwrap();
    std::fs::write(fx.root.join("clashdir/child.txt"), b"local child").unwrap();
    wait_for(|| {
        fx.db
            .node_by_path("clashdir/child.txt")
            .ok()
            .flatten()
            .is_some()
    })
    .await;
    let mut activity = fx.handle.subscribe_activity();

    fx.mock
        .remote_create_file("root", "clashdir", b"remote file bytes");
    fx.handle.resume().await.unwrap();

    wait_for(|| {
        fx.root.join("clashdir").is_file()
            && fx.db
                .node_by_path("clashdir")
                .ok()
                .flatten()
                .is_some_and(|r| r.is_file())
            && entry_with_prefix(&fx.root, "clashdir (conflict ").is_some_and(|p| {
                p.is_dir()
                    && std::fs::read(p.join("child.txt")).ok().as_deref()
                        == Some(b"local child".as_slice())
            })
    })
    .await;
    assert_eq!(
        std::fs::read(fx.root.join("clashdir")).unwrap(),
        b"remote file bytes"
    );
    // The nested child followed its renamed parent (same uid).
    let conflict_dir = entry_with_prefix(&fx.root, "clashdir (conflict ").unwrap();
    let conflict_name = conflict_dir.file_name().unwrap().to_string_lossy();
    let child_row = fx
        .db
        .node_by_path(&format!("{conflict_name}/child.txt"))
        .unwrap()
        .expect("child row follows the folder rename");
    let conflict_uid = fx
        .db
        .node_by_path(&conflict_name)
        .unwrap()
        .expect("conflict folder row")
        .node_uid;
    assert_eq!(child_row.parent_uid.as_deref(), Some(conflict_uid.as_str()));
    assert!(fx.mock.trashed_uids().is_empty());
    let items = drain_activity(&mut activity);
    assert!(
        items
            .iter()
            .any(|i| i.kind == ActivityKind::Conflict && i.path == "clashdir"),
        "expected a conflict activity entry, got {items:?}"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_rename_onto_existing_local_path_keeps_both() {
    let fx = start().await;
    std::fs::write(fx.root.join("occupied.txt"), b"local occupant").unwrap();
    wait_for_synced(&fx.db, "occupied.txt").await;
    let occupant_uid = fx.mock.find_uid("root", "occupied.txt").unwrap();
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_rename("node-hello", "occupied.txt");

    // The renamed remote node takes its new path; the local occupant is
    // renamed aside, never clobbered.
    wait_for(|| {
        fx.db
            .node_by_path("occupied.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.node_uid == "node-hello")
            && entry_with_prefix(&fx.root, "occupied (conflict ")
                .is_some_and(|p| std::fs::read(&p).ok().as_deref() == Some(b"local occupant".as_slice()))
    })
    .await;
    assert_eq!(
        std::fs::read(fx.root.join("occupied.txt")).unwrap(),
        b"hello drive"
    );
    let conflict = entry_with_prefix(&fx.root, "occupied (conflict ").unwrap();
    assert_eq!(std::fs::read(&conflict).unwrap(), b"local occupant");
    let occupant_row = fx.db.node_by_uid(&occupant_uid).unwrap().unwrap();
    assert!(
        occupant_row.local_path.starts_with("occupied (conflict "),
        "occupant row re-keyed to {}, got {}",
        conflict.display(),
        occupant_row.local_path
    );
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn non_utf8_filename_is_skipped_and_reported_once() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let fx = start().await;
    let mut activity = fx.handle.subscribe_activity();
    let bad = fx.root.join(OsStr::from_bytes(b"bad\xffname.txt"));
    std::fs::write(&bad, b"not representable").unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut report = None;
    while Instant::now() < deadline {
        for item in drain_activity(&mut activity) {
            if item.kind == ActivityKind::Skipped && item.path.contains("bad") {
                report = Some(item);
            }
        }
        if report.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let report = report.expect("non-UTF-8 filename must surface in activity");
    assert!(
        report
            .detail
            .as_deref()
            .is_some_and(|d| d.contains("ff") && d.to_ascii_lowercase().contains("hex")),
        "raw bytes must be visible as hex, got {:?}",
        report.detail
    );
    // The item survives untouched: never uploaded, never deleted.
    assert_eq!(std::fs::read(&bad).unwrap(), b"not representable");
    assert!(
        !fx.mock
            .uploaded_names()
            .iter()
            .any(|n| n.contains("bad") || n.contains('\u{fffd}')),
        "non-UTF-8 file must not be uploaded"
    );
    assert!(fx.db.tombstones().unwrap().is_empty());

    // Settle: the warning is once per path, not once per watcher batch.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let repeats = drain_activity(&mut activity)
        .into_iter()
        .filter(|i| i.kind == ActivityKind::Skipped && i.path.contains("bad"))
        .count();
    assert_eq!(repeats, 0, "non-UTF-8 warning repeated for the same path");

    // Engine stays healthy and keeps syncing normal files.
    std::fs::write(fx.root.join("normal.txt"), b"normal").unwrap();
    wait_for(|| fx.mock.uploaded_names().contains(&"normal.txt".to_string())).await;
    assert_eq!(
        fx.handle.status().await.unwrap().state,
        EngineState::Running
    );
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// Wave 1a — local path lifecycle: delete/re-create, folder rename/move,
// folder delete, remote trash→restore, and event-ordering regressions.
// ---------------------------------------------------------------------------

/// Seed `n` remote filler files and wait until each is tracked and synced.
/// Keeps a delete under the mass-deletion guard's 25% ratio limb in small
/// fixtures (a single delete on a tiny tree would otherwise trip it).
async fn seed_synced_fillers(fx: &Fixture, n: usize) {
    for i in 0..n {
        fx.mock
            .remote_create_file("root", &format!("filler{i:02}.txt"), b"filler");
    }
    for i in 0..n {
        wait_for_synced(&fx.db, &format!("filler{i:02}.txt")).await;
    }
}

// TG-05: a remove followed immediately by a write of new bytes at the same
// path must converge on the new content — never trash the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_delete_then_recreate_in_same_debounce_window_keeps_new_content() {
    let fx = start().await;
    std::fs::write(fx.root.join("churn.txt"), b"v1").unwrap();
    wait_for_synced(&fx.db, "churn.txt").await;
    let uid = fx.mock.find_uid("root", "churn.txt").expect("uploaded");
    let (expected, _) = pds_core::sha1_file(&fx.root.join("churn.txt")).unwrap();
    assert_eq!(fx.db.node_by_path("churn.txt").unwrap().unwrap().sha1, Some(expected));

    // Remove and re-create inside one watcher debounce window.
    std::fs::remove_file(fx.root.join("churn.txt")).unwrap();
    std::fs::write(fx.root.join("churn.txt"), b"v2 re-created").unwrap();

    wait_for(|| {
        fx.mock.file_content(&uid).as_deref() == Some(b"v2 re-created".as_slice())
    })
    .await;
    wait_for_synced(&fx.db, "churn.txt").await;
    // Settle: a stale tombstone would drain after the upload and trash the
    // very node that just received the new content.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(
        fx.mock
            .find_uid("root", "churn.txt")
            .as_deref(),
        Some(uid.as_str()),
        "the re-created content must stay on the original node"
    );
    assert_eq!(
        fx.mock
            .child_names("root")
            .iter()
            .filter(|n| *n == "churn.txt")
            .count(),
        1,
        "exactly one remote node at the path: {:?}",
        fx.mock.child_names("root")
    );
    assert!(
        !fx.mock.trashed_uids().contains(&uid),
        "stale delete reached the remote: {:?}",
        fx.mock.trashed_uids()
    );
    assert!(conflict_entry(&fx.root, "churn").is_none());
    assert!(fx.db.tombstones().unwrap().is_empty());
    let row = fx.db.node_by_path("churn.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
    let (expected, _) = pds_core::sha1_file(&fx.root.join("churn.txt")).unwrap();
    assert_eq!(row.sha1.as_deref(), Some(expected.as_str()));
    fx.shutdown().await;
}

// TG-05 (deterministic variant): the delete batch is received and its
// tombstone is pending while the file is re-created before the next drain.
// The write must cancel that tombstone; resuming must not trash the node it
// is about to update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn recreated_file_cancels_pending_tombstone() {
    let fx = start().await;
    seed_synced_fillers(&fx, 10).await;
    std::fs::write(fx.root.join("churn.txt"), b"v1").unwrap();
    wait_for_synced(&fx.db, "churn.txt").await;
    let uid = fx.mock.find_uid("root", "churn.txt").unwrap();

    // Pause so the delete and the re-create land as separate watcher batches
    // and nothing drains between them.
    fx.handle.pause().await.unwrap();
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::Paused)
            .unwrap_or(false)
    })
    .await;

    std::fs::remove_file(fx.root.join("churn.txt")).unwrap();
    wait_for(|| {
        fx.db
            .tombstones()
            .map(|t| t.iter().any(|t| t.local_path == "churn.txt"))
            .unwrap_or(false)
    })
    .await;
    std::fs::write(fx.root.join("churn.txt"), b"v2 re-created").unwrap();
    wait_for(|| {
        fx.db
            .node_by_path("churn.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.sync_state == pds_core::SyncState::Dirty)
    })
    .await;

    fx.handle.resume().await.unwrap();
    wait_for(|| {
        fx.mock.file_content(&uid).as_deref() == Some(b"v2 re-created".as_slice())
    })
    .await;
    wait_for_synced(&fx.db, "churn.txt").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert!(
        !fx.mock.trashed_uids().contains(&uid),
        "pending delete trashed the re-created node: {:?}",
        fx.mock.trashed_uids()
    );
    assert_eq!(fx.mock.find_uid("root", "churn.txt").as_deref(), Some(uid.as_str()));
    assert_eq!(
        fx.mock
            .child_names("root")
            .iter()
            .filter(|n| *n == "churn.txt")
            .count(),
        1
    );
    assert!(fx.db.tombstones().unwrap().is_empty());
    assert!(conflict_entry(&fx.root, "churn").is_none());
    let row = fx.db.node_by_path("churn.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
    let (expected, _) = pds_core::sha1_file(&fx.root.join("churn.txt")).unwrap();
    assert_eq!(row.sha1.as_deref(), Some(expected.as_str()));
    fx.shutdown().await;
}

// TG-05 variant: the delete has already drained (node trashed, row gone)
// while the replacement upload has not run yet. The replacement must arrive
// as a node of its own and the new contents must not be lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn recreate_after_delete_drained_uploads_new_node() {
    let fx = start().await;
    seed_synced_fillers(&fx, 10).await;
    std::fs::write(fx.root.join("churn.txt"), b"v1").unwrap();
    wait_for_synced(&fx.db, "churn.txt").await;
    let old_uid = fx.mock.find_uid("root", "churn.txt").unwrap();

    // Delete and let the tombstone drain completely.
    std::fs::remove_file(fx.root.join("churn.txt")).unwrap();
    wait_for(|| fx.mock.trashed_uids().contains(&old_uid)).await;
    wait_for(|| fx.db.node_by_path("churn.txt").unwrap().is_none()).await;

    // Re-create while paused so its upload cannot start yet: this is the
    // "delete drained, upload not completed" window.
    fx.handle.pause().await.unwrap();
    std::fs::write(fx.root.join("churn.txt"), b"v2 replacement").unwrap();
    wait_for(|| {
        fx.db
            .node_by_path("churn.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.sync_state == pds_core::SyncState::Dirty)
    })
    .await;
    assert!(
        fx.mock.find_uid("root", "churn.txt").is_none(),
        "the re-created file must not exist remotely until its upload runs"
    );

    fx.handle.resume().await.unwrap();
    let new_uid = wait_for_mock(&fx.mock, "churn.txt").await;
    wait_for(|| {
        fx.mock.file_content(&new_uid).as_deref() == Some(b"v2 replacement".as_slice())
    })
    .await;
    wait_for_synced(&fx.db, "churn.txt").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Exactly one node at the path, carrying the replacement bytes; the old
    // node was trashed once, not the new one.
    assert_eq!(
        fx.mock
            .child_names("root")
            .iter()
            .filter(|n| *n == "churn.txt")
            .count(),
        1
    );
    assert_eq!(fx.mock.find_uid("root", "churn.txt").as_deref(), Some(new_uid.as_str()));
    assert_eq!(
        fx.mock.trashed_uids().iter().filter(|u| **u == old_uid).count(),
        1
    );
    assert!(!fx.mock.trashed_uids().contains(&new_uid));
    assert!(fx.db.tombstones().unwrap().is_empty());
    assert!(conflict_entry(&fx.root, "churn").is_none());
    fx.shutdown().await;
}

// TG-06: a local folder rename/move in mirror mode mirrors as rename+move —
// never trash + re-upload — and keeps every descendant uid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_folder_rename_and_move_preserves_uids() {
    let fx = start().await;
    std::fs::create_dir_all(fx.root.join("Project/sub")).unwrap();
    std::fs::write(fx.root.join("Project/a.txt"), b"alpha").unwrap();
    std::fs::write(fx.root.join("Project/b.txt"), b"beta").unwrap();
    std::fs::write(fx.root.join("Project/sub/c.txt"), b"gamma").unwrap();
    for rel in ["Project/a.txt", "Project/b.txt", "Project/sub/c.txt"] {
        wait_for_synced(&fx.db, rel).await;
    }
    wait_for(|| fx.mock.find_uid("root", "Project").is_some()).await;
    let project_uid = fx.mock.find_uid("root", "Project").unwrap();
    let sub_uid = fx.mock
        .find_uid(&project_uid, "sub")
        .expect("sub remote");
    let a_uid = fx.mock.find_uid(&project_uid, "a.txt").unwrap();
    let b_uid = fx.mock.find_uid(&project_uid, "b.txt").unwrap();
    let c_uid = fx.mock.find_uid(&sub_uid, "c.txt").unwrap();
    let all_uids = [project_uid.clone(), sub_uid.clone(), a_uid.clone(), b_uid.clone(), c_uid.clone()];
    let downloads = fx.mock.downloaded_uids().len();
    let uploads = fx.mock.uploaded_names().len();

    // A real folder elsewhere for the move step.
    std::fs::create_dir(fx.root.join("Archive")).unwrap();
    wait_for(|| fx.mock.find_uid("root", "Archive").is_some()).await;
    let archive_uid = fx.mock.find_uid("root", "Archive").unwrap();

    // 1. Rename at the root.
    std::fs::rename(fx.root.join("Project"), fx.root.join("Renamed")).unwrap();
    wait_for(|| {
        fx.db.node_by_path("Renamed/sub/c.txt").ok().flatten().is_some()
            && fx.db.node_by_path("Project").ok().flatten().is_none()
    })
    .await;
    wait_for(|| fx.mock.find_uid("root", "Renamed").as_deref() == Some(project_uid.as_str())).await;
    assert_eq!(fx.mock.find_uid(&project_uid, "sub").as_deref(), Some(sub_uid.as_str()));
    assert_eq!(fx.mock.find_uid(&project_uid, "a.txt").as_deref(), Some(a_uid.as_str()));
    assert_eq!(fx.mock.find_uid(&sub_uid, "c.txt").as_deref(), Some(c_uid.as_str()));

    // 2. Move under Archive.
    std::fs::rename(fx.root.join("Renamed"), fx.root.join("Archive/Renamed")).unwrap();
    wait_for(|| {
        fx.db.node_by_path("Archive/Renamed/sub/c.txt").ok().flatten().is_some()
            && fx.db.node_by_path("Renamed").ok().flatten().is_none()
    })
    .await;
    wait_for(|| {
        fx.mock
            .child_names(&archive_uid)
            .iter()
            .any(|n| n == "Renamed")
    })
    .await;
    assert_eq!(
        fx.db
            .node_by_path("Archive/Renamed")
            .unwrap()
            .expect("folder row re-keyed")
            .parent_uid
            .as_deref(),
        Some(archive_uid.as_str())
    );

    // 3. Move back to the root.
    std::fs::rename(fx.root.join("Archive/Renamed"), fx.root.join("Renamed")).unwrap();
    wait_for(|| {
        fx.db.node_by_path("Renamed/sub/c.txt").ok().flatten().is_some()
            && fx.db
                .node_by_path("Archive/Renamed")
                .ok()
                .flatten()
                .is_none()
    })
    .await;
    wait_for(|| fx.mock.child_names("root").iter().any(|n| n == "Renamed")).await;

    // Rename/move only: no trash, no re-upload, same uids throughout.
    assert!(
        fx.mock.trashed_uids().is_empty(),
        "folder rename/move must never trash: {:?}",
        fx.mock.trashed_uids()
    );
    assert_eq!(fx.mock.uploaded_names().len(), uploads, "no re-uploads");
    assert_eq!(fx.mock.downloaded_uids().len(), downloads, "no re-downloads");
    for uid in &all_uids {
        assert_eq!(
            fx.db
                .all_nodes()
                .unwrap()
                .iter()
                .filter(|r| &r.node_uid == uid)
                .count(),
            1,
            "uid {uid} must still have exactly one row"
        );
    }
    // Content stays intact and the mock saw the remote rename/move calls.
    assert_eq!(fx.mock.file_content(&a_uid).as_deref(), Some(b"alpha".as_slice()));
    assert_eq!(fx.mock.file_content(&b_uid).as_deref(), Some(b"beta".as_slice()));
    assert_eq!(fx.mock.file_content(&c_uid).as_deref(), Some(b"gamma".as_slice()));
    assert_eq!(fx.mock.find_uid(&sub_uid, "c.txt").as_deref(), Some(c_uid.as_str()));
    assert!(
        fx.mock.renames().iter().any(|(uid, name)| uid == &project_uid && name == "Renamed"),
        "expected a remote rename of {project_uid}: {:?}",
        fx.mock.renames()
    );
    assert!(
        fx.mock
            .moves()
            .iter()
            .any(|(uid, parent)| uid == &project_uid && parent == &archive_uid),
        "expected a remote move of {project_uid} under {archive_uid}: {:?}",
        fx.mock.moves()
    );
    // DB paths re-keyed including the nested descendant.
    let nested = fx.db.node_by_path("Renamed/sub/c.txt").unwrap().unwrap();
    assert_eq!(nested.node_uid, c_uid);
    assert_eq!(nested.parent_uid.as_deref(), Some(sub_uid.as_str()));
    fx.shutdown().await;
}

// TG-07: deleting a whole non-empty folder locally trashes every tracked
// node under it exactly once and removes all rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_delete_of_nonempty_folder_trashes_each_node_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    let vault = mock.remote_create_folder("root", "Vault");
    let a = mock.remote_create_file(&vault, "a.txt", b"a bytes");
    let b = mock.remote_create_file(&vault, "b.txt", b"b bytes");
    let sub = mock.remote_create_folder(&vault, "sub");
    let c = mock.remote_create_file(&sub, "c.txt", b"c bytes");
    // Filler nodes keep five deletes comfortably under the >25% guard limb.
    for i in 0..20 {
        mock.remote_create_file("root", &format!("filler{i:02}.txt"), b"filler");
    }
    let fx = start_with_client(
        dir,
        root.clone(),
        client,
        mock,
        pds_core::RETRY_BASE_DEFAULT,
    )
    .await;
    for rel in ["Vault/a.txt", "Vault/b.txt", "Vault/sub/c.txt"] {
        wait_for_synced(&fx.db, rel).await;
    }
    assert_eq!(
        fx.db.node_by_path("Vault").unwrap().unwrap().node_uid,
        vault
    );

    // Pause so every remove in the burst is recorded as a tombstone before
    // the first one drains (the watcher can split bursts into batches under
    // load; a mid-drain folder trash would hide its children's tombstones).
    fx.handle.pause().await.unwrap();
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::Paused)
            .unwrap_or(false)
    })
    .await;
    std::fs::remove_dir_all(root.join("Vault")).unwrap();
    wait_for(|| fx.db.tombstones().map(|t| t.len() == 5).unwrap_or(false)).await;

    fx.handle.resume().await.unwrap();
    wait_for(|| {
        fx.db.tombstones().map(|t| t.is_empty()).unwrap_or(false)
            && fx.db.node_by_path("Vault").unwrap().is_none()
            && fx
                .db
                .node_by_path("Vault/sub/c.txt")
                .unwrap()
                .is_none()
    })
    .await;
    wait_for(|| fx.mock.child_names("root").iter().all(|n| n != "Vault")).await;

    assert!(!root.join("Vault").exists(), "local tree must be gone");
    let trashed = fx.mock.trashed_uids();
    for uid in [&vault, &a, &b, &sub, &c] {
        assert_eq!(
            trashed.iter().filter(|u| *u == uid).count(),
            1,
            "uid {uid} must be trashed exactly once: {trashed:?}"
        );
    }
    fx.shutdown().await;
}

// TG-08: a remote trash event removes the local file; the restore event
// brings it back at its original path, uid, and content — cursor advances
// for both events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_trash_then_restore_round_trips_mirror() {
    let fx = start().await;
    assert_eq!(
        std::fs::read(fx.root.join("hello.txt")).unwrap(),
        b"hello drive"
    );
    let downloads = fx.mock.downloaded_uids().len();
    let cursor_before = fx.db.event_cursor("scope-1").unwrap();

    fx.mock.remote_trash("node-hello");
    wait_for(|| {
        !fx.root.join("hello.txt").exists()
            && fx.db.node_by_path("hello.txt").unwrap().is_none()
    })
    .await;
    let cursor_trashed = fx.db.event_cursor("scope-1").unwrap();
    assert!(
        cursor_trashed.is_some() && cursor_trashed != cursor_before,
        "trash event must advance the cursor ({cursor_before:?} → {cursor_trashed:?})"
    );

    fx.mock.remote_restore("node-hello");
    wait_for(|| {
        std::fs::read(fx.root.join("hello.txt")).ok().as_deref() == Some(b"hello drive".as_slice())
            && fx
                .db
                .node_by_path("hello.txt")
                .ok()
                .flatten()
                .is_some_and(|r| r.node_uid == "node-hello")
    })
    .await;
    let cursor_restored = fx.db.event_cursor("scope-1").unwrap();
    assert!(
        cursor_restored.is_some() && cursor_restored != cursor_trashed,
        "restore event must advance the cursor ({cursor_trashed:?} → {cursor_restored:?})"
    );

    assert!(
        fx.mock.downloaded_uids().len() > downloads,
        "restore must fetch the content again"
    );
    assert!(conflict_entry(&fx.root, "hello").is_none());
    assert!(fx.mock.trashed_uids().is_empty(), "remote-originated trash never calls back");
    fx.shutdown().await;
}

// TG-02 follow-up: the child's update event arrives BEFORE its moved parent
// folder's update. The child must still end up under the parent's final path
// once both events are applied — no duplicate row, no stray local copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn child_update_before_moved_parent_lands_at_final_path() {
    let fx = start().await;
    fx.mock
        .remote_create_file("node-docs", "nested.txt", b"before move");
    wait_for(|| fx.root.join("Documents/nested.txt").is_file()).await;
    let nested_uid = fx
        .mock
        .find_uid("node-docs", "nested.txt")
        .expect("nested remote");

    // Remote state: a new Outer folder holds Documents, but the engine has
    // not seen Outer yet (its event is still queued behind the child's).
    let outer = fx.mock.remote_create_folder_silent("root", "Outer");
    fx.mock.remote_move_silent("node-docs", &outer);

    // Child update first: its parent row still points at the old root path.
    fx.mock
        .remote_update_file(&nested_uid, b"changed before parent event");
    wait_for(|| {
        std::fs::read(fx.root.join("Documents/nested.txt"))
            .ok()
            .as_deref()
            == Some(b"changed before parent event".as_slice())
    })
    .await;

    // Now the parent folder's move update, without an Outer creation event.
    fx.mock.emit_event(&DriveEvent {
        scope_id: "scope-1".into(),
        event_id: "evt-parent-move".into(),
        kind: EventKind::NodeUpdated,
        node_uid: Some("node-docs".into()),
        parent_node_uid: Some(outer.clone()),
        is_trashed: None,
    });

    wait_for(|| {
        std::fs::read(fx.root.join("Outer/Documents/nested.txt"))
            .ok()
            .as_deref()
            == Some(b"changed before parent event".as_slice())
            && fx
                .db
                .node_by_path("Outer/Documents/nested.txt")
                .ok()
                .flatten()
                .is_some_and(|r| r.node_uid == nested_uid)
    })
    .await;
    assert!(
        !fx.root.join("Documents").exists(),
        "the old root-level folder must not linger"
    );
    assert!(fx.db.node_by_path("Documents").unwrap().is_none());
    assert!(fx.db.node_by_path("Documents/nested.txt").unwrap().is_none());
    assert_eq!(
        fx.db
            .all_nodes()
            .unwrap()
            .iter()
            .filter(|r| r.node_uid == nested_uid)
            .count(),
        1,
        "child must have exactly one row"
    );
    assert!(fx.mock.trashed_uids().is_empty());
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
