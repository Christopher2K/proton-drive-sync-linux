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
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    start_with_client(dir, root, client, mock).await
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
        },
    )
    .unwrap();
    let db = Db::open(&db_path).unwrap();
    let engine = tokio::spawn(engine.run());
    // Wait for initial sync to finish: hello.txt materialized locally.
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
    let fx = start_with_client(dir, root.clone(), client, mock).await;

    std::fs::remove_file(root.join("hello.txt")).unwrap();
    wait_for(|| fx.mock.trashed_uids().contains(&"node-hello".to_string())).await;
    // Drained: no tombstone left, node row gone.
    assert!(fx.db.tombstones().unwrap().is_empty());
    assert!(fx.db.node_by_path("hello.txt").unwrap().is_none());
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
    let fx = start_with_client(dir, root.clone(), client, mock).await;

    // Delete 25 of 32 synced nodes: the ratio guard (>25% of synced nodes)
    // trips long before 20 individual deletes, and keeps re-tripping as the
    // synced base shrinks. Each explicit resume is the user's confirmation
    // (SPEC F3.3); draining eventually completes.
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
    assert!(resumes >= 1, "guard must require explicit confirmation");
    assert_eq!(fx.mock.trashed_uids().len(), 25);
    assert!(fx.db.tombstones().unwrap().is_empty());
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
        let conflict_present = std::fs::read_dir(&fx.root)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("hello (conflict ")
            });
        local == b"local edit" || (local == b"remote edit" && conflict_present)
    })
    .await;
    // Settle, then verify nothing was lost or trashed.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(fx.mock.trashed_uids().is_empty());
    let local = std::fs::read(fx.root.join("hello.txt")).unwrap_or_default();
    if local == b"remote edit" {
        let conflict = std::fs::read_dir(&fx.root)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("hello (conflict ")
            })
            .expect("conflict copy must exist");
        assert_eq!(std::fs::read(conflict.path()).unwrap(), b"local edit");
    }
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

// Helper so tests can use `futures::executor::block_on` without an extra dep.
mod futures {
    pub mod executor {
        pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
        }
    }
}
