//! Placeholder-mode engine tests (M3): no materialization, VFS snapshot
//! publication, and the hydrate request → backing-file round trip. Uses the
//! in-process MockSidecar; no real account, no FUSE mount needed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pds_core::store::Store;
use pds_core::{Engine, EngineConfig, EngineState, STATE_DIR};
use pds_sidecar_client::{MockHandle, MockSidecar};
use serial_test::serial;

fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pds_core=debug,pds_sidecar_client=debug")
        .with_test_writer()
        .try_init();
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    handle: pds_core::EngineHandle,
    mock: MockHandle,
    store: Store,
    engine: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start() -> Fixture {
    start_with_cap(None).await
}

async fn start_with_cap(eviction_cap: Option<u64>) -> Fixture {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (mut client, mock) = MockSidecar::new().connect();
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: root.join(STATE_DIR).join("state.db"),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap,
            ..Default::default()
        },
    )
    .unwrap();
    // The engine publishes snapshots into its own store; grab the handle's
    // clone for assertions.
    let store = handle.store().cloned().expect("placeholder store");
    let engine = tokio::spawn(engine.run());
    Fixture {
        _dir: dir,
        root,
        handle,
        mock,
        store,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn external_backing_adopts_previously_materialized_content() {
    init_logs();
    // The desktop-app layout: engine state + backing cache OUTSIDE the root
    // (the app mounts the placeholder view at the root itself). A root that
    // was previously mirrored has real files in it; adopting it into
    // placeholder mode must move them into the backing cache (no
    // re-download) and mark them content-ready.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    let state_dir = dir.path().join("state");
    let backing = state_dir.join("backing");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(root.join("hello.txt"), b"hello drive").unwrap();

    // Pre-existing mirror state: the row is synced and the file bytes are
    // on disk in the root.
    let db_path = state_dir.join("state.db");
    let db = pds_core::Db::open(&db_path).unwrap();
    db.upsert_node(&pds_core::NodeRow {
        local_path: "hello.txt".into(),
        node_uid: "node-hello".into(),
        parent_uid: Some("root".into()),
        name: "hello.txt".into(),
        kind: pds_sidecar_client::NodeKind::File,
        size: Some(11),
        sha1: None,
        mtime: Some(1_700_000_000),
        sync_state: pds_core::SyncState::Synced,
        pinned: false,
        synced_sha1: None,
    })
    .unwrap();
    drop(db);

    let (mut client, _mock) = MockSidecar::new().connect();
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path,
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap: None,
            backing_dir: Some(backing.clone()),
            // The root IS the FUSE mountpoint in this layout.
            watch_local_writes: false,
        },
    )
    .unwrap();
    let store = handle.store().cloned().expect("placeholder store");
    let engine = tokio::spawn(engine.run());

    // The previously materialized file moved into the backing cache, so
    // the snapshot must show it hydrated from the start — no download.
    wait_for(|| {
        store
            .snapshot()
            .by_path
            .get("hello.txt")
            .is_some_and(|n| n.content_ready)
    })
    .await;
    assert!(
        !root.join("hello.txt").exists(),
        "adoption leaves nothing materialized in the root"
    );
    assert_eq!(std::fs::read(backing.join("hello.txt")).unwrap(), b"hello drive");

    handle.shutdown().await.unwrap();
    let _ = engine.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn placeholders_skip_materialization_and_publish_snapshot() {
    let fx = start().await;
    // Remote has hello.txt (the default mock tree); it must NOT appear on
    // disk in placeholder mode, but must be in the snapshot as a
    // non-ready placeholder.
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("hello.txt")
    })
    .await;
    assert!(
        !fx.root.join("hello.txt").exists(),
        "placeholder mode must not materialize files"
    );
    let snap = fx.store.snapshot();
    let hello = &snap.by_path["hello.txt"];
    assert!(!hello.is_dir);
    assert!(!hello.content_ready);
    assert!(hello.size > 0, "placeholder reports remote size");
    assert_eq!(fx.handle.status().await.unwrap().state, EngineState::Running);
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn hydrate_request_downloads_to_backing() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // VFS-side: queue the request.
    assert!(fx.store.begin_hydrate("hello.txt"));
    // Engine-side: it should land as a backing file (mock writes content).
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut backing = None;
    while Instant::now() < deadline {
        if let Some(p) = fx.store.backing_path("hello.txt") {
            backing = Some(p);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let backing = backing.expect("backing file registered");
    assert!(
        backing.is_file(),
        "backing file should exist at {backing:?}"
    );
    assert_eq!(
        std::fs::read(&backing).unwrap(),
        b"hello drive",
        "backing content matches the mock's remote content"
    );
    // Snapshot now flags the node ready.
    wait_for(|| fx.store.snapshot().by_path["hello.txt"].content_ready).await;
    // Visible root stays clean.
    assert!(!fx.root.join("hello.txt").exists());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn vfs_snapshot_reflects_remote_events() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    fx.mock.remote_create_file("root", "later.txt", b"later");
    wait_for(|| fx.store.snapshot().by_path.contains_key("later.txt")).await;
    assert!(!fx.root.join("later.txt").exists());
    assert!(!fx.store.snapshot().by_path["later.txt"].content_ready);
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pin_downloads_and_unpin_allows_eviction() {
    // 1-byte cap: everything hydrated is over cap, so the pass after any
    // hydration evicts everything not pinned/protected/open.
    let fx = start_with_cap(Some(1)).await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // Pin downloads immediately and the pin survives in the DB.
    fx.handle.pin("hello.txt").await.unwrap();
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;

    // Unpin; the next hydration evicts hello.txt (LRU, unpinned).
    fx.handle.unpin("hello.txt").await.unwrap();
    fx.mock.remote_create_file("root", "b.txt", b"bbbb");
    wait_for(|| fx.store.snapshot().by_path.contains_key("b.txt")).await;
    fx.store.begin_hydrate("b.txt");
    wait_for(|| {
        fx.store
            .backing_path("b.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    wait_for(|| fx.store.backing_path("hello.txt").is_none()).await;
    // Snapshot flips content_ready back off for the evicted file.
    wait_for(|| !fx.store.snapshot().by_path["hello.txt"].content_ready).await;
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pinned_file_survives_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (mut client, mock) = MockSidecar::new().connect();
    mock.remote_create_file("root", "a.txt", b"aaa");
    mock.remote_create_file("root", "b.txt", b"bbb");
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: root.join(STATE_DIR).join("state.db"),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let store = handle.store().cloned().unwrap();
    let engine = tokio::spawn(engine.run());
    wait_for(|| store.snapshot().by_path.contains_key("a.txt")).await;

    handle.pin("a.txt").await.unwrap();
    wait_for(|| {
        store
            .backing_path("a.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;

    // Hydrate b: eviction pass runs, but a.txt is pinned and stays.
    store.begin_hydrate("b.txt");
    wait_for(|| {
        store
            .backing_path("b.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        store
            .backing_path("a.txt")
            .map(|p| p.is_file())
            .unwrap_or(false),
        "pinned file must survive the eviction pass"
    );
    handle.shutdown().await.ok();
    let _ = engine.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn set_cache_cap_applies_live_with_immediate_eviction() {
    // Start unlimited; shrinking the cap below current usage via the live
    // command reclaims the over-cap content on the spot — no restart, no
    // waiting for an idle tick. Growing it again is live too.
    let fx = start_with_cap(None).await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    fx.handle.pin("hello.txt").await.unwrap();
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    fx.handle.unpin("hello.txt").await.unwrap();

    // Shrink to 1 byte: the hydrated file is over cap and unpinned, so the
    // on-the-spot eviction pass reclaims it.
    fx.handle.set_cache_cap(Some(1)).await.unwrap();
    assert_eq!(fx.handle.status().await.unwrap().cache_cap, Some(1));
    wait_for(|| fx.store.backing_path("hello.txt").is_none()).await;
    wait_for(|| !fx.store.snapshot().by_path["hello.txt"].content_ready).await;

    // Back to unlimited: a fresh hydration survives the eviction pass.
    fx.handle.set_cache_cap(None).await.unwrap();
    assert_eq!(fx.handle.status().await.unwrap().cache_cap, None);
    fx.store.begin_hydrate("hello.txt");
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false),
        "unlimited cap must not evict"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn backing_dir_events_never_enter_the_sync_pipeline() {
    // Regression (found in M3.3): the watcher and the initial scan saw
    // `.pds-vfs-backing/` and treated hydration as a local change —
    // uploading files into a phantom remote `.pds-vfs-backing` folder.
    let fx = start_with_cap(None).await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // Hydrate through the normal path; the backing write must NOT produce
    // a pending DB row or a remote upload under `.pds-vfs-backing/…`.
    fx.store.begin_hydrate("hello.txt");
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;

    // Give the debounced watcher (500 ms) ample time to misfire.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let uploaded = fx.mock.uploaded_names();
    assert!(
        uploaded
            .iter()
            .all(|n| !n.contains(".pds-vfs-backing")),
        "backing-dir files leaked into the upload pipeline: {uploaded:?}"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn evicted_placeholder_rehydrates_on_request() {
    // Eviction (backing file deleted by the engine) must likewise never
    // trip the delete path — the placeholder stays in the snapshot.
    let fx = start_with_cap(Some(1)).await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    fx.handle.pin("hello.txt").await.unwrap();
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    fx.handle.unpin("hello.txt").await.unwrap();

    // Trigger a hydration pass that evicts hello.txt.
    fx.mock.remote_create_file("root", "c.txt", b"cccc");
    wait_for(|| fx.store.snapshot().by_path.contains_key("c.txt")).await;
    fx.store.begin_hydrate("c.txt");
    wait_for(|| fx.store.backing_path("hello.txt").is_none()).await;

    // No trashes, no phantom uploads, and the snapshot still lists it.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(fx.mock.trashed_uids().is_empty(), "eviction must not trash");
    assert!(
        fx.store.snapshot().by_path.contains_key("hello.txt"),
        "evicted file must remain a placeholder"
    );
    // Rehydration restores content.
    fx.store.begin_hydrate("hello.txt");
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| p.is_file())
            .unwrap_or(false)
    })
    .await;
    fx.shutdown().await;
}

// Keep imports honest even if a test is skipped.
#[allow(dead_code)]
fn _t(_: HashMap<String, String>) {}
