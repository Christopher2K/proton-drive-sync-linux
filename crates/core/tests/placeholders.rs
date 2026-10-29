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

// Keep imports honest even if a test is skipped.
#[allow(dead_code)]
fn _t(_: HashMap<String, String>) {}
