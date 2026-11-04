//! Wave 5b scale coverage (TG-31): large trees stay within generous bounds,
//! and per-event work in mirror mode does not scale with the whole tree.
//!
//! The 10k runs are deliberately structural, not micro-benchmarks: timings use
//! broad ceilings and ratios so a loaded debug CI never turns them flaky. The
//! 50k variant is `#[ignore]`d for manual runs:
//!
//! ```text
//! cargo test -p pds-core --test scale large_tree_full_50000 -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use pds_core::{Db, Engine, EngineConfig, EngineHandle, NodeRow, STATE_DIR, SyncState};
use pds_sidecar_client::{MockHandle, MockSidecar, NodeKind};
use serial_test::serial;

fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pds_core=warn,pds_sidecar_client=warn")
        .with_test_writer()
        .try_init();
}

async fn wait_until(timeout: Duration, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met within {timeout:?}");
}

/// Seed `count` cheap, already-synced file rows. No bytes ever need to exist
/// on disk: these tests exercise indexing, snapshots, and event application.
fn seed_db(db_path: &std::path::Path, count: usize) -> Db {
    let db = Db::open(db_path).unwrap();
    for i in 0..count {
        let name = format!("bulk-{i:05}.txt");
        let uid = format!("bulk-uid-{i:05}");
        let sha1 = format!("{i:040x}");
        db.upsert_node(&NodeRow {
            local_path: name.clone(),
            node_uid: uid,
            parent_uid: Some("root".into()),
            name,
            kind: NodeKind::File,
            size: Some(1),
            sha1: Some(sha1.clone()),
            mtime: Some(1_700_000_000),
            mode: None,
            sync_state: SyncState::Synced,
            pinned: false,
            synced_sha1: Some(sha1),
        })
        .unwrap();
    }
    db.set_meta("initial_sync_done", "1").unwrap();
    db.set_meta("materialized_for_mirror", "1").unwrap();
    db
}

/// Build a placeholder engine over a pre-seeded DB and measure the initial
/// snapshot publish (DB scan + VFS map construction).
async fn snapshot_publish_duration(count: usize) -> Duration {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let db_path = root.join(STATE_DIR).join("state.db");
    seed_db(&db_path, count);
    let (client, _mock) = MockSidecar::new().connect();
    let (_tx, notifications) = tokio::sync::mpsc::channel(1);
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root,
            db_path,
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap: None,
            watch_local_writes: false,
            ..Default::default()
        },
    )
    .unwrap();
    let store = handle.store().cloned().expect("placeholder store");
    let started = Instant::now();
    let task = tokio::spawn(engine.run());
    wait_until(Duration::from_secs(30), || {
        store.snapshot().by_path.len() == count
    })
    .await;
    let elapsed = started.elapsed();
    handle.shutdown().await.unwrap();
    let _ = task.await;
    elapsed
}

/// Seeded remote tree (flat, `count` files) → initial enumeration + DB
/// insert + first snapshot.
async fn initial_enumeration_duration(count: usize) -> Duration {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();

    let mock = MockSidecar::new();
    let seed = mock.handle();
    for i in 0..count {
        seed.remote_create_file("root", &format!("remote-{i:05}.txt"), b"x");
    }
    let (mut client, mock) = mock.connect();
    let notifications = client.take_notifications();
    let db_path = root.join(STATE_DIR).join("state.db");
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root,
            db_path,
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap: None,
            watch_local_writes: false,
            ..Default::default()
        },
    )
    .unwrap();
    let store = handle.store().cloned().expect("placeholder store");
    let started = Instant::now();
    let task = tokio::spawn(engine.run());
    // + Documents/hello.txt from the mock's default tree.
    wait_until(Duration::from_secs(60), || {
        store.snapshot().by_path.len() >= count + 2
    })
    .await;
    let elapsed = started.elapsed();
    assert!(mock.trashed_uids().is_empty());
    handle.shutdown().await.unwrap();
    let _ = task.await;
    elapsed
}

/// Mirror-mode event application: 20 remote file updates, each waited on via
/// the local file. Nothing in this path scans the full tree, so the 5k and
/// 10k timings should stay in the same ballpark.
async fn mirror_event_batch(count: usize, events: usize) -> (Duration, usize, usize) {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let db_path = root.join(STATE_DIR).join("state.db");
    let db = seed_db(&db_path, count);
    drop(db);

    let (mut client, mock) = MockSidecar::new().connect();
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path,
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
            // Isolate event application from watcher-driven uploads.
            watch_local_writes: false,
            ..Default::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(engine.run());
    wait_until(Duration::from_secs(30), || {
        futures::executor::block_on(handle.status()).is_ok()
    })
    .await;

    let started = Instant::now();
    for i in 0..events {
        let body = format!("event-body-{i}");
        mock.remote_update_file("node-hello", body.as_bytes());
        wait_until(Duration::from_secs(30), || {
            std::fs::read(root.join("hello.txt")).ok().as_deref() == Some(body.as_bytes())
        })
        .await;
    }
    let elapsed = started.elapsed();
    let downloads = mock.downloaded_uids().len();
    let rows = Db::open(&root.join(STATE_DIR).join("state.db"))
        .unwrap()
        .all_nodes()
        .unwrap()
        .len();
    handle.shutdown().await.unwrap();
    let _ = task.await;
    (elapsed, downloads, rows)
}

fn rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|kb| kb.parse().ok())
        .expect("VmRSS")
}

/// Placeholder-mode events rebuild the served snapshot; prove the map does
/// not accumulate duplicates or leak per event (RSS is reported, not
/// asserted tightly).
async fn placeholder_event_batch(count: usize, events: usize) -> (Duration, u64, u64, usize) {
    init_logs();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let db_path = root.join(STATE_DIR).join("state.db");
    seed_db(&db_path, count);

    let (mut client, mock) = MockSidecar::new().connect();
    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root,
            db_path,
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap: None,
            watch_local_writes: false,
            ..Default::default()
        },
    )
    .unwrap();
    let store = handle.store().cloned().expect("placeholder store");
    let task = tokio::spawn(engine.run());
    wait_until(Duration::from_secs(30), || store.generation() >= 1).await;

    let before = rss_kb();
    let start_gen = store.generation();
    let started = Instant::now();
    for i in 0..events {
        mock.remote_update_file("node-hello", format!("v{i}").as_bytes());
    }
    wait_until(Duration::from_secs(60), || {
        store.generation() >= start_gen + events as u64
    })
    .await;
    let elapsed = started.elapsed();
    let after = rss_kb();
    let nodes = store.snapshot().by_path.len();

    handle.shutdown().await.unwrap();
    let _ = task.await;
    (elapsed, before, after, nodes)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn large_tree_snapshot_publish_is_bounded_and_scales_smoothly() {
    let small = snapshot_publish_duration(5_000).await;
    let large = snapshot_publish_duration(10_000).await;
    eprintln!("snapshot publish: 5k={small:?}, 10k={large:?}");
    assert!(
        large < Duration::from_secs(10),
        "10k-node snapshot publish took {large:?} (debug build)"
    );
    assert!(
        large.as_secs_f64() < small.as_secs_f64() * 5.0 + 0.25,
        "10k publish must not blow up relative to 5k: {small:?} vs {large:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn large_tree_initial_enumeration_is_bounded() {
    let small = initial_enumeration_duration(5_000).await;
    let large = initial_enumeration_duration(10_000).await;
    eprintln!("initial enumeration: 5k={small:?}, 10k={large:?}");
    assert!(
        large < Duration::from_secs(30),
        "10k-node initial enumeration took {large:?}"
    );
    assert!(
        large.as_secs_f64() < small.as_secs_f64() * 5.0 + 0.5,
        "10k enumeration must not blow up relative to 5k: {small:?} vs {large:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn single_event_application_does_not_scale_with_tree_size() {
    // Warm-up so the first event's file materialization is not compared
    // against a cold path.
    let _ = mirror_event_batch(5_000, 1).await;

    let (small, small_downloads, small_rows) = mirror_event_batch(5_000, 20).await;
    let (large, large_downloads, large_rows) = mirror_event_batch(10_000, 20).await;
    eprintln!(
        "20 mirror events: 5k={small:?} ({small_rows} rows), 10k={large:?} ({large_rows} rows)"
    );
    assert_eq!(small_downloads, 20, "one download per applied event");
    assert_eq!(large_downloads, 20, "one download per applied event");
    assert_eq!(small_rows, 5_001, "event adds only its own row");
    assert_eq!(large_rows, 10_001, "event adds only its own row");
    assert!(
        large < Duration::from_secs(5),
        "20 events on 10k nodes took {large:?}"
    );
    assert!(
        large.as_secs_f64() < small.as_secs_f64() * 3.0 + 0.1,
        "per-event work grew with the tree: 5k={small:?}, 10k={large:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn placeholder_event_burst_has_stable_snapshot_and_memory() {
    let (elapsed, before, after, nodes) = placeholder_event_batch(10_000, 100).await;
    let per_event = elapsed / 100;
    let grown_kb = after.saturating_sub(before);
    eprintln!(
        "100 placeholder events on 10k nodes: total={elapsed:?}, per-event≈{per_event:?}, \
         RSS {before} KiB → {after} KiB (+{grown_kb} KiB), snapshot nodes={nodes}"
    );
    assert_eq!(nodes, 10_001, "one new row, no duplicate snapshot entries");
    assert!(
        grown_kb < 256 * 1024,
        "per-event snapshot churn ballooned RSS by {grown_kb} KiB"
    );
    assert!(
        per_event < Duration::from_secs(2),
        "one placeholder event took {per_event:?} on a 10k tree"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "manual full-count run: cargo test -p pds-core --test scale large_tree_full_50000 -- --ignored --nocapture"]
async fn large_tree_full_50000() {
    let started = Instant::now();
    let publish = snapshot_publish_duration(50_000).await;
    let enumeration = initial_enumeration_duration(50_000).await;
    eprintln!(
        "50k tree: snapshot publish={publish:?}, initial enumeration={enumeration:?}, \
         setup+both={:?}",
        started.elapsed()
    );
    assert!(publish < Duration::from_secs(30));
    assert!(enumeration < Duration::from_secs(90));
}

// Helper so tests can wait on an async status call without an extra dep.
mod futures {
    pub mod executor {
        pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
        }
    }
}

// Keep the unused-import warning away when this test file is compiled with
// only a subset of helpers enabled.
#[allow(dead_code)]
fn _assert_handle_type(_: &EngineHandle, _: &MockHandle) {}
