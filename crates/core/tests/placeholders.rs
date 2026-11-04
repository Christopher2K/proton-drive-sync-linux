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
async fn status_reports_backing_cache_usage() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // Before any hydration the cache is empty.
    let s = fx.handle.status().await.unwrap();
    assert_eq!(s.mode, pds_core::SyncMode::Placeholders);
    assert_eq!(s.cache_used, 0, "nothing hydrated yet");
    assert!(s.cache_path.is_some(), "cache path is surfaced");

    // Hydrate hello.txt ("hello drive" = 11 bytes).
    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| fx.store.snapshot().by_path["hello.txt"].content_ready).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while fx.handle.status().await.unwrap().cache_used != 11 {
        assert!(Instant::now() < deadline, "cache_used never reached 11 bytes");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // After eviction (cap=0 evicts everything) usage drops back to 0.
    fx.handle.set_cache_cap(Some(0)).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while fx.handle.status().await.unwrap().cache_used != 0 {
        assert!(Instant::now() < deadline, "cache_used never dropped to 0");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

// TG-09: a remote 0-byte file is a valid placeholder. Hydration must finish
// immediately (content_ready + finish_hydrate), not be mistaken for "not
// hydrated" and hang, and a second hydrate after eviction must work again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn empty_remote_file_placeholder_hydrates_and_rehydrates() {
    let fx = start().await;
    let uid = fx.mock.remote_create_file("root", "empty.txt", b"");
    wait_for(|| fx.store.snapshot().by_path.contains_key("empty.txt")).await;

    {
        let snap = fx.store.snapshot();
        let node = &snap.by_path["empty.txt"];
        assert!(!node.is_dir);
        assert_eq!(node.size, 0, "snapshot reports the remote size (zero)");
        assert!(!node.content_ready, "not hydrated yet");
    }

    // First open: hydration must complete, not wait forever on a 0-byte file.
    assert!(fx.store.begin_hydrate("empty.txt"));
    wait_for(|| fx.store.snapshot().by_path["empty.txt"].content_ready).await;
    wait_for(|| fx.store.hydrating_count() == 0).await;
    let backing = fx
        .store
        .backing_path("empty.txt")
        .expect("backing file registered");
    assert_eq!(std::fs::metadata(&backing).unwrap().len(), 0);
    assert_eq!(std::fs::read(&backing).unwrap(), Vec::<u8>::new());

    // Force an eviction: hydrate the non-empty neighbour so bytes are over
    // the cap, then shrink the cap to zero. The empty placeholder must drop
    // out of the ready state like any other file.
    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| fx.store.snapshot().by_path["hello.txt"].content_ready).await;
    fx.handle.set_cache_cap(Some(0)).await.unwrap();
    wait_for(|| !fx.store.snapshot().by_path["empty.txt"].content_ready).await;
    assert!(!backing.exists(), "backing content evicted");

    // Second open after eviction: rehydrate to the same empty content.
    fx.handle.set_cache_cap(None).await.unwrap();
    assert!(fx.store.begin_hydrate("empty.txt"));
    wait_for(|| fx.store.snapshot().by_path["empty.txt"].content_ready).await;
    wait_for(|| fx.store.hydrating_count() == 0).await;
    let backing = fx.store.backing_path("empty.txt").unwrap();
    assert_eq!(std::fs::read(&backing).unwrap(), Vec::<u8>::new());
    assert_eq!(
        fx.mock
            .downloaded_uids()
            .iter()
            .filter(|u| *u == &uid)
            .count(),
        2,
        "one download per hydration"
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn bare_mkdir_from_vfs_creates_remote_folder_and_rmdir_trashes_it() {
    // Regression: a folder created through the mount with NOTHING inside
    // it used to sit in pending_mkdirs forever — the only consumer lived
    // in upload_write_one, which runs when a *file* write is queued. The
    // folder never reached the remote and never got a DB row (it existed
    // only in the FUSE overlay), and rmdir then failed with ENOTEMPTY
    // because Tree::ino_of resolved the unknown path to the ROOT inode
    // (root's children answered the emptiness check).
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // VFS-side: mkdir queues the folder (no write follows).
    fx.store.record_mkdir("NewFolder");
    fx.store.record_mkdir("NewFolder/inner");

    // Engine drains the queue on its own: both folders exist remotely…
    wait_for(|| fx.mock.child_names("root").iter().any(|n| n == "NewFolder")).await;
    let uid = fx.mock.find_uid("root", "NewFolder").unwrap();
    wait_for(|| fx.mock.child_names(&uid).iter().any(|n| n == "inner")).await;

    // …and the snapshot serves them as directories (DB rows written).
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("NewFolder")
            .is_some_and(|n| n.is_dir)
    })
    .await;
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("NewFolder/inner")
            .is_some_and(|n| n.is_dir)
    })
    .await;

    // rmdir path: children first, then the parent — both trash remotely
    // (the tombstone drain), and the rows leave the snapshot.
    fx.store.record_delete("NewFolder/inner");
    wait_for(|| !fx.store.snapshot().by_path.contains_key("NewFolder/inner")).await;
    wait_for(|| fx.mock.child_names(&uid).is_empty()).await;

    fx.store.record_delete("NewFolder");
    // The mass-deletion guard counts percentage limbs too, and this tiny
    // tree (3 synced nodes, 2 deletes) trips it. Resume once to approve
    // the batch — same UX as a real user confirming a small tree.
    //
    // Resume only approves tombstones the engine has *received*; this
    // delete may still be in flight on the store event queue, so retry
    // resume until the row actually leaves the snapshot (a resume
    // processed before the delete lands approves nothing and the guard
    // re-pauses on the next drain).
    let deadline = Instant::now() + Duration::from_secs(30);
    while fx.store.snapshot().by_path.contains_key("NewFolder") {
        assert!(Instant::now() < deadline, "NewFolder never left the snapshot");
        fx.handle.resume().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for(|| !fx.mock.child_names("root").iter().any(|n| n == "NewFolder")).await;
    assert!(
        fx.mock.trashed_uids().contains(&uid),
        "NewFolder ({uid}) must be trashed remotely, got {:?}",
        fx.mock.trashed_uids()
    );
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn overwrite_rename_does_not_lose_the_destination() {
    // Regression (Plasticity-style atomic save): an app writes
    // `model.plasticity.<millis>.<n>.tmp`, closes it (write queued), then
    // renames it over the existing `model.plasticity` (destination delete
    // + rename queued). The engine used to drain renames before deletes:
    // apply_local_rename re-keyed the tmp row onto the destination path,
    // then the stale destination delete trashed the just-renamed node and
    // removed the row — the file vanished from the DB while every tmp
    // uploaded as a separate remote file. Deletes must drain first, and
    // the source's queued write must follow the rename.
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // Seed a hydrated destination: `model.plasticity` exists remotely,
    // locally, and in the snapshot. Extra siblings keep the mass-deletion
    // guard's ratio limb out of this test (1 delete of 3 synced trips it).
    for i in 0..10 {
        fx.mock.remote_create_file("root", &format!("filler{i}.bin"), b"filler");
    }
    fx.mock.remote_create_file("root", "model.plasticity", b"v1 content");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("filler9.bin")
    })
    .await;
    fx.store.begin_hydrate("model.plasticity");
    wait_for(|| fx.store.backing_path("model.plasticity").is_some()).await;
    let model_uid = fx.mock.find_uid("root", "model.plasticity").unwrap();

    // Atomic save, exactly as the FUSE layer sees it:
    // 1. tmp created through the mount and closed → write queued
    //    (VFS write path: backing file written, record_write called).
    let tmp = "model.plasticity.1793648988585.3.tmp";
    let backing_dir = fx
        .store
        .backing_path("model.plasticity")
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    std::fs::write(backing_dir.join(tmp), b"v2 content").unwrap();
    fx.store.record_write(tmp);
    // 2. rename tmp → model.plasticity: the VFS queues the destination
    //    delete and the rename atomically (record_overwrite).
    std::fs::rename(backing_dir.join(tmp), backing_dir.join("model.plasticity")).unwrap();
    fx.store.record_overwrite("model.plasticity", tmp, "model.plasticity");

    // Settle before asserting remote state: the overwrite delete resolves
    // as a tombstone that the in-place upload cancels, or that drains —
    // either way the assertions below must observe the FINAL state, not a
    // mid-flight one (an early version of this test asserted while the
    // stale tombstone could still land after the upload, ~30% flaky).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (deletes, renames, writes) = fx.store.pending_counts();
        let tombstones = fx
            .handle
            .status()
            .await
            .map(|s| s.queued_tombstones)
            .unwrap_or(usize::MAX);
        if deletes == 0 && renames == 0 && writes == 0 && tombstones == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "engine did not settle in time (deletes={deletes}, renames={renames}, \
             writes={writes}, tombstones={tombstones})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The single delete plus the write/rename must land on their own: the
    // guard no longer trips with the filler files present. The tmp must
    // never appear remotely, and exactly one node named model.plasticity
    // must exist — with v2 content.
    wait_for(|| {
        fx.mock.file_content(&model_uid).as_deref() == Some(&b"v2 content"[..])
    })
    .await;
    assert!(
        !fx.mock.uploaded_names().iter().any(|n| n.contains(".tmp")),
        "temp file must not upload as its own remote node: {:?}",
        fx.mock.uploaded_names()
    );
    assert!(
        fx.mock.child_names("root").iter().filter(|n| *n == "model.plasticity").count() == 1,
        "exactly one model.plasticity remotely: {:?}",
        fx.mock.child_names("root")
    );
    assert!(
        !fx.mock.trashed_uids().contains(&model_uid),
        "the renamed-onto node must never be trashed: {:?}",
        fx.mock.trashed_uids()
    );
    // And the row survives under the destination path.
    wait_for(|| fx.store.snapshot().by_path.contains_key("model.plasticity")).await;
    fx.shutdown().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn vfs_rename_rekeys_rows_and_renames_remotely() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;

    // Same-folder rename: hello.txt → hi.txt. The engine must rename the
    // remote node (NOT trash + re-upload) and re-key the DB row.
    fx.store.record_rename("hello.txt", "hi.txt");
    wait_for(|| fx.store.snapshot().by_path.contains_key("hi.txt")).await;
    wait_for(|| {
        fx.mock
            .child_names("root")
            .iter()
            .any(|n| n == "hi.txt")
    })
    .await;
    assert!(
        !fx.store.snapshot().by_path.contains_key("hello.txt"),
        "old path must be gone from the snapshot"
    );
    assert!(
        !fx.mock.trashed_uids().contains(&"node-hello".to_string()),
        "rename must never trash; trashed: {:?}",
        fx.mock.trashed_uids()
    );
    assert_eq!(
        fx.mock.renames(),
        vec![("node-hello".to_string(), "hi.txt".to_string())]
    );

    // Cross-folder move: hi.txt → Documents/moved.txt. The folder already
    // exists remotely (seeded Documents); move + rename apply.
    fx.store.record_rename("hi.txt", "Documents/moved.txt");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("Documents/moved.txt")
    })
    .await;
    let docs_uid = fx.mock.find_uid("root", "Documents").unwrap();
    wait_for(|| {
        fx.mock
            .child_names(&docs_uid)
            .iter()
            .any(|n| n == "moved.txt")
    })
    .await;
    assert!(!fx.store.snapshot().by_path.contains_key("hi.txt"));
    assert!(fx.mock.trashed_uids().is_empty(), "no trashes at all");

    // Rename of an unknown path is a no-op (no error, no remote calls).
    let renames_before = fx.mock.renames().len();
    fx.store.record_rename("ghost.txt", "phantom.txt");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(fx.mock.renames().len(), renames_before);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_folder_rename_moves_hydrated_backing_subtree() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    fx.mock
        .remote_create_file("node-docs", "nested.txt", b"nested bytes");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("Documents/nested.txt")
    })
    .await;
    assert!(fx.store.begin_hydrate("Documents/nested.txt"));
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("Documents/nested.txt")
            .is_some_and(|n| n.content_ready)
    })
    .await;
    let downloads = fx.mock.downloaded_uids().len();

    fx.mock.remote_rename("node-docs", "Docs");

    // The snapshot re-keys with the row; the hydrated backing file moved
    // under the new prefix, so the rename never re-downloads.
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("Docs/nested.txt")
            .is_some_and(|n| n.content_ready)
            && !fx
                .store
                .snapshot()
                .by_path
                .contains_key("Documents/nested.txt")
    })
    .await;
    let backing = fx
        .store
        .backing_path("Docs/nested.txt")
        .expect("backing entry follows the folder rename");
    assert_eq!(std::fs::read(&backing).unwrap(), b"nested bytes");
    assert!(fx.store.backing_path("Documents/nested.txt").is_none());
    assert_eq!(fx.mock.downloaded_uids().len(), downloads);

    let db = pds_core::Db::open(&fx.root.join(STATE_DIR).join("state.db")).unwrap();
    assert_eq!(
        db.node_by_path("Docs/nested.txt")
            .unwrap()
            .expect("child row re-keyed")
            .parent_uid
            .as_deref(),
        Some("node-docs")
    );
    assert!(db.node_by_path("Documents/nested.txt").unwrap().is_none());
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

// Keep imports honest even if a test is skipped.
#[allow(dead_code)]
fn _t(_: HashMap<String, String>) {}
