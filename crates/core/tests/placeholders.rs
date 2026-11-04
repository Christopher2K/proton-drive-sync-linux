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
    db: pds_core::Db,
    db_path: std::path::PathBuf,
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
    let db_path = root.join(STATE_DIR).join("state.db");
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: db_path.clone(),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
            eviction_cap,
            ..Default::default()
        },
    )
    .unwrap();
    let db = pds_core::Db::open(&db_path).unwrap();
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
        db,
        db_path,
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
    /// restart/persistence assertions against the same DB file.
    async fn shutdown_keep_dir(self) {
        self.handle.shutdown().await.unwrap();
        let _ = self.engine.await;
        std::mem::forget(self._dir); // leaked on purpose; OS cleans /tmp
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
        mode: None,
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

// TG-06 (placeholder mode): renaming a hydrated folder subtree moves the
// backing files with the rows; rehydration works from the new path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn local_folder_rename_moves_hydrated_subtree_and_rehydrates() {
    let fx = start().await;
    let photos = fx.mock.remote_create_folder("root", "Photos");
    fx.mock.remote_create_file(&photos, "a.txt", b"alpha");
    fx.mock.remote_create_file(&photos, "b.txt", b"beta");
    let sub = fx.mock.remote_create_folder(&photos, "sub");
    fx.mock.remote_create_file(&sub, "c.txt", b"gamma");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("Photos/sub/c.txt")
    })
    .await;
    for rel in ["Photos/a.txt", "Photos/b.txt", "Photos/sub/c.txt"] {
        assert!(fx.store.begin_hydrate(rel));
        wait_for(|| {
            fx.store
                .snapshot()
                .by_path
                .get(rel)
                .is_some_and(|n| n.content_ready)
        })
        .await;
    }
    let downloads = fx.mock.downloaded_uids().len();

    // The VFS queues a rename of the whole folder (the mount reports it as
    // one path rename; children follow).
    fx.store.record_rename("Photos", "Albums");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("Albums/sub/c.txt")
            .is_some_and(|n| n.content_ready)
            && !fx.store.snapshot().by_path.contains_key("Photos")
    })
    .await;

    // Backing content moved under the new prefix, not re-downloaded.
    for (rel, bytes) in [
        ("Albums/a.txt", b"alpha".as_slice()),
        ("Albums/b.txt", b"beta".as_slice()),
        ("Albums/sub/c.txt", b"gamma".as_slice()),
    ] {
        let backing = fx.store.backing_path(rel).expect("backing follows rename");
        assert_eq!(std::fs::read(&backing).unwrap(), bytes);
        assert!(fx.store.backing_path(&rel.replacen("Albums", "Photos", 1)).is_none());
    }
    assert_eq!(fx.mock.downloaded_uids().len(), downloads, "rename must not re-download");
    assert!(fx.mock.trashed_uids().is_empty());
    assert_eq!(
        fx.mock.find_uid("root", "Albums").as_deref(),
        Some(photos.as_str()),
        "remote folder uid preserved"
    );
    assert!(
        fx.mock
            .renames()
            .iter()
            .any(|(uid, name)| uid == &photos && name == "Albums"),
        "expected a remote rename: {:?}",
        fx.mock.renames()
    );

    // Dehydrate, then rehydrate through the new path.
    fx.handle.set_cache_cap(Some(0)).await.unwrap();
    wait_for(|| fx.store.backing_path("Albums/sub/c.txt").is_none()).await;
    let downloads = fx.mock.downloaded_uids().len();
    assert!(fx.store.begin_hydrate("Albums/sub/c.txt"));
    wait_for(|| {
        fx.store
            .backing_path("Albums/sub/c.txt")
            .map(|p| std::fs::read(p).ok().as_deref() == Some(b"gamma".as_slice()))
            .unwrap_or(false)
    })
    .await;
    assert!(fx.mock.downloaded_uids().len() > downloads);
    fx.shutdown().await;
}

// TG-07 (placeholder mode): deleting a folder subtree through the mount
// trashes each node once and drops every path from the VFS snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn placeholder_folder_delete_trashes_once_and_leaves_snapshot() {
    let fx = start().await;
    // Filler nodes keep the guard's ratio limb out of the way.
    for i in 0..20 {
        fx.mock
            .remote_create_file("root", &format!("filler{i:02}.txt"), b"filler");
    }
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .contains_key("filler19.txt")
    })
    .await;
    let vault = fx.mock.remote_create_folder("root", "Vault");
    let a = fx.mock.remote_create_file(&vault, "a.txt", b"a bytes");
    let b = fx.mock.remote_create_file(&vault, "b.txt", b"b bytes");
    let sub = fx.mock.remote_create_folder(&vault, "sub");
    let c = fx.mock.remote_create_file(&sub, "c.txt", b"c bytes");
    wait_for(|| fx.store.snapshot().by_path.contains_key("Vault/sub/c.txt")).await;

    // Pause so all five deletes are recorded before the first drains.
    fx.handle.pause().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while fx.handle.status().await.unwrap().state != EngineState::Paused {
        assert!(Instant::now() < deadline, "engine did not pause");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for rel in ["Vault/a.txt", "Vault/b.txt", "Vault/sub/c.txt", "Vault/sub", "Vault"] {
        fx.store.record_delete(rel);
    }
    wait_for(|| {
        pds_core::Db::open(&fx.root.join(STATE_DIR).join("state.db"))
            .ok()
            .and_then(|db| db.tombstones().ok())
            .is_some_and(|t| t.len() == 5)
    })
    .await;
    fx.handle.resume().await.unwrap();

    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .keys()
            .all(|p| !p.starts_with("Vault"))
    })
    .await;
    wait_for(|| fx.mock.child_names("root").iter().all(|n| n != "Vault")).await;
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

// TG-08 (placeholder mode): the trashed row leaves the snapshot; restore
// recreates the row and a hydratable placeholder at the original path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn remote_trash_then_restore_round_trips_placeholders() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    let cursor_before = pds_core::Db::open(&fx.root.join(STATE_DIR).join("state.db"))
        .unwrap()
        .event_cursor("scope-1")
        .unwrap();

    fx.mock.remote_trash("node-hello");
    wait_for(|| !fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    let db = pds_core::Db::open(&fx.root.join(STATE_DIR).join("state.db")).unwrap();
    assert!(db.node_by_path("hello.txt").unwrap().is_none());
    let cursor_trashed = db.event_cursor("scope-1").unwrap();
    assert!(cursor_trashed != cursor_before);

    fx.mock.remote_restore("node-hello");
    wait_for(|| {
        fx.store
            .snapshot()
            .by_path
            .get("hello.txt")
            .is_some_and(|n| !n.content_ready)
    })
    .await;
    let db = pds_core::Db::open(&fx.root.join(STATE_DIR).join("state.db")).unwrap();
    let row = db.node_by_path("hello.txt").unwrap().expect("restore recreates the row");
    assert_eq!(row.node_uid, "node-hello");
    assert!(db.event_cursor("scope-1").unwrap() != cursor_trashed);

    // Hydratable from the restored placeholder.
    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .map(|p| std::fs::read(p).ok().as_deref() == Some(b"hello drive".as_slice()))
            .unwrap_or(false)
    })
    .await;
    assert!(fx.mock.trashed_uids().is_empty());
    fx.shutdown().await;
}

// ---------------------------------------------------------------------------
// TG-10/TG-11 — metadata fidelity (mtime, executable bit)
// ---------------------------------------------------------------------------

fn mtime_secs(path: &std::path::Path) -> Option<i64> {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

fn mode_of(path: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .ok()
        .map(|m| m.permissions().mode() & 0o777)
}

/// TG-10 (placeholder): the remote mtime survives hydration, eviction and
/// rehydration — the placeholder never falls back to "now".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn placeholder_mtime_survives_hydrate_evict_rehydrate() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    let uid = fx.mock.find_uid("root", "hello.txt").expect("remote hello");
    let remote_mtime = fx.mock.node_mtime(&uid).expect("mock mtime");
    assert_eq!(remote_mtime, 1_700_000_000, "distinctive seeded mtime");

    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .is_some_and(|p| p.is_file())
    })
    .await;
    let backing = fx.store.backing_path("hello.txt").unwrap();
    wait_for(|| mtime_secs(&backing) == Some(remote_mtime)).await;

    // Evict everything, then hydrate again through the normal queue.
    fx.handle.set_cache_cap(Some(0)).await.unwrap();
    wait_for(|| fx.store.backing_path("hello.txt").is_none()).await;
    assert!(!backing.exists());

    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .is_some_and(|p| p.is_file())
    })
    .await;
    wait_for(|| mtime_secs(&backing) == Some(remote_mtime)).await;
    fx.shutdown().await;
}

/// TG-10 (FUSE setattr path): a timestamp-only change updates the row and
/// the served snapshot, survives a restart, and deliberately does NOT queue
/// a new revision — the content hash is unchanged, so uploading would only
/// create metadata churn. The next real edit still carries this mtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn setattr_mtime_updates_row_and_snapshot_without_upload_churn() {
    let fx = start().await;
    wait_for(|| fx.store.snapshot().by_path.contains_key("hello.txt")).await;
    let stamped: i64 = 1_600_000_000;

    fx.store.record_metadata("hello.txt", Some(stamped), None);
    wait_for(|| {
        fx.db
            .node_by_path("hello.txt")
            .ok()
            .flatten()
            .is_some_and(|r| r.mtime == Some(stamped))
            && fx
                .store
                .snapshot()
                .by_path
                .get("hello.txt")
                .is_some_and(|n| n.mtime == stamped)
    })
    .await;
    assert!(
        fx.mock.uploaded_names().is_empty(),
        "timestamp-only change must not upload content: {:?}",
        fx.mock.uploaded_names()
    );
    let row = fx.db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);

    // The stored mtime is also what a later hydration applies locally.
    assert!(fx.store.begin_hydrate("hello.txt"));
    wait_for(|| {
        fx.store
            .backing_path("hello.txt")
            .is_some_and(|p| p.is_file())
    })
    .await;
    let backing = fx.store.backing_path("hello.txt").unwrap();
    wait_for(|| mtime_secs(&backing) == Some(stamped)).await;

    // Restart (fresh connection to the same DB file) still sees it.
    let db_path = fx.db_path.clone();
    fx.shutdown_keep_dir().await;
    let db = pds_core::Db::open(&db_path).unwrap();
    let row = db.node_by_path("hello.txt").unwrap().unwrap();
    assert_eq!(row.mtime, Some(stamped));
    assert_eq!(row.sync_state, pds_core::SyncState::Synced);
}

/// TG-11 (placeholder): `chmod +x` on a hydrated placeholder is persisted
/// in the local row, exposed in the snapshot, and reapplied after eviction
/// and rehydration. The mode is local-only (never sent to Drive).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn executable_bit_survives_evict_and_rehydrate() {
    use std::os::unix::fs::PermissionsExt as _;

    let fx = start().await;
    fx.mock
        .remote_create_file("root", "script.sh", b"#!/bin/sh\necho ok\n");
    wait_for(|| fx.store.snapshot().by_path.contains_key("script.sh")).await;

    assert!(fx.store.begin_hydrate("script.sh"));
    wait_for(|| {
        fx.store
            .backing_path("script.sh")
            .is_some_and(|p| p.is_file())
    })
    .await;
    let backing = fx.store.backing_path("script.sh").unwrap();
    std::fs::set_permissions(&backing, std::fs::Permissions::from_mode(0o755)).unwrap();

    // This is what the VFS `setattr(mode)` handler queues.
    fx.store.record_metadata("script.sh", None, Some(0o755));
    wait_for(|| {
        fx.db
            .node_by_path("script.sh")
            .ok()
            .flatten()
            .is_some_and(|r| r.mode == Some(0o755))
            && fx
                .store
                .snapshot()
                .by_path
                .get("script.sh")
                .is_some_and(|n| n.mode & 0o111 == 0o111)
    })
    .await;

    fx.handle.set_cache_cap(Some(0)).await.unwrap();
    wait_for(|| fx.store.backing_path("script.sh").is_none()).await;

    assert!(fx.store.begin_hydrate("script.sh"));
    wait_for(|| {
        fx.store
            .backing_path("script.sh")
            .is_some_and(|p| p.is_file())
    })
    .await;
    let backing = fx.store.backing_path("script.sh").unwrap();
    wait_for(|| mode_of(&backing) == Some(0o755)).await;
    fx.shutdown().await;
}

// Keep imports honest even if a test is skipped.
#[allow(dead_code)]
fn _t(_: HashMap<String, String>) {}
