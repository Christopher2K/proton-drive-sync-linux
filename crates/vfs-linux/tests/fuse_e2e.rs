//! End-to-end FUSE proof: engine (MockSidecar) + real kernel mount.
//!
//! Boots the engine in placeholder mode, mounts PdsFs at a temp dir, then
//! from a plain std::fs client: lists the dir, stats a placeholder, reads
//! the whole file (forcing hydration), and checks the visible root stayed
//! clean. Run in the VM where /dev/fuse exists:
//!
//! ```sh
//! cargo test -p pds-vfs-linux --test fuse_e2e -- --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use fuser::{Config, MountOption};
use pds_core::{Engine, EngineConfig, STATE_DIR};
use pds_sidecar_client::MockSidecar;
use pds_vfs_linux::PdsFs;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn fuse_mount_serves_placeholders_and_hydrates() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pds_core=debug,pds_sidecar_client=debug")
        .with_test_writer()
        .try_init();

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    // ~3 MB remote file: more than any single FUSE read request.
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    mock.remote_create_file("root", "hello.txt", b"hello drive");
    mock.remote_create_file("root", "big.bin", &big);
    let notifications = client.take_notifications();

    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: root.join(STATE_DIR).join("state.db"),
            retry_base: pds_core::RETRY_BASE_DEFAULT,
            placeholders: true,
        },
    )
    .unwrap();
    let store = handle.store().cloned().unwrap();
    let engine_task = tokio::spawn(engine.run());

    // Mount and keep the session handle alive. fuser's spawn_mount runs its
    // own session threads; fusermount3 needs a moment to settle the mount.
    let fs = PdsFs::new(store, root.join(STATE_DIR));
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("pds-e2e".into()),
    ];
    let mnt_for_thread = mnt.clone();
    let session = tokio::task::spawn_blocking(move || {
        fuser::spawn_mount(fs, &mnt_for_thread, &cfg).expect("FUSE mount")
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 1. The visible root must stay empty (no materialization)…
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let entries: Vec<_> = std::fs::read_dir(&mnt)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        if entries.len() >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let names: Vec<String> = std::fs::read_dir(&mnt)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "hello.txt"),
        "hello.txt not listed: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "big.bin"),
        "big.bin not listed: {names:?}"
    );
    assert!(
        !root.join("hello.txt").exists() && !root.join("big.bin").exists(),
        "files must not be materialized in the visible root"
    );

    // 2. Placeholder metadata reports the remote size before any read.
    let meta = std::fs::metadata(mnt.join("big.bin")).unwrap();
    assert_eq!(meta.len(), big.len() as u64, "placeholder size");

    // 3. Read through the mount → hydration → correct content.
    let started = Instant::now();
    let got = std::fs::read(mnt.join("big.bin")).expect("read big.bin");
    assert_eq!(got.len(), big.len());
    assert!(got == big, "content mismatch through the mount");
    println!("read {} bytes through FUSE in {:?}", got.len(), started.elapsed());

    let got_small = std::fs::read(mnt.join("hello.txt")).expect("read hello.txt");
    assert_eq!(got_small, b"hello drive");

    // 4. Hydrations live outside the visible root.
    assert!(!root.join("big.bin").exists());
    assert!(root.join(".pds-vfs-backing").join("big.bin").is_file());

    // 5. The mount is read-only.
    let err = std::fs::write(mnt.join("hello.txt"), b"nope")
        .expect_err("write must fail on RO mount");
    assert_eq!(err.raw_os_error(), Some(30)); // EROFS

    handle.shutdown().await.ok();
    drop(session);
    let _ = engine_task.await;
}
