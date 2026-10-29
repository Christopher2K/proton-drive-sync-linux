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
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "pds_core=debug,pds_sidecar_client=debug".to_string()
        }))
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
            eviction_cap: None,
        },
    )
    .unwrap();
    let store = handle.store().cloned().unwrap();
    let engine_task = tokio::spawn(engine.run());

    // Mount writable; the fs itself enforces read-only per file
    // (snapshot-backed content is RO, write-created files are RW).
    let fs = PdsFs::new(store.clone(), root.join(STATE_DIR), root.join(".pds-vfs-backing"));
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("pds-e2e".into()),
        MountOption::Subtype("pds".into()),
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

    // 5. Snapshot-backed files stay read-only in M3.2 (revision writes are
    // a later slice)…
    let err = std::fs::write(mnt.join("hello.txt"), b"nope")
        .expect_err("write to a hydrated file must fail with EROFS");
    assert_eq!(err.raw_os_error(), Some(30)); // EROFS

    // 6. Write-through: create a file through the mount, upload fires.
    std::fs::write(mnt.join("notes.txt"), b"written via fuse").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.uploaded_names().iter().any(|n| n == "notes.txt") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "write-through upload never happened"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Visible root stays clean; content waits in the backing dir.
    assert!(!root.join("notes.txt").exists());
    assert_eq!(
        std::fs::read(root.join(".pds-vfs-backing").join("notes.txt")).unwrap(),
        b"written via fuse"
    );
    // The engine's snapshot now serves the file with its row (uid assigned).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(node) = store.snapshot().by_path.get("notes.txt") {
            assert!(node.content_ready, "uploaded file is content-ready");
            break;
        }
        assert!(Instant::now() < deadline, "no snapshot entry after upload");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // …and a fresh open reads the uploaded content back.
    assert_eq!(
        std::fs::read(mnt.join("notes.txt")).unwrap(),
        b"written via fuse"
    );

    // 7. mkdir + nested create upload the folder chain too.
    std::fs::create_dir(mnt.join("docs")).unwrap();
    std::fs::write(mnt.join("docs").join("a.md"), b"# hi").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.uploaded_names().iter().any(|n| n == "a.md") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "nested write-through upload never happened"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 8. unlink through the mount trashes remotely (tombstone drains).
    std::fs::remove_file(mnt.join("notes.txt")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let trashed = mock.trashed_uids();
        if !trashed.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "unlink never trashed remotely");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The mount no longer lists it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let names: Vec<String> = std::fs::read_dir(&mnt)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!names.contains(&"notes.txt".to_string()), "{names:?}");

    handle.shutdown().await.ok();
    drop(session);
    let _ = engine_task.await;
}
