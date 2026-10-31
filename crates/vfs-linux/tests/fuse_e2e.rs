//! End-to-end FUSE proof: engine (MockSidecar) + real kernel mount.
//!
//! Boots the engine in placeholder mode, mounts PdsFs at a temp dir, then
//! from a plain std::fs client: lists the dir, stats a placeholder, reads
//! the whole file (forcing hydration), and checks the visible root stayed
//! clean.
//!
//! Needs a real kernel FUSE setup: `/dev/fuse` plus a setuid fusermount3
//! (Nix store binaries cannot be setuid; devenv points fuser at the host's
//! helper via `FUSERMOUNT_PATH`). On hosts without one — e.g. GitHub-hosted
//! runners — the test skips itself instead of hanging until the CI timeout.
//!
//! ```sh
//! cargo test -p pds-vfs-linux --test fuse_e2e -- --nocapture
//! ```

use std::collections::HashMap;
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fuser::{Config, MountOption};
use pds_core::{Engine, EngineConfig, STATE_DIR, store::VNode};
use pds_sidecar_client::MockSidecar;
use pds_vfs_linux::PdsFs;

/// Bounds the whole test: if anything wedges (typically std::fs blocked in
/// the kernel FUSE queue, which no in-test timeout can interrupt), kill the
/// process so the suite fails in minutes instead of eating the CI timeout.
/// Disarmed when dropped at the end of a passing test.
struct Watchdog {
    disarm: mpsc::Sender<()>,
}

impl Watchdog {
    fn arm(secs: u64) -> Self {
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::spawn(move || {
            if rx.recv_timeout(Duration::from_secs(secs)).is_err() {
                eprintln!(
                    "fuse_e2e: exceeded {secs}s — IO through the mount is stuck; \
                     failing fast instead of hanging"
                );
                std::process::exit(101);
            }
        });
        Watchdog { disarm: tx }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.disarm.send(());
    }
}

/// Real-kernel FUSE needs /dev/fuse and a setuid fusermount3. Returns why
/// this host cannot run the test, or None if it can.
fn fuse_unavailable_reason() -> Option<String> {
    if std::fs::metadata("/dev/fuse").is_err() {
        return Some("/dev/fuse does not exist on this host".into());
    }
    let helper =
        std::env::var("FUSERMOUNT_PATH").unwrap_or_else(|_| "fusermount3".to_string());
    match Command::new(&helper)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(status) if status.success() => None,
        Ok(status) => {
            Some(format!("{helper} ran but exited with {status} (setuid bit missing?)"))
        }
        Err(e) => Some(format!("{helper} is not runnable: {e}")),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn fuse_mount_serves_placeholders_and_hydrates() {
    let _watchdog = Watchdog::arm(180);

    if let Some(reason) = fuse_unavailable_reason() {
        println!("skipping fuse_mount_serves_placeholders_and_hydrates: {reason}");
        println!(
            "hint: real-kernel FUSE tests need /dev/fuse and a setuid fusermount3 \
             (e.g. the Linux dev VM); see crates/vfs-linux/tests/fuse_e2e.rs"
        );
        return;
    }

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
    // hello.txt already comes seeded by the mock ("hello drive"); only
    // big.bin needs creating. A second hello.txt here would desync
    // `find_uid` from the node the engine revises.
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
            ..Default::default()
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

    // 2b. `listxattr` must answer both forms of the request (size query and
    // supplied buffer). Replying with the size form to a buffer request
    // makes the kernel return EIO — visible as `ls -l` errors on every file.
    let xattr_path = std::ffi::CString::new(mnt.join("big.bin").to_string_lossy().as_bytes())
        .expect("path has no NUL");
    let size_query = unsafe { libc::llistxattr(xattr_path.as_ptr(), std::ptr::null_mut(), 0) };
    assert_eq!(size_query, 0, "listxattr size query");
    let mut xattr_buf = [0u8; 64];
    let with_buffer = unsafe {
        libc::llistxattr(
            xattr_path.as_ptr(),
            xattr_buf.as_mut_ptr().cast(),
            xattr_buf.len(),
        )
    };
    assert_eq!(with_buffer, 0, "listxattr with buffer");

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

    // 5. Revision writes are allowed on hydrated/snapshot-backed files
    // since M3.3 (checked below); a plain read-only handle still refuses
    // nothing — instead verify a write handle revises in place later. For
    // now assert hydration actually landed (prerequisite for revisions).
    assert!(
        store.backing_path("hello.txt").map(|p| p.is_file()) == Some(true),
        "hello.txt hydrated by the earlier read"
    );

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

    // 6b. Revision write (M3.3): open the hydrated big.bin for write and
    // overwrite its head. The remote content must end up matching the
    // backing file (same node_uid — a revision, not a new file).
    let uid_big = mock.find_uid("root", "big.bin").expect("big.bin uid");
    let mut rev = std::fs::OpenOptions::new()
        .write(true)
        .open(mnt.join("big.bin"))
        .expect("revision write open");
    rev.write_all(b"REVISED").unwrap();
    drop(rev);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let head = mock
            .file_content(&uid_big)
            .map(|c| c[..7.min(c.len())].to_vec());
        if head.as_deref() == Some(&b"REVISED"[..]) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "revision upload never landed remotely: root children {:?}, uploads {:?}",
            mock.child_names("root"),
            mock.uploaded_names(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Same node, exactly one revision upload for big.bin so far.
    assert_eq!(mock.upload_attempts("big.bin"), 1);
    // The truncate-free revision preserved the tail bytes.
    let remote = mock.file_content(&uid_big).unwrap();
    assert_eq!(&remote[..7], b"REVISED");
    assert_eq!(remote.len(), big.len());

    // 6c. O_TRUNC rewrite: `fs::write` opens with O_TRUNC → setattr(size=0)
    // then writes fresh content; remote must converge to the new content.
    std::fs::write(mnt.join("hello.txt"), b"trunc+rewrite").unwrap();
    let uid_hello = mock.find_uid("root", "hello.txt").expect("hello.txt uid");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.file_content(&uid_hello).as_deref() == Some(&b"trunc+rewrite"[..]) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "O_TRUNC rewrite never landed remotely"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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

/// Desktop-app regression (M3.4): the app mounts the placeholder view AT the
/// sync root, which can still hold real leftover content (a previous mirror
/// mode, an interrupted layout conversion). The mount must shadow those
/// leftovers — never merge, upload, or delete them — and they must reappear
/// untouched once the mount is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn fuse_mount_over_nonempty_root_shadows_and_preserves_leftovers() {
    let _watchdog = Watchdog::arm(120);

    if let Some(reason) = fuse_unavailable_reason() {
        println!(
            "skipping fuse_mount_over_nonempty_root_shadows_and_preserves_leftovers: {reason}"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    // The mountpoint IS the sync root and it is NOT empty.
    let root = dir.path().join("sync");
    std::fs::create_dir_all(root.join("old stuff")).unwrap();
    std::fs::write(root.join("leftover.txt"), b"local leftover").unwrap();
    std::fs::write(root.join("old stuff").join("junk.bin"), b"junk").unwrap();

    // A small remote tree, served engine-less (shadowing needs no engine).
    let store = pds_core::Store::new();
    let node = |rel: &str, is_dir: bool, size: u64| {
        (
            rel.to_string(),
            VNode {
                rel: rel.to_string(),
                parent: String::new(),
                name: rel.to_string(),
                is_dir,
                size,
                mtime: 1_700_000_000,
                content_ready: false,
            },
        )
    };
    store.publish_snapshot(HashMap::from([
        node("hello.txt", false, 11),
        node("Documents", true, 0),
    ]));

    let proxy = dir.path().join("proxy");
    std::fs::create_dir_all(&proxy).unwrap();
    let fs = PdsFs::new(store, proxy, dir.path().join("backing"));
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("pds-e2e-nonempty".into()),
        MountOption::Subtype("pds".into()),
    ];
    let root_for_thread = root.clone();
    let session = tokio::task::spawn_blocking(move || {
        fuser::spawn_mount(fs, &root_for_thread, &cfg).expect("FUSE mount over non-empty root")
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Through the mount: the remote tree is visible (plus the `.pds` proxy)…
    let names: Vec<String> = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|n| n == "hello.txt"), "{names:?}");
    assert!(names.iter().any(|n| n == "Documents"), "{names:?}");
    // The `.pds` proxy is advertised by readdir AND resolvable: lookup must
    // answer for the state dir itself (same target as `getattr`), or `ls`
    // shows a phantom entry that cannot be stated or entered.
    let proxy_meta = std::fs::metadata(root.join(".pds")).expect("lookup .pds proxy");
    assert!(proxy_meta.is_dir(), ".pds proxy is a directory");
    assert_eq!(
        std::fs::read_dir(root.join(".pds")).unwrap().count(),
        0,
        ".pds proxy is empty"
    );
    // …and the leftover real content is shadowed, not merged.
    assert!(!names.iter().any(|n| n == "leftover.txt"), "{names:?}");
    assert!(!names.iter().any(|n| n == "old stuff"), "{names:?}");
    // Even a direct stat goes through the mount and misses.
    assert!(std::fs::metadata(root.join("leftover.txt")).is_err());

    drop(session);
    // The unmount is async; wait for the real folder to reappear.
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::metadata(root.join("leftover.txt")).is_err() {
        assert!(Instant::now() < deadline, "mount did not unmount");
        std::thread::sleep(Duration::from_millis(50));
    }

    // Every leftover byte survived, untouched by the mount.
    assert_eq!(
        std::fs::read(root.join("leftover.txt")).unwrap(),
        b"local leftover"
    );
    assert_eq!(
        std::fs::read(root.join("old stuff").join("junk.bin")).unwrap(),
        b"junk"
    );
}
