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
use std::io::{BufRead as _, BufReader, Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::io::AsRawFd as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fuser::{Config, MountOption};
use pds_core::{Engine, EngineConfig, STATE_DIR, store::VNode};
use pds_sidecar_client::MockSidecar;
use pds_vfs_linux::PdsFs;
use pds_vfs_linux::mount::{
    MountState, MountpointError, ensure_placeholder_mountpoint, mountpoint_state,
};

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

/// Shared sink for the tracing subscriber. The e2e test asserts on it to
/// prove the daemon's `copy_file_range` handler answered a copy: the kernel
/// silently substitutes a splice copy when the filesystem replies
/// `EOPNOTSUPP`, so a correct destination file alone does not prove which
/// path ran.
#[derive(Clone, Default)]
struct LogBuf(Arc<std::sync::Mutex<Vec<u8>>>);

impl LogBuf {
    fn contains(&self, needle: &str) -> bool {
        let buf = self.0.lock().unwrap();
        String::from_utf8_lossy(&buf).contains(needle)
    }
}

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
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

/// Best-effort lazy unmount used by the mount-safety tests (a stale mount has
/// no session to drop, so it must be released through the helper).
fn lazy_unmount(path: &std::path::Path) {
    let target = path.to_string_lossy().into_owned();
    let helper = std::env::var("FUSERMOUNT_PATH").unwrap_or_else(|_| "fusermount3".to_string());
    let _ = Command::new(&helper)
        .args(["-uz", &target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = Command::new("fusermount")
        .args(["-uz", &target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn wait_for_mount_state(
    path: &std::path::Path,
    pred: impl Fn(&MountState) -> bool,
    timeout: Duration,
) -> MountState {
    let deadline = Instant::now() + timeout;
    loop {
        let state = mountpoint_state(path, Duration::from_millis(500));
        if pred(&state) {
            return state;
        }
        if Instant::now() >= deadline {
            panic!("mountpoint state never matched (last: {state:?})");
        }
        std::thread::sleep(Duration::from_millis(50));
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

    let logs = LogBuf::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "pds_core=debug,pds_sidecar_client=debug,pds_vfs_linux=debug".to_string()
        }))
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

    // 6d. Regression: replace an OVERLAY file (created this session,
    // upload still in flight) with open(O_WRONLY|O_TRUNC) — the `cp`
    // pattern. open() used to return fh 0 for writable overlay opens,
    // so every write(2) failed EIO.
    std::fs::write(mnt.join("overlay-replace.txt"), b"first version").unwrap();
    // Do NOT wait for the upload: the rewrite must work even while the
    // file is still overlay-only. `cp src dst` = open(dst, O_WRONLY|O_TRUNC)
    // + write, i.e. File::create over an existing path.
    let mut replace = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(mnt.join("overlay-replace.txt"))
        .expect("writable open of overlay file");
    replace.write_all(b"second version").unwrap();
    replace.flush().unwrap();
    drop(replace);
    assert_eq!(
        std::fs::read(mnt.join("overlay-replace.txt")).unwrap(),
        b"second version",
        "overlay rewrite must be readable back"
    );
    // …and the second version is what lands remotely.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let content = mock
            .find_uid("root", "overlay-replace.txt")
            .and_then(|uid| mock.file_content(&uid));
        if content.as_deref() == Some(&b"second version"[..]) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "overlay replace never landed remotely: children {:?}, uploads {:?}",
            mock.child_names("root"),
            mock.uploaded_names(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 6e. Server-side copy (`copy_file_range(2)`): same-mount copies are
    // routed to the daemon instead of being read back through userspace.
    // fuser's default answer was ENOSYS, so the kernel used its splice
    // fallback — the path apps like Plasticity hit as intermittent EIO /
    // 0-byte saves. A full-length return alone does not prove which path
    // ran (the kernel silently substitutes splice on EOPNOTSUPP), so the
    // handler's log line is asserted below as well.
    let src = std::fs::File::open(mnt.join("big.bin")).expect("copy source open");
    let dst = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(mnt.join("big-copy.bin"))
        .expect("copy destination create");
    let mut off_in: libc::loff_t = 0;
    let mut off_out: libc::loff_t = 0;
    let copied = unsafe {
        libc::copy_file_range(
            src.as_raw_fd(),
            &mut off_in,
            dst.as_raw_fd(),
            &mut off_out,
            big.len(),
            0,
        )
    };
    assert_eq!(
        copied,
        big.len() as isize,
        "copy_file_range must copy every byte (errno {:?})",
        std::io::Error::last_os_error()
    );
    assert_eq!(off_out, big.len() as i64);
    assert!(
        logs.contains("copy_file_range: server-side copy"),
        "the daemon must answer copy_file_range itself; the kernel's splice \
         fallback (EOPNOTSUPP) produces the same bytes but reintroduces the \
         page-cache path this handler exists to bypass"
    );
    drop(dst);
    drop(src);
    let expect_copy = std::fs::read(mnt.join("big.bin")).unwrap();
    assert_eq!(expect_copy.len(), big.len());
    assert_eq!(
        std::fs::read(mnt.join("big-copy.bin")).unwrap(),
        expect_copy,
        "copied file must match the source byte for byte"
    );
    // …and the copy uploads remotely as a new node with the same bytes.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let remote = mock
            .find_uid("root", "big-copy.bin")
            .and_then(|uid| mock.file_content(&uid));
        if remote.as_deref() == Some(expect_copy.as_slice()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "copy never landed remotely intact: children {:?}, uploads {:?}",
            mock.child_names("root"),
            mock.uploaded_names(),
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

    // 7b. Regression: a BARE mkdir (no file written inside) must reach
    // the remote on its own — the engine used to drain pending_mkdirs
    // only from upload_write_one, so a childless folder never synced and
    // stayed FUSE-overlay-only.
    std::fs::create_dir(mnt.join("emptydir")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.child_names("root").iter().any(|n| n == "emptydir") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bare mkdir never reached the remote: root children {:?}",
            mock.child_names("root"),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // …and the snapshot serves it as a directory (DB row written).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if store
            .snapshot()
            .by_path
            .get("emptydir")
            .is_some_and(|n| n.is_dir)
        {
            break;
        }
        assert!(Instant::now() < deadline, "no snapshot row for emptydir");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 7c. Regression: rmdir of that folder must succeed. It used to fail
    // with ENOTEMPTY because Tree::ino_of resolved the overlay-only path
    // to the ROOT inode and the root's children answered the emptiness
    // check. First delete while it is still overlay-only (before the
    // snapshot row lands) — the exact user-visible failure — then repeat
    // after the row exists.
    let emptydir_uid = mock.find_uid("root", "emptydir").expect("emptydir uid");
    std::fs::create_dir(mnt.join("gone-soon")).unwrap();
    std::fs::remove_dir(mnt.join("gone-soon")).expect("rmdir overlay-only folder");
    std::fs::remove_dir(mnt.join("emptydir")).expect("rmdir snapshot-backed folder");
    // Both remove cleanly; emptydir trashes remotely (gone-soon may or
    // may not have a row yet depending on drain order — either way the
    // engine handles it). Two rapid deletes on this small tree can trip
    // the percentage limb of the mass-deletion guard; explicit resumes
    // approve the batch. Resume only approves tombstones the engine has
    // received, so keep resuming until emptydir is actually trashed (a
    // resume processed before a delete lands approves nothing).
    let deadline = Instant::now() + Duration::from_secs(30);
    while !mock.trashed_uids().contains(&emptydir_uid) {
        assert!(
            Instant::now() < deadline,
            "emptydir rmdir never trashed remotely: {:?}",
            mock.trashed_uids()
        );
        handle.resume().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The mount no longer lists either folder.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let names: Vec<String> = std::fs::read_dir(&mnt)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!names.contains(&"emptydir".to_string()), "{names:?}");
    assert!(!names.contains(&"gone-soon".to_string()), "{names:?}");

    // 8. unlink through the mount trashes remotely (tombstone drains).
    std::fs::remove_file(mnt.join("notes.txt")).unwrap();
    // Third delete in the window on this small tree: the ratio limb can
    // trip again. Resume only approves tombstones already received, so
    // keep resuming until the trash lands and the row leaves the mount
    // (earlier deletes may already be in trashed_uids).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let names: Vec<String> = std::fs::read_dir(&mnt)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        if !names.contains(&"notes.txt".to_string()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "unlink never trashed remotely: {:?}",
            mock.trashed_uids()
        );
        handle.resume().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 9. rename through the mount (same folder): the file must move
    // locally AND rename remotely — never trash + re-upload.
    let docs_uid = mock.find_uid("root", "docs").expect("docs uid");
    let uid_docs_a = mock.find_uid(&docs_uid, "a.md").expect("a.md uid");
    std::fs::rename(mnt.join("docs").join("a.md"), mnt.join("docs").join("renamed.md"))
        .expect("rename through the mount");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.child_names(&docs_uid).iter().any(|n| n == "renamed.md") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rename never reached the remote: docs children {:?}, renames {:?}, trashed {:?}",
            mock.child_names(&docs_uid),
            mock.renames(),
            mock.trashed_uids(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !mock.trashed_uids().contains(&uid_docs_a),
        "rename must not trash the source; trashed: {:?}",
        mock.trashed_uids()
    );
    assert!(mock
        .renames()
        .iter()
        .any(|(u, n)| u == &uid_docs_a && n == "renamed.md"));
    // The mount serves the file under its new name, old one is gone, and
    // the content survived (same node, no re-upload). Wait for the
    // engine's snapshot publish (content-ready), then outlast the kernel's
    // negative-dentry TTL (the rename syscall's destination lookup ran
    // before the row existed and was cached as ENOENT for TTL=1s).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if store
            .snapshot()
            .by_path
            .get("docs/renamed.md")
            .is_some_and(|n| n.content_ready)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "renamed file never became content-ready in the snapshot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        std::fs::read(mnt.join("docs").join("renamed.md")).unwrap(),
        b"# hi"
    );
    assert!(!mnt.join("docs").join("a.md").exists());

    // 9b. Cross-folder move: renamed.md → root as moved.md. Same TTL note
    // as above applies to the moved name's first lookup.
    std::fs::rename(
        mnt.join("docs").join("renamed.md"),
        mnt.join("moved.md"),
    )
    .expect("cross-folder rename through the mount");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.child_names("root").iter().any(|n| n == "moved.md") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cross-folder move never reached the remote: root children {:?}",
            mock.child_names("root"),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(std::fs::read(mnt.join("moved.md")).unwrap(), b"# hi");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if !mnt.join("docs").join("renamed.md").exists() {
            break;
        }
        assert!(Instant::now() < deadline, "old rename path still visible");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!mock.trashed_uids().contains(&uid_docs_a));

    // 9c. Regression: `rm -rf` of a folder with contents through the
    // mount. The child unlinks queue deletes but the children stay in
    // the served snapshot until the engine drains their tombstones and
    // republishes; rmdir milliseconds later used to hit them in
    // dir_is_empty and fail with ENOTEMPTY.
    std::fs::write(mnt.join("docs").join("b.md"), b"# bye").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if mock.uploaded_names().iter().any(|n| n == "b.md") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "b.md upload never happened: uploads {:?}",
            mock.uploaded_names(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Both children synced and served by the snapshot before we delete
    // (docs/a.md was renamed to moved.md in step 9b; only b.md remains
    // alongside the folder row itself).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snap = store.snapshot();
        if snap.by_path.get("docs").is_some_and(|n| n.is_dir)
            && snap.by_path.contains_key("docs/b.md")
        {
            break;
        }
        assert!(Instant::now() < deadline, "docs children never snapshotted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    std::fs::remove_dir_all(mnt.join("docs")).expect(
        "rm -rf of a snapshot-backed folder must not fail ENOTEMPTY while child deletes are still queued",
    );
    // Everything under docs/ trashes remotely (children + folder). The
    // mass-deletion guard may pause on this small tree; keep resuming —
    // resume only approves tombstones the engine has already received.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let kids = mock.child_names(&docs_uid);
        if kids.is_empty() && !mock.child_names("root").iter().any(|n| n == "docs") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rm -rf never trashed remotely: docs children {kids:?}, trashed {:?}",
            mock.trashed_uids(),
        );
        handle.resume().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
                mode: if is_dir { 0o755 } else { 0o644 },
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

// ---------------------------------------------------------------------------
// Wave 4b — large sparse placeholders (TG-16)
// ---------------------------------------------------------------------------

/// TG-16: a ~5 GiB remote placeholder is served through a real kernel mount;
/// reads at an offset beyond 4 GiB return the sparse backing file's markers,
/// and the on-disk footprint stays sparse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn fuse_reads_large_sparse_placeholder_beyond_4gib() {
    let _watchdog = Watchdog::arm(180);

    if let Some(reason) = fuse_unavailable_reason() {
        println!("skipping fuse_reads_large_sparse_placeholder_beyond_4gib: {reason}");
        return;
    }

    const SIZE: u64 = 5 * 1024 * 1024 * 1024;
    const TAIL: &[u8] = b"PDS-5G-TAIL";
    const BOUNDARY: u64 = 4 * 1024 * 1024 * 1024 - 4;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    mock.remote_create_sparse_file(
        "root",
        "huge.bin",
        SIZE,
        &[(BOUNDARY, b"BND!"), (SIZE - TAIL.len() as u64, TAIL)],
    );
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

    let fs = PdsFs::new(
        store.clone(),
        root.join(STATE_DIR),
        root.join(".pds-vfs-backing"),
    );
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("pds-e2e-large".into()),
        MountOption::Subtype("pds".into()),
    ];
    let mnt_for_thread = mnt.clone();
    let session = tokio::task::spawn_blocking(move || {
        fuser::spawn_mount(fs, &mnt_for_thread, &cfg).expect("FUSE mount")
    })
    .await
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    while !store
        .snapshot()
        .by_path
        .get("huge.bin")
        .is_some_and(|n| n.size == SIZE)
    {
        assert!(Instant::now() < deadline, "huge.bin never appeared in the snapshot");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let meta = std::fs::metadata(mnt.join("huge.bin")).expect("stat huge.bin");
    assert_eq!(meta.len(), SIZE, "placeholder reports the full 5 GiB");

    let mut file = std::fs::File::open(mnt.join("huge.bin")).expect("open huge.bin");
    file.seek(SeekFrom::Start(BOUNDARY)).unwrap();
    let mut boundary = [0u8; 4];
    file.read_exact(&mut boundary).unwrap();
    assert_eq!(&boundary, b"BND!", "read across the 4 GiB boundary");
    file.seek(SeekFrom::Start(SIZE - TAIL.len() as u64)).unwrap();
    let mut tail = vec![0u8; TAIL.len()];
    file.read_exact(&mut tail).unwrap();
    assert_eq!(tail, TAIL, "tail marker beyond 4 GiB");

    // Hydration stays outside the visible root and keeps the file sparse.
    assert!(!root.join("huge.bin").exists());
    let backing = store.backing_path("huge.bin").expect("backing entry");
    use std::os::unix::fs::MetadataExt as _;
    let backing_meta = std::fs::metadata(&backing).unwrap();
    assert_eq!(backing_meta.len(), SIZE);
    assert!(
        backing_meta.blocks() * 512 < 64 * 1024 * 1024,
        "backing file must stay sparse: {} bytes on disk",
        backing_meta.blocks() * 512
    );
    assert_eq!(store.snapshot().by_path["huge.bin"].size, SIZE);

    handle.shutdown().await.ok();
    drop(session);
    let _ = engine_task.await;
}

// ---------------------------------------------------------------------------
// Wave 4b — stale/busy mounts (TG-30)
// ---------------------------------------------------------------------------

fn hello_snapshot(store: &pds_core::Store) {
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
                mode: if is_dir { 0o755 } else { 0o644 },
                content_ready: false,
            },
        )
    };
    store.publish_snapshot(HashMap::from([
        node("hello.txt", false, 11),
        node("Documents", true, 0),
    ]));
}

/// TG-30.1: a second session on a busy/live mountpoint is refused quickly by
/// the app preflight, and the kernel mount attempt itself fails (EBUSY)
/// instead of hanging. The first session keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn second_session_on_live_mountpoint_fails_fast_and_first_keeps_serving() {
    let _watchdog = Watchdog::arm(120);

    if let Some(reason) = fuse_unavailable_reason() {
        println!(
            "skipping second_session_on_live_mountpoint_fails_fast_and_first_keeps_serving: {reason}"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();
    let store = pds_core::Store::new();
    hello_snapshot(&store);
    let fs = PdsFs::new(store, dir.path().join("state"), dir.path().join("backing"));
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("pds-e2e-busy".into()),
        MountOption::Subtype("pds".into()),
    ];
    let mnt_for_thread = mnt.clone();
    let first = tokio::task::spawn_blocking(move || {
        fuser::spawn_mount(fs, &mnt_for_thread, &cfg).expect("first FUSE mount")
    })
    .await
    .unwrap();
    wait_for_mount_state(&mnt, |s| matches!(s, MountState::Live { .. }), Duration::from_secs(10));

    // Production decision: refuse, with the actionable busy message, bounded.
    let started = Instant::now();
    let refusal = ensure_placeholder_mountpoint(&mnt, Duration::from_secs(2))
        .expect_err("a live mount must not be reused");
    assert!(matches!(refusal, MountpointError::Busy { .. }));
    assert!(started.elapsed() < Duration::from_secs(5), "preflight must be bounded");
    assert!(refusal.to_string().contains("already mounted"));

    // A second real kernel session on the busy mountpoint. Hold the mount as
    // a child's cwd so mount(2) reports EBUSY rather than stacking a mount.
    let mut holder = Command::new("sh")
        .arg("-c")
        .arg("cd \"$1\" && exec sleep 120")
        .arg("sh")
        .arg(&mnt)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn cwd holder");
    std::thread::sleep(Duration::from_millis(200));

    let (tx, rx) = mpsc::channel();
    let second_mnt = mnt.clone();
    let second_dir = dir.path().to_path_buf();
    std::thread::spawn(move || {
        let store = pds_core::Store::new();
        hello_snapshot(&store);
        let fs = PdsFs::new(
            store,
            second_dir.join("state2"),
            second_dir.join("backing2"),
        );
        let mut cfg = Config::default();
        cfg.mount_options = vec![
            MountOption::FSName("pds-e2e-second".into()),
            MountOption::Subtype("pds".into()),
        ];
        let result = fuser::Session::new(fs, &second_mnt, &cfg).map(drop);
        let _ = tx.send(result.map_err(|e| (e.kind(), e.raw_os_error(), e.to_string())));
    });
    let second = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("second mount attempt must not hang");
    // The kernel/helper must reject the second session promptly rather than
    // stacking a mount. `fusermount3` reports the busy mountpoint through an
    // exit status (fuser surfaces it as an `Other` io::Error), so assert the
    // bounded failure plus a non-empty reason; the typed refusal is the
    // `MountpointError::Busy` preflight assertion above.
    match second {
        Err((_kind, _code, message)) => {
            assert!(!message.is_empty(), "refusal must carry a reason");
        }
        Ok(()) => panic!("a second mount on a busy mountpoint must fail"),
    }

    // The first session still answers.
    let names: Vec<String> = std::fs::read_dir(&mnt)
        .expect("first mount still serves")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|n| n == "hello.txt"), "{names:?}");

    let _ = holder.kill();
    let _ = holder.wait();
    drop(first);
    lazy_unmount(&mnt);
    wait_for_mount_state(&mnt, |s| matches!(s, MountState::Free), Duration::from_secs(10));
}

/// Child half of the unclean-shutdown test: mount, announce readiness, then
/// sleep until the parent `SIGKILL`s this process (leaving a dead mount).
#[test]
#[ignore = "spawned by stale_mount_after_crashed_session_is_detected"]
fn stale_mount_helper_child() {
    let Ok(root) = std::env::var("PDS_STALE_MOUNT_HELPER_ROOT") else {
        return;
    };
    if let Some(reason) = fuse_unavailable_reason() {
        println!("SKIPPED: {reason}");
        return;
    }
    let root = std::path::PathBuf::from(root);
    let store = pds_core::Store::new();
    hello_snapshot(&store);
    let fs = PdsFs::new(
        store,
        root.join("state"),
        root.join("backing"),
    );
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("pds-e2e-stale".into()),
        MountOption::Subtype("pds".into()),
    ];
    let _session = match fuser::spawn_mount(fs, &root, &cfg) {
        Ok(session) => session,
        Err(e) => {
            println!("SKIPPED: helper mount failed: {e}");
            return;
        }
    };
    println!("MOUNT_READY");
    std::io::stdout().flush().ok();
    std::thread::sleep(Duration::from_secs(300));
}

/// TG-30.2: kill a serving process without unmounting, then assert the next
/// start detects the stale mount and refuses with an actionable message
/// instead of hanging or writing into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn stale_mount_after_crashed_session_is_detected_and_refused() {
    let _watchdog = Watchdog::arm(120);

    if let Some(reason) = fuse_unavailable_reason() {
        println!(
            "skipping stale_mount_after_crashed_session_is_detected_and_refused: {reason}"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("stale");
    std::fs::create_dir_all(&root).unwrap();

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "stale_mount_helper_child",
            "--nocapture",
        ])
        .env("PDS_STALE_MOUNT_HELPER_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn stale-mount helper");

    let stdout = child.stdout.take().expect("helper stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = match line {
                Ok(line) => line,
                Err(_) => break,
            };
            if line.contains("MOUNT_READY") {
                let _ = tx.send(true);
                return;
            }
            if line.contains("SKIPPED") {
                let _ = tx.send(false);
                return;
            }
        }
        let _ = tx.send(false);
    });
    let ready = rx.recv_timeout(Duration::from_secs(30)).unwrap_or(false);
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        lazy_unmount(&root);
        println!("skipping stale-mount test: helper could not mount");
        return;
    }
    wait_for_mount_state(&root, |s| matches!(s, MountState::Live { .. }), Duration::from_secs(10));

    // Unclean shutdown: SIGKILL the serving process. The kernel keeps
    // the mount, but the session is gone.
    child.kill().expect("SIGKILL helper");
    let _ = child.wait();
    let stale = wait_for_mount_state(
        &root,
        |s| matches!(s, MountState::Stale { .. }),
        Duration::from_secs(10),
    );
    let MountState::Stale { reason, .. } = &stale else {
        panic!("expected a stale mount, got {stale:?}");
    };

    let refusal = ensure_placeholder_mountpoint(&root, Duration::from_secs(2))
        .expect_err("a stale mount must be refused");
    assert!(matches!(refusal, MountpointError::Stale { .. }));
    let message = refusal.to_string();
    assert!(message.contains("stale"), "{message}");
    assert!(message.contains("fusermount3 -uz"), "{message}");
    println!("stale mount detected ({reason}); app message: {message}");

    lazy_unmount(&root);
    wait_for_mount_state(&root, |s| matches!(s, MountState::Free), Duration::from_secs(10));
}
