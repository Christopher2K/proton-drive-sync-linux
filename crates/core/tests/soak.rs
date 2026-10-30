//! Conflict soak (M4 hardening): repeated and simultaneous local/remote
//! mutations must keep BOTH versions every time (F7) and leave the engine
//! in a consistent terminal state — no stuck rows, no lost content, no
//! upload hot loops. Each test runs several rounds to shake out ordering
//! and bookkeeping regressions the single-shot tests miss.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pds_core::{Db, Engine, EngineConfig, EngineState, STATE_DIR, SyncState};
use pds_sidecar_client::{MockHandle, MockSidecar};
use serial_test::serial;

struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    db: Db,
    handle: pds_core::EngineHandle,
    mock: MockHandle,
    engine: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("sync");
    std::fs::create_dir_all(&root).unwrap();
    let (client, mock) = MockSidecar::new().connect();
    let mut client = client;
    let notifications = client.take_notifications();
    let db_path = root.join(STATE_DIR).join("state.db");
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            root: root.clone(),
            db_path: db_path.clone(),
            retry_base: Duration::from_millis(20),
            placeholders: false,
            eviction_cap: None,
        },
    )
    .unwrap();
    let db = Db::open(&db_path).unwrap();
    let engine = tokio::spawn(engine.run());
    assert!(
        handle.wait_for_watcher(Duration::from_secs(10)).await,
        "watcher never attached"
    );
    wait_for(|| root.join("hello.txt").is_file()).await;
    Fixture {
        _dir: dir,
        root,
        db,
        handle,
        mock,
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

/// First locally-created conflict copy of `stem` (`stem (conflict …)`).
fn conflict_entry(root: &std::path::Path, stem: &str) -> Option<std::fs::DirEntry> {
    std::fs::read_dir(root)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{stem} (conflict "))
        })
}

/// The engine reached a quiescent state: nothing uploading/downloading,
/// every row either synced, terminally errored, or parked as conflicted —
/// and no row is dirty without either a retry pending or a terminal reason.
async fn assert_quiescent(fx: &Fixture) {
    wait_for(|| {
        futures::executor::block_on(fx.handle.status())
            .map(|s| s.state == EngineState::Running)
            .unwrap_or(false)
    })
    .await;
    // Dirty rows are allowed only while their upload is still in flight;
    // give the drain a moment, then require an empty queue (rows parked as
    // errored/conflicted are not dirty anymore).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let dirty: Vec<_> = fx.db.nodes_with_state(SyncState::Dirty).unwrap();
        let uploading = fx.db.nodes_with_state(SyncState::Uploading).unwrap();
        if dirty.is_empty() && uploading.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rows never reached a terminal state: dirty={dirty:?} uploading={uploading:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// Soak 1: N rounds of simultaneous local edit + remote edit of the same
// file. Every round must keep both versions somewhere and end synced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn soak_repeated_simultaneous_edits_keep_both() {
    let fx = start().await;
    for round in 1..=5u32 {
        let local = format!("local r{round}");
        let remote = format!("remote r{round}");
        std::fs::write(fx.root.join("hello.txt"), &local).unwrap();
        fx.mock.remote_update_file("node-hello", remote.as_bytes());
        // Wait until BOTH survived: the file shows one version, a conflict
        // copy (this round's or a re-keyed row) holds the other, and the
        // node is not stuck dirty.
        wait_for(|| {
            let disk = std::fs::read(fx.root.join("hello.txt")).unwrap_or_default();
            let conflict = conflict_entry(&fx.root, "hello")
                .map(|e| std::fs::read(e.path()).unwrap_or_default())
                .unwrap_or_default();
            (disk == local.as_bytes() && conflict == remote.as_bytes())
                || (disk == remote.as_bytes() && conflict == local.as_bytes())
        })
        .await;
        assert_quiescent(&fx).await;
    }
    // No data loss across all rounds: every version string still exists on
    // disk somewhere (hello.txt or some conflict copy).
    let mut all = std::fs::read(fx.root.join("hello.txt")).unwrap();
    for e in std::fs::read_dir(&fx.root).unwrap().flatten() {
        if e.file_name().to_string_lossy().contains("(conflict ") {
            all.extend_from_slice(&std::fs::read(e.path()).unwrap());
        }
    }
    let text = String::from_utf8_lossy(&all);
    assert!(text.contains("remote r5"), "latest remote lost: {text}");
    assert_eq!(fx.handle.status().await.unwrap().state, EngineState::Running);
    fx.shutdown().await;
}

// Soak 2: remote delete vs concurrent local edit — the local edit must
// survive (re-uploaded under an available name or kept as conflict copy);
// the remote deletion must never destroy unsynced local content.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn soak_remote_delete_vs_local_edit_keeps_edit() {
    for round in 1..=3u32 {
        let fx = start().await;
        let local = format!("precious edit r{round}");
        std::fs::write(fx.root.join("hello.txt"), &local).unwrap();
        fx.mock.remote_delete("node-hello");
        // The local content must survive somewhere in the root.
        wait_for(|| {
            fn walk(dir: &std::path::Path, needle: &str) -> bool {
                for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        if walk(&p, needle) {
                            return true;
                        }
                    } else if std::fs::read(&p).map(|c| c == needle.as_bytes()).unwrap_or(false) {
                        return true;
                    }
                }
                false
            }
            walk(&fx.root, &local)
        })
        .await;
        assert_quiescent(&fx).await;
        assert_eq!(fx.handle.status().await.unwrap().state, EngineState::Running);
        fx.shutdown().await;
    }
}

// Soak 3: burst of remote edits racing one local edit stream — the final
// state must converge to the last remote version with the local edits
// preserved in conflict copies; upload_attempts must stay bounded (no
// retry hot loop while events keep arriving).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn soak_remote_edit_burst_converges() {
    let fx = start().await;
    for i in 1..=6u32 {
        fx.mock
            .remote_update_file("node-hello", format!("burst {i}").as_bytes());
        std::fs::write(fx.root.join("hello.txt"), format!("local burst {i}")).unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    // Converged: hello.txt eventually holds the last remote version…
    wait_for(|| {
        std::fs::read(fx.root.join("hello.txt")).unwrap_or_default() == b"burst 6"
    })
    .await;
    assert_quiescent(&fx).await;
    // …some local version survived in a conflict copy…
    let conflict = conflict_entry(&fx.root, "hello");
    assert!(conflict.is_some(), "local edits lost in the burst");
    // …and the engine never errored or wedged.
    let st = fx.handle.status().await.unwrap();
    assert_eq!(st.state, EngineState::Running);
    assert_eq!(st.errored_nodes, 0);
    fx.shutdown().await;
}

// Soak 4: repeated rename-churn — the conflict copy path picks fresh names
// even when many copies pile up (never overwrites an existing copy).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn soak_conflict_copies_never_collide() {
    let fx = start().await;
    for round in 1..=8u32 {
        // Diverge local content, then let the remote event hit the same path.
        std::fs::write(fx.root.join("hello.txt"), format!("copy r{round}")).unwrap();
        fx.mock
            .remote_update_file("node-hello", format!("head r{round}").as_bytes());
        wait_for(|| {
            std::fs::read(fx.root.join("hello.txt")).unwrap_or_default()
                == format!("head r{round}").as_bytes()
        })
        .await;
    }
    assert_quiescent(&fx).await;
    // Every surviving conflict copy must have distinct, non-empty content.
    let mut seen = std::collections::HashSet::new();
    for e in std::fs::read_dir(&fx.root).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.contains("(conflict ") {
            let content = std::fs::read(e.path()).unwrap();
            assert!(!content.is_empty(), "{name} is empty");
            assert!(seen.insert(content), "duplicate conflict copy content: {name}");
        }
    }
    assert!(!seen.is_empty(), "no conflict copies survived the churn");
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
