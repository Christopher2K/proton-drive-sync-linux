//! `pds-engine-headless` — headless sync engine controlled over a Unix socket.
//!
//! This is a Phase 1 MyGo migration experiment. It runs the same sync engine
//! and optional FUSE placeholder mount as the GPUI desktop app, but exposes a
//! JSON-line control protocol over a Unix domain socket instead of a window.
//!
//! The goal is to let a MyGo-native UI shell launch this binary, send commands
//! like StartLogin / Pause / Resume / Shutdown, and receive streamed state
//! updates (AuthRequired, LoginUrl, EngineStarted, Status, Activity, …).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use pds_core::{
    Db, EVICTION_CAP_DEFAULT, Engine, EngineConfig, EngineHandle, RETRY_BASE_DEFAULT, SyncMode,
    engine_state_dir,
};
use pds_sidecar_client::{Notification, SidecarApi, spawn::spawn_sidecar, types::error_codes};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

/// Commands the UI shell sends to the engine over the Unix socket.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "cmd")]
enum UiCommand {
    StartLogin,
    Pause,
    Resume,
    SignOut,
    Shutdown,
    SetCacheCap(Option<u64>),
    SetMode(String),
}

/// Updates the engine streams back to every connected UI client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "update", rename_all = "PascalCase")]
enum EngineUpdate {
    AuthRequired,
    AuthOk {
        email: String,
    },
    LoginStarted,
    LoginUrl {
        url: String,
        user_code: String,
    },
    LoginFailed {
        error: String,
    },
    EngineStarted,
    LayoutMigrated,
    MountUnavailable {
        error: String,
    },
    Status(EngineStatusJson),
    Activity(ActivityItemJson),
    Inbox {
        conflicts: Vec<String>,
        errored: Vec<ErroredItemJson>,
    },
    SignedOut,
    ModeSwitched {
        mode: String,
    },
    Failed {
        error: String,
    },
    Exited {
        reason: String,
    },
}

/// JSON-serializable conflict/error inbox item.
#[derive(Debug, Clone, Serialize)]
struct ErroredItemJson {
    path: String,
    error: String,
}

impl From<&(String, String)> for ErroredItemJson {
    fn from((path, error): &(String, String)) -> Self {
        Self {
            path: path.clone(),
            error: error.clone(),
        }
    }
}

/// JSON-serializable mirror of `pds_core::EngineStatus`.
#[derive(Debug, Clone, Serialize)]
struct EngineStatusJson {
    state: String,
    synced_nodes: usize,
    dirty_nodes: usize,
    queued_tombstones: usize,
    errored_nodes: usize,
    conflicted_nodes: usize,
    mode: String,
    cache_cap: Option<u64>,
    cache_used: u64,
    needs_sign_in: bool,
    sidecar_unavailable: bool,
}

impl From<&pds_core::EngineStatus> for EngineStatusJson {
    fn from(s: &pds_core::EngineStatus) -> Self {
        Self {
            state: s.state.to_string(),
            synced_nodes: s.synced_nodes,
            dirty_nodes: s.dirty_nodes,
            queued_tombstones: s.queued_tombstones,
            errored_nodes: s.errored_nodes,
            conflicted_nodes: s.conflicted_nodes,
            mode: s.mode.as_meta_value().to_string(),
            cache_cap: s.cache_cap,
            cache_used: s.cache_used,
            needs_sign_in: s.needs_sign_in,
            sidecar_unavailable: s.sidecar_unavailable,
        }
    }
}

/// JSON-serializable mirror of `pds_core::ActivityItem`.
#[derive(Debug, Clone, Serialize)]
struct ActivityItemJson {
    at_unix: i64,
    kind: String,
    path: String,
    detail: Option<String>,
}

impl From<&pds_core::ActivityItem> for ActivityItemJson {
    fn from(item: &pds_core::ActivityItem) -> Self {
        Self {
            at_unix: item.at_unix,
            kind: format!("{:?}", item.kind),
            path: item.path.clone(),
            detail: item.detail.clone(),
        }
    }
}

#[derive(Parser)]
#[command(
    name = "pds-engine-headless",
    version,
    about = "Headless Proton Drive sync engine (JSON control socket)"
)]
struct Cli {
    /// Local sync root folder (created if missing).
    #[arg(short, long)]
    root: Option<PathBuf>,

    /// Unix socket path for control. Defaults to
    /// `$XDG_RUNTIME_DIR/pds-engine-<root_hash>.sock`.
    #[arg(short, long)]
    socket: Option<PathBuf>,

    /// Sidecar command override, e.g. `node sidecar/dist/index.js`.
    #[arg(long)]
    sidecar_cmd: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    init_logging();

    let root = cli.root.map(PathBuf::from).unwrap_or_else(default_root);
    let sidecar_cmd = cli.sidecar_cmd;
    let socket_path = cli.socket.unwrap_or_else(|| default_socket_path(&root));

    info!(root = %root.display(), socket = %socket_path.display(), "starting headless engine");

    // Engine thread ↔ socket runtime channels.
    let (updates_tx, _) = tokio::sync::broadcast::channel::<EngineUpdate>(256);
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<UiCommand>();

    let updates_tx_for_engine = updates_tx.clone();
    let engine_handle = std::thread::Builder::new()
        .name("pds-engine".into())
        .spawn(move || {
            let updates = updates_tx_for_engine.clone();
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = updates.send(EngineUpdate::Failed {
                        error: format!("could not start the async runtime: {e}"),
                    });
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) = run_engine(root, sidecar_cmd, updates.clone(), cmd_rx).await {
                    let _ = updates.send(EngineUpdate::Failed {
                        error: format!("{e:#}"),
                    });
                }
            });
        })
        .expect("spawn engine thread");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build socket runtime");

    rt.block_on(async move {
        if let Err(e) = run_socket_server(&socket_path, cmd_tx, updates_tx, engine_handle).await {
            error!("socket server failed: {e:#}");
        }
    });
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pds_core=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();
}

fn default_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("PDS_SYNC_ROOT") {
        return PathBuf::from(dir);
    }
    pds_xdg::home().join("ProtonDrive")
}

fn default_socket_path(root: &Path) -> PathBuf {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir());
    let key = pds_core::mode::root_key(root);
    runtime.join(format!("pds-engine-{key}.sock"))
}

// ---------------------------------------------------------------------------
// Socket server
// ---------------------------------------------------------------------------

async fn run_socket_server(
    socket_path: &Path,
    cmd_tx: UnboundedSender<UiCommand>,
    updates_tx: tokio::sync::broadcast::Sender<EngineUpdate>,
    engine_handle: std::thread::JoinHandle<()>,
) -> Result<()> {
    if let Some(dir) = socket_path.parent() {
        std::fs::create_dir_all(dir).context("create socket parent dir")?;
    }
    std::fs::remove_file(socket_path).ok();
    let listener = UnixListener::bind(socket_path).context("bind control socket")?;

    // Clean up the socket file on exit.
    let socket_path = socket_path.to_path_buf();
    let cleanup_socket = Arc::new(socket_path);
    let _guard = SocketFileGuard(cleanup_socket.clone());

    let engine_done = wait_thread(engine_handle);
    tokio::pin!(engine_done);

    loop {
        tokio::select! {
            accept = listener.accept() => match accept {
                Ok((stream, _)) => {
                    let cmd_tx = cmd_tx.clone();
                    let mut updates_rx = updates_tx.subscribe();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, cmd_tx, &mut updates_rx).await {
                            warn!("client disconnected: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received, shutting down engine");
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break;
            }
            _ = &mut engine_done => {
                info!("engine thread ended");
                break;
            }
        }
    }

    // Give the engine a moment to finish before the runtime drops.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}

async fn wait_thread(handle: std::thread::JoinHandle<()>) {
    tokio::task::spawn_blocking(move || handle.join())
        .await
        .ok();
}

struct SocketFileGuard(Arc<PathBuf>);

impl Drop for SocketFileGuard {
    fn drop(&mut self) {
        std::fs::remove_file(&*self.0).ok();
    }
}

async fn handle_client(
    stream: UnixStream,
    cmd_tx: UnboundedSender<UiCommand>,
    updates_rx: &mut tokio::sync::broadcast::Receiver<EngineUpdate>,
) -> Result<()> {
    let (read, write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut writer = write;

    // Spawn a task to pump updates to this client.
    let mut updates_rx_local = updates_rx.resubscribe();
    let writer_pump = tokio::spawn(async move {
        loop {
            match updates_rx_local.recv().await {
                Ok(update) => {
                    let line = match serde_json::to_vec(&update) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    if writer.write_all(&line).await.is_err()
                        || writer.write_all(b"\n").await.is_err()
                        || writer.flush().await.is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Read commands from this client.
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<UiCommand>(trimmed) {
                    Ok(cmd) => {
                        let is_shutdown = matches!(cmd, UiCommand::Shutdown);
                        let _ = cmd_tx.send(cmd);
                        if is_shutdown {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("invalid command: {e}");
                    }
                }
            }
            Err(e) => {
                warn!("read failed: {e}");
                break;
            }
        }
    }

    writer_pump.abort();
    Ok(())
}

// ---------------------------------------------------------------------------
// Engine orchestration (adapted from crates/app/src/engine.rs)
// ---------------------------------------------------------------------------

enum EngineExit {
    Quit(String),
    /// The UI asked to sign out: the engine is stopped; the caller logs out
    /// and loops back to the sign-in flow.
    SignOut,
    /// The UI changed the persisted sync mode: the engine is stopped; the
    /// caller restarts the sidecar and engine into the new mode.
    ModeSwitch(SyncMode),
}

async fn run_engine(
    root: PathBuf,
    sidecar_cmd: Option<String>,
    updates: tokio::sync::broadcast::Sender<EngineUpdate>,
    mut cmds: UnboundedReceiver<UiCommand>,
) -> Result<()> {
    // Session loop: sign-out drops the sidecar (its notification stream is
    // consumed by the engine and can't be reclaimed) and restarts into the
    // login flow so a different account can sign in. A mode switch restarts
    // into the new persisted mode while staying signed in.
    loop {
        let (mut client, child) = spawn_sidecar(sidecar_cmd.as_deref())?;
        let mut notifications = client.take_notifications();
        let client = Arc::new(client);

        let auth = client.auth_status().await.context("auth.status")?;
        let email = if auth.logged_in {
            let email = auth.email.unwrap_or_else(|| "unknown account".into());
            let _ = updates.send(EngineUpdate::AuthOk {
                email: email.clone(),
            });
            email
        } else {
            let _ = updates.send(EngineUpdate::AuthRequired);
            match login_flow(client.as_ref(), &mut notifications, &mut cmds, &updates).await? {
                Some(email) => email,
                None => {
                    let _ = updates.send(EngineUpdate::Exited {
                        reason: "shutdown".into(),
                    });
                    return Ok(());
                }
            }
        };
        info!(email = %email, root = %root.display(), "starting sync engine");

        match engine_session(
            client.clone(),
            root.clone(),
            notifications,
            &mut cmds,
            &updates,
        )
        .await
        {
            EngineExit::Quit(reason) => {
                let _ = updates.send(EngineUpdate::Exited { reason });
                return Ok(());
            }
            EngineExit::SignOut => {
                // The engine is already stopped (graceful shutdown above).
                // `auth.logout` deletes the session server-side and wipes
                // session.json; a 1001 "already gone" is success enough.
                match client.logout().await {
                    Ok(()) => info!("signed out"),
                    Err(e) if e.code() == Some(error_codes::AUTH_REQUIRED) => {
                        info!("auth.logout: session already gone (1001), proceeding");
                    }
                    Err(e) => {
                        warn!(error = %e, "auth.logout failed; proceeding to signed-out state");
                    }
                }
                drop(child); // kill_on_drop: stop the old sidecar process
                let _ = updates.send(EngineUpdate::SignedOut);
            }
            EngineExit::ModeSwitch(mode) => {
                drop(child); // restart the sidecar to get a fresh notification stream
                let _ = updates.send(EngineUpdate::ModeSwitched {
                    mode: mode.as_meta_value().to_string(),
                });
            }
        }
    }
}

async fn engine_session(
    client: Arc<pds_sidecar_client::SidecarClient>,
    root: PathBuf,
    notifications: tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &tokio::sync::broadcast::Sender<EngineUpdate>,
) -> EngineExit {
    let state_dir = engine_state_dir(&root);
    let legacy_db = root.join(pds_core::STATE_DIR).join("state.db");
    let mode = if let Some(env) = SyncMode::from_env() {
        env
    } else if state_dir.join("state.db").is_file() {
        read_persisted_mode(&state_dir).unwrap_or_default()
    } else if legacy_db.is_file() {
        SyncMode::Mirror
    } else {
        SyncMode::default()
    };

    if mode == SyncMode::Placeholders
        && let Err(e) =
            pds_vfs_linux::mount::ensure_placeholder_mountpoint(&root, Duration::from_secs(2))
    {
        warn!(root = %root.display(), error = %e, "refusing to start with an unusable placeholder mountpoint");
        let _ = updates.send(EngineUpdate::MountUnavailable {
            error: e.to_string(),
        });
        return EngineExit::Quit(e.to_string());
    }

    let layout = match prepare_layout(&root) {
        Ok(l) => l,
        Err(e) => return EngineExit::Quit(format!("{e:#}")),
    };
    if layout.adopted_legacy_db {
        let _ = updates.send(EngineUpdate::LayoutMigrated);
    }

    let db_path = layout.state_dir.join("state.db");

    let want_placeholders = mode == SyncMode::Placeholders;
    let placeholders = if want_placeholders && !fuse_available() {
        warn!("FUSE unavailable; running mirror mode for this session");
        let _ = updates.send(EngineUpdate::MountUnavailable {
            error: "FUSE is not available on this system".into(),
        });
        false
    } else {
        want_placeholders
    };

    let (engine, handle) = match build_engine(
        client,
        notifications,
        &layout,
        db_path.clone(),
        placeholders,
    ) {
        Ok(v) => v,
        Err(e) => return EngineExit::Quit(format!("{e:#}")),
    };
    let engine_task = tokio::spawn(engine.run());

    let mut mount = None;
    if placeholders {
        if !handle.wait_for_watcher(Duration::from_secs(600)).await {
            warn!("engine readiness timed out; mounting the placeholder view anyway");
        }
        match mount_placeholder_view(&layout, &handle) {
            Ok(session) => mount = Some(session),
            Err(e) => {
                let failure = pds_vfs_linux::mount::ensure_placeholder_mountpoint(
                    &layout.root,
                    Duration::from_secs(1),
                )
                .err()
                .map(|m| m.to_string())
                .unwrap_or_else(|| format!("{e:#}"));
                warn!(
                    root = %layout.root.display(),
                    error = %e,
                    "placeholder mount failed after the FUSE probe"
                );
                let _ = updates.send(EngineUpdate::MountUnavailable {
                    error: failure.clone(),
                });
                let _ = handle.shutdown().await;
                let _ = engine_task.await;
                return EngineExit::Quit(format!(
                    "{failure} Restart the engine after resolving the mount problem."
                ));
            }
        }
    }

    let _ = updates.send(EngineUpdate::EngineStarted);
    let exit = drive_engine_task(engine_task, handle, &db_path, cmds, updates).await;

    if let Some(session) = mount {
        info!(mount = %layout.root.display(), "unmounting placeholder view");
        drop(session);
        unmount_lazy(&layout.root);
    }
    exit
}

fn fuse_available() -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let probe = std::env::temp_dir().join(format!("pds-fuse-probe-{}-{seq}", std::process::id()));
    let mnt = probe.join("mnt");
    if std::fs::create_dir_all(&mnt).is_err() {
        return false;
    }
    let store = pds_core::Store::new();
    let fs = pds_vfs_linux::PdsFs::new(store, probe.join("state"), probe.join("backing"));
    let mut cfg = fuser::Config::default();
    cfg.mount_options = vec![fuser::MountOption::FSName("pds-probe".into())];
    let ok = match fuser::Session::new(fs, &mnt, &cfg) {
        Ok(session) => match session.spawn() {
            Ok(bg) => {
                let (tx, rx) = std::sync::mpsc::channel();
                let target = mnt.clone();
                std::thread::spawn(move || {
                    let _ = tx.send(std::fs::metadata(&target).is_ok());
                });
                let served = rx.recv_timeout(Duration::from_secs(5)).unwrap_or(false);
                drop(bg);
                let target = mnt.to_string_lossy().into_owned();
                let _ = std::process::Command::new("fusermount3")
                    .args(["-uz", &target])
                    .status();
                let _ = std::process::Command::new("fusermount")
                    .args(["-uz", &target])
                    .status();
                served
            }
            Err(_) => false,
        },
        Err(_) => false,
    };
    let _ = std::fs::remove_dir_all(&probe);
    ok
}

fn mount_placeholder_view(
    layout: &RootLayout,
    handle: &EngineHandle,
) -> Result<fuser::BackgroundSession> {
    let store = handle
        .store()
        .cloned()
        .context("engine has no VFS store (not in placeholder mode)")?;
    let proxy_dir = layout.state_dir.join("mount");
    std::fs::create_dir_all(&proxy_dir)?;
    let fs = pds_vfs_linux::PdsFs::new(store, proxy_dir, layout.backing_dir.clone());
    let mut cfg = fuser::Config::default();
    cfg.mount_options = vec![
        fuser::MountOption::FSName("proton-drive-sync".into()),
        fuser::MountOption::Subtype("pds".into()),
    ];
    let session = fuser::Session::new(fs, &layout.root, &cfg)?;
    let served = session.spawn()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let target = layout.root.clone();
    std::thread::spawn(move || {
        let _ = tx.send(std::fs::metadata(&target).is_ok());
    });
    if !rx.recv_timeout(Duration::from_secs(5)).unwrap_or(false) {
        anyhow::bail!(
            "the placeholder view mounted at {} but does not answer requests",
            layout.root.display()
        );
    }
    info!(mount = %layout.root.display(), "placeholder view mounted and serving");
    Ok(served)
}

struct RootLayout {
    root: PathBuf,
    state_dir: PathBuf,
    backing_dir: PathBuf,
    adopted_legacy_db: bool,
}

fn prepare_layout(root: &Path) -> Result<RootLayout> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("create sync root {}", root.display()))?;
    let root = root
        .canonicalize()
        .with_context(|| format!("sync root {}", root.display()))?;
    let state_dir = engine_state_dir(&root);
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("create engine state dir {}", state_dir.display()))?;
    let db_path = state_dir.join("state.db");
    let mut adopted_legacy_db = false;

    if !db_path.exists() {
        let legacy_db = root.join(pds_core::STATE_DIR).join("state.db");
        if legacy_db.is_file() {
            std::fs::copy(&legacy_db, &db_path).with_context(|| {
                format!(
                    "adopting state DB from {} to {}",
                    legacy_db.display(),
                    db_path.display()
                )
            })?;
            for suffix in ["-wal", "-shm"] {
                std::fs::remove_file(format!("{}{suffix}", legacy_db.display())).ok();
            }
            let verified = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .and_then(|c| {
                c.query_row("SELECT count(*) FROM sqlite_master", [], |r| {
                    r.get::<_, i64>(0)
                })
            })
            .map(|n| n >= 0)
            .unwrap_or(false);
            if !verified {
                std::fs::remove_file(&db_path).ok();
                anyhow::bail!(
                    "adopted state DB at {} did not verify; keeping the original at {}",
                    db_path.display(),
                    legacy_db.display()
                );
            }
            std::fs::remove_file(&legacy_db).with_context(|| {
                format!("removing adopted legacy state DB {}", legacy_db.display())
            })?;
            adopted_legacy_db = true;
            info!(
                from = %legacy_db.display(),
                to = %db_path.display(),
                "adopted legacy in-root state DB"
            );
        }
    }
    std::fs::write(state_dir.join("root"), root.to_string_lossy().as_bytes())?;

    Ok(RootLayout {
        root,
        backing_dir: state_dir.join("backing"),
        state_dir,
        adopted_legacy_db,
    })
}

fn read_persisted_mode(state_dir: &Path) -> Option<SyncMode> {
    let conn = rusqlite::Connection::open_with_flags(
        state_dir.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    let value: String = conn
        .query_row(
            &format!(
                "SELECT value FROM meta WHERE key = '{}'",
                pds_core::META_SYNC_MODE
            ),
            [],
            |row| row.get(0),
        )
        .ok()?;
    SyncMode::from_meta_value(Some(&value))
}

fn build_engine(
    client: Arc<pds_sidecar_client::SidecarClient>,
    notifications: tokio::sync::mpsc::Receiver<Notification>,
    layout: &RootLayout,
    db_path: PathBuf,
    placeholders: bool,
) -> Result<(Engine<pds_sidecar_client::SidecarClient>, EngineHandle)> {
    let eviction_cap = match pds_core::cache_cap_from_env() {
        Some(session) => session,
        None => pds_core::persisted_cache_cap(&db_path).unwrap_or(Some(EVICTION_CAP_DEFAULT)),
    };
    Engine::new(
        client,
        notifications,
        EngineConfig {
            db_path,
            root: layout.root.clone(),
            retry_base: RETRY_BASE_DEFAULT,
            placeholders,
            eviction_cap,
            backing_dir: Some(layout.backing_dir.clone()),
            watch_local_writes: !placeholders,
        },
    )
}

fn unmount_lazy(root: &Path) {
    let target = root.to_string_lossy().into_owned();
    let ok = std::process::Command::new("fusermount3")
        .args(["-uz", &target])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
        || std::process::Command::new("fusermount")
            .args(["-uz", &target])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
    if !ok {
        warn!(root = %root.display(), "lazy unmount failed (will disappear at process exit)");
    }
}

async fn drive_engine_task(
    mut engine_task: tokio::task::JoinHandle<Result<()>>,
    handle: EngineHandle,
    db_path: &Path,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &tokio::sync::broadcast::Sender<EngineUpdate>,
) -> EngineExit {
    let mut activity_rx = handle.subscribe_activity();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut db = None;

    loop {
        tokio::select! {
            cmd = cmds.recv() => match cmd {
                Some(UiCommand::Pause) => {
                    if let Err(e) = handle.pause().await {
                        warn!("pause: {e}");
                    }
                }
                Some(UiCommand::Resume) => {
                    if let Err(e) = handle.resume().await {
                        warn!("resume: {e}");
                    }
                }
                Some(UiCommand::Shutdown) | None => {
                    let _ = handle.shutdown().await;
                    let res = engine_task.await;
                    return EngineExit::Quit(engine_result(res));
                }
                Some(UiCommand::SignOut) => {
                    let _ = handle.shutdown().await;
                    let res = engine_task.await;
                    info!(reason = %engine_result(res), "engine stopped for sign-out");
                    return EngineExit::SignOut;
                }
                Some(UiCommand::SetMode(raw)) => {
                    // Persist the new mode so the next session picks it up,
                    // then stop the engine. The caller will respawn the sidecar
                    // and start a fresh engine session in the new mode.
                    let Some(mode) = SyncMode::from_meta_value(Some(&raw)) else {
                        warn!(mode = %raw, "ignoring invalid SetMode value");
                        continue;
                    };
                    if db.is_none() {
                        db = Db::open(db_path).ok();
                    }
                    if let Some(conn) = db.as_ref() {
                        if let Err(e) = pds_core::set_sync_mode(conn, mode) {
                            warn!(error = %e, "persisting sync mode failed");
                        }
                    }
                    let _ = handle.shutdown().await;
                    let res = engine_task.await;
                    info!(reason = %engine_result(res), "engine stopped for mode switch");
                    return EngineExit::ModeSwitch(mode);
                }
                Some(UiCommand::SetCacheCap(cap)) => {
                    if let Err(e) = handle.set_cache_cap(cap).await {
                        warn!("set cache cap: {e}");
                    }
                }
                Some(UiCommand::StartLogin) => {}
            },
            item = activity_rx.recv() => match item {
                Ok(item) => {
                    let _ = updates.send(EngineUpdate::Activity((&item).into()));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "activity feed lagged; entries dropped");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return EngineExit::Quit("activity channel closed".into());
                }
            },
            _ = tick.tick() => {
                if let Ok(st) = handle.status().await {
                    let _ = updates.send(EngineUpdate::Status((&st).into()));
                }
                poll_inbox(&mut db, db_path, updates);
            }
            res = &mut engine_task => {
                return EngineExit::Quit(engine_result(res));
            }
        }
    }
}

fn engine_result(res: Result<Result<()>, tokio::task::JoinError>) -> String {
    match res {
        Ok(Ok(())) => "stopped".into(),
        Ok(Err(e)) => format!("{e:#}"),
        Err(e) => format!("engine task join: {e}"),
    }
}

fn poll_inbox(
    db: &mut Option<Db>,
    db_path: &Path,
    updates: &tokio::sync::broadcast::Sender<EngineUpdate>,
) {
    if db.is_none() {
        *db = Db::open(db_path).ok();
    }
    let Some(conn) = db.as_ref() else {
        return;
    };
    let conflicts = match conn.remote_conflicted_paths() {
        Ok(c) => c,
        Err(_) => {
            *db = None;
            return;
        }
    };
    let errored: Vec<_> = conn
        .errored_paths()
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            let error = conn.upload_error(&p).ok().flatten().unwrap_or_default();
            ErroredItemJson { path: p, error }
        })
        .collect();
    let _ = updates.send(EngineUpdate::Inbox { conflicts, errored });
}

async fn login_flow(
    client: &impl SidecarApi,
    notifications: &mut tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &tokio::sync::broadcast::Sender<EngineUpdate>,
) -> Result<Option<String>> {
    loop {
        match cmds.recv().await {
            Some(UiCommand::StartLogin) => {
                let _ = updates.send(EngineUpdate::LoginStarted);
                match await_login(client, notifications, cmds, updates).await {
                    Ok(Some(email)) => return Ok(Some(email)),
                    Ok(None) => return Ok(None),
                    Err(e) => {
                        let _ = updates.send(EngineUpdate::LoginFailed {
                            error: format!("{e:#}"),
                        });
                    }
                }
            }
            Some(UiCommand::Shutdown) | None => return Ok(None),
            _ => {}
        }
    }
}

async fn await_login(
    client: &impl SidecarApi,
    notifications: &mut tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &tokio::sync::broadcast::Sender<EngineUpdate>,
) -> Result<Option<String>> {
    let login = client.login_fork();
    tokio::pin!(login);
    loop {
        tokio::select! {
            res = &mut login => return Ok(Some(res.map_err(|e| anyhow::anyhow!("{e}"))?)),
            n = notifications.recv() => match n {
                Some(Notification::AuthSignInUrl { url, user_code }) => {
                    let _ = updates.send(EngineUpdate::LoginUrl { url, user_code });
                }
                Some(other) => {
                    info!(notification = ?other, "notification during login ignored");
                }
                None => return Err(anyhow::anyhow!("sidecar connection lost during sign-in")),
            },
            cmd = cmds.recv() => match cmd {
                Some(UiCommand::Shutdown) | None => return Ok(None),
                _ => {}
            },
        }
    }
}
