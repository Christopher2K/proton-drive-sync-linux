//! The tokio side of the desktop app.
//!
//! A single dedicated thread owns the Node sidecar and the sync engine. The
//! GPUI thread never touches tokio types: commands go out through an mpsc
//! channel and progress comes back as [`AppUpdate`] values. This keeps the
//! UI responsive even while a large transfer or a key-unlock operation is in
//! flight.
//!
//! Sync mode (F5): new roots default to placeholder mode and the app mounts
//! the FUSE placeholder view AT the sync root — `~/ProtonDrive` shows the
//! whole Drive with ~0-byte placeholder files that hydrate on open and an
//! LRU-capped local cache. The engine's state (SQLite DB, VFS backing
//! cache, control socket) lives under
//! `$XDG_DATA_HOME/proton-drive-sync/roots/<hash>/`, OUTSIDE the root: a
//! directory cannot serve both its real contents and a FUSE view mounted
//! over it, and running the DB through the mount would risk corruption.
//! `PDS_SYNC_MODE=mirror` (or the Settings toggle) selects the classic
//! fully materialized folder instead.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use pds_core::{
    Db, Engine, EngineConfig, EngineHandle, EVICTION_CAP_DEFAULT, RETRY_BASE_DEFAULT, SyncMode,
    engine_state_dir,
};
use pds_sidecar_client::types::error_codes;
use pds_sidecar_client::{Notification, SidecarApi, spawn::spawn_sidecar};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

use crate::state::AppUpdate;

/// Commands the UI sends to the background engine thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiCommand {
    /// Start the browser fork-login flow (only valid while signed out).
    StartLogin,
    Pause,
    Resume,
    /// Confirmed sign-out: stop the engine, call `auth.logout`, and return to
    /// the sign-in screen.
    SignOut,
    Shutdown,
    /// Settings changed the backing-cache cap; applied live by the engine.
    SetCacheCap(Option<u64>),
}

/// Cloneable handle to the background thread.
#[derive(Clone)]
pub struct EngineService {
    tx: UnboundedSender<UiCommand>,
}

impl EngineService {
    pub fn send(&self, cmd: UiCommand) {
        let _ = self.tx.send(cmd);
    }
}

/// Start the sidecar + engine thread. Returns immediately; all progress is
/// reported through `updates`.
pub fn start(
    root: PathBuf,
    sidecar_cmd: Option<String>,
    updates: UnboundedSender<AppUpdate>,
) -> EngineService {
    let (tx, cmds) = tokio::sync::mpsc::unbounded_channel();

    std::thread::Builder::new()
        .name("pds-engine".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = updates.send(AppUpdate::Failed {
                        error: format!("could not start the async runtime: {e}"),
                    });
                    return;
                }
            };

            rt.block_on(async move {
                if let Err(e) = run(root, sidecar_cmd, &updates, cmds).await {
                    let _ = updates.send(AppUpdate::Failed {
                        error: format!("{e:#}"),
                    });
                }
            });
        })
        .expect("spawn engine thread");

    EngineService { tx }
}

/// Why an engine session ended.
enum EngineExit {
    /// App is quitting, or the engine task ended on its own.
    Quit(String),
    /// `UiCommand::SignOut`: engine already stopped; the caller logs out and
    /// loops back to the sign-in flow.
    SignOut,
}

async fn run(
    root: PathBuf,
    sidecar_cmd: Option<String>,
    updates: &UnboundedSender<AppUpdate>,
    mut cmds: UnboundedReceiver<UiCommand>,
) -> Result<()> {
    // Session loop: sign-out drops the sidecar (its notification stream is
    // consumed by the engine and can't be reclaimed) and restarts into the
    // login flow so a different account can sign in.
    loop {
        let (mut client, child) = spawn_sidecar(sidecar_cmd.as_deref())?;
        let mut notifications = client.take_notifications();
        let client = Arc::new(client);

        let auth = client.auth_status().await.context("auth.status")?;
        let email = if auth.logged_in {
            let email = auth.email.unwrap_or_else(|| "unknown account".into());
            let _ = updates.send(AppUpdate::AuthOk {
                email: email.clone(),
            });
            email
        } else {
            let _ = updates.send(AppUpdate::AuthRequired);
            match login_flow(client.as_ref(), &mut notifications, &mut cmds, updates).await? {
                Some(email) => email,
                None => {
                    let _ = updates.send(AppUpdate::Exited {
                        reason: "shutdown".into(),
                    });
                    return Ok(());
                }
            }
        };
        info!(email = %email, root = %root.display(), "starting sync engine");

        // The single notification stream (taken once at spawn): the login
        // flow borrowed it; ownership moves into the engine session now.
        match engine_session(
            client.clone(),
            root.clone(),
            notifications,
            &mut cmds,
            updates,
        )
        .await
        {
            EngineExit::Quit(reason) => {
                let _ = updates.send(AppUpdate::Exited { reason });
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
                        warn!("auth.logout failed ({e}); proceeding to signed-out state")
                    }
                }
                drop(child); // kill_on_drop: stop the old sidecar process
                let _ = updates.send(AppUpdate::SignedOut);
            }
        }
    }
}

/// One signed-in engine session: resolve layout + mode, mount the FUSE
/// placeholder view (when applicable), run the engine, tear down in
/// reverse order. A mode switch from Settings lands here on the next
/// sign-in-less restart: the UI restarts the whole service, which re-enters
/// this function and picks up the new persisted mode.
async fn engine_session(
    client: Arc<pds_sidecar_client::SidecarClient>,
    root: PathBuf,
    notifications: tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
) -> EngineExit {
    // Mode for THIS session, from $PDS_SYNC_MODE > persisted `sync_mode` >
    // defaults. A legacy in-root state DB (pre-0.3 mirror user) defaults to
    // mirror so an upgrade changes nothing for them; brand-new roots get
    // placeholders (F5). The engine itself re-resolves the persisted value
    // authoritatively; this pre-read only has to agree on the layout.
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

    let layout = match prepare_layout(&root) {
        Ok(l) => l,
        Err(e) => return EngineExit::Quit(format!("{e:#}")),
    };
    if layout.adopted_legacy_db {
        let _ = updates.send(AppUpdate::LayoutMigrated);
    }

    let db_path = layout.state_dir.join("state.db");

    // Decide placeholder vs mirror BEFORE building the engine: the engine
    // consumes the sidecar's single notification stream, so a later
    // fallback rebuild would be deaf to remote events. Probe FUSE with a
    // throwaway mount; a failure downgrades THIS SESSION to mirror (the
    // persisted mode stays placeholders — next start retries the mount).
    let want_placeholders = mode == SyncMode::Placeholders;
    let placeholders = if want_placeholders && !fuse_available() {
        warn!("FUSE unavailable; running mirror mode for this session");
        let _ = updates.send(AppUpdate::MountUnavailable {
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
        // Mount only AFTER the engine's initial sync: its first-run local
        // scan and the layout conversion must see the REAL folder (the
        // mount shadows the root and serves an empty tree until the engine
        // publishes a snapshot, which would hide every pre-existing local
        // file from the scan). The watcher-ready flag flips right after
        // the initial sync — exactly when the mount can go up.
        if !handle.wait_for_watcher(Duration::from_secs(600)).await {
            warn!("engine readiness timed out; mounting the placeholder view anyway");
        }
        match mount_placeholder_view(&layout, &handle) {
            Ok(session) => mount = Some(session),
            Err(e) => {
                warn!(
                    root = %layout.root.display(),
                    error = %e,
                    "placeholder mount failed after the FUSE probe"
                );
                let _ = updates.send(AppUpdate::MountUnavailable {
                    error: format!("{e:#}"),
                });
                // No safe fallback: a mirror rebuild would never receive
                // remote events (the notification stream is spent), and a
                // placeholder engine without its mount sees no local
                // writes. Stop and let the user retry (Restart button).
                let _ = handle.shutdown().await;
                let _ = engine_task.await;
                return EngineExit::Quit(
                    "The placeholder view could not be mounted even though FUSE is available; \
                     restart the app to retry"
                        .into(),
                );
            }
        }
    }

    let _ = updates.send(AppUpdate::EngineStarted);
    let exit = drive_engine_task(engine_task, handle, &db_path, cmds, updates).await;

    if let Some(session) = mount {
        info!(mount = %layout.root.display(), "unmounting placeholder view");
        drop(session);
        unmount_lazy(&layout.root);
    }
    exit
}

/// Cheap FUSE liveness probe: mount a throwaway placeholder filesystem at
/// a temp dir, then unmount. Deciding mountability up front (rather than
/// after the engine owns the notification stream) keeps the mirror
/// fallback able to receive remote events.
fn fuse_available() -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let probe = std::env::temp_dir().join(format!(
        "pds-fuse-probe-{}-{seq}",
        std::process::id()
    ));
    let mnt = probe.join("mnt");
    if std::fs::create_dir_all(&mnt).is_err() {
        return false;
    }
    let store = pds_core::Store::new();
    let fs = pds_vfs_linux::PdsFs::new(
        store,
        probe.join("state"),
        probe.join("backing"),
    );
    let mut cfg = fuser::Config::default();
    cfg.mount_options = vec![fuser::MountOption::FSName("pds-probe".into())];
    let ok = match fuser::Session::new(fs, &mnt, &cfg) {
        Ok(session) => {
            drop(session);
            let target = mnt.to_string_lossy().into_owned();
            let _ = std::process::Command::new("fusermount3")
                .args(["-uz", &target])
                .status();
            let _ = std::process::Command::new("fusermount")
                .args(["-uz", &target])
                .status();
            true
        }
        Err(_) => false,
    };
    let _ = std::fs::remove_dir_all(&probe);
    ok
}

/// Mount the FUSE placeholder view at the sync root.
fn mount_placeholder_view(
    layout: &RootLayout,
    handle: &EngineHandle,
) -> Result<fuser::Session<pds_vfs_linux::PdsFs>> {
    let store = handle
        .store()
        .cloned()
        .context("engine has no VFS store (not in placeholder mode)")?;
    // The fs reserves `.pds` as a proxy for its `state_dir` (a CLI-mount
    // feature so the control socket stays reachable). The app's real state
    // dir holds the LIVE SQLite DB and the backing content cache — serving
    // it inside the user-facing mount would let a stray `rm -rf .pds` (or a
    // backup tool) destroy sync state behind the engine's back. Hand the
    // mount an empty dir instead: `.pds` shows up as an empty folder.
    let proxy_dir = layout.state_dir.join("mount");
    std::fs::create_dir_all(&proxy_dir)?;
    let fs = pds_vfs_linux::PdsFs::new(store, proxy_dir, layout.backing_dir.clone());
    let mut cfg = fuser::Config::default();
    cfg.mount_options = vec![
        fuser::MountOption::FSName("proton-drive-sync".into()),
        fuser::MountOption::Subtype("pds".into()),
    ];
    Ok(fuser::Session::new(fs, &layout.root, &cfg)?)
}

/// Sync-root layout: where the engine state and (in placeholder mode) the
/// backing cache live. `prepare_layout` migrates legacy state out of the
/// root; it never deletes anything.
struct RootLayout {
    /// The canonical root path (the FUSE mountpoint in placeholder mode).
    root: PathBuf,
    /// Engine state dir: state.db, the `backing/` VFS cache, root marker.
    state_dir: PathBuf,
    /// VFS backing cache in placeholder mode (outside the root).
    backing_dir: PathBuf,
    /// True when the state DB was just adopted from the legacy in-root
    /// location (`<root>/.pds/state.db`).
    adopted_legacy_db: bool,
}

/// Resolve and prepare the local layout for `root`. Creates the state dir,
/// adopts a legacy in-root state DB when present (verified copy, then the
/// original is removed), and writes the root marker. Never deletes content.
fn prepare_layout(root: &Path) -> Result<RootLayout> {
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
            // The legacy WAL/SHM sidecars belong to the old DB; the adopted
            // copy was checkpointed into the main file at close.
            for suffix in ["-wal", "-shm"] {
                std::fs::remove_file(format!("{}{suffix}", legacy_db.display())).ok();
            }
            // Only drop the original once the copy provably opens as the
            // same database — a lingering stale copy could be mistaken for
            // live state by older/future code paths.
            let verified = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .and_then(|c| c.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get::<_, i64>(0)))
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

/// Read the persisted sync mode straight from the state DB (read-only — the
/// engine owns the writable handle). `None` → caller default.
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

/// Construct the engine with the layout resolved above. `placeholders` is
/// this session's intent; the engine re-reads the persisted mode and keeps
/// it when they disagree (the persisted value always wins).
fn build_engine(
    client: Arc<pds_sidecar_client::SidecarClient>,
    notifications: tokio::sync::mpsc::Receiver<Notification>,
    layout: &RootLayout,
    db_path: PathBuf,
    placeholders: bool,
) -> Result<(Engine<pds_sidecar_client::SidecarClient>, EngineHandle)> {
    // Cache cap: $PDS_CACHE_CAP (session-only) > the persisted Settings
    // value > the 10 GiB default. Applied live later via SetCacheCap when
    // the user changes it in Settings (no restart needed).
    let eviction_cap = match pds_core::cache_cap_from_env() {
        Some(session) => session,
        None => pds_core::persisted_cache_cap(&db_path)
            .unwrap_or(Some(EVICTION_CAP_DEFAULT)),
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
            // Placeholder sessions mount FUSE AT the root: writes arrive
            // through the VFS store queues, not inotify (watching the
            // mount would double-upload write-through files). Mirror
            // sessions have a plain folder and need the watcher (F2).
            watch_local_writes: !placeholders,
        },
    )
}

/// Best-effort lazy unmount (`fusermount -uz`) after dropping the session.
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

/// Run one engine session until a UI command or engine exit ends it. The
/// engine task is already spawned (the caller may have waited on readiness
/// or remounted around it).
async fn drive_engine_task(
    mut engine_task: tokio::task::JoinHandle<Result<()>>,
    handle: EngineHandle,
    db_path: &Path,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
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
                    // Graceful engine stop first; logout happens in `run`.
                    let _ = handle.shutdown().await;
                    let res = engine_task.await;
                    info!(reason = %engine_result(res), "engine stopped for sign-out");
                    return EngineExit::SignOut;
                }
                Some(UiCommand::StartLogin) => {} // already signed in
                Some(UiCommand::SetCacheCap(cap)) => {
                    if let Err(e) = handle.set_cache_cap(cap).await {
                        warn!("set cache cap: {e}");
                    }
                }
            },
            item = activity_rx.recv() => match item {
                Ok(item) => {
                    let _ = updates.send(AppUpdate::Activity(item));
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
                    let _ = updates.send(AppUpdate::Status(st));
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

/// Scan the state DB for the conflicts/errors inbox.
fn poll_inbox(db: &mut Option<Db>, db_path: &Path, updates: &UnboundedSender<AppUpdate>) {
    if db.is_none() {
        *db = Db::open(db_path).ok();
    }
    let Some(conn) = db.as_ref() else {
        return;
    };
    let conflicts = match conn.remote_conflicted_paths() {
        Ok(c) => c,
        Err(_) => {
            *db = None; // reopen next tick
            return;
        }
    };
    let errored = conn
        .errored_paths()
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            let err = conn.upload_error(&p).ok().flatten().unwrap_or_default();
            (p, err)
        })
        .collect();
    let _ = updates.send(AppUpdate::Inbox { conflicts, errored });
}

/// Drive `auth.loginFork` until it succeeds or the app shuts down.
/// `Ok(None)` means the app asked to quit before login completed.
async fn login_flow(
    client: &impl SidecarApi,
    notifications: &mut tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
) -> Result<Option<String>> {
    loop {
        match cmds.recv().await {
            Some(UiCommand::StartLogin) => {
                let _ = updates.send(AppUpdate::LoginStarted);
                match await_login(client, notifications, cmds, updates).await {
                    Ok(Some(email)) => return Ok(Some(email)),
                    Ok(None) => return Ok(None),
                    Err(e) => {
                        let _ = updates.send(AppUpdate::LoginFailed {
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
    updates: &UnboundedSender<AppUpdate>,
) -> Result<Option<String>> {
    let login = client.login_fork();
    tokio::pin!(login);
    loop {
        tokio::select! {
            res = &mut login => return Ok(Some(res.map_err(|e| anyhow::anyhow!("{e}"))?)),
            n = notifications.recv() => match n {
                Some(Notification::AuthSignInUrl { url, user_code }) => {
                    let _ = updates.send(AppUpdate::LoginUrl { url, user_code });
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
