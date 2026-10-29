//! The tokio side of the desktop app.
//!
//! A single dedicated thread owns the Node sidecar and the sync engine. The
//! GPUI thread never touches tokio types: commands go out through an mpsc
//! channel and progress comes back as [`AppUpdate`] values. This keeps the UI
//! responsive even while a large transfer or a key-unlock operation is in
//! flight.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use pds_core::{Db, Engine, EngineConfig, RETRY_BASE_DEFAULT, STATE_DIR};
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

        let db_path = root.join(STATE_DIR).join("state.db");
        let (engine, handle) = Engine::new(
            client.clone(),
            notifications,
            EngineConfig {
                db_path: db_path.clone(),
                root: root.clone(),
                retry_base: RETRY_BASE_DEFAULT,
            },
        )?;
        let _ = updates.send(AppUpdate::EngineStarted { email });

        match drive_engine(engine, handle, &db_path, &mut cmds, updates).await {
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

/// Run one engine session until a UI command or engine exit ends it.
async fn drive_engine(
    engine: Engine<pds_sidecar_client::SidecarClient>,
    handle: pds_core::EngineHandle,
    db_path: &Path,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
) -> EngineExit {
    let mut engine_task = tokio::spawn(engine.run());
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
