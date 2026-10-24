//! The tokio side of the shell: owns a dedicated runtime thread that spawns
//! the sidecar, drives login, runs the M1 sync engine, and streams
//! status/activity/inbox updates to the UI. The UI talks back through an
//! unbounded command channel — the [`EngineHandle`] never leaves this
//! thread, so the UI crate never touches tokio types.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use pds_core::{Db, Engine, EngineConfig, RETRY_BASE_DEFAULT, STATE_DIR};
use pds_sidecar_client::{Notification, SidecarApi, spawn::spawn_sidecar};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

use crate::model::{AppUpdate, UiCommand};

/// Start the engine thread. Returns immediately; progress arrives as
/// [`AppUpdate`]s. Dropping `cmds` (app quit) shuts the engine down.
pub fn start_engine_runtime(
    root: PathBuf,
    sidecar_cmd: Option<String>,
    updates: UnboundedSender<AppUpdate>,
    cmds: UnboundedReceiver<UiCommand>,
) {
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
                        error: format!("tokio runtime: {e}"),
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
}

async fn run(
    root: PathBuf,
    sidecar_cmd: Option<String>,
    updates: &UnboundedSender<AppUpdate>,
    mut cmds: UnboundedReceiver<UiCommand>,
) -> Result<()> {
    let (mut client, _child) = spawn_sidecar(sidecar_cmd.as_deref())?;
    let mut notifications = client.take_notifications();

    let auth = client.auth_status().await.context("auth.status")?;
    let email = if auth.logged_in {
        let email = auth.email.unwrap_or_else(|| "unknown account".into());
        let _ = updates.send(AppUpdate::AuthOk {
            email: email.clone(),
        });
        email
    } else {
        let _ = updates.send(AppUpdate::AuthRequired);
        login_flow(&client, &mut notifications, &mut cmds, updates).await?
    };
    info!(email = %email, root = %root.display(), "starting sync engine");

    let db_path = root.join(STATE_DIR).join("state.db");
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            db_path: db_path.clone(),
            root: root.clone(),
            retry_base: RETRY_BASE_DEFAULT,
        },
    )?;
    let _ = updates.send(AppUpdate::EngineStarted { email });

    let mut engine_task = tokio::spawn(engine.run());
    let mut activity_rx = handle.subscribe_activity();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut db = None;

    loop {
        tokio::select! {
            cmd = cmds.recv() => match cmd {
                Some(UiCommand::Pause) => {
                    let _ = handle.pause().await;
                }
                Some(UiCommand::Resume) => {
                    let _ = handle.resume().await;
                }
                Some(UiCommand::Shutdown) | None => {
                    let _ = handle.shutdown().await;
                    break;
                }
                Some(UiCommand::StartLogin) => {} // already logged in
            },
            item = activity_rx.recv() => match item {
                Ok(item) => {
                    let _ = updates.send(AppUpdate::Activity(item));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "activity feed lagged; entries dropped");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
            _ = tick.tick() => {
                if let Ok(st) = handle.status().await {
                    let _ = updates.send(AppUpdate::Status(st));
                }
                poll_inbox(&mut db, &db_path, updates);
            }
            res = &mut engine_task => {
                let _ = updates.send(AppUpdate::Exited {
                    reason: engine_result(res),
                });
                return Ok(());
            }
        }
    }

    // Requested shutdown: wait for the engine to finish draining, then tell
    // the UI it may quit.
    let res = engine_task.await;
    let _ = updates.send(AppUpdate::Exited {
        reason: engine_result(res),
    });
    Ok(())
}

fn engine_result(res: Result<Result<()>, tokio::task::JoinError>) -> String {
    match res {
        Ok(Ok(())) => "stopped".into(),
        Ok(Err(e)) => format!("{e:#}"),
        Err(e) => format!("engine task join: {e}"),
    }
}

/// Scan the state DB for the conflicts/errors inbox. The DB is opened once
/// and reopened on failure; a busy/locked tick just sends empty lists less
/// often — the next tick fills in.
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

/// Drive `auth.loginFork` until it succeeds or the app shuts down. The
/// `auth.signInUrl` notification is forwarded so the UI can display/open
/// it; other notifications can't meaningfully arrive before the engine
/// starts (no event scopes, no transfers) and are logged.
async fn login_flow(
    client: &impl SidecarApi,
    notifications: &mut tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
) -> Result<String> {
    loop {
        match cmds.recv().await {
            Some(UiCommand::StartLogin) => {
                let _ = updates.send(AppUpdate::LoginStarted);
                match await_login(client, notifications, cmds, updates).await {
                    Ok(email) => return Ok(email),
                    Err(e) => {
                        let _ = updates.send(AppUpdate::LoginFailed {
                            error: format!("{e:#}"),
                        });
                    }
                }
            }
            Some(UiCommand::Shutdown) | None => {
                return Err(anyhow!("shutdown before login completed"));
            }
            _ => {}
        }
    }
}

async fn await_login(
    client: &impl SidecarApi,
    notifications: &mut tokio::sync::mpsc::Receiver<Notification>,
    cmds: &mut UnboundedReceiver<UiCommand>,
    updates: &UnboundedSender<AppUpdate>,
) -> Result<String> {
    let login = client.login_fork();
    tokio::pin!(login);
    loop {
        tokio::select! {
            res = &mut login => return res.map_err(|e| anyhow!("{e}")),
            n = notifications.recv() => match n {
                Some(Notification::AuthSignInUrl { url, user_code }) => {
                    let _ = updates.send(AppUpdate::LoginUrl { url, user_code });
                }
                Some(other) => {
                    info!(notification = ?other, "notification during login ignored");
                }
                // The transport is shared: if the notification stream died,
                // the pending login call is dead too.
                None => return Err(anyhow!("sidecar connection lost during login")),
            },
            cmd = cmds.recv() => match cmd {
                Some(UiCommand::Shutdown) | None => {
                    return Err(anyhow!("shutdown during login"));
                }
                _ => {}
            },
        }
    }
}
