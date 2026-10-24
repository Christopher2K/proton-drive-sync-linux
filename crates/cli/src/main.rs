//! `pds` — headless proton-drive-sync CLI (M1).
//!
//! All Drive traffic goes through the spawned Node.js sidecar; `sync` runs
//! the engine against a local folder (full materialization in M1 — VFS
//! placeholders arrive in M3/M4, see SPEC.md).

mod control;
mod spawn;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pds_core::{Engine, EngineConfig, STATE_DIR};
use pds_sidecar_client::{Notification, SidecarApi};
use tracing::info;

/// Mandated third-party disclosure (SPEC.md §5 / AGENT.md hard rules).
const DISCLOSURE: &str = "This is a third-party application not officially supported by Proton.";

#[derive(Parser)]
#[command(
    name = "pds",
    version,
    about = "proton-drive-sync (unofficial) headless CLI"
)]
struct Cli {
    /// Sidecar command override, e.g. --sidecar-cmd "node sidecar/dist/index.js"
    /// (default: $PDS_SIDECAR_CMD, else `npx tsx sidecar/src/index.ts`).
    #[arg(long, global = true)]
    sidecar_cmd: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Browser session-fork login (prints the sign-in URL).
    Login,
    /// Log out and wipe the persisted session.
    Logout,
    /// Show auth + engine status.
    Status {
        /// Sync root to query (default: auto-detect when exactly one engine runs).
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Sync a local folder with Proton Drive (long-running). The folder is
    /// created if it does not exist.
    Sync {
        /// Local sync root folder.
        folder: PathBuf,
    },
    /// Pause the running sync (queues frozen, changes buffered).
    Pause {
        /// Sync root to pause (default: auto-detect when exactly one engine runs).
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Resume a paused (or guard-paused) sync.
    Resume {
        /// Sync root to resume (default: auto-detect when exactly one engine runs).
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pds_core=debug,pds_sidecar_client=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Login => login(cli.sidecar_cmd.as_deref()).await,
        Cmd::Logout => logout(cli.sidecar_cmd.as_deref()).await,
        Cmd::Status { root } => status(root, cli.sidecar_cmd.as_deref()).await,
        Cmd::Sync { folder } => sync(folder, cli.sidecar_cmd.as_deref()).await,
        Cmd::Pause { root } => control::send_simple("pause", root.as_deref()).await,
        Cmd::Resume { root } => control::send_simple("resume", root.as_deref()).await,
    }
}

async fn login(sidecar_cmd: Option<&str>) -> Result<()> {
    println!("{DISCLOSURE}");
    let (mut client, _child) = spawn::spawn_sidecar(sidecar_cmd)?;
    let mut notifications = client.take_notifications();

    // Print the sign-in URL as soon as the sidecar sends it.
    let printer = tokio::spawn(async move {
        while let Some(n) = notifications.recv().await {
            if let Notification::AuthSignInUrl { url, user_code } = n {
                println!("\nOpen this URL in your browser to sign in:\n\n  {url}\n");
                if !user_code.is_empty() {
                    println!("Verification code: {user_code}\n");
                }
            }
        }
    });

    match client.login_fork().await {
        Ok(email) => {
            println!("Signed in as {email}");
            printer.abort();
            Ok(())
        }
        Err(e) => {
            printer.abort();
            bail!("login failed: {e}")
        }
    }
}

async fn logout(sidecar_cmd: Option<&str>) -> Result<()> {
    let (client, _child) = spawn::spawn_sidecar(sidecar_cmd)?;
    client.logout().await.context("auth.logout")?;
    println!("Logged out.");
    Ok(())
}

async fn status(root: Option<PathBuf>, sidecar_cmd: Option<&str>) -> Result<()> {
    // Engine status first (if a sync is running).
    match control::query_status(root.as_deref()).await {
        Ok(line) => println!("engine: {line}"),
        Err(e) => println!("engine: not running ({e:#})"),
    }
    let (client, _child) = spawn::spawn_sidecar(sidecar_cmd)?;
    let st = client.auth_status().await.context("auth.status")?;
    match st.email {
        Some(email) if st.logged_in => println!("auth: logged in as {email}"),
        _ => println!("auth: not logged in"),
    }
    Ok(())
}

async fn sync(folder: PathBuf, sidecar_cmd: Option<&str>) -> Result<()> {
    if !folder.exists() {
        std::fs::create_dir_all(&folder)
            .with_context(|| format!("create sync folder {}", folder.display()))?;
        info!(folder = %folder.display(), "created sync folder");
    }
    let root = folder.canonicalize().context("sync folder")?;
    if !root.is_dir() {
        bail!("{} is not a directory", root.display());
    }

    let (mut client, _child) = spawn::spawn_sidecar(sidecar_cmd)?;
    let st = client.auth_status().await.context("auth.status")?;
    if !st.logged_in {
        bail!("not logged in — run `pds login` first");
    }
    info!(root = %root.display(), email = ?st.email, "starting sync");

    let notifications = client.take_notifications();
    let (engine, handle) = Engine::new(
        Arc::new(client),
        notifications,
        EngineConfig {
            db_path: root.join(STATE_DIR).join("state.db"),
            root: root.clone(),
        },
    )?;

    // Control socket for `pds pause|resume|status`.
    let _control = control::serve(&root, handle.clone()).await?;
    control::register_root(&root)?;

    let engine_task = tokio::spawn(engine.run());

    tokio::select! {
        res = engine_task => {
            control::deregister_root(&root);
            res.context("engine task")?
        }
        sig = tokio::signal::ctrl_c() => {
            sig.context("ctrl-c")?;
            info!("shutting down");
            handle.shutdown().await.ok();
            control::deregister_root(&root);
            Ok(())
        }
    }
}
