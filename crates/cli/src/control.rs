//! Local control channel for a running `pds sync`: a unix socket at
//! `<root>/.pds/control.sock` speaking one-line text commands, plus a
//! `current` marker in the platform data dir so `pds pause|resume|status`
//! can find the socket without a folder argument.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use pds_core::{EngineHandle, STATE_DIR};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Guard that removes the socket file on drop.
pub struct ControlServer {
    path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
        std::fs::remove_file(&self.path).ok();
    }
}

fn socket_path(root: &Path) -> PathBuf {
    root.join(STATE_DIR).join("control.sock")
}

fn data_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return PathBuf::from(xdg).join("proton-drive-sync");
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    #[cfg(target_os = "macos")]
    return PathBuf::from(home).join("Library/Application Support/proton-drive-sync");
    #[cfg(not(target_os = "macos"))]
    return PathBuf::from(home).join(".local/share/proton-drive-sync");
}

fn current_file() -> PathBuf {
    data_dir().join("current")
}

pub fn register_current_root(root: &Path) -> Result<()> {
    let f = current_file();
    if let Some(dir) = f.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&f, root.to_string_lossy().as_bytes())?;
    Ok(())
}

pub fn clear_current_root() {
    std::fs::remove_file(current_file()).ok();
}

fn current_root() -> Result<PathBuf> {
    let s = std::fs::read_to_string(current_file())
        .context("no running sync found (is `pds sync` running?)")?;
    Ok(PathBuf::from(s.trim()))
}

/// Start the control listener for a running engine.
pub async fn serve(root: &Path, handle: EngineHandle) -> Result<ControlServer> {
    let path = socket_path(root);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::remove_file(&path).ok(); // stale socket
    let listener = UnixListener::bind(&path).context("bind control socket")?;

    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let handle = handle.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_conn(stream, handle).await {
                            tracing::warn!("control connection failed: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("control accept failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(ControlServer { path, task })
}

async fn serve_conn(stream: UnixStream, handle: EngineHandle) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    let reply = match line.trim() {
        "pause" => handle.pause().await.map(|_| "ok: paused".to_string()),
        "resume" => handle.resume().await.map(|_| "ok: resumed".to_string()),
        "status" => handle.status().await.map(|s| {
            format!(
                "{} (synced={}, dirty={}, tombstones={})",
                s.state, s.synced_nodes, s.dirty_nodes, s.queued_tombstones
            )
        }),
        other => Ok(format!("error: unknown command `{other}`")),
    };
    let text = match reply {
        Ok(t) => t,
        Err(e) => format!("error: {e}"),
    };
    write.write_all(text.as_bytes()).await?;
    write.write_all(b"\n").await?;
    Ok(())
}

async fn send(cmd: &str) -> Result<String> {
    let root = current_root()?;
    let stream = UnixStream::connect(socket_path(&root))
        .await
        .context("connect control socket (is `pds sync` running?)")?;
    let (read, mut write) = stream.into_split();
    write.write_all(cmd.as_bytes()).await?;
    write.write_all(b"\n").await?;
    let mut reply = String::new();
    BufReader::new(read).read_line(&mut reply).await?;
    Ok(reply.trim().to_string())
}

/// `pds pause` / `pds resume`.
pub async fn send_simple(cmd: &str) -> Result<()> {
    let reply = send(cmd).await?;
    if reply.starts_with("error:") {
        bail!(reply);
    }
    println!("{reply}");
    Ok(())
}

/// Engine status line for `pds status`.
pub async fn query_status() -> Result<String> {
    send("status").await
}
