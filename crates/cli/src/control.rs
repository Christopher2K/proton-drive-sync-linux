//! Local control channel for a running `pds sync`: a unix socket at
//! `<root>/.pds/control.sock` speaking one-line text commands.
//!
//! Running engines register their root in `<data dir>/roots/`. `pds
//! status|pause|resume` resolve the target engine by probing the
//! registered control sockets for liveness (stale registrations and
//! socket files are cleaned up along the way). When several engines are
//! live, `--root <path>` picks one explicitly.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pds_core::{EngineHandle, STATE_DIR};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Max time to wait for a control socket to answer a `status` probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

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

/// Control socket for a user-supplied `--root` (canonicalize so `--root
/// .` resolves to the same socket the engine bound).
fn socket_for_root(root: &Path) -> PathBuf {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    socket_path(&root)
}

fn data_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return PathBuf::from(xdg).join("proton-drive-sync");
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".local/share/proton-drive-sync")
}

// --- root registry -----------------------------------------------------

fn registry_dir_in(data_dir: &Path) -> PathBuf {
    data_dir.join("roots")
}

/// Registry filename for a root: a hash, so arbitrary paths are safe.
fn registry_name(root: &Path) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    root.hash(&mut h);
    format!("{:016x}", h.finish())
}

fn register_root_in(data_dir: &Path, root: &Path) -> Result<()> {
    let dir = registry_dir_in(data_dir);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join(registry_name(root)),
        root.to_string_lossy().as_bytes(),
    )?;
    Ok(())
}

fn deregister_root_in(data_dir: &Path, root: &Path) {
    std::fs::remove_file(registry_dir_in(data_dir).join(registry_name(root))).ok();
}

/// Roots of currently registered engines (may include stale entries).
fn registry_roots_in(data_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(registry_dir_in(data_dir)) {
        for entry in entries.flatten() {
            if let Ok(s) = std::fs::read_to_string(entry.path()) {
                let root = PathBuf::from(s.trim());
                if !root.as_os_str().is_empty() {
                    out.push(root);
                }
            }
        }
    }
    out
}

/// Register this sync root so `pds status|pause|resume` can find it.
pub fn register_root(root: &Path) -> Result<()> {
    register_root_in(&data_dir(), root)
}

/// Remove this sync root from the registry (engine is shutting down).
pub fn deregister_root(root: &Path) {
    deregister_root_in(&data_dir(), root);
}

// --- discovery ---------------------------------------------------------

/// A live engine found while scanning the registry.
pub struct LiveEngine {
    pub root: PathBuf,
    pub status: String,
}

/// Probe a control socket: connect, ask for `status`, expect a reply.
async fn probe(socket: &Path) -> Option<String> {
    let fut = async {
        let stream = UnixStream::connect(socket).await.ok()?;
        let (read, mut write) = stream.into_split();
        write.write_all(b"status\n").await.ok()?;
        let mut reply = String::new();
        BufReader::new(read).read_line(&mut reply).await.ok()?;
        let reply = reply.trim().to_string();
        (!reply.is_empty() && !reply.starts_with("error:")).then_some(reply)
    };
    tokio::time::timeout(PROBE_TIMEOUT, fut)
        .await
        .ok()
        .flatten()
}

/// Scan `data_dir` for live engines, cleaning up stale registrations,
/// stale socket files, and the legacy `current` marker as we go.
async fn discover_live_in(data_dir: &Path) -> Vec<LiveEngine> {
    let mut candidates = registry_roots_in(data_dir);

    // Legacy single-root `current` marker (pre-fix versions): still honor
    // it when it points at a live engine, otherwise delete it.
    let legacy = data_dir.join("current");
    if let Ok(s) = std::fs::read_to_string(&legacy) {
        let root = PathBuf::from(s.trim());
        if !root.as_os_str().is_empty() && probe(&socket_path(&root)).await.is_some() {
            candidates.push(root);
        } else {
            std::fs::remove_file(&legacy).ok();
        }
    }

    let mut seen = HashSet::new();
    let mut live = Vec::new();
    for root in candidates {
        if !seen.insert(root.clone()) {
            continue;
        }
        let socket = socket_path(&root);
        match probe(&socket).await {
            Some(status) => live.push(LiveEngine { root, status }),
            None => {
                // Engine exited (or crashed): drop the stale registration
                // and any leftover socket file.
                deregister_root_in(data_dir, &root);
                std::fs::remove_file(&socket).ok();
            }
        }
    }
    live
}

/// Scan the platform data dir for live engines.
pub async fn discover_live() -> Vec<LiveEngine> {
    discover_live_in(&data_dir()).await
}

/// Resolve which control socket to talk to: the explicit `--root`, or
/// the single live engine found by scanning the registry.
async fn resolve_socket(root: Option<&Path>) -> Result<PathBuf> {
    if let Some(root) = root {
        return Ok(socket_for_root(root));
    }
    let live = discover_live().await;
    match live.len() {
        0 => bail!("no running sync engine found (is `pds sync` running?)"),
        1 => Ok(socket_for_root(&live[0].root)),
        _ => {
            let mut msg =
                String::from("multiple sync engines running; pick one with --root <path>:");
            for engine in &live {
                msg.push_str(&format!(
                    "\n  {} — {}",
                    engine.root.display(),
                    engine.status
                ));
            }
            bail!("{msg}")
        }
    }
}

// --- server ------------------------------------------------------------

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
    let trimmed = line.trim();
    let reply: Result<String> = if let Some(rel) = trimmed.strip_prefix("pin ") {
        pin_unpin(&handle, true, rel.trim()).await
    } else if let Some(rel) = trimmed.strip_prefix("unpin ") {
        pin_unpin(&handle, false, rel.trim()).await
    } else {
        match trimmed {
            "pause" => handle.pause().await.map(|_| "ok: paused".to_string()),
            "resume" => handle.resume().await.map(|_| "ok: resumed".to_string()),
            "status" => handle.status().await.map(|s| {
                format!(
                    "{} mode={} (synced={}, dirty={}, tombstones={}, errored={})",
                    s.state,
                    s.mode.as_meta_value(),
                    s.synced_nodes,
                    s.dirty_nodes,
                    s.queued_tombstones,
                    s.errored_nodes
                )
            }),
            other => Ok(format!("error: unknown command `{other}`")),
        }
    };
    let text = match reply {
        Ok(t) => t,
        Err(e) => format!("error: {e}"),
    };
    write.write_all(text.as_bytes()).await?;
    write.write_all(b"\n").await?;
    Ok(())
}

// --- client ------------------------------------------------------------

async fn send(cmd: &str, root: Option<&Path>) -> Result<String> {
    let socket = resolve_socket(root).await?;
    let stream = UnixStream::connect(&socket).await.with_context(|| {
        format!(
            "connect {} (is `pds sync` running for this root?)",
            socket.display()
        )
    })?;
    let (read, mut write) = stream.into_split();
    write.write_all(cmd.as_bytes()).await?;
    write.write_all(b"\n").await?;
    let mut reply = String::new();
    BufReader::new(read).read_line(&mut reply).await?;
    Ok(reply.trim().to_string())
}

/// `pds pause` / `pds resume`.
pub async fn send_simple(cmd: &str, root: Option<&Path>) -> Result<()> {
    let reply = send(cmd, root).await?;
    if reply.starts_with("error:") {
        bail!(reply);
    }
    println!("{reply}");
    Ok(())
}

/// `pds pin <mount-folder> <path>` / `pds unpin …`: paths are relative to
/// the sync root and resolved against the running engine's own root (the
/// control socket carries only the rel).
pub async fn send_pin(cmd: &str, folder: &Path, rel: &str) -> Result<()> {
    let root = folder.canonicalize()?;
    let reply = send(&format!("{cmd} {rel}"), Some(&root)).await?;
    if reply.starts_with("error:") {
        bail!(reply);
    }
    println!("{reply}");
    Ok(())
}

/// Engine status line for `pds status`.
pub async fn query_status(root: Option<&Path>) -> Result<String> {
    send("status", root).await
}

/// Shared tail of the `pin`/`unpin` control commands: validate the rel,
/// dispatch to the engine, format the reply line.
async fn pin_unpin(handle: &EngineHandle, pin: bool, rel: &str) -> Result<String> {
    if rel.is_empty() || rel.contains("..") || rel.starts_with('/') {
        bail!("pin takes a root-relative path (e.g. `docs/report.pdf`)");
    }
    let result = if pin {
        handle.pin(rel).await
    } else {
        handle.unpin(rel).await
    };
    result.map(|_| {
        format!(
            "ok: {} {rel}",
            if pin { "pin" } else { "unpin" }
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_roundtrip() {
        let data = tempfile::tempdir().unwrap();
        let root = Path::new("/tmp/pds-test-root");
        register_root_in(data.path(), root).unwrap();
        assert_eq!(registry_roots_in(data.path()), vec![root.to_path_buf()]);
        deregister_root_in(data.path(), root);
        assert!(registry_roots_in(data.path()).is_empty());
    }

    #[tokio::test]
    async fn discovery_finds_live_engine_and_cleans_stale() {
        let data = tempfile::tempdir().unwrap();
        let live_root = tempfile::tempdir().unwrap();
        let stale_root = tempfile::tempdir().unwrap();

        // Live engine: a real unix socket answering `status`.
        let sock = socket_path(live_root.path());
        std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut line = String::new();
                    if BufReader::new(read).read_line(&mut line).await.is_ok() {
                        let _ = write
                            .write_all(b"running (synced=1, dirty=0, tombstones=0)\n")
                            .await;
                    }
                });
            }
        });

        register_root_in(data.path(), live_root.path()).unwrap();
        register_root_in(data.path(), stale_root.path()).unwrap();
        // Stale engine: leftover socket file with no listener behind it.
        let stale_sock = socket_path(stale_root.path());
        std::fs::create_dir_all(stale_sock.parent().unwrap()).unwrap();
        std::fs::write(&stale_sock, b"").unwrap();

        let live = discover_live_in(data.path()).await;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].root, live_root.path());
        assert!(live[0].status.contains("running"));

        // Stale registration and socket file were cleaned up.
        assert!(
            registry_roots_in(data.path())
                .iter()
                .all(|r| r != stale_root.path())
        );
        assert!(!stale_sock.exists());

        server.abort();
    }

    #[tokio::test]
    async fn discovery_cleans_dead_legacy_marker() {
        let data = tempfile::tempdir().unwrap();
        std::fs::write(data.path().join("current"), b"/nonexistent/pds-root").unwrap();
        let live = discover_live_in(data.path()).await;
        assert!(live.is_empty());
        assert!(!data.path().join("current").exists());
    }

    #[tokio::test]
    async fn discovery_honors_live_legacy_marker() {
        let data = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let sock = socket_path(root.path());
        std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut line = String::new();
                    if BufReader::new(read).read_line(&mut line).await.is_ok() {
                        let _ = write.write_all(b"running\n").await;
                    }
                });
            }
        });

        std::fs::write(
            data.path().join("current"),
            root.path().to_string_lossy().as_bytes(),
        )
        .unwrap();
        let live = discover_live_in(data.path()).await;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].root, root.path());
        // Live marker is kept for the still-running legacy engine.
        assert!(data.path().join("current").exists());

        server.abort();
    }
}
