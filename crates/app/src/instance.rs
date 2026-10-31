//! Single-instance guard.
//!
//! With start-at-login enabled the app is already running in the tray when the
//! user clicks it in the menu (or runs `proton-drive-sync` again). A second
//! process would start a second sync engine on the same root — two writers on
//! one SQLite DB and two FUSE mounts — so the second launch instead asks the
//! primary instance to show its dashboard and exits.
//!
//! A Unix socket in `$XDG_RUNTIME_DIR` (per-user, mode 0700) carries the
//! `show` request. When the runtime dir is unavailable we fall back to the
//! app's data directory, which is per-user too.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{info, warn};

/// Result of the startup handshake.
pub enum Launch {
    /// We own the instance socket; the receiver yields one event per
    /// "show the dashboard" request from later launches.
    Primary {
        events: tokio::sync::mpsc::UnboundedReceiver<()>,
        _guard: ServerGuard,
    },
    /// Another process owns the instance socket and has been asked to show
    /// its dashboard. This process should exit.
    Secondary,
}

/// Owns the listening socket and its accept thread.
pub struct ServerGuard {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Keeps an unused channel open when the guard could not bind a socket.
    _dormant_sender: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        std::fs::remove_file(&self.path).ok();
    }
}

fn socket_path() -> PathBuf {
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime).join("proton-drive-sync.sock");
    }
    let data = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".local/share")
        });
    data.join("proton-drive-sync/instance.sock")
}

/// Check for a running instance; become the primary if there is none.
pub fn acquire() -> Launch {
    acquire_at(&socket_path())
}

fn acquire_at(path: &Path) -> Launch {
    if request_show(path) {
        info!("another instance is running; asked it to show the dashboard");
        return Launch::Secondary;
    }
    // No listener answered: either the socket is stale or missing. Replace it.
    std::fs::remove_file(path).ok();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let listener = match UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(e) => {
            // Lost a race with another instance starting at the same moment.
            if request_show(path) {
                info!("another instance won the startup race; showing its dashboard");
                return Launch::Secondary;
            }
            warn!(path = %path.display(), "single-instance guard unavailable: {e}");
            let (tx, events) = tokio::sync::mpsc::unbounded_channel();
            return Launch::Primary {
                events,
                _guard: ServerGuard {
                    path: path.to_path_buf(),
                    stop: Arc::new(AtomicBool::new(false)),
                    thread: None,
                    _dormant_sender: Some(tx),
                },
            };
        }
    };

    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let stop = stop.clone();
        std::thread::Builder::new()
            .name("pds-instance".into())
            .spawn(move || accept_loop(listener, tx, stop))
            .ok()
    };

    Launch::Primary {
        events,
        _guard: ServerGuard {
            path: path.to_path_buf(),
            stop,
            thread,
            _dormant_sender: None,
        },
    }
}

fn accept_loop(
    listener: UnixListener,
    events: tokio::sync::mpsc::UnboundedSender<()>,
    stop: Arc<AtomicBool>,
) {
    listener.set_nonblocking(true).ok();
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let mut line = String::new();
                if BufReader::new(&mut stream).read_line(&mut line).is_ok()
                    && line.trim() == "show"
                {
                    let _ = events.send(());
                }
                let _ = stream.flush();
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Ask a running instance to show its dashboard. Returns true when a listener
/// accepted the request.
fn request_show(path: &Path) -> bool {
    let Ok(mut stream) = UnixStream::connect(path) else {
        return false;
    };
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .ok();
    stream.write_all(b"show\n").is_ok() && stream.flush().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_launch_is_secondary_and_primary_gets_the_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.sock");

        let Launch::Primary { mut events, _guard } = acquire_at(&path) else {
            panic!("first acquire should be primary");
        };

        let Launch::Secondary = acquire_at(&path) else {
            panic!("second acquire should be secondary");
        };

        // The accept thread polls; give it a moment to deliver the request.
        let mut got = false;
        for _ in 0..50 {
            if events.try_recv().is_ok() {
                got = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(got, "primary should receive the show request");
    }

    #[test]
    fn stale_socket_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.sock");
        std::fs::write(&path, b"not a socket").unwrap();

        let Launch::Primary { .. } = acquire_at(&path) else {
            panic!("stale socket should be replaced by a primary");
        };
    }
}
