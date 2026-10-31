//! Linux desktop integration: XDG paths and opening URLs/folders with the
//! user's desktop environment.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tracing::warn;

/// The sync root: `$PDS_SYNC_ROOT` or `~/ProtonDrive`.
pub fn sync_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("PDS_SYNC_ROOT") {
        return PathBuf::from(dir);
    }
    pds_xdg::home().join("ProtonDrive")
}

/// Open a URL in the user's default browser.
pub fn open_url(url: &str) {
    spawn_detached("xdg-open", &[url]);
}

/// Open a folder in the user's default file manager.
pub fn open_folder(path: &Path) {
    let path = path.to_string_lossy();
    spawn_detached("xdg-open", &[&path]);
}

fn spawn_detached(program: &str, args: &[&str]) {
    match Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(mut child) => {
            // Reap the short-lived launcher without blocking the UI thread.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => warn!(program, "failed to launch desktop handler: {e}"),
    }
}
