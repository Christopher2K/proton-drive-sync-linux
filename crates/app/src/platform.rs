//! Linux desktop integration: XDG paths and opening URLs/folders with the
//! user's desktop environment.

use std::path::{Path, PathBuf};

use tracing::warn;

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// The sync root: `$PDS_SYNC_ROOT` or `~/ProtonDrive`.
pub fn sync_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("PDS_SYNC_ROOT") {
        return PathBuf::from(dir);
    }
    home().join("ProtonDrive")
}

/// Open a URL in the user's default browser.
pub fn open_url(url: &str) {
    spawn_detached("xdg-open", &[url]);
}

/// Open a folder in the user's default file manager.
///
/// `xdg-open` keys off the path's MIME type. A FUSE mount point (such as the
/// placeholder-mode sync root, which is a mount rather than an ordinary
/// directory) reports `inode/mount-point`, which no `.desktop` entry claims,
/// so `xdg-open` falls through to its browser fallback and fails. Detect that
/// case and launch a file manager directly instead.
pub fn open_folder(path: &Path) {
    use std::os::unix::fs::MetadataExt;

    let path_str = path.to_string_lossy().into_owned();

    let is_mount_point = path
        .parent()
        .and_then(|parent| {
            let child = std::fs::metadata(path).ok()?;
            let parent_meta = std::fs::metadata(parent).ok()?;
            Some(child.dev() != parent_meta.dev())
        })
        .unwrap_or(false);

    if is_mount_point {
        // xdg-open cannot handle mount points; try the file managers that are
        // commonly installed, in preference order, then a terminal as a last
        // resort. Spawn detached so a slow/absent binary never blocks the UI.
        let candidates: &[(&str, Vec<String>)] = &[
            ("nautilus", vec![path_str.clone()]),
            ("dolphin", vec![path_str.clone()]),
            ("thunar", vec![path_str.clone()]),
            ("nemo", vec![path_str.clone()]),
            ("pcmanfm", vec![path_str.clone()]),
            ("caja", vec![path_str.clone()]),
            ("kitty", vec!["+open".to_string(), path_str.clone()]),
        ];
        for (program, args) in candidates {
            if spawn_detached(program, args) {
                return;
            }
        }
    }

    // Ordinary directories: honour the user's preferred handler.
    let arg = path_str;
    spawn_detached("xdg-open", &[arg]);
}

/// Spawn `program` with `args` detached from the UI thread. Returns `true` if
/// the process was successfully spawned.
fn spawn_detached(program: &str, args: &[impl AsRef<std::ffi::OsStr>]) -> bool {
    // Convert to a uniform &[&str]-ish by collecting owned strings? `args` may
    // be `&[String]` or `&[&str]`; both implement AsRef<OsStr>. Command::args
    // accepts any IntoIterator of AsRef<OsStr>, so pass through directly.
    match std::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(mut child) => {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            true
        }
        Err(e) => {
            warn!(program, "failed to launch desktop handler: {e}");
            false
        }
    }
}
