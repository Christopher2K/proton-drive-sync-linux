//! Start-at-login integration through the XDG autostart specification.
//!
//! The app is its own sync service: the engine, the placeholder mount, and the
//! tray icon all live in one process. Autostart therefore just launches that
//! process with `--minimized` (tray only, no dashboard window) after the
//! graphical session comes up. We use
//! `~/.config/autostart` (honoring `$XDG_CONFIG_HOME`) rather than a systemd
//! user unit because the app needs the user's graphical session for the tray
//! icon, and the FUSE mount must live exactly as long as the process does.
//!
//! The user's choice is remembered in `$XDG_CONFIG_HOME/proton-drive-sync/app.conf`
//! so that a manually deleted autostart file is never silently recreated for
//! someone who turned the feature off.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

pub const DESKTOP_FILE: &str = "proton-drive-sync.desktop";
const CONFIG_FILE: &str = "app.conf";

/// `$XDG_CONFIG_HOME/proton-drive-sync`.
pub fn app_config_dir() -> PathBuf {
    pds_xdg::app_config_dir()
}

fn autostart_dir() -> PathBuf {
    pds_xdg::config_home().join("autostart")
}

fn desktop_path() -> PathBuf {
    autostart_dir().join(DESKTOP_FILE)
}

/// Whether the XDG autostart entry is currently installed.
pub fn is_enabled() -> bool {
    desktop_path().is_file()
}

/// Turn start-at-login on or off, persisting the decision and updating the
/// XDG entry.
///
/// A login start is always `--minimized`: an autostart entry must never open
/// a dashboard window on its own, whether or not the panel (and therefore the
/// tray icon) is already up when the session launches us.
pub fn set_enabled(enabled: bool) -> Result<()> {
    set_enabled_in(&app_config_dir(), &autostart_dir(), &launch_path()?, enabled)
}

/// First-run setup: install the autostart entry unless the user already
/// decided otherwise. Development builds (`target/debug`, `target/release`)
/// stay opt-in so `cargo run` never adds an autostart entry behind the
/// developer's back; installed builds are enabled by default because the
/// whole point of the product is background sync.
///
/// Called once at startup. Headless/smoke runs skip it.
pub fn init_on_launch() {
    let exe = match launch_path() {
        Ok(exe) => exe,
        Err(e) => {
            warn!("autostart: cannot resolve the executable path: {e:#}");
            return;
        }
    };
    let config = app_config_dir();
    let dir = autostart_dir();
    match read_choice_in(&config) {
        Some(true) => {
            // The entry is app-managed: rewrite it on every launch so an
            // existing entry picks up `--minimized` (older builds omitted it
            // when the panel was not visible yet) and the current executable
            // path.
            if let Err(e) = install_in(&dir, &exe) {
                warn!("autostart: could not write the login entry: {e:#}");
            }
        }
        Some(false) => {
            if dir.join(DESKTOP_FILE).is_file() {
                // Keep the files consistent with the persisted decision.
                let _ = remove_in(&dir);
            }
        }
        None => {
            let default_enabled = !is_dev_build(&exe);
            if let Err(e) = set_enabled_in(&config, &dir, &exe, default_enabled) {
                warn!("autostart: first-run setup failed: {e:#}");
                return;
            }
            if default_enabled {
                info!("autostart: enabled for the next login");
            }
        }
    }
}

/// The path to put in `Exec=`. Prefer `argv[0]` when it is absolute: Nix and
/// other wrapper scripts `exec -a "$0"` the real binary so the launcher keeps
/// the wrapper's environment (sidecar path, Node on PATH). Falls back to the
/// resolved binary for a bare `PATH` launch.
fn launch_path() -> Result<PathBuf> {
    if let Some(argv0) = std::env::args_os().next().map(PathBuf::from)
        && argv0.is_absolute()
    {
        return Ok(argv0);
    }
    std::env::current_exe().context("resolve the running executable")
}

/// A checkout build (`.../target/{debug,release}/proton-drive-sync`) should
/// not enable autostart on first run.
fn is_dev_build(exe: &Path) -> bool {
    let mut components = exe.components().rev();
    // ... /target/<profile>/<binary>
    let _binary = components.next();
    matches!(
        components.next().and_then(|c| c.as_os_str().to_str()),
        Some("debug" | "release")
    ) && components
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .is_some_and(|c| c == "target")
}

fn read_choice_in(config_dir: &Path) -> Option<bool> {
    let text = std::fs::read_to_string(config_dir.join(CONFIG_FILE)).ok()?;
    for line in text.lines() {
        let line = line.trim();
        let Some(value) = line.strip_prefix("autostart=") else {
            continue;
        };
        return match value.trim() {
            "on" | "true" | "1" => Some(true),
            "off" | "false" | "0" => Some(false),
            _ => None,
        };
    }
    None
}

fn write_choice_in(config_dir: &Path, enabled: bool) -> Result<()> {
    std::fs::create_dir_all(config_dir)
        .with_context(|| format!("create {}", config_dir.display()))?;
    let path = config_dir.join(CONFIG_FILE);
    let body = format!("# proton-drive-sync application settings\nautostart={}\n", if enabled { "on" } else { "off" });
    write_atomic(&path, body.as_bytes())
}

fn set_enabled_in(config_dir: &Path, dir: &Path, exe: &Path, enabled: bool) -> Result<()> {
    if enabled {
        install_in(dir, exe)?;
    } else {
        remove_in(dir)?;
    }
    write_choice_in(config_dir, enabled)
}

fn install_in(dir: &Path, exe: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let body = desktop_entry(exe);
    write_atomic(&dir.join(DESKTOP_FILE), body.as_bytes())
}

fn remove_in(dir: &Path) -> Result<()> {
    match std::fs::remove_file(dir.join(DESKTOP_FILE)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove {}", dir.join(DESKTOP_FILE).display())),
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.sync_all().ok();
    }
    std::fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))
}

/// The `.desktop` entry launched by the session at login. It always starts
/// minimized: the login entry must never open a window on its own.
fn desktop_entry(exe: &Path) -> String {
    format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Version=1.0\n\
         Name=Proton Drive Sync\n\
         GenericName=File Synchronizer\n\
         Comment=Background sync with Proton Drive (unofficial)\n\
         Exec={} --minimized\n\
         Icon=proton-drive-sync\n\
         Terminal=false\n\
         NoDisplay=true\n\
         X-GNOME-Autostart-enabled=true\n\
         X-GNOME-Autostart-Delay=3\n\
         X-KDE-autostart-after=panel\n",
        exec_quote(exe)
    )
}

/// Quote a path for a `Exec=` line: the freedesktop spec splits on spaces,
/// so wrap in double quotes and escape the reserved characters.
fn exec_quote(path: &Path) -> String {
    let raw = path.to_string_lossy();
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        if matches!(ch, '"' | '`' | '$' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn desktop_entry_launches_minimized_with_quoted_path() {
        let entry = desktop_entry(Path::new("/home/me/My Apps/proton-drive-sync"));
        assert!(entry.contains(
            "Exec=\"/home/me/My Apps/proton-drive-sync\" --minimized"
        ));
        assert!(entry.contains("Type=Application"));
        assert!(entry.contains("NoDisplay=true"));
        assert!(entry.ends_with('\n'));
    }

    #[test]
    fn existing_entry_is_upgraded_to_minimized() {
        let root = temp();
        let autostart = root.path().join("autostart");
        std::fs::create_dir_all(&autostart).unwrap();
        // An entry written by an older build that probed the tray too early:
        // no --minimized, so it opened the dashboard at login.
        std::fs::write(
            autostart.join(DESKTOP_FILE),
            "[Desktop Entry]\nExec=\"/opt/pds/proton-drive-sync\"\n",
        )
        .unwrap();

        install_in(&autostart, Path::new("/opt/pds/proton-drive-sync")).unwrap();
        let text = std::fs::read_to_string(autostart.join(DESKTOP_FILE)).unwrap();
        assert!(text.contains("--minimized"));
    }

    #[test]
    fn exec_quote_escapes_reserved_characters() {
        assert_eq!(exec_quote(Path::new("/a b/c")), "\"/a b/c\"");
        assert_eq!(exec_quote(Path::new("/a\"b$c")), "\"/a\\\"b\\$c\"");
    }

    #[test]
    fn enable_disable_roundtrip_and_choice_persistence() {
        let root = temp();
        let config = root.path().join("config");
        let autostart = root.path().join("autostart");
        let exe = Path::new("/opt/pds/proton-drive-sync");

        set_enabled_in(&config, &autostart, exe, true).unwrap();
        assert!(autostart.join(DESKTOP_FILE).is_file());
        assert_eq!(read_choice_in(&config), Some(true));
        let text = std::fs::read_to_string(autostart.join(DESKTOP_FILE)).unwrap();
        assert!(text.contains("--minimized"));

        set_enabled_in(&config, &autostart, exe, false).unwrap();
        assert!(!autostart.join(DESKTOP_FILE).is_file());
        assert_eq!(read_choice_in(&config), Some(false));
    }

    #[test]
    fn disabling_twice_is_idempotent() {
        let root = temp();
        let config = root.path().join("config");
        let autostart = root.path().join("autostart");
        let exe = Path::new("/opt/pds/proton-drive-sync");
        set_enabled_in(&config, &autostart, exe, false).unwrap();
        set_enabled_in(&config, &autostart, exe, false).unwrap();
        assert_eq!(read_choice_in(&config), Some(false));
    }

    #[test]
    fn missing_config_means_undecided() {
        let root = temp();
        assert_eq!(read_choice_in(root.path()), None);
    }

    #[test]
    fn dev_builds_are_detected() {
        assert!(is_dev_build(Path::new(
            "/home/me/proton-drive-sync/target/debug/proton-drive-sync"
        )));
        assert!(is_dev_build(Path::new(
            "/home/me/proton-drive-sync/target/release/proton-drive-sync"
        )));
        assert!(!is_dev_build(Path::new("/usr/local/bin/proton-drive-sync")));
        assert!(!is_dev_build(Path::new("/nix/store/x/bin/proton-drive-sync")));
    }
}
