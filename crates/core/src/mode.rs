//! Sync mode selection (SPEC F5): placeholder (on-demand) vs materialized
//! (mirror).
//!
//! The mode is a per-sync-root setting persisted in the state DB's `meta`
//! table; new roots default to placeholders (nothing downloads until a file
//! is opened or pinned). `PDS_SYNC_MODE` overrides the persisted setting as
//! an escape hatch. The desktop app also keeps engine state (DB, VFS backing
//! cache, control socket) outside the sync root so it can mount the FUSE
//! placeholder view at the root itself — [`app_data_dir`] and
//! [`engine_state_dir`] are the shared locations for that.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

/// `meta` key holding the persisted [`SyncMode`] value.
pub const META_SYNC_MODE: &str = "sync_mode";

/// Environment variable overriding the persisted mode (`mirror` or
/// `placeholders`). An invalid value is ignored (with a warning logged by
/// the caller when it matters).
pub const ENV_SYNC_MODE: &str = "PDS_SYNC_MODE";

/// How local file content is stored (F5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// On-demand placeholders: synced files live in the engine's backing
    /// cache, hydrated when opened, evicted LRU. Default for new roots.
    #[default]
    Placeholders,
    /// Full mirror: every synced file is materialized in the sync root
    /// (pre-0.2 behavior).
    Mirror,
}

impl SyncMode {
    /// Parse a persisted meta value; `None`/unknown → `None` (caller
    /// applies the default).
    pub fn from_meta_value(value: Option<&str>) -> Option<Self> {
        match value {
            Some("placeholders") => Some(Self::Placeholders),
            Some("mirror") => Some(Self::Mirror),
            _ => None,
        }
    }

    /// Persisted value form.
    pub fn as_meta_value(self) -> &'static str {
        match self {
            Self::Placeholders => "placeholders",
            Self::Mirror => "mirror",
        }
    }

    /// Human-readable label (UI, logs).
    pub fn label(self) -> &'static str {
        match self {
            Self::Placeholders => "On demand (placeholders)",
            Self::Mirror => "Mirror everything",
        }
    }

    /// The mode from `$PDS_SYNC_MODE`, if it holds a recognized value.
    /// Accepted: `placeholders|on-demand|ondemand` and `mirror|full`.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(ENV_SYNC_MODE).ok()?;
        match raw.trim().to_ascii_lowercase().as_str() {
            "placeholders" | "on-demand" | "ondemand" | "placeholder" => {
                Some(Self::Placeholders)
            }
            "mirror" | "full" => Some(Self::Mirror),
            _ => None,
        }
    }
}

/// XDG data dir for proton-drive-sync state that must live outside the sync
/// root (the desktop app mounts the FUSE view at the root itself, so engine
/// state under the root would be shadowed / unreachable through the mount).
pub fn app_data_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return PathBuf::from(xdg).join("proton-drive-sync");
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".local/share/proton-drive-sync")
}

/// Registry/state key for a sync root: a hash, so arbitrary paths are safe
/// as file names. Shared by the CLI root registry and the app's per-root
/// engine state dir.
pub fn root_key(root: &Path) -> String {
    let mut h = DefaultHasher::new();
    root.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Directory holding the engine state that lives outside the sync root:
/// `state.db`, the `backing/` VFS cache, and a `root` marker file.
pub fn engine_state_dir(root: &Path) -> PathBuf {
    app_data_dir().join("roots").join(root_key(root))
}

/// The canonical root path recorded inside an engine state dir (or `None`).
pub fn stored_root_of(state_dir: &Path) -> Option<PathBuf> {
    std::fs::read_to_string(state_dir.join("root"))
        .ok()
        .map(|s| PathBuf::from(s.trim()))
        .filter(|p| !p.as_os_str().is_empty())
}

/// Resolve the effective mode for a sync root (idempotent): `$PDS_SYNC_MODE`
/// (when `honor_env`) > persisted `sync_mode` > `default_for_new`.
///
/// The `$PDS_SYNC_MODE` override is SESSION-ONLY: it never rewrites the
/// persisted setting (an escape hatch must not clobber the user's saved
/// choice). Only a first-run default is stored, so later runs agree even
/// without the env var.
pub fn apply_mode_setting(
    db: &crate::db::Db,
    default_for_new: SyncMode,
    honor_env: bool,
) -> Result<SyncMode, rusqlite::Error> {
    if honor_env
        && let Some(env) = SyncMode::from_env()
    {
        return Ok(env);
    }
    if let Some(persisted) = SyncMode::from_meta_value(db.meta(META_SYNC_MODE)?.as_deref()) {
        return Ok(persisted);
    }
    set_sync_mode(db, default_for_new)?;
    Ok(default_for_new)
}

/// Persist `mode` as the sync root's setting (the caller restarts the
/// engine to apply it).
pub fn set_sync_mode(db: &crate::db::Db, mode: SyncMode) -> Result<(), rusqlite::Error> {
    db.set_meta(META_SYNC_MODE, mode.as_meta_value())
}

/// Move a file, falling back to copy+delete across filesystems (the app's
/// backing cache may live on a different mount than the sync root).
/// The copy fallback verifies the byte size before deleting the source.
pub fn move_file(from: &Path, to: &Path) -> std::io::Result<()> {
    if let Some(dir) = to.parent() {
        std::fs::create_dir_all(dir)?;
    }
    match std::fs::rename(from, to) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {}
        Err(e) => return Err(e),
    }
    let size = std::fs::metadata(from)?.len();
    std::fs::copy(from, to)?;
    let copied = std::fs::metadata(to)?.len();
    if copied != size {
        std::fs::remove_file(to).ok();
        return Err(std::io::Error::other(format!(
            "copy size mismatch: {size} → {copied}"
        )));
    }
    std::fs::remove_file(from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_roundtrip() {
        assert_eq!(
            SyncMode::from_meta_value(Some("placeholders")),
            Some(SyncMode::Placeholders)
        );
        assert_eq!(SyncMode::from_meta_value(Some("mirror")), Some(SyncMode::Mirror));
        assert_eq!(SyncMode::from_meta_value(None), None);
        assert_eq!(SyncMode::from_meta_value(Some("bogus")), None);
        assert_eq!(
            SyncMode::Placeholders.as_meta_value(),
            "placeholders"
        );
    }

    #[test]
    fn default_is_placeholders() {
        assert_eq!(SyncMode::default(), SyncMode::Placeholders);
    }

    #[test]
    fn root_key_is_stable_and_path_safe() {
        let a = root_key(Path::new("/home/u/ProtonDrive"));
        let b = root_key(Path::new("/home/u/ProtonDrive"));
        let c = root_key(Path::new("/home/u/Other"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn engine_state_dir_is_under_app_data() {
        let dir = engine_state_dir(Path::new("/tmp/some-root"));
        assert!(dir.starts_with(app_data_dir()));
        assert!(dir.to_string_lossy().contains("roots"));
    }

    #[test]
    fn env_override_is_session_only() {
        // With $PDS_SYNC_MODE set, resolution honors it but leaves the
        // persisted setting untouched (no clobbering the user's choice).
        // SAFETY: test-only mutation of this process's env. No other test
        // reads PDS_SYNC_MODE, and the var is cleared before the second
        // (env-less) resolution below.
        unsafe {
            std::env::set_var(ENV_SYNC_MODE, "mirror");
        }
        let db = crate::db::Db::open_memory().unwrap();
        let resolved = apply_mode_setting(&db, SyncMode::Placeholders, true).unwrap();
        assert_eq!(resolved, SyncMode::Mirror);
        assert_eq!(db.meta(META_SYNC_MODE).unwrap(), None);
        unsafe {
            std::env::remove_var(ENV_SYNC_MODE);
        }

        // Without the env var the first-run default is stored.
        let resolved = apply_mode_setting(&db, SyncMode::Placeholders, true).unwrap();
        assert_eq!(resolved, SyncMode::Placeholders);
        assert_eq!(
            db.meta(META_SYNC_MODE).unwrap().as_deref(),
            Some("placeholders")
        );
    }
}
