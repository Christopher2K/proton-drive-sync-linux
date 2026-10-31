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

/// `meta` key holding the persisted backing-cache cap in bytes; the literal
/// value `unlimited` means eviction is disabled.
pub const META_CACHE_CAP: &str = "cache_cap_bytes";

/// Environment variable overriding the persisted backing-cache cap for one
/// session: a byte count with an optional `K`/`M`/`G`/`T` (binary) suffix,
/// or `unlimited`. Never rewrites the persisted setting.
pub const ENV_CACHE_CAP: &str = "PDS_CACHE_CAP";

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
    pds_xdg::app_data_dir()
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

/// Parse a human cache-cap value: plain bytes, or bytes with a binary
/// `K`/`M`/`G`/`T` suffix (`10G`, `500M`, `2g`…). `unlimited`/`none`/`off`/
/// `0` disable eviction (`Ok(None)`).
pub fn parse_cache_cap(raw: &str) -> Result<Option<u64>, String> {
    let s = raw.trim();
    if matches!(s.to_ascii_lowercase().as_str(), "unlimited" | "none" | "off" | "0") {
        return Ok(None);
    }
    let (digits, suffix) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| s.split_at(i))
        .unwrap_or((s, ""));
    let value: u64 = digits.parse().map_err(|_| format!("invalid size `{raw}`"))?;
    if digits.is_empty() {
        return Err(format!("invalid size `{raw}`"));
    }
    let mult: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        other => return Err(format!("unknown size suffix `{other}` in `{raw}`")),
    };
    value
        .checked_mul(mult)
        .ok_or_else(|| format!("size `{raw}` overflows u64"))
        .map(Some)
}

/// The `$PDS_CACHE_CAP` override: `Some(None)` = explicitly unlimited this
/// session; `Some(Some(bytes))` = explicit cap; `None` = unset or invalid
/// (invalid values are ignored so a typo never disables eviction).
pub fn cache_cap_from_env() -> Option<Option<u64>> {
    let raw = std::env::var(ENV_CACHE_CAP).ok()?;
    match parse_cache_cap(&raw) {
        Ok(cap) => Some(cap),
        Err(e) => {
            tracing::warn!(value = %raw, error = %e, "ignoring invalid {ENV_CACHE_CAP}");
            None
        }
    }
}

/// Read the persisted cache cap straight from a state DB (read-only).
/// `None` = not set; `Some(None)` = persisted unlimited;
/// `Some(Some(bytes))` = persisted cap.
pub fn persisted_cache_cap(db_path: &Path) -> Option<Option<u64>> {
    let conn = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    let value: String = conn
        .query_row(
            &format!("SELECT value FROM meta WHERE key = '{META_CACHE_CAP}'"),
            [],
            |row| row.get(0),
        )
        .ok()?;
    match value.as_str() {
        "unlimited" => Some(None),
        _ => value.parse::<u64>().ok().map(Some),
    }
}

/// Persist the cache cap chosen in Settings. `Some(bytes)` stores the byte
/// count; `None` (Unlimited) stores the literal `unlimited`. There is no
/// "reset to unset": roots that never touched the setting have no key and
/// fall through to the default, exactly like `META_SYNC_MODE`.
pub fn set_cache_cap(db: &crate::db::Db, cap: Option<u64>) -> Result<(), rusqlite::Error> {
    match cap {
        Some(bytes) => db.set_meta(META_CACHE_CAP, &bytes.to_string()),
        None => db.set_meta(META_CACHE_CAP, "unlimited"),
    }
}

/// Resolve the effective cache cap. Precedence: an explicit CLI flag >
/// `$PDS_CACHE_CAP` (both session-only, never persisted) > the persisted
/// Settings value > `default_cap`. The result is `None` for unlimited.
pub fn resolve_cache_cap(
    flag: Option<Option<u64>>,
    env: Option<Option<u64>>,
    persisted: Option<Option<u64>>,
    default_cap: Option<u64>,
) -> Option<u64> {
    match flag.or(env) {
        Some(inner) => inner,
        None => persisted.unwrap_or(default_cap),
    }
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

    #[test]
    fn cache_cap_parse_sizes() {
        assert_eq!(parse_cache_cap("0"), Ok(None));
        assert_eq!(parse_cache_cap("unlimited"), Ok(None));
        assert_eq!(parse_cache_cap("UNLIMITED"), Ok(None));
        assert_eq!(parse_cache_cap(" none "), Ok(None));
        assert_eq!(parse_cache_cap("off"), Ok(None));
        assert_eq!(parse_cache_cap("500"), Ok(Some(500)));
        assert_eq!(parse_cache_cap("1k"), Ok(Some(1 << 10)));
        assert_eq!(parse_cache_cap("10K"), Ok(Some(10 << 10)));
        assert_eq!(parse_cache_cap("500M"), Ok(Some(500 << 20)));
        assert_eq!(parse_cache_cap("2g"), Ok(Some(2 << 30)));
        assert_eq!(parse_cache_cap("1GiB"), Ok(Some(1 << 30)));
        assert_eq!(parse_cache_cap("3 TB"), Ok(Some(3 << 40)));
        assert!(parse_cache_cap("").is_err());
        assert!(parse_cache_cap("lots").is_err());
        assert!(parse_cache_cap("10x").is_err());
        assert!(parse_cache_cap("18446744073709551616G").is_err());
    }

    #[test]
    fn cache_cap_env_override_is_session_only() {
        // SAFETY: test-only mutation of this process's env. PDS_CACHE_CAP is
        // cleared before the env-less resolution below; the placeholders
        // integration tests that read the env run in another process.
        unsafe {
            std::env::set_var(ENV_CACHE_CAP, "500M");
        }
        assert_eq!(cache_cap_from_env(), Some(Some(500 << 20)));
        unsafe {
            std::env::remove_var(ENV_CACHE_CAP);
        }
        assert_eq!(cache_cap_from_env(), None);

        unsafe {
            std::env::set_var(ENV_CACHE_CAP, "unlimited");
        }
        assert_eq!(cache_cap_from_env(), Some(None));
        unsafe {
            std::env::remove_var(ENV_CACHE_CAP);
        }

        // Invalid values are ignored, never treated as unlimited.
        unsafe {
            std::env::set_var(ENV_CACHE_CAP, "bogus");
        }
        assert_eq!(cache_cap_from_env(), None);
        unsafe {
            std::env::remove_var(ENV_CACHE_CAP);
        }
    }

    #[test]
    fn cache_cap_precedence_flag_env_persisted_default() {
        // Flag beats everything, including an explicit unlimited.
        assert_eq!(
            resolve_cache_cap(Some(None), Some(Some(1)), Some(Some(2)), Some(3)),
            None
        );
        assert_eq!(
            resolve_cache_cap(Some(Some(1)), Some(Some(2)), Some(Some(3)), Some(4)),
            Some(1)
        );
        // Env beats persisted + default; explicit unlimited wins too.
        assert_eq!(
            resolve_cache_cap(None, Some(Some(1)), Some(Some(2)), Some(3)),
            Some(1)
        );
        assert_eq!(
            resolve_cache_cap(None, Some(None), Some(Some(2)), Some(3)),
            None
        );
        // Persisted beats the default; persisted unlimited is honored.
        assert_eq!(resolve_cache_cap(None, None, Some(Some(2)), Some(3)), Some(2));
        assert_eq!(resolve_cache_cap(None, None, Some(None), Some(3)), None);
        // Fall through to the default.
        assert_eq!(resolve_cache_cap(None, None, None, Some(3)), Some(3));
        assert_eq!(resolve_cache_cap(None, None, None, None), None);
    }

    #[test]
    fn cache_cap_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let db = crate::db::Db::open(&db_path).unwrap();

        // Unset falls through to the default.
        assert_eq!(persisted_cache_cap(&db_path), None);

        // A byte cap persists and reads back.
        set_cache_cap(&db, Some(10 << 30)).unwrap();
        assert_eq!(persisted_cache_cap(&db_path), Some(Some(10 << 30)));

        // Unlimited persists as the literal `unlimited` (and reads back as
        // Some(None) — not "unset").
        set_cache_cap(&db, None).unwrap();
        assert_eq!(db.meta(META_CACHE_CAP).unwrap().as_deref(), Some("unlimited"));
        assert_eq!(persisted_cache_cap(&db_path), Some(None));

        // Re-picking a byte cap overwrites the unlimited literal.
        set_cache_cap(&db, Some(2 << 30)).unwrap();
        assert_eq!(persisted_cache_cap(&db_path), Some(Some(2 << 30)));
    }
}
