//! XDG base-directory resolution, shared by every crate that touches the
//! filesystem (app, CLI, sidecar spawner, core).
//!
//! Follows the [XDG Base Directory specification][spec]: each variable is
//! honoured when set to a **non-empty** value, otherwise the spec's default
//! under `$HOME` applies. `XDG_RUNTIME_DIR` is the exception — the spec
//! defines no default, so [`runtime_dir`] returns `None` when it is unset.
//!
//! [spec]: https://specifications.freedesktop.org/basedir-spec/latest/

use std::ffi::OsString;
use std::path::PathBuf;

/// Directory name this product keeps its config/state under.
pub const APP_DIR: &str = "proton-drive-sync";

type Getenv<'a> = dyn Fn(&str) -> Option<OsString> + 'a;

fn env(key: &str) -> Option<OsString> {
    std::env::var_os(key)
}

/// One environment variable as a path, treating an empty value as unset.
fn var(get: &Getenv<'_>, key: &str) -> Option<PathBuf> {
    get(key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn home_with(get: &Getenv<'_>) -> PathBuf {
    var(get, "HOME").unwrap_or_else(|| PathBuf::from("."))
}

fn base_with(get: &Getenv<'_>, key: &str, fallback: &str) -> PathBuf {
    var(get, key).unwrap_or_else(|| home_with(get).join(fallback))
}

/// `$HOME`, falling back to `.` (never panics on a missing home).
pub fn home() -> PathBuf {
    home_with(&env)
}

/// `$XDG_CONFIG_HOME` or `~/.config`.
pub fn config_home() -> PathBuf {
    base_with(&env, "XDG_CONFIG_HOME", ".config")
}

/// `$XDG_DATA_HOME` or `~/.local/share`.
pub fn data_home() -> PathBuf {
    base_with(&env, "XDG_DATA_HOME", ".local/share")
}

/// `$XDG_STATE_HOME` or `~/.local/state`.
pub fn state_home() -> PathBuf {
    base_with(&env, "XDG_STATE_HOME", ".local/state")
}

/// `$XDG_CACHE_HOME` or `~/.cache`.
pub fn cache_home() -> PathBuf {
    base_with(&env, "XDG_CACHE_HOME", ".cache")
}

/// `$XDG_RUNTIME_DIR`, or `None` — the spec defines no fallback for it.
pub fn runtime_dir() -> Option<PathBuf> {
    var(&env, "XDG_RUNTIME_DIR")
}

/// This product's config directory: `$XDG_CONFIG_HOME/proton-drive-sync`.
pub fn app_config_dir() -> PathBuf {
    config_home().join(APP_DIR)
}

/// This product's data directory: `$XDG_DATA_HOME/proton-drive-sync`.
pub fn app_data_dir() -> PathBuf {
    data_home().join(APP_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, OsString> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), OsString::from(v)))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn xdg_vars_win_when_set() {
        let get = getter(&[
            ("HOME", "/home/me"),
            ("XDG_CONFIG_HOME", "/cfg"),
            ("XDG_DATA_HOME", "/data"),
            ("XDG_STATE_HOME", "/state"),
            ("XDG_CACHE_HOME", "/cache"),
        ]);
        assert_eq!(
            base_with(&get, "XDG_CONFIG_HOME", ".config"),
            PathBuf::from("/cfg")
        );
        assert_eq!(
            base_with(&get, "XDG_DATA_HOME", ".local/share"),
            PathBuf::from("/data")
        );
        assert_eq!(
            base_with(&get, "XDG_STATE_HOME", ".local/state"),
            PathBuf::from("/state")
        );
        assert_eq!(
            base_with(&get, "XDG_CACHE_HOME", ".cache"),
            PathBuf::from("/cache")
        );
    }

    #[test]
    fn home_defaults_when_xdg_unset_or_empty() {
        let get = getter(&[("HOME", "/home/me")]);
        assert_eq!(
            base_with(&get, "XDG_CONFIG_HOME", ".config"),
            PathBuf::from("/home/me/.config")
        );

        // The spec says an empty variable is equivalent to unset.
        let empty = getter(&[("HOME", "/home/me"), ("XDG_CONFIG_HOME", "")]);
        assert_eq!(
            base_with(&empty, "XDG_CONFIG_HOME", ".config"),
            PathBuf::from("/home/me/.config")
        );
    }

    #[test]
    fn missing_home_falls_back_to_cwd() {
        let get = getter(&[]);
        assert_eq!(home_with(&get), PathBuf::from("."));
        assert_eq!(
            base_with(&get, "XDG_DATA_HOME", ".local/share"),
            PathBuf::from("./.local/share")
        );
    }

    #[test]
    fn runtime_dir_has_no_default() {
        assert_eq!(var(&getter(&[]), "XDG_RUNTIME_DIR"), None);
        assert_eq!(
            var(
                &getter(&[("XDG_RUNTIME_DIR", "/run/user/1000")]),
                "XDG_RUNTIME_DIR"
            ),
            Some(PathBuf::from("/run/user/1000"))
        );
    }
}
