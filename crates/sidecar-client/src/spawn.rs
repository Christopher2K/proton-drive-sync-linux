//! Sidecar child-process spawning and command resolution, shared by the
//! CLI (`pds`) and the desktop app (`proton-drive-sync`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tokio::process::Child;

use crate::client::SidecarClient;

/// Resolve and spawn the sidecar.
///
/// With an explicit `cmd` (CLI flag / app setting) or `$PDS_SIDECAR_CMD`,
/// that command line is run verbatim from the current directory. Otherwise a
/// sidecar directory is located (see [`discover_sidecar_dir`]) and the right
/// launch line is chosen inside it, with the child's working directory set to
/// the sidecar directory so module resolution works regardless of where the
/// host was started.
pub fn spawn_sidecar(cmd: Option<&str>) -> Result<(SidecarClient, Child)> {
    let line = cmd
        .map(str::to_string)
        .or_else(|| std::env::var("PDS_SIDECAR_CMD").ok());
    match line {
        Some(line) => {
            let (program, args) = split_cmd(&line)?;
            SidecarClient::spawn(program, &args, None).context("spawn sidecar")
        }
        None => {
            let dir = discover_sidecar_dir().context(
                "could not locate the sidecar; install it (see packaging/linux) or set \
                 $PDS_SIDECAR_CMD to a command that runs sidecar/src/index.ts",
            )?;
            spawn_in(&dir)
        }
    }
}

/// Where to find the sidecar when no explicit command is given, in order:
///
/// 1. `$PDS_SIDECAR_DIR`
/// 2. `<exe dir>/sidecar`, then `<exe dir>/../sidecar` (installed layouts:
///    `bin/proton-drive-sync` with a sibling `sidecar/`, or a flat install)
/// 3. `$XDG_DATA_HOME/proton-drive-sync/sidecar` (default install location)
/// 4. `./sidecar` (development: running from the repository root)
///
/// A directory counts if it can be launched: has `dist/index.js` or
/// `src/index.ts` (plus `node_modules` for the TypeScript entry).
pub fn discover_sidecar_dir() -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(dir) = std::env::var("PDS_SIDECAR_DIR") {
        candidates.push(PathBuf::from(dir));
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        candidates.push(dir.join("sidecar"));
        candidates.push(dir.join("../sidecar"));
    }
    candidates.push(xdg_data_dir().join("sidecar"));
    candidates.push(PathBuf::from("sidecar"));

    candidates.into_iter().find(|dir| is_launchable(dir))
}

fn is_launchable(dir: &Path) -> bool {
    dir.join("dist/index.js").is_file()
        || (dir.join("src/index.ts").is_file() && dir.join("node_modules").is_dir())
}

fn spawn_in(dir: &Path) -> Result<(SidecarClient, Child)> {
    // `@protontech/crypto` ships raw TypeScript, so the source entry needs
    // `tsx`; the compiled entry runs on plain node. See AGENT.md sidecar
    // gotchas.
    if dir.join("dist/index.js").is_file() {
        return SidecarClient::spawn("node", &["dist/index.js"], Some(dir))
            .with_context(|| format!("spawn sidecar from {}", dir.display()));
    }
    if dir.join("src/index.ts").is_file() {
        return SidecarClient::spawn("npx", &["tsx", "src/index.ts"], Some(dir))
            .with_context(|| format!("spawn sidecar from {}", dir.display()));
    }
    bail!(
        "no runnable sidecar in {} (need dist/index.js or src/index.ts + node_modules)",
        dir.display()
    );
}

/// `$XDG_DATA_HOME/proton-drive-sync`, defaulting to `~/.local/share/...`.
fn xdg_data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(dir).join("proton-drive-sync");
    }
    let home = std::env::var_os("HOME").unwrap_or_default();
    PathBuf::from(home).join(".local/share/proton-drive-sync")
}

fn split_cmd(line: &str) -> Result<(&str, Vec<&str>)> {
    let mut parts = line.split_whitespace();
    let program = parts.next().context("empty sidecar command")?;
    if program.is_empty() {
        bail!("empty sidecar command");
    }
    Ok((program, parts.collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"").unwrap();
    }

    #[test]
    fn ts_entry_requires_node_modules() {
        let tmp = std::env::temp_dir().join(format!("pds-spawn-{}", std::process::id()));
        let dir = tmp.join("with-nm");
        write(&dir.join("src/index.ts"));
        std::fs::create_dir_all(dir.join("node_modules")).unwrap();
        assert!(is_launchable(&dir));

        let bare = tmp.join("bare");
        write(&bare.join("src/index.ts"));
        assert!(!is_launchable(&bare));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn dist_entry_does_not_need_node_modules() {
        let tmp = std::env::temp_dir().join(format!("pds-spawn-d{}", std::process::id()));
        let dir = tmp.join("dist-only");
        write(&dir.join("dist/index.js"));
        assert!(is_launchable(&dir));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn split_rejects_empty() {
        assert!(split_cmd("   ").is_err());
        let (program, args) = split_cmd("npx tsx src/index.ts").unwrap();
        assert_eq!(program, "npx");
        assert_eq!(args, vec!["tsx", "src/index.ts"]);
    }
}
