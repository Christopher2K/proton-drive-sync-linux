//! Sidecar child-process spawning and command resolution.

use anyhow::{Context, Result, bail};
use pds_sidecar_client::SidecarClient;
use tokio::process::Child;

/// Resolve and spawn the sidecar.
///
/// Order: explicit `--sidecar-cmd`, `$PDS_SIDECAR_CMD`, `node
/// sidecar/dist/index.js` if built, else `npx tsx sidecar/src/index.ts`
/// from the current directory (dev mode — `@protontech/crypto` ships raw
/// TypeScript, see AGENT.md sidecar gotchas).
pub fn spawn_sidecar(cmd: Option<&str>) -> Result<(SidecarClient, Child)> {
    let line = cmd
        .map(str::to_string)
        .or_else(|| std::env::var("PDS_SIDECAR_CMD").ok())
        .unwrap_or_else(default_cmd);
    let mut parts = line.split_whitespace();
    let program = parts.next().context("empty sidecar command")?;
    let args: Vec<&str> = parts.collect();
    if program.is_empty() {
        bail!("empty sidecar command");
    }
    SidecarClient::spawn(program, &args, None).context("spawn sidecar")
}

fn default_cmd() -> String {
    if std::path::Path::new("sidecar/dist/index.js").is_file() {
        "node sidecar/dist/index.js".to_string()
    } else {
        "npx tsx sidecar/src/index.ts".to_string()
    }
}
