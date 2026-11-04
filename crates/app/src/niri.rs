//! Minimal client for niri's IPC socket.
//!
//! niri has no minimize: `xdg_toplevel.set_minimized` is accepted by the
//! protocol but ignored, and a Wayland client cannot unmap its last toplevel
//! without ending the event loop. So the dashboard "closes" by moving its
//! window to the trailing workspace of its own output — niri always keeps an
//! empty workspace after the last one with windows — and comes back by moving
//! it to the focused workspace. The window stays mapped the whole time, which
//! keeps the GPUI loop (and therefore syncing) alive.
//!
//! `NIRI_SOCKET` advertises the socket; the protocol is newline-delimited
//! JSON, one request and one reply per connection.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tracing::{debug, info};

/// niri is a local socket, so this only guards the UI thread against a wedged
/// compositor.
const TIMEOUT: Duration = Duration::from_secs(2);

fn socket_path() -> Option<PathBuf> {
    std::env::var_os("NIRI_SOCKET")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn request(request: &Value) -> Result<Value> {
    let path = socket_path().ok_or_else(|| anyhow!("NIRI_SOCKET is not set"))?;
    let mut stream = UnixStream::connect(&path)
        .with_context(|| format!("connect to niri IPC at {}", path.display()))?;
    stream
        .set_read_timeout(Some(TIMEOUT))
        .context("set niri IPC read timeout")?;
    stream
        .set_write_timeout(Some(TIMEOUT))
        .context("set niri IPC write timeout")?;

    let mut line = serde_json::to_vec(request).context("encode niri IPC request")?;
    line.push(b'\n');
    stream
        .write_all(&line)
        .context("write niri IPC request")?;

    let mut reply = String::new();
    BufReader::new(stream)
        .read_line(&mut reply)
        .context("read niri IPC reply")?;
    if reply.trim().is_empty() {
        bail!("niri closed the IPC connection without replying");
    }
    debug!(request = %request, reply = %reply.trim(), "niri IPC");

    let reply: Value = serde_json::from_str(&reply).context("decode niri IPC reply")?;
    if let Some(error) = reply.get("Err").and_then(Value::as_str) {
        bail!("niri rejected the request: {error}");
    }
    reply
        .get("Ok")
        .cloned()
        .ok_or_else(|| anyhow!("unexpected niri IPC reply: {reply}"))
}

/// Park the dashboard on the trailing workspace of its own output. Focus does
/// not follow, so the user stays where they were while the window leaves the
/// visible workspace.
pub fn park() -> Result<()> {
    let pid = i64::from(std::process::id());
    let windows = windows()?;
    let window = window_of(pid, &windows)
        .ok_or_else(|| anyhow!("niri does not know a window for PID {pid}"))?
        .clone();

    let workspaces = workspaces()?;
    let target = parking_index(&workspaces, &window)
        .ok_or_else(|| anyhow!("no free workspace to park the dashboard on"))?;

    move_to_workspace(window.id, json!({ "Index": target }), false)?;
    info!(
        window = window.id,
        workspace = target,
        "dashboard parked off screen"
    );
    Ok(())
}

/// Bring a parked dashboard back to the focused workspace and focus it.
pub fn restore() -> Result<()> {
    let pid = i64::from(std::process::id());
    let windows = windows()?;
    let Some(window) = window_of(pid, &windows).cloned() else {
        // No window yet (first open): it will appear focused on its own.
        return Ok(());
    };

    let workspaces = workspaces()?;
    let Some(focused) = workspaces.iter().find(|workspace| workspace.is_focused) else {
        return focus_window(window.id);
    };

    if window.workspace_id != Some(focused.id) {
        move_to_workspace(window.id, json!({ "Id": focused.id }), true)?;
    }
    // Also switches to the window's output when the move above could not take
    // the focus (a parked window is not the focused window).
    focus_window(window.id)?;
    info!(
        window = window.id,
        workspace = focused.id,
        "dashboard restored"
    );
    Ok(())
}

fn windows() -> Result<Vec<WindowInfo>> {
    parse_windows(&request(&json!("Windows"))?)
}

fn workspaces() -> Result<Vec<WorkspaceInfo>> {
    parse_workspaces(&request(&json!("Workspaces"))?)
}

fn move_to_workspace(window_id: u64, reference: Value, focus: bool) -> Result<()> {
    request(&json!({
        "Action": {
            "MoveWindowToWorkspace": {
                "window_id": window_id,
                "reference": reference,
                "focus": focus,
            }
        }
    }))?;
    Ok(())
}

fn focus_window(window_id: u64) -> Result<()> {
    request(&json!({ "Action": { "FocusWindow": { "id": window_id } } }))?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WindowInfo {
    id: u64,
    pid: Option<i64>,
    workspace_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkspaceInfo {
    id: u64,
    idx: u8,
    /// `None` when no output is connected.
    output: Option<String>,
    is_focused: bool,
}

fn window_of(pid: i64, windows: &[WindowInfo]) -> Option<&WindowInfo> {
    windows.iter().find(|window| window.pid == Some(pid))
}

/// 1-based workspace index to park `window` on, or `None` when its output has
/// no other workspace. niri's IPC indices are 1-based (the same ones
/// `focus-workspace` takes). The trailing workspace is the empty one niri
/// keeps after the last occupied workspace, so it is the natural hiding
/// place; if the window already sits there, the one before it is used
/// instead.
fn parking_index(workspaces: &[WorkspaceInfo], window: &WindowInfo) -> Option<u64> {
    let current = workspaces
        .iter()
        .find(|workspace| Some(workspace.id) == window.workspace_id)?;
    let output = current.output.as_deref();
    let count = workspaces
        .iter()
        .filter(|workspace| workspace.output.as_deref() == output)
        .count() as u64;
    if count < 2 {
        return None;
    }
    let trailing = count;
    if u64::from(current.idx) == trailing {
        Some(trailing - 1)
    } else {
        Some(trailing)
    }
}

fn u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn parse_windows(ok: &Value) -> Result<Vec<WindowInfo>> {
    let list = ok
        .get("Windows")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("niri did not return a window list"))?;
    Ok(list
        .iter()
        .filter_map(|item| {
            Some(WindowInfo {
                id: u64_field(item, "id")?,
                pid: item.get("pid").and_then(Value::as_i64),
                workspace_id: u64_field(item, "workspace_id"),
            })
        })
        .collect())
}

fn parse_workspaces(ok: &Value) -> Result<Vec<WorkspaceInfo>> {
    let list = ok
        .get("Workspaces")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("niri did not return a workspace list"))?;
    Ok(list
        .iter()
        .filter_map(|item| {
            Some(WorkspaceInfo {
                id: u64_field(item, "id")?,
                idx: u8::try_from(u64_field(item, "idx")?).ok()?,
                output: item
                    .get("output")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                is_focused: item
                    .get("is_focused")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(id: u64, workspace: u64) -> WindowInfo {
        WindowInfo {
            id,
            pid: Some(42),
            workspace_id: Some(workspace),
        }
    }

    fn workspace(id: u64, idx: u8, output: &str) -> WorkspaceInfo {
        WorkspaceInfo {
            id,
            idx,
            output: Some(output.to_owned()),
            is_focused: false,
        }
    }

    #[test]
    fn finds_the_window_by_pid() {
        let windows = vec![
            WindowInfo {
                id: 1,
                pid: Some(7),
                workspace_id: Some(10),
            },
            WindowInfo {
                id: 2,
                pid: Some(42),
                workspace_id: Some(10),
            },
        ];
        assert_eq!(window_of(42, &windows).map(|w| w.id), Some(2));
        assert_eq!(window_of(99, &windows).map(|w| w.id), None);
    }

    #[test]
    fn parks_on_the_trailing_workspace() {
        // Window on the second of three workspaces (indices are 1-based):
        // park on the third.
        let spaces = vec![
            workspace(1, 1, "eDP-1"),
            workspace(2, 2, "eDP-1"),
            workspace(3, 3, "eDP-1"),
        ];
        assert_eq!(parking_index(&spaces, &window(9, 2)), Some(3));
    }

    #[test]
    fn parks_on_the_previous_workspace_when_already_last() {
        let spaces = vec![workspace(1, 1, "eDP-1"), workspace(2, 2, "eDP-1")];
        assert_eq!(parking_index(&spaces, &window(9, 2)), Some(1));
    }

    #[test]
    fn ignores_workspaces_on_other_outputs() {
        let spaces = vec![
            workspace(1, 1, "eDP-1"),
            workspace(2, 2, "eDP-1"),
            workspace(3, 1, "DP-1"),
        ];
        // Two workspaces on eDP-1: the trailing one is index 2.
        assert_eq!(parking_index(&spaces, &window(9, 1)), Some(2));
    }

    #[test]
    fn cannot_park_without_another_workspace() {
        let spaces = vec![workspace(1, 1, "eDP-1")];
        assert_eq!(parking_index(&spaces, &window(9, 1)), None);
    }

    #[test]
    fn parses_window_and_workspace_replies() {
        let windows = parse_windows(&json!({
            "Windows": [
                {"id": 5, "pid": 42, "workspace_id": 8, "title": "x"},
                {"id": 6, "pid": null, "workspace_id": null}
            ]
        }))
        .unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].workspace_id, Some(8));
        assert_eq!(windows[1].pid, None);

        let spaces = parse_workspaces(&json!({
            "Workspaces": [
                {"id": 8, "idx": 1, "output": "eDP-1", "is_focused": true},
                {"id": 9, "idx": 2, "output": null}
            ]
        }))
        .unwrap();
        assert_eq!(spaces.len(), 2);
        assert!(spaces[0].is_focused);
        assert_eq!(spaces[1].output, None);
    }
}
