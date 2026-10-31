# pds-app — `proton-drive-sync` Linux desktop app (M2)

Native Linux GPUI application embedding the M1 sync engine, single main
window with sidebar navigation (upstream `gpui 0.2`, Wayland/X11).

- **Login view** when the sidecar has no session: mandated disclosure text
  ("This is a third-party application not officially supported by Proton."),
  **Sign in with Proton** → `auth.loginFork`, shows the URL + user code from
  `auth.signInUrl` and opens the URL in the system browser (`xdg-open`).
- **Overview**: account, engine state badge, sync counts, big pause/resume
  toggle, delete-guard banner when the guard tripped.
- **Activity**: feed of uploads, downloads, trashes, conflicts, guard and
  upload-error entries with timestamps and filters.
- **Conflicts**: conflicts + failed uploads inbox (scanned from the state
  DB), with reveal-in-file-manager actions.
- **Settings**: sync root, sync mode + cache, start-at-login, engine
  details.
- **Sign-out** (sidebar footer) asks for confirmation, stops the engine
  gracefully, calls `auth.logout` (deletes the session server-side and wipes
  `session.json`), then returns to the login view. Files on disk and the
  local state DB are kept.
- **Background service**: the process is the sync service. It can start at
  login (`--minimized`, installed as an XDG autostart entry) and keeps a
  StatusNotifierItem tray icon with live state + actions. Closing the window
  minimizes it (when a tray host exists) so sync continues; Quit stops the
  engine gracefully. Only one instance runs per user — a second launch asks
  the primary to show its dashboard.

Layout: `main.rs` (window + CLI + tray wiring) · `ui/` (pages + shell) ·
`state.rs` (pure state + reducer, tested) · `engine.rs` (tokio engine thread) ·
`tray.rs` (StatusNotifierItem, status icon, menu actions) ·
`autostart.rs` (XDG login entry + persisted choice) · `instance.rs`
(single-instance socket) · `components.rs`/`theme.rs` (design system) ·
`actions.rs` (key bindings) · `platform.rs` (XDG paths, `xdg-open`).

## Run it (dev)

From the **repository root** (the sidecar is discovered relative to the
working directory):

```sh
cargo run -p pds-app
```

Environment knobs:

| Var | Meaning |
|---|---|
| `PDS_SYNC_ROOT` | Sync folder (default: `~/ProtonDrive`, created if missing) |
| `PDS_SIDECAR_CMD` | Sidecar command override, e.g. `node sidecar/dist/index.js` |
| `PDS_SIDECAR_DIR` | Sidecar directory override for auto-discovery |
| `PDS_DEBUG_SIGNOUT` | Dev hook: confirm sign-out automatically once the engine runs (real logout — the session is deleted!) |
| `RUST_LOG` | Log filter (default `info,pds_core=debug`), logs go to stderr |

## Headless smoke proof (no display needed)

Boots the GPUI event loop without a window and quits — proves the binary
links and the platform backend initializes (use under `xvfb-run` on
headless machines):

```sh
xvfb-run -a target/debug/proton-drive-sync --headless
# headless proof: event loop OK
```

## Window identity

The window sets the Wayland app id / X11 WM_CLASS to `proton-drive-sync`,
matching `packaging/linux/proton-drive-sync.desktop` (`StartupWMClass`) so
desktop shells associate the window with the installed icon.
