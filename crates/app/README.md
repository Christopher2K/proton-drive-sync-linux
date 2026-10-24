# pds-app — `proton-drive-sync` desktop shell (M2)

Windowless menu-bar app embedding the M1 sync engine in-process, built with
upstream `gpui 0.2` + `gpui-tray 0.3` (see `docs/research/gpui.md`).

- Tray icon with **Open Status / Pause|Resume Syncing / Quit** (label flips
  via `Tray::refresh_menu`; the menu rebuilds itself from app state after
  every click and on each engine status tick).
- **Login view** when the sidecar has no session: mandated disclosure text,
  "Sign in with Proton" → `auth.loginFork`, shows the URL + user code from
  `auth.signInUrl` and opens the URL in the system browser.
- **Status window**: account email, engine state badge (running / paused /
  guard-paused), synced/pending/deletes/conflicts/errors counts, a big
  pause/resume toggle, a virtualized activity feed (uploaded, downloaded,
  trashed, conflict, guard, upload-error entries with timestamps), a
  conflicts + failed-uploads inbox (scanned from the state DB), and the sync
  root in the footer.
- Closing windows never quits the app; the tray owns the lifetime. Quit
  asks the engine to shut down gracefully (3 s fallback force-quit).

## Run it (dev)

From the **repository root** (the sidecar path is resolved relative to the
working directory):

```sh
cargo run -p pds-app
```

Environment knobs:

| Var | Meaning |
|---|---|
| `PDS_SYNC_ROOT` | Sync folder (default: `~/ProtonDrive`, created if missing) |
| `PDS_SIDECAR_CMD` | Sidecar command override, e.g. `node sidecar/dist/index.js` |
| `RUST_LOG` | Log filter (default `info,pds_core=debug`), logs go to stderr |

First run: the login window opens, click **Sign in with Proton**, finish in
the browser; the status window appears once the engine starts.

## Headless smoke proof (no GUI interaction)

Boots the app with `Application::headless()`, builds the tray, runs the
event loop briefly, and quits — proves the binary links and starts:

```sh
PDS_HEADLESS_PROOF=1 ./target/debug/proton-drive-sync
# headless proof: tray built
# headless proof: event loop OK
```

## Build notes

- gpui's Metal shaders are compiled **at runtime** (`runtime_shaders`
  feature) because this machine has no full Xcode (no `metal` CLI). Runtime
  compilation uses the Metal framework and works without Xcode; the cost is
  a small startup delay. With Xcode installed the feature can be dropped.
- Shipping as a real menu-bar app (no Dock icon) needs a `.app` bundle with
  `LSUIElement=YES` — see gpui.md §6 for the minimal `Info.plist`.
