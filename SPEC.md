# Proton Drive Sync — Product Specification (v0.2)

## 1. Product summary

A **Linux** desktop app that syncs a Proton Drive account into a local folder. Built in **Rust with a GPUI user interface**, driving the **official Proton Drive TypeScript SDK** running in an embedded Node.js sidecar. (macOS and Windows were dropped in v0.2; the desktop app defaults to **on-demand placeholders** — the sync root is a FUSE view that hydrates on open — with a full materialized mirror available in Settings, see F5.)

## 2. Goals / non-goals

**Goals**

- G1: One Proton account, one sync root folder.
- G2: Files copied into the folder upload automatically.
- G3: Files deleted locally are trashed remotely — conservatively, never irreversibly.
- G4: Pause / resume sync at any time, resumable transfers.
- G5: On-demand placeholder files (M3): near-zero SSD usage until a file is opened or pinned.
- G6: Event-based remote change detection (mandated by Proton's usage guidelines — no polling or recursive scans).

**Non-goals (v1)**

- macOS support, multiple accounts, multiple sync roots, share-link management, file versioning UI, mobile.

## 3. Architecture

Three components:

| Component | Tech | Role |
|---|---|---|
| **Host app (UI + orchestrator)** | Rust + GPUI (native Linux desktop window) | Single main window, owns state DB, sync engine, upload/download queues |
| **SDK sidecar** | Node.js + `ProtonDriveApps/sdk` (client module) | All Drive traffic: auth session, listing, up/download, trash, event stream |
| **VFS (M3 ✅)** | Rust FUSE daemon (`fuser` crate) | Placeholder view of the remote tree — mounted at the sync root by the desktop app, or at a separate mountpoint by `pds mount`; reads hydrate, writes queue uploads |

- **IPC:** JSON-RPC over stdio between host ↔ Node sidecar.
- **State:** SQLite in the host — node map (local path ↔ Drive node UID), pins, tombstones, transfer queue, eviction metadata, event cursor (resume token).
- **Auth:** The SDK *deliberately excludes* login. The sidecar implements Proton auth (browser session-fork; SRP kept behind `PDS_AUTH=srp`) against official endpoints, modeled on how first-party clients wire it. The session is a `0600` file (`session.json`) under `$XDG_DATA_HOME/proton-drive-sync/`. Login screen shows the mandated disclosure: *"This is a third-party application not officially supported by Proton."*

**Why the sidecar:** the official SDK is the only implementation guaranteed to track the **2026/27 cryptographic migration**. Upgrading the sidecar's npm dependency is our migration strategy — no Rust crypto to rewrite.

## 4. Feature specifications

### F1 — Initial setup

Login (browser session-fork, 2FA supported) → sync root (default `~/ProtonDrive`) → remote tree materializes → event cursor stored. **First-run rule: an empty local folder is never interpreted as "delete everything."**

### F2 — Upload on new/changed file

Filesystem watcher reports create/close-write → hash → upload via sidecar (SDK handles block splitting/encryption) → SQLite mapping updated. Debounce rapid writes; retry with exponential backoff; resumable block uploads for large files. **M4 ✅:** exponential schedule verified (`base·2ⁿ` capped at 5 min, unit-tested); a server `Retry-After` hint (rate-limit 1003 → `transfer.error.retryAfter`) is honored in place of the computed schedule; explicit resume drops pending backoffs ("try now"); a terminally errored node heals on a fresh local edit.

### F3 — Trash on local delete (conservative)

1. Only propagate deletes for nodes **previously confirmed synced** (present in SQLite). Unknown/never-synced items deleted locally are simply removed locally.
2. Remote action is always **Trash** (recoverable in Proton Drive), never permanent delete.
3. **Mass-deletion guard:** >20 deletes or >25% of synced nodes within a short window → auto-pause + user confirmation dialog.
4. Offline deletes are recorded as tombstones and applied on resume/reconnect — same guard applies.

### F4 — Pause / resume

Pause: freezes upload + download queues mid-transfer (block-level resume), stops event application (cursor kept), watcher keeps buffering changes. Resume: drains tombstones (with guard), then uploads, then applies pending remote events.

### F5 — On-demand placeholders (SSD bloat control; M3, desktop default)

- All remote items visible in the sync root at ~0 bytes locally.
- **Desktop app (M3.4 ✅ 2026-10-30):** placeholder mode is the **default for new sync roots**. The app mounts the FUSE view **at the sync root itself** (`~/ProtonDrive` *is* the placeholder view), so users find files where they expect them. The engine's state (SQLite DB, backing cache) lives under `$XDG_DATA_HOME/proton-drive-sync/roots/<hash>/`, outside the root — a directory cannot serve both its real contents and a mount over it, and running the DB through the mount would risk corruption. The mount goes up only after the engine's initial sync (the first-run local scan must see the real folder). If FUSE is unavailable the app falls back to a materialized session and says so in Settings. A **Settings → Sync mode** toggle switches between *On demand* and *Mirror* (confirm dialog; the engine restarts into the new mode). Mode is persisted per root in the DB's `meta` table (`sync_mode`); `$PDS_SYNC_MODE` overrides for one session; roots with a legacy (pre-0.3) in-root state DB default to mirror so upgrades change nothing for existing users.
- **Conversion:** switching modes moves existing content instead of re-transferring — mirror→placeholders moves every tracked file into the backing cache (hydrated, no download; untracked leftovers become dirty pending-uploads so nothing is stranded under the mount), placeholders→mirror moves hydrated files back into the root (never evicted content re-downloads via the normal materialization pass). Idempotent, crash-restartable, never deletes.
- **Hydration:** on file open (FUSE read), content is fetched on demand.
- **Write-through (M3.2 ✅ 2026-10-29):** files created through the mount upload on close (`release`/`fsync`); `mkdir`/`unlink`/`rmdir` mirror remotely (trash path). Backing content lives in `.pds-vfs-backing/`; uploads reuse the standard engine machinery (F7 collision check, retries, pause buffering). The mount itself is RW.
- **Revision writes (M3.3 ✅ 2026-10-29):** snapshot-backed files open RW through the mount; the open waits (bounded) for hydration, edits land on the backing file, and `release`/`fsync` upload the revision against the existing `node_uid` (F7 `check_remote_head` guards stale heads). `O_TRUNC` (e.g. `fs::write`) works: fuser does not negotiate `ATOMIC_O_TRUNC`, so truncation arrives as `setattr(size, fh)` and defers to the open handle's `release` — one upload per write sequence, no empty intermediate. `truncate(2)` outside a handle queues the upload directly.
- **Pin/unpin (M3.2 ✅ 2026-10-29):** `pds pin <folder> <path>` marks the row pinned in the DB and hydrates it immediately; folder paths pin recursively. Pinned files are never evicted.
- **Eviction (M3.2 ✅ 2026-10-29):** LRU cache with a configurable size cap. Never evicts pinned, dirty (un-uploaded), or open files, nor the file that just hydrated. Runs on the engine's idle path after hydrations; the snapshot flips `content_ready` back so the placeholder reappears immediately. **Cap (M3.5 ✅ 2026-10-30):** configurable on three surfaces — a per-root persisted Settings choice (Settings → Sync mode → Local cache, presets 2/5/10/25 GB or Unlimited), `$PDS_CACHE_CAP` (session-only), and `pds mount --cache-cap <SIZE>` (one invocation) — with precedence flag > env > Settings > the 10 GiB default. Settings changes apply **live** via `Command::SetEvictionCap` (immediate eviction pass when the new cap is already exceeded); `unlimited` disables eviction on every surface.

M3 progress (2026-10-29): placeholder mount, hydration, write-through, pins, LRU eviction, and in-place revision writes all work end-to-end (engine tests + real-kernel FUSE E2E in the VM). M3 is complete; M4 (hardening) next.

### F6 — Remote → local reconciliation

Sidecar consumes the SDK event stream; host applies create/update/trash events to the local tree. Cursor persisted so restarts catch up without full scans.

### F7 — Conflicts

Keep-both: the losing version is renamed `name (conflict YYYY-MM-DD HHmm).ext`; no silent overwrites in either direction.

### F8 — Background service, start at login, tray (M5 ✅ 2026-10-31)

The desktop app is the long-running sync service; there is no separate daemon.

- **Start at login:** an XDG autostart entry
  (`$XDG_CONFIG_HOME/autostart/proton-drive-sync.desktop`) launches
  `proton-drive-sync --minimized` after the graphical session comes up
  (systemd user units are not used: the tray needs the session and the FUSE
  mount must share the process lifetime). Installed builds enable it on
  first run; `Settings → Startup` toggles it, and the decision is persisted
  in `$XDG_CONFIG_HOME/proton-drive-sync/app.conf` so a manual removal is
  never silently undone. The `Exec=` path preserves distribution wrappers
  (`argv[0]`) so Nix/Node environment injection survives.
- **Tray icon:** StatusNotifierItem via `gpui-tray` (zbus on Linux). The icon
  is drawn in code (no asset/theme dependency) with a status dot: running,
  paused, delete-guard, sign-in required, stopped. Menu: Open Dashboard,
  Pause/Resume, Open Sync Folder, Preferences…, Quit; primary activation
  opens the dashboard.
- **Window lifecycle:** closing the dashboard minimizes it while the tray is
  usable (GPUI 0.2's Linux backend stops the event loop when the last window
  is destroyed, and exposes no unminimize); with no tray host the close
  quits and login start opens the dashboard instead of hiding it. `Quit`
  lives in the tray and in Settings.
- **Single instance:** a later launch sends `show` over
  `$XDG_RUNTIME_DIR/proton-drive-sync.sock` and exits, so two engines can
  never share one SQLite DB / FUSE mount.
- Tray availability is probed over D-Bus (`org.kde.StatusNotifierWatcher`).
  GNOME needs the AppIndicator extension; bare compositors need a panel
  (waybar etc.). The app degrades to "dashboard at login" without one.
- Explicitly **not** done: syncing while nobody is logged in (no system
  daemon), multiple instances, and GNOME's built-in tray (it has none).

## 5. Compliance requirements (from Proton's SDK terms)

- `x-pm-appversion: external-drive-{name}@{semver}-{channel}` — honest, non-spoofed.
- Event-based sync only; SDK-grade caching, parallelism limits, exponential backoff.
- No Proton logos/branding; clearly unofficial; credential-prompt disclosure text.

## 6. UI outline (GPUI, Linux)

One main window: sidebar navigation (**Overview / Activity / Conflicts / Settings**) · account + engine-state badge · big pause/resume toggle · activity/event log · conflicts + failed-uploads inbox (from the state DB) · settings for the sync root, the **sync mode** (on-demand placeholders ⇄ mirror, with a confirm dialog and one-time layout notices), and **start at login**. Browser-based sign-in with the mandated disclosure; sign-out confirmation modal. Keyboard-first: actions bound in `crates/app/src/actions.rs`.

A tray icon mirrors the engine state and offers Open Dashboard / Pause-Resume / Open Sync Folder / Preferences / Quit (F8). Closing the window keeps the service running; the dashboard is one click away.

## 7. Milestones

- **M0 — Spike:** headless Node script proving auth + list/upload/download/trash + event stream via the SDK. *Go/no-go gate.* ✅ 2026-10-23.
- **M1 — Headless sync engine (CLI):** local folder ↔ Drive, F2/F3/F4/F7 with real files (no VFS yet), SQLite state. ✅ 2026-10-24 (live E2E).
- **M2 — GPUI desktop shell (Linux):** single-window app; login, overview, activity, conflicts, settings, sign-out. ✅ 2026-10-29 (rewritten from the v0.1 tray shell when the project went Linux-only).
- **M3 — Linux VFS:** FUSE mount, placeholders, hydration (M3.1 ✅ 2026-10-29), write-through + pins + LRU eviction (M3.2 ✅ 2026-10-29, E2E-proven in the VM), revision writes (M3.3 ✅ 2026-10-29), desktop integration: placeholder mode as the app default, mount at the sync root, external engine state, mode setting + safe conversion (M3.4 ✅ 2026-10-30).
- **M4 — Hardening:** conflict soak tests, backoff/rate-limit validation, SDK upgrade rehearsal (M4 ✅ 2026-10-30 — see `docs/sdk-upgrade-rehearsal.md`; soak found + fixed the F7 remote-delete race, crash-interrupted transfers now recover on startup).
- **M5 — Background service, login start, tray (F8):** ✅ 2026-10-31 — XDG autostart with a persisted toggle, StatusNotifierItem tray with status icon and actions, close-to-tray window lifecycle, single-instance socket, no-tray-host fallback.

## 8. Key risks

1. **Auth is DIY** — biggest unknown; de-risked in M0 (reference: first-party clients, `proton-sdk-rs2`).
2. **Crypto migration (end 2026/early 2027)** — mitigated by pinning to the official SDK and tracking releases.
3. **SDK interface churn** — isolate all SDK calls behind one sidecar module.
4. **GPUI on Linux** — GPU drivers vary widely; the app must start (or degrade cleanly) under software rendering (llvmpipe); validated with Xvfb before each release.

## 9. Open questions

- ~~FUSE mountpoint UX: M3.1 ships the **parallel mount** (`pds mount <folder> <mountpoint>`); whether to also bind-mount into the real root is deferred until desktop-app UX decides where users look for files.~~ **Resolved (M3.4):** the desktop app mounts the placeholder view **at the sync root** and keeps all engine state (SQLite, backing cache) in `$XDG_DATA_HOME/proton-drive-sync/roots/<hash>/` outside it; the parallel `pds mount` remains for CLI use.
- ~~FUSE write-through (M3.2): direct vs through-the-watcher~~ **Resolved (M3.2):** neither — the VFS queues writes in the engine↔VFS `Store` (`record_write`/`record_mkdir`/`record_delete`) and the engine's main loop drains them through the same upload/delete machinery the watcher uses (F7 precondition, retries, pause buffering). No watcher noise from the backing dir; no duplicate dedup logic in the FUSE layer.
- Exact mass-deletion guard thresholds after real-world usage data.
