# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.6.2 - 2026-11-09


### Fixed

- Include GLib in runtime libraries (#58)
- Correct binary smoke test (#60)
- Restore sign-in and engine startup (#61)

## v0.6.1 - 2026-11-09


### Fixed

- Include GLib in runtime libraries (#58)

## v0.6.0 - 2026-11-08


### Added

- Full MyGo UI replacement and headless engine promotion
- Change the sync folder from Settings
- Phase 2 skeleton — protocol-compatible Go engine socket server
- Port guard, conflict naming, and state DB with test batteries
- Port VFS store with hydration, write queues, LRU, and overlay
- Port mode — sync mode, root-key hashing, cache cap
- Port sidecar client — JSON-RPC stdio, spawn discovery, mock
- Port vfs-linux — FUSE placeholder view on go-fuse
- Port sync engine core — mirror-mode pipeline, watcher, backend wiring
- Placeholder-mode orchestration — layout conversion, FUSE probe/mount, session loop
- Implement placeholder-mode hydration, eviction, pins, and write-through
- Default to Go engine with Rust fallback

### Changed

- Clippy cleanups
- Split main.go into component-based files

### Documentation

- Add runtime MyGo shell screenshots
- Capture app screenshots per-window without stealing focus
- Add headless UI testing skill; cross-link Tester and screenshots
- Live-account E2E runbook for the Go engine
- Correct runbook finding on empty event cursor
- Live upload+trash paths proven against the real account
- Refresh runtime shots with populated sync state

### Fixed

- Runtime layout and truncation issues
- Match the GPUI shell design 1:1
- Tray deadlock, settings scrolling, engine state casing, login card layout
- Open sync folder in file manager instead of terminal
- Failure and splash screens match the GPUI card layout
- Add busy_timeout so engine restarts survive the WAL handoff
- Retain open fds across unlink/rename-over and fix .pds readdir
- Apply dir rename pairs parents-first (sortByDepth was a stub)

### Miscellaneous

- Add opencode skills for mygo UI work and screenshot verification
- Mark fsnotify as a direct dependency
- Add Go engine test leg alongside the Rust suite
- Remove the Rust engine after the Go port
- Remove the Rust cross-compat test; bump devenv Go to 1.27
- Consolidate to the runtime shot set

### Other

- Phase 1 mygo migration poc

### Testing

- Drive the change-folder UI headlessly with mygo Tester
- Port remaining Rust engine battery
- Port FUSE e2e battery and soak suite
- Full-engine kernel battery plus placeholder soak and conversion unskips
- Widen waitFor timeout for loaded hosts
- Tempdir for sidecar mock upload fixtures

## v0.5.9 - 2026-11-04


### Added

- Hide the dashboard with gpui-ce on every compositor

### Fixed

- Park the dashboard on niri instead of quitting (#51)
- Never open a window on a login start

## v0.5.8 - 2026-11-04


### Fixed

- Close to the tray only where minimize actually works (#49)

## v0.5.7 - 2026-11-04


### Added

- Preserve file mtime and executable bit locally
- Refuse busy and stale placeholder mounts
- Honor POSIX semantics for mmap, writes, statfs, and special files

### Fixed

- Re-key remote renames and keep both on collisions
- Preserve node identity across local lifecycle events
- Keep syncing through environment failures
- Park sync work across session expiry and sidecar loss
- Preserve name identity for case-only renames and long paths
- Persist download state and recover interrupted downloads
- Index ancestor folder inodes before rename detection
- Close mass-delete, eviction, and reserved-name gaps
- Harden conflict naming and mid-flight settings
- Draw a titlebar with a close button

### Other

- Merge main into t3/fd82cb41
- Relax conflict dedupe assertion across minute boundaries
- Merge main into t3/9d0fbfc9
- Merge main into t3/ea63b762
- Merge commit 'f5d7b0b' into t3/ea63b762
- Merge main into t3/9d0fbfc9
- Merge main into t3/f0c8bcce
- Merge main into t3/f40a2442
- Merge remote-tracking branch 'origin/main' into t3/f0c8bcce
- Merge main into t3/9df11324
- Merge main into t3/2f9c704f
- Merge remote-tracking branch 'origin/main' into t3/2f9c704f
- Merge remote-tracking branch 'origin/main' into t3/9df11324
- Merge main into t3/9df11324

### Testing

- Cover large sparse files and many-file convergence
- Wait for pause to apply before re-creating file
- Wait for parent rows after folder re-key
- Wait for cursor advance after trash and restore
- Wait for pause to apply before re-creating file
- Wait for parent rows after folder re-key
- Wait for cursor advance after trash and restore
- Cover session expiry and sidecar crash mid-transfer
- Wait for pause to apply before re-creating file
- Wait for parent rows after folder re-key
- Wait for cursor advance after trash and restore
- Wait for pause to apply before re-creating file
- Wait for parent rows after folder re-key
- Wait for cursor advance after trash and restore
- Cover wave 5b scale, settings, and conflict gaps
- Wait for pause to apply before re-creating file
- Wait for parent rows after folder re-key
- Wait for cursor advance after trash and restore

## v0.5.6 - 2026-11-04


### Fixed

- Start at login in the tray without opening the window (#34)

## v0.5.5 - 2026-11-04


### Fixed

- Answer copy_file_range instead of splicing through the page cache (#32)

## v0.5.4 - 2026-11-03


### Fixed

- Start-at-login toggle no longer collapses at narrow window widths (#28)
- Stop losing the destination of an overwrite-rename (#30)

## v0.5.3 - 2026-11-03


### Fixed

- Rmdir no longer fails ENOTEMPTY right after emptying a folder

## v0.5.2 - 2026-11-02


### Fixed

- Writable open of overlay file no longer fails EIO

## v0.5.1 - 2026-11-02


### Fixed

- Overwrite-rename no longer loses the destination file (#22)

## v0.5.0 - 2026-11-02


### Added

- Add trash restore (drive.restore + drive.listTrashed)
- Support rename through the placeholder mount

### Fixed

- Bind ctrl-w to close the window (#14)
- Stop clobbering parent_uid on write-through uploads

### Testing

- Keep resuming after guard trips in delete tests (#15)

## v0.4.2 - 2026-11-02


### Fixed

- Bind ctrl-w to close the window (#14)
- Stop clobbering parent_uid on write-through uploads

### Testing

- Keep resuming after guard trips in delete tests (#15)

## v0.4.1 - 2026-11-02


### Fixed

- Bind ctrl-w to close the window (#14)

### Testing

- Keep resuming after guard trips in delete tests (#15)

## v0.4.0 - 2026-11-02


### Added

- Report backing-cache usage in engine status (#12)

### Fixed

- Sync bare mkdirs to the remote and stop rmdir's spurious ENOTEMPTY (#11)

## v0.3.0 - 2026-10-31


### Added

- Background service — XDG autostart, tray icon, single instance (M5/F8)

### Changed

- Extract shared XDG base-dir resolution into pds-xdg

### Documentation

- Require screenshot/video proof in every PR (#9)

### Fixed

- Open folder button on FUSE mount points

### Miscellaneous

- Cut releases through a PR and tag after merge (#5)

### Testing

- Make mass-delete guard tests wait for batched deletes (#6)

## v0.2.2 - 2026-10-31


### Fixed

- Answer both listxattr reply forms and resolve the .pds proxy
- Create the sync root before canonicalizing it

### Miscellaneous

- NixOS dev-shell Vulkan libs, ignore nix result, FUSE gotchas

## v0.2.1 - 2026-10-31


### Fixed

- Serve the placeholder FUSE session (M3.4 freeze) (#3)

## v0.2.0 - 2026-10-31


### Added

- Sync mode setting, external backing cache, and safe mirror<=>placeholders layout conversion (M3.4)
- Placeholder mode by default — mount the FUSE view at the sync root (M3.4)
- Mode-aware sync/mount defaults and mode= in status
- Configurable cache cap — parse/resolve/persist helpers and live SetEvictionCap command (M3.5)
- Settings cache-cap chips with live engine update (M3.5)
- Pds mount --cache-cap and env/persisted resolution (M3.5)

### Documentation

- M3.4 — placeholder-by-default desktop app, sync mode setting
- M3.5 — configurable backing-cache cap

### Fixed

- Stop release.sh from truncating the changelog history

### Testing

- FUSE e2e for mounting over a non-empty sync root
- Live set_cache_cap applies immediately with on-the-spot eviction (M3.5)

## v0.1.0 - 2026-10-30


### Added

- M0 spike — SRP auth, key unlock, SDK bootstrap, upload/download/trash round-trip
- Switch M0 spike to browser session-fork login
- JSON-RPC stdio service per docs/protocol.md
- M1 headless sync engine + pds CLI
- MacOS .app bundle script + Info.plist (LSUIElement)
- M2 GPUI shell — tray app, fork login view, status dashboard
- Sign-out flow — tray item + dashboard button, confirm panel, session wipe
- Drop macOS/Windows, rebuild app as native Linux GPUI client
- M3.1 — read-only FUSE placeholders with on-demand hydration
- Write-through uploads, mkdir/unlink mirroring (M3.2)
- Pins and LRU eviction for the backing cache (M3.2)
- Add nix flake packaging for Linux systems (#2)

### Documentation

- Product specification and agent working notes
- Research reports on Proton auth flow and Drive SDK client API
- Agent notes — M0 code complete, subagent auto-approve rule, sidecar gotchas
- M0 spike PASSED — auth + SDK client viable, go decision confirmed
- Sidecar JSON-RPC IPC protocol v1 for M1
- M1 code complete, live E2E pending
- M1 live E2E test script (scratch account)
- M1 PASSED live E2E — all F7/guard/CLI/event-loop bugs closed and re-validated
- GPUI integration research — crate choice, tray, tokio bridge, packaging
- M2 code complete, pending interactive run
- File Provider research — entitlements, appex IPC, build integration, test plan (M4)
- M2 done (user-approved), M4 active — research complete
- Host<->appex IPC protocol v1 (M4 coordination contract)
- Add experimental/no-liability disclaimer to the README

### Fixed

- Route SDK telemetry to stderr — stdout is the JSON-RPC channel
- E2E bugs #1-3 — F7 remote-head precondition, guard batch approval, per-root CLI
- Keep-both on server-side revision conflict (409), bounded retries
- Defer 409 conflicts to the event keep-both path, self-healing
- Poll the event stream — iterateEvents drains once by design
- GPUI re-entrant AppModel read panic + sidecar EPIPE crash

### Miscellaneous

- Scaffold sidecar (Node/TS), Rust workspace, README
- Add devenv environment and run tests on PRs (#1)

### Other

- In-place revision writes through the FUSE mount
- Hardening — crash recovery, Retry-After, conflict soak, flake fix

### Testing

- M1 live E2E results — F1-F4/F6 pass, F7 race bug found, 3 bugs filed<!-- generated by git-cliff -->
