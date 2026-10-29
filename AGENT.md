# AGENT.md — Working notes for coding agents

## What this project is

A **Linux** desktop app that syncs Proton Drive into a local folder. **Read `SPEC.md` first** — it is the source of truth for features, architecture, and milestones. macOS and Windows support was dropped in v0.2; do not reintroduce platform-conditional code without asking.

## Tech stack

- **Host app:** Rust + [GPUI](https://www.gpui.rs/) 0.2 (UI, orchestration, sync engine, SQLite state) — native Linux desktop window (Wayland/X11)
- **Drive SDK sidecar:** Node.js + official [`ProtonDriveApps/sdk`](https://github.com/ProtonDriveApps/sdk) (TypeScript client module), spawned as a child process, JSON-RPC over stdio
- **VFS (M3, planned):** FUSE (`fuser` crate)
- **State:** SQLite

## Hard rules (non-negotiable)

1. **Conservative deletes.** Remote deletes are always Trash (never permanent), only for nodes previously confirmed synced in SQLite, and subject to the mass-deletion guard (SPEC.md F3). An empty local folder must never wipe the Drive.
2. **Proton compliance.** Event-based sync only (no polling, no recursive tree scans). Set an honest `x-pm-appversion` header in the `external-drive-{name}@{semver}-{channel}` format. No Proton branding. Login UI must show: *"This is a third-party application not officially supported by Proton."*
3. **All Drive API traffic goes through the Node sidecar.** Never call Proton endpoints from Rust directly — the sidecar isolates SDK churn and the upcoming 2026/27 crypto migration.
4. **Keep-both on conflicts** — never silently overwrite in either direction.
5. **Never evict** pinned, dirty (un-uploaded), or open files from the local cache.

## Current status

- [x] Product specification written (`SPEC.md`, v0.2 Linux-only)
- [x] M0 — Spike: **PASSED 2026-10-23** against a real account — fork auth, key unlock, SDK bootstrap, list, upload, verified download, trash, event cursor (FastForward). Go decision confirmed.
- [x] M1 — Headless sync engine: **PASSED live E2E 2026-10-24** (scratch account) — F1–F4, F6, F7 all verified against real Proton Drive. 54/54 Rust tests, sidecar smoke + event-loop tests green. E2E bugs found & fixed: telemetry-on-stdout, F7 stale-base race (server 409 → parked → event-path keep-both), guard cascade, CLI per-root resolution, event-stream polling (sidecar now polls /events/{cursor} every ~30s).
- [x] M2 — GPUI shell: **done 2026-10-24** (tray app). **Rewritten 2026-10-29** as a single-window Linux desktop app (sidebar + Overview/Activity/Conflicts/Settings, login screen, sign-out modal) when macOS support was dropped; `gpui-tray` removed.
- [ ] M3 — Linux FUSE VFS ← **next**
- [ ] M4 — Hardening

## Linux development (OrbStack VM)

The host Mac is not a build target anymore — `pds-app` does not compile on macOS (gpui Linux backend). Build and test inside the Ubuntu 24.04 arm64 VM:

```sh
ssh pds-linux@orb
cd /Users/christopher/Developer/proton-drive-sync   # repo is shared into the VM
export PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/pds-target
cargo check --workspace --all-targets && cargo test --workspace
```

Notes:

- `libc` is pinned to `0.2.186` in `Cargo.lock` — `0.2.19x` removed `ENOATTR` on Linux, breaking `xattr 0.2.3` (via `zed-async-tar`/`gpui_http_client`). Re-pin after xattr updates.
- The repo sits on a virtiofs share in the VM: **inotify events are unreliable there**, so the local-watcher engine tests can flake under load (`cargo test -p pds-core --test engine`). They pass on VM-native disk (copy the tree to `~/pds-src`) and on macOS.
- GUI smoke test without a desktop: `xvfb-run -a target/debug/proton-drive-sync --headless` proves the event loop. For real UI screenshots use a headless Wayland compositor — Xvfb maps the window but Vulkan cannot present there (captures come back black):

  ```sh
  WLR_BACKENDS=headless WLR_LIBINPUT_NO_DEVICES=1 WLR_RENDERER=pixman \
    XDG_RUNTIME_DIR=/tmp/xdg sway -c /dev/null &   # mkdir -p /tmp/xdg; chmod 700 first
  WAYLAND_DISPLAY=wayland-1 PDS_SYNC_ROOT=/tmp/root target/debug/proton-drive-sync &
  sleep 15 && WAYLAND_DISPLAY=wayland-1 grim shot.png
  ```

  Rendering works with Mesa lavapipe (`VK_ICD_FILENAMES` unset — pointing it at a wrong filename *disables* software Vulkan).

## Standing instructions from the user

- **Auto-approve subagent permission prompts** (user's directive, 2026-10-23): when Herdr subagents block on permission dialogs, approve them without asking, as long as the request is research/build-scoped and non-destructive.
- Research reports live in `docs/research/` — consult them before touching auth or SDK code.

## Sidecar gotchas (learned in M0)

- `@protontech/crypto` ships raw TypeScript → always run via `tsx`, tsconfig uses Bundler resolution + `allowImportingTsExtensions`.
- `Uint8Array` base64/hex polyfill must be the first import (`src/polyfill.ts`); types live in `src/types/uint8array-encoding.d.ts`.
- `openpgp/lightweight` has no Node export → patched via `patch-package` (see `sidecar/patches/`), applied by `postinstall`.
- SDK deep imports need the `dist/` prefix: `@protontech/drive-sdk/dist/cache`.
- Never run the spike repeatedly with wrong credentials — `/auth/*` is the most rate-limited/CAPTCHA-prone path in the API.
- **Password-SRP `POST /auth/v4` is rejected for `external-drive-*` clients** (422 code 2028 "This version of the app is no longer supported", observed 2026-10-23). The browser session-fork flow (`src/fork.ts`, same as Proton's own CLI and pdcli) is the primary auth path: no password handling, no CAPTCHA risk, key password arrives in the fork payload. SRP code in `src/auth.ts` is kept for reference behind `PDS_AUTH=srp`.
- Fork sign-in URL format: `https://account.proton.me/desktop/login?app=drive&pv=3#payload=0:{UserCode}:{base64-AES-key}:external-drive`; poll `GET /auth/v4/sessions/forks/{selector}` (422 = not ready).

## Repo layout

```
proton-drive-sync/
├── SPEC.md            # Product spec — source of truth
├── AGENT.md           # This file
├── crates/
│   ├── app/           # GPUI Linux desktop app (single window)
│   ├── cli/           # `pds` — headless engine + status/pause/resume control
│   ├── core/          # Sync engine, state DB, queues, trash guards
│   └── sidecar-client/ # Sidecar spawn + JSON-RPC protocol (+ MockSidecar for tests)
├── sidecar/           # Node.js + official Proton Drive SDK
└── packaging/linux/   # .desktop entry, icon, install script
```

## Conventions

- Rust: edition 2024, `cargo clippy -- -D warnings` clean, `tokio` for async.
- Sidecar: TypeScript, strict mode, ESM. SDK version pinned exactly in `package.json`.
- Commit messages: conventional commits (`feat:`, `fix:`, `chore:` …).
- When a milestone is completed, check it off above and note any spec deviations in `SPEC.md` §9.
