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
- [x] M3.1 — FUSE placeholders: **done 2026-10-29** — `pds mount` with on-demand hydration; engine placeholder mode (`placeholders: true` → VFS store, backing dir `.pds-vfs-backing/`, no materialization); real-kernel E2E in the VM (3 MB file read through the mount).
- [x] M3.2 — FUSE write-through: **done 2026-10-29** — `create`→upload on close, `mkdir`/`unlink`/`rmdir` mirror remotely; store write-queues drained by the engine main loop (reuses F7/retry/pause machinery); write overlay + `reconcile_overlay` keep pre-upload files visible without double-listing; snapshot-backed files stay RO (revision writes = M3.3). E2E covers create→upload→read-back→mkdir→nested upload→unlink→trash. Pins + LRU eviction next (same milestone).
- [x] M3.2 — FUSE write-through + pins + LRU eviction: **done 2026-10-29** — write-through (see previous entry); `pds pin/unpin <folder> <path>` via the control socket (folders recursive, immediate hydration); LRU eviction (`EngineConfig::eviction_cap`, default 10 GiB in `pds mount`) on the engine idle path — skips pinned/dirty/open/just-hydrated files, publishes the snapshot so placeholders reappear. Tests: store LRU/open-handle unit tests + two engine eviction tests + full FUSE E2E. Revision writes = M3.3.
- [x] M3.3 — FUSE revision writes: **done 2026-10-29** — snapshot-backed files open RW (open waits bounded for hydration, fd on the backing file); `release`/`fsync` upload the revision preserving `node_uid` (F7 `check_remote_head` still guards stale heads); `O_TRUNC` = `setattr(size, fh)` (fuser never negotiates `ATOMIC_O_TRUNC`) → marks the handle dirty, upload deferred to `release` so `open(O_TRUNC)`→write→close is ONE upload, no empty intermediate; bare `truncate(2)` queues via `record_write`. Prerequisite bug fixed: the watcher/initial scan did not exclude `.pds-vfs-backing/`, so hydration/eviction/revision events re-entered the pipeline as phantom uploads/deletes — `is_internal_path()` now guards all three filter sites (+ regression tests). Revision-handle bookkeeping: open-handle tracked (evict-protected), `write`/`fsync`/`release` use the handle's authoritative `rel`. E2E covers revision write (head bytes replaced, tail preserved, same node, one upload), `fs::write` O_TRUNC rewrite, and full M3.2 flow.
- [x] M3.4 — Desktop placeholder integration: **done 2026-10-30** — the app defaults NEW sync roots to placeholder mode and mounts the FUSE view **at the sync root** (`~/ProtonDrive` is the placeholder view). Key decisions: (1) all engine state (SQLite DB, VFS backing cache, `root` marker) lives under `$XDG_DATA_HOME/proton-drive-sync/roots/<hash>/` — OUTSIDE the root — because mounting over the root shadows everything under it and running the DB through FUSE would risk corruption; `crates/core/src/mode.rs` owns these paths (`engine_state_dir`, `root_key`). (2) The app **probes FUSE with a throwaway temp-dir mount BEFORE building the engine** — the engine consumes the sidecar's single notification stream, so a post-hoc mirror fallback would be deaf to remote events; probe failure downgrades the session to mirror (`MountUnavailable` notice in Settings). (3) The real mount goes up **only after `wait_for_watcher`** — the engine's first-run local scan and the layout conversion must see the real folder (the mount serves an empty tree until the first snapshot publish, which would hide every pre-existing local file). (4) Mode is a persisted per-root setting (`meta.sync_mode`, `$PDS_SYNC_MODE` override); Settings toggle → confirm modal → persist → restart the engine thread. Legacy in-root state DBs are adopted into the external dir (copy, original removed after success) and default to mirror so upgrades change nothing. (5) `convert_layout` (engine, runs in `Engine::new` when `backing_dir` is external): mirror→placeholders moves tracked files into the backing cache (hydrated, no re-download) AND adopts untracked leftovers as dirty pending-uploads (nothing stranded under the mount); placeholders→mirror moves hydrated content back; idempotent via a DIRECTION-AWARE `.layout-converted` marker (records which layout it produced — a ran-once marker would strand content on the second toggle), never deletes. (6) Placeholder-mode bug fixed while tracing content paths: F7 conflict renames and remote-name-collision renames wrote into the VISIBLE root instead of the backing dir — all content paths now flow through `content_dir()`. (7) Self-review hardening: `$PDS_SYNC_MODE` is session-only (never rewrites the persisted `sync_mode`); legacy in-root DBs are removed only after the adopted copy verifies as a readable SQLite DB; the app's mount gets an EMPTY `.pds` proxy dir (the fs's state-dir proxy would otherwise serve the live DB + backing cache inside the user-facing mount); the engine never unlinks visible paths in placeholder mode when the root is the mount (an unlink would round-trip through FUSE into a spurious store delete — placeholders vanish via snapshot refresh instead); e2e regression proves mounting over a non-empty root shadows leftovers and preserves every byte. Tests: mode unit tests (incl. env-session-only), `convert_layout` round-trips, `Engine::new` mode-resolution, external-backing adoption integration test, non-empty-root FUSE e2e; full `devenv test` green (Rust + sidecar) plus headless GUI smoke.
- [x] M3.5 — Configurable cache cap: **done 2026-10-30** — the placeholder backing-cache cap (until now hardwired to `EVICTION_CAP_DEFAULT`, 10 GiB) is user-configurable on three surfaces with precedence **`pds mount --cache-cap <SIZE>` > `$PDS_CACHE_CAP` (session-only, never rewrites the persisted value — same rule as `PDS_SYNC_MODE`) > the persisted per-root Settings choice > 10 GiB default**. `unlimited` (no eviction) is expressible on all three. Sizes parse as bytes with an optional binary `K`/`M`/`G`/`T` suffix (`parse_cache_cap`; `0`/`none`/`off`/`unlimited` → unlimited; invalid values are ignored with a warning, never silently disabling eviction). Persistence: meta key `cache_cap_bytes` (Unlimited stored as the literal `unlimited`; unset key = never configured → default). **Settings applies live**: `Command::SetEvictionCap(Option<u64>)` mutates the running engine's cap and runs an immediate `maybe_evict` pass when the new cap is already exceeded (no restart, unlike the mode toggle); `EngineStatus.cache_cap` feeds the Settings chips (2/5/10/25 GB + Unlimited on the Sync mode card, with a `$PDS_CACHE_CAP`-override notice). App engine build resolves env > persisted > default; `pds sync`/`pds mount` resolve the same way (mount additionally honoring the flag). Tests: parse/precedence/persistence unit tests, live `set_cache_cap` integration test (immediate eviction on shrink, no eviction after re-widening to unlimited); full `devenv test` green incl. real-kernel FUSE e2e.
- [x] M4 — Hardening: **done 2026-10-30** — (1) crash recovery: rows stuck `Uploading` from a previous run reset to Dirty at startup (re-uploaded through the full F7/retry machinery; vanished files tombstone pre-approved so the guard never trips on recovery); stale `*.pds-part` temps swept from both content roots. (2) Rate limits: `transfer.error` now carries `data.retryAfter` (sidecar + Rust decode) and the engine honors the server hint over the computed backoff; `retry_backoff` unit-tested. (3) Soak battery (`tests/soak.rs`): repeated simultaneous edits, remote-delete-vs-edit, remote burst convergence, conflict-copy churn — **found a real F7 hole**: `apply_remote_delete` trusted `sync_state` alone, so a local write inside the watcher debounce window was DESTROYED by a concurrent remote delete; now hashes on-disk content first (mirrors `apply_node_upsert`). (4) Test-flake root fix: `EngineHandle::wait_for_watcher` — fixtures synchronized on initial-sync completion, which can precede watcher attach, losing inotify events for immediate writes (the "rotating flake" across engine tests). (5) SDK rehearsal documented (`docs/sdk-upgrade-rehearsal.md`); current stack verified green (SDK 0.23.0, crypto 2.2.0 + openpgp patch, Node 22).

## Linux development (OrbStack VM)

The host Mac is not a build target anymore — `pds-app` does not compile on macOS (gpui Linux backend). Build and test inside the Ubuntu 24.04 arm64 VM:

```sh
ssh pds-linux@orb
cd /Users/christopher/Developer/proton-drive-sync   # repo is shared into the VM
devenv shell                                      # same env as CI (devenv.nix/lock)
export CARGO_TARGET_DIR=$HOME/pds-target          # keep the VM-local build cache
cargo check --workspace --all-targets && cargo test --workspace
```

`devenv test` runs the whole suite (workspace tests + sidecar typecheck/tests)
in the pinned environment; install Nix/devenv in the VM once, then it matches
CI exactly.

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

## Engine timing gotchas (learned in M4)

- **Tests must synchronize on the watcher, not initial sync.** A fixture that returns when initial-sync materializes files can beat the watcher attach (step 3 of `run()`); any file written immediately after is invisible to inotify. That was the "rotating flake" hitting a different engine test each run under VM load. Fix: `EngineHandle::wait_for_watcher` (Arc<AtomicBool> flipped right after `start_watcher`).
- **`apply_remote_delete` cannot trust `sync_state`** — a local write inside the 500 ms debounce window leaves the row `Synced` while the disk content is already new. Hash before deleting (same discipline as `apply_node_upsert`). Found by the soak battery.
- **Attempt counting races failure bookkeeping**: `upload_attempts` increments when the transfer STARTS; the backoff is scheduled when the `transfer.error` is processed. Tests that act between the two (e.g. resume clearing backoffs) must wait for the row to come back Dirty first.
- **Mass-delete guard tests must let the deletes land first.** The watcher's ~500 ms debounce can split a burst of removals into several batches under CI load; a guard-paused engine keeps recording tombstones but drains nothing, so a resume issued after the first batch approves only part of the set and the late ones legitimately re-trip the guard. Synchronize on "every delete accounted for" (`wait_for_deletes_accounted`: pending tombstones + trashed == total) before resuming — CI flake, 2026-10-31.

## fuser / FUSE gotchas (learned in M3.1)

- **A `Filesystem` method that never replies wedges the kernel caller** (not just that request): `read_dir` hangs with no error, no panic. Every path must consume `reply` exactly once.
- **A reply with the wrong data can fail silently at the mount level.** The M3.1 bug: `Tree::node("")` returned `None` for the root (`""` is not in `by_path`), so root `getattr` answered ENOENT; the kernel then failed *all* `read_dir` on the mountpoint with ENOENT while `mount | grep` still showed the fs present and `init`/`getattr` logs looked healthy. Test the root-inode path explicitly.
- **fuser 0.18 API**: trait methods take `&self` (not `&mut self`); `readdir` offset is `u64` (1 = "after entry 0"); `ReplyDirectory::add(ino, offset, kind, name)` returns `true` when the buffer is full; `ReplyStatfs::statfs` takes 8 args (blocks, bfree, bavail, files, ffree, bsize, namelen, frsize); errors are `fuser::Errno::ENOENT`-style constants; `Config` is `#[non_exhaustive]` — mutate `cfg.mount_options` after `Config::default()`, no struct literal.
- `editions = 2024`: `gen` is a reserved keyword — don't name struct fields or locals `gen`.
- Panic in a fuser session thread is swallowed (session thread dies, callers get ENOENT/EIO). Wrap suspect methods with `std::panic::catch_unwind` + `eprintln` when debugging; a custom panic hook also works.

## fuser / FUSE gotchas (learned in M3.2)

- **Inodes the kernel holds must keep resolving.** After `create` replies with an overlay inode, the kernel caches that dentry (attr TTL). If the engine's snapshot then covers the path and the overlay drops its inode mapping, a later `open(overlay_ino)` fails ENOENT. Fix: overlay keeps a `ino → rel` alias (`retire`, not `remove`) so cached dentries resolve; only unlink drops the alias.
- **`write` trait signature (fuser 0.18)** takes 10 params: `_req, ino, fh, offset, data, WriteFlags, OpenFlags, Option<LockOwner>, reply` — easy to under-declare; the compiler error names the trait signature.
- **RW mounts need the flag dropped at the kernel level.** `MountOption::RO` blocks `open(O_CREAT)` before the fs code runs (fs-level EROFS is never asked). Mount RW and enforce per-file RO in `open`/`setattr` instead.
- **The test's tracing filter ignores RUST_LOG** when `with_env_filter("literal")` is hardcoded — read `RUST_LOG` with a fallback or VFS debug logs never show up in `cargo test -- --nocapture`.
- **No `FUSE_ATOMIC_O_TRUNC` (M3.3).** fuser's `INIT_FLAGS` is just `ASYNC_READ | BIG_WRITES`, so `open(O_TRUNC)` never truncates atomically — the kernel follows up with a separate `setattr(size, fh)` *after* `open`. Truncate handling must therefore cover `setattr` with a passed fh, and defer the upload to `release` or `open(O_TRUNC)`→write→close double-uploads (an empty intermediate revision first).
- **Mock default tree (M3.3 E2E).** `MockSidecar::new()` already seeds `hello.txt`; `remote_create_file("root", "hello.txt", …)` in a test creates a DUPLICATE node, and `find_uid` may resolve to the one the engine never touched. Check `child_names` for surprises before asserting on `file_content`.

## fuser / FUSE gotchas (learned in M3.4 app wiring)

- **`listxattr` must choose the reply form from `size`.** `ReplyXattr::size`
  is only valid for the size query (`size == 0`). Answering a request that
  supplied a buffer with the size form makes the kernel fail the caller with
  EIO — symptom: `ls -l` errors on every entry while plain `ls` (names only)
  works.
- **Create the sync root before canonicalizing it.** `prepare_layout`
  canonicalized `~/ProtonDrive` first, so first launch on a fresh machine
  failed before the engine started. `create_dir_all` precedes the
  `canonicalize` now.

(The `Session::new`-without-`spawn()` freeze is covered by the upstream
`fix(app): serve the placeholder FUSE session` commit and its probe test.)

## Standing instructions from the user

- **Every PR must include screenshot/video proof.** Before opening or marking a PR ready, attach visual proof of the change: screenshots for UI changes, a screen recording for flows/interactions, or terminal output for CLI/engine work. If the change isn't visually verifiable, say so explicitly in the PR description.
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
│   ├── cli/           # `pds` — headless engine + status/pause/resume/mount
│   ├── core/          # Sync engine, state DB, queues, trash guards, VFS store
│   ├── vfs-linux/     # FUSE placeholder filesystem (fuser; M3)
│   └── sidecar-client/ # Sidecar spawn + JSON-RPC protocol (+ MockSidecar for tests)
├── sidecar/           # Node.js + official Proton Drive SDK
└── packaging/linux/   # .desktop entry, icon, install script
```

## Conventions

- Dev environment: `devenv.nix` (pinned by `devenv.lock`) is the source of truth for
  toolchains and native deps; CI runs `devenv test`. Add new libraries there, not as
  ad-hoc apt/brew installs.
- Rust: edition 2024, `cargo clippy -- -D warnings` clean, `tokio` for async.
- Sidecar: TypeScript, strict mode, ESM. SDK version pinned exactly in `package.json`.
- Commit messages: conventional commits (`feat:`, `fix:`, `chore:` …) — they
  drive the changelog (`cliff.toml`), so keep scopes meaningful.
- Releases: single workspace version (`[workspace.package]` in `Cargo.toml`;
  crates inherit). Cut releases only via `scripts/release.sh <major|minor|patch>`
  — it syncs the sidecar `package.json`/`APP_VERSION` (Proton compliance
  string) and the Nix derivations, regenerates `CHANGELOG.md` (git-cliff), and
  opens a `release/vX.Y.Z` PR. `main` is protected: merge it by PR, never push
  the bump directly. After the merge, `.github/workflows/tag-release.yml` tags
  `vX.Y.Z` and dispatches `.github/workflows/release.yml` (tarball + deb + rpm);
  tags pushed by CI do not fire `push: tags`, hence the explicit dispatch.
  `scripts/check-versions.sh` runs in CI to catch drift. `actionlint` +
  `shellcheck` are in the devenv — run them after touching workflows or
  release scripts.
- When a milestone is completed, check it off above and note any spec deviations in `SPEC.md` §9.
