# AGENT.md — Working notes for coding agents

## What this project is

A cross-platform (macOS + Linux) desktop app that syncs Proton Drive into a local folder with on-demand placeholder files. **Read `SPEC.md` first** — it is the source of truth for features, architecture, and milestones.

## Tech stack

- **Host app:** Rust + [GPUI](https://www.gpui.rs/) (UI, orchestration, sync engine, SQLite state)
- **Drive SDK sidecar:** Node.js + official [`ProtonDriveApps/sdk`](https://github.com/ProtonDriveApps/sdk) (TypeScript client module), spawned as a child process, JSON-RPC over stdio
- **VFS:** Linux → FUSE (`fuser` crate); macOS → Swift File Provider extension (fallback option: FUSE-T/macFUSE — see SPEC.md §9)
- **State:** SQLite

## Hard rules (non-negotiable)

1. **Conservative deletes.** Remote deletes are always Trash (never permanent), only for nodes previously confirmed synced in SQLite, and subject to the mass-deletion guard (SPEC.md F3). An empty local folder must never wipe the Drive.
2. **Proton compliance.** Event-based sync only (no polling, no recursive tree scans). Set an honest `x-pm-appversion` header in the `external-drive-{name}@{semver}-{channel}` format. No Proton branding. Login UI must show: *"This is a third-party application not officially supported by Proton."*
3. **All Drive API traffic goes through the Node sidecar.** Never call Proton endpoints from Rust directly — the sidecar isolates SDK churn and the upcoming 2026/27 crypto migration.
4. **Keep-both on conflicts** — never silently overwrite in either direction.
5. **Never evict** pinned, dirty (un-uploaded), or open files from the local cache.

## Current status

- [x] Product specification written (`SPEC.md`)
- [ ] M0 — Spike: Node sidecar auth (SRP/2FA) + SDK list/upload/download/trash + event stream ← **code complete, awaiting live run with real credentials**
- [ ] M1 — Headless sync engine (CLI)
- [ ] M2 — GPUI shell
- [ ] M3 — Linux FUSE VFS
- [ ] M4 — macOS File Provider
- [ ] M5 — Hardening

## Standing instructions from the user

- **Auto-approve subagent permission prompts** (user's directive, 2026-10-23): when Herdr subagents block on permission dialogs, approve them without asking, as long as the request is research/build-scoped and non-destructive.
- Research reports live in `docs/research/` — consult them before touching auth or SDK code.

## Sidecar gotchas (learned in M0)

- `@protontech/crypto` ships raw TypeScript → always run via `tsx`, tsconfig uses Bundler resolution + `allowImportingTsExtensions`.
- `Uint8Array` base64/hex polyfill must be the first import (`src/polyfill.ts`); types live in `src/types/uint8array-encoding.d.ts`.
- `openpgp/lightweight` has no Node export → patched via `patch-package` (see `sidecar/patches/`), applied by `postinstall`.
- SDK deep imports need the `dist/` prefix: `@protontech/drive-sdk/dist/cache`.
- Never run the spike repeatedly with wrong credentials — `/auth/*` is the most rate-limited/CAPTCHA-prone path in the API.

## Repo layout (planned)

```
proton-drive-sync/
├── SPEC.md            # Product spec — source of truth
├── AGENT.md           # This file
├── crates/
│   ├── app/           # GPUI application (M2+)
│   ├── core/          # Sync engine, state DB, queues, trash guards (M1)
│   ├── sidecar/       # Rust side of sidecar IPC (spawn, JSON-RPC protocol)
│   └── vfs-linux/     # FUSE mount (M3)
├── sidecar/           # Node.js + official Proton Drive SDK (M0)
└── macos/
    └── FileProvider/  # Swift extension (M4)
```

## Conventions

- Rust: edition 2024, `cargo clippy -- -D warnings` clean, `tokio` for async.
- Sidecar: TypeScript, strict mode, ESM. SDK version pinned exactly in `package.json`.
- Commit messages: conventional commits (`feat:`, `fix:`, `chore:` …).
- When a milestone is completed, check it off above and note any spec deviations in `SPEC.md` §9.
