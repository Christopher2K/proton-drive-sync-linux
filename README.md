# proton-drive-sync

A **Linux** desktop app that syncs a folder with [Proton Drive](https://proton.me/drive) — files you drop into `~/ProtonDrive` upload automatically, remote changes appear locally, and deletions are conservative (remote deletes go to Trash, with a mass-delete guard).

- **Product spec:** [SPEC.md](SPEC.md) — source of truth
- **Agent notes:** [AGENT.md](AGENT.md)

Stack: Rust + GPUI (native Linux desktop UI) · Node.js sidecar running the official [Proton Drive SDK](https://github.com/ProtonDriveApps/sdk) · SQLite state. (macOS and Windows were supported in an earlier iteration and have been dropped.)

> This is a third-party application not officially supported by Proton.

## Features (M1/M2, materialized folder)

- Browser-based Proton sign-in (session-fork; no password handling in the app)
- Two-way sync of one local root (`~/ProtonDrive` by default) with your Drive
- Upload on new/changed files, download on remote create/update, trash-on-delete
- Mass-deletion guard: bulk deletes pause sync until you confirm
- Pause / resume; conflict handling keeps both versions (`name (conflict …).ext`)
- Native GPUI window: Overview, Activity, Conflicts, and Settings pages

On-demand placeholder files (open a file → it hydrates) are planned for M3 via FUSE; today the sync root is a fully materialized folder.

## Development environment (devenv)

The Rust toolchain (pinned in `rust-toolchain.toml`), Node.js, and every native
library GPUI/FUSE need are declared in `devenv.nix` and locked in
`devenv.lock`, so local development and CI build against the same environment.

Install [Nix](https://nixos.org/download) and [devenv](https://devenv.sh/getting-started/)
once, then from the repository root:

```sh
devenv shell        # enter the environment
# or, with direnv installed: direnv allow
```

`devenv test` builds the environment and runs the full test suite. FUSE tests
additionally need the host's setuid `fusermount3` (package `fuse3`), which Nix
cannot provide; the shell points `fuser` at the host binary automatically.

## Build (Linux)

```sh
cargo build --release -p pds-app -p pds-cli
```

A GPU stack for GPUI (Vulkan/Mesa; under a Wayland or X11 session this is
already present) is not part of devenv — it is host-specific.

Run from the repository root (dev mode — the sidecar is found relative to the repo):

```sh
cargo run -p pds-app
```

Environment knobs:

| Var | Meaning |
|---|---|
| `PDS_SYNC_ROOT` | Sync folder (default: `~/ProtonDrive`) |
| `PDS_SIDECAR_CMD` | Sidecar command override, e.g. `node sidecar/dist/index.js` |
| `PDS_SIDECAR_DIR` | Sidecar directory override for auto-discovery |
| `RUST_LOG` | Log filter (default `info,pds_core=debug`), logs go to stderr |

## Install (system-wide)

```sh
cargo build --release -p pds-app -p pds-cli
sudo packaging/linux/install.sh            # PREFIX=/usr/local by default
# or, user-local (no root):
PREFIX=$HOME/.local packaging/linux/install.sh
```

This installs the `proton-drive-sync` binary, the `pds` CLI, the sidecar (with vendored npm deps), a `.desktop` entry, and an icon. The app then appears in your application menu.

## Install (Nix)

On Nix-based Linux systems (NixOS or any distro with Nix), build and install straight from this repository:

```sh
# try the app without installing (apps: proton-drive-sync, pds)
nix run .#proton-drive-sync

# install the app + CLI + sidecar into a profile
nix profile install .#default
```

Or from a remote machine directly:

```sh
nix profile install github:Christopher2K/proton-drive-sync-linux
```

The flake exposes `packages.proton-drive-sync` (app + CLI + sidecar, wrapped so
the binaries find their sidecar in the Nix store), `packages.sidecar`, and a
`nix develop` shell mirroring `devenv.nix`. Only Linux systems are supported —
the GPUI shell does not build on macOS. The Rust tests run via
`nix flake check` (the FUSE e2e test self-skips without `/dev/fuse`).

## CLI companion

`pds sync` runs the engine headless; `pds status|pause|resume` control a running engine; `pds conflicts` lists the conflict inbox. `pds mount <folder> <mountpoint>` serves a **placeholder view** of a sync root (requires `/dev/fuse`): files hydrate from Drive on first open, files/folders you create through the mount upload to Drive on close, and edits to existing files upload as new revisions of the same Drive file (M3). Hydrated content is an LRU cache capped at 10 GiB — `pds pin <folder> <path>` keeps a file (or folder, recursively) offline-safe, `pds unpin` releases it. See `pds --help`.

## Testing

```sh
devenv test                     # Rust workspace + sidecar typecheck/tests
devenv shell -- cargo test --workspace   # Rust only, inside the env
```

Notes for VM development (OrbStack et al.): inotify over shared folders is unreliable — run the engine tests on VM-native disk. See [AGENT.md](AGENT.md) for details.
