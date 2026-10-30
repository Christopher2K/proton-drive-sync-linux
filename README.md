# proton-drive-sync

> **Experimental, unofficial, and used at your own risk.** Not affiliated with
> Proton. Built for my own personal use case; I am not responsible for any data
> loss. See [Read this first](#read-this-first-important).

A **Linux** desktop app that syncs a folder with [Proton Drive](https://proton.me/drive) — files you drop into `~/ProtonDrive` upload automatically, remote changes appear locally, and deletions are conservative (remote deletes go to Trash, with a mass-delete guard).

- **Product spec:** [SPEC.md](SPEC.md) — source of truth
- **Agent notes:** [AGENT.md](AGENT.md)

Stack: Rust + GPUI (native Linux desktop UI) · Node.js sidecar running the official [Proton Drive SDK](https://github.com/ProtonDriveApps/sdk) · SQLite state. (macOS and Windows were supported in an earlier iteration and have been dropped.)

## Read this first (important)

- **Experimental.** This project is a work in progress and is **not** ready for
  production or for data you cannot afford to lose. Expect bugs, sync edge
  cases, and breaking changes.
- **Not affiliated with Proton.** This is an independent, third-party project.
  It is not endorsed by, supported by, or connected to Proton AG in any way,
  and it is not an official Proton product.
- **Personal project.** I built this for my own personal use case. With Proton
  Drive now having an [official SDK](https://github.com/ProtonDriveApps/sdk), I
  wanted to build a Linux desktop sync client on top of it for fun and for my
  own workflow. Your needs may differ.
- **Use at your own risk.** I am **not responsible for any data loss** or
  corruption caused by using this software. Keep independent backups of
  anything important before pointing this at a folder.

If any of that is a dealbreaker, please use the official Proton Drive apps
instead.

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

## Releases

Releases are versioned as a single product (`MAJOR.MINOR.PATCH`, semver while
pre-1.0: breaking changes bump the minor). One version is shared by the app,
CLI, sidecar, and Nix packages — `scripts/check-versions.sh` enforces this in
CI.

To cut a release (inside the devenv shell, so `git-cliff` and `cargo` are
available):

```sh
scripts/release.sh minor        # or: patch | major
git push origin main vX.Y.Z     # review the diff first
```

The script bumps every version site, regenerates `CHANGELOG.md` from
conventional commits, and tags `vX.Y.Z`. Pushing the tag triggers the release
workflow, which builds on Ubuntu 22.04 (binaries run on glibc ≥ 2.35) for
amd64 and arm64 and publishes to [GitHub Releases](https://github.com/Christopher2K/proton-drive-sync-linux/releases):

- `proton-drive-sync-<version>-<arch>.tar.xz` — binaries + vendored sidecar +
  `install.sh`; untar and run `sudo install.sh` (or `PREFIX=$HOME/.local …`)
- `.deb` / `.rpm` packages (require `nodejs >= 20` and `npm` at runtime)
- `checksums.txt` (sha256)

Install from a tarball without cloning the repository:

```sh
tar -xJf proton-drive-sync-*.tar.xz
sudo proton-drive-sync-*/install.sh
```

## CLI companion

`pds sync` runs the engine headless; `pds status|pause|resume` control a running engine; `pds conflicts` lists the conflict inbox. `pds mount <folder> <mountpoint>` serves a **placeholder view** of a sync root (requires `/dev/fuse`): files hydrate from Drive on first open, files/folders you create through the mount upload to Drive on close, and edits to existing files upload as new revisions of the same Drive file (M3). Hydrated content is an LRU cache capped at 10 GiB — `pds pin <folder> <path>` keeps a file (or folder, recursively) offline-safe, `pds unpin` releases it. See `pds --help`.

## Testing

```sh
devenv test                     # Rust workspace + sidecar typecheck/tests
devenv shell -- cargo test --workspace   # Rust only, inside the env
```

Notes for VM development (OrbStack et al.): inotify over shared folders is unreliable — run the engine tests on VM-native disk. See [AGENT.md](AGENT.md) for details.
