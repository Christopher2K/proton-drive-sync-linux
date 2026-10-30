#!/usr/bin/env bash
# Install proton-drive-sync for the local system (Linux only).
#
# Works from a repository checkout (expects a `cargo build --release` under
# target/ and the ./sidecar source tree) or from an extracted release tarball
# (binaries under bin/, vendored sidecar under share/proton-drive-sync/sidecar).
#
# Layout (PREFIX defaults to /usr/local):
#   $PREFIX/bin/proton-drive-sync          GPUI desktop app
#   $PREFIX/bin/pds                        CLI companion (if built)
#   $PREFIX/share/proton-drive-sync/sidecar/  Node sidecar (runtime deps vendored)
#   $PREFIX/share/applications/proton-drive-sync.desktop
#   $PREFIX/share/icons/hicolor/scalable/apps/proton-drive-sync.svg
#
# Usage:
#   cargo build --release -p pds-app -p pds-cli   # in the repository root
#   sudo packaging/linux/install.sh               # or: PREFIX=$HOME/.local ...
#
# Environment:
#   PDS_BIN   path to the built `proton-drive-sync` binary
#             (default: <repo>/target/release/proton-drive-sync)
#   PDS_SKIP_SIDECAR_DEPS=1  skip `npm ci`; you must run it later in the
#             installed sidecar directory (Node >= 20 required either way)

set -euo pipefail

# Repository checkout: script lives in packaging/linux/, root is two up.
# Release tarball: script lives at the tarball root itself.
script_dir="$(cd "$(dirname "$0")" && pwd)"
if [ -d "$script_dir/../../crates" ]; then
  cd "$script_dir/../.."
else
  cd "$script_dir"
fi

PREFIX="${PREFIX:-/usr/local}"
APP_DIR="$PREFIX/share/proton-drive-sync"

fail() { echo "install.sh: $*" >&2; exit 1; }

# Binary location: a repository build (target/release) or an extracted
# release tarball (bin/). Override with PDS_BIN.
BIN="${PDS_BIN:-}"
if [ -z "$BIN" ]; then
  if [ -f target/release/proton-drive-sync ]; then
    BIN=target/release/proton-drive-sync
  elif [ -f bin/proton-drive-sync ]; then
    BIN=bin/proton-drive-sync
  else
    fail "binary not found — run 'cargo build --release -p pds-app -p pds-cli' \
first, or run this script from an extracted release tarball"
  fi
fi
BIN_DIR="$(dirname "$BIN")"

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
else
  SUDO="$(command -v sudo || true)"
  [ -n "$SUDO" ] || fail "not root and sudo is unavailable; run as root or set PREFIX to a writable dir"
fi

echo "==> Installing binary to $PREFIX/bin"
$SUDO install -Dm755 "$BIN" "$PREFIX/bin/proton-drive-sync"
for cand in "$BIN_DIR/pds" target/release/pds; do
  if [ -f "$cand" ]; then
    $SUDO install -Dm755 "$cand" "$PREFIX/bin/pds"
    break
  fi
done

echo "==> Installing sidecar to $APP_DIR/sidecar"
# Repository layout keeps the sidecar in ./sidecar; release tarballs carry it
# under share/proton-drive-sync/sidecar (already vendored by CI).
SRC_SIDECAR="sidecar"
if [ ! -d "$SRC_SIDECAR" ] && [ -d share/proton-drive-sync/sidecar ]; then
  SRC_SIDECAR="share/proton-drive-sync/sidecar"
fi
[ -d "$SRC_SIDECAR" ] || fail "sidecar not found (expected ./sidecar or ./share/proton-drive-sync/sidecar)"
$SUDO mkdir -p "$APP_DIR"
$SUDO rm -rf "$APP_DIR/sidecar"
$SUDO cp -R "$SRC_SIDECAR" "$APP_DIR/sidecar"
rm -f "$APP_DIR/sidecar/package-lock.json" 2>/dev/null || true
if [ -d "$APP_DIR/sidecar/node_modules" ]; then
  echo "==> Sidecar dependencies already vendored (from the release tarball)"
elif [ "${PDS_SKIP_SIDECAR_DEPS:-}" = "1" ]; then
  echo "==> Skipping sidecar deps (PDS_SKIP_SIDECAR_DEPS=1)"
  echo "    Run 'npm ci' in $APP_DIR/sidecar before first use."
else
  echo "==> Vendoring sidecar dependencies (npm ci; Node >= 20 required)"
  ( cd "$APP_DIR/sidecar" && $SUDO env HOME="$HOME" npm ci --no-audit --no-fund )
fi

echo "==> Installing desktop entry and icon"
# Repository checkout keeps assets in packaging/linux/; a release tarball has
# them staged under share/ already.
if [ -f packaging/linux/proton-drive-sync.desktop ]; then
  DESKTOP_SRC=packaging/linux/proton-drive-sync.desktop
  ICON_SRC=packaging/linux/icon.svg
else
  DESKTOP_SRC=share/applications/proton-drive-sync.desktop
  ICON_SRC=share/icons/hicolor/scalable/apps/proton-drive-sync.svg
fi
$SUDO install -Dm644 "$DESKTOP_SRC" \
  "$PREFIX/share/applications/proton-drive-sync.desktop"
$SUDO install -Dm644 "$ICON_SRC" \
  "$PREFIX/share/icons/hicolor/scalable/apps/proton-drive-sync.svg"

echo "==> Done. Launch 'Proton Drive Sync' from your application menu,"
echo "    or run 'proton-drive-sync' from a terminal."
