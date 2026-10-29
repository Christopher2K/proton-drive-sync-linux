#!/usr/bin/env bash
# Install proton-drive-sync for the local system (Linux only).
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

cd "$(dirname "$0")/../.." # repository root

PREFIX="${PREFIX:-/usr/local}"
APP_DIR="$PREFIX/share/proton-drive-sync"
BIN="${PDS_BIN:-target/release/proton-drive-sync}"

fail() { echo "install.sh: $*" >&2; exit 1; }

[ -f "$BIN" ] || fail "binary not found at $BIN — run 'cargo build --release -p pds-app' first"

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
else
  SUDO="$(command -v sudo || true)"
  [ -n "$SUDO" ] || fail "not root and sudo is unavailable; run as root or set PREFIX to a writable dir"
fi

echo "==> Installing binary to $PREFIX/bin"
$SUDO install -Dm755 "$BIN" "$PREFIX/bin/proton-drive-sync"
if [ -f target/release/pds ]; then
  $SUDO install -Dm755 target/release/pds "$PREFIX/bin/pds"
fi

echo "==> Installing sidecar to $APP_DIR/sidecar"
$SUDO mkdir -p "$APP_DIR"
$SUDO rm -rf "$APP_DIR/sidecar"
$SUDO cp -R sidecar "$APP_DIR/sidecar"
rm -f "$APP_DIR/sidecar/package-lock.json" 2>/dev/null || true
if [ ! "${PDS_SKIP_SIDECAR_DEPS:-}" = "1" ]; then
  echo "==> Vendoring sidecar dependencies (npm ci; Node >= 20 required)"
  ( cd "$APP_DIR/sidecar" && $SUDO env HOME="$HOME" npm ci )
else
  echo "==> Skipping sidecar deps (PDS_SKIP_SIDECAR_DEPS=1)"
  echo "    Run 'npm ci' in $APP_DIR/sidecar before first use."
fi

echo "==> Installing desktop entry and icon"
$SUDO install -Dm644 packaging/linux/proton-drive-sync.desktop \
  "$PREFIX/share/applications/proton-drive-sync.desktop"
$SUDO install -Dm644 packaging/linux/icon.svg \
  "$PREFIX/share/icons/hicolor/scalable/apps/proton-drive-sync.svg"

echo "==> Done. Launch 'Proton Drive Sync' from your application menu,"
echo "    or run 'proton-drive-sync' from a terminal."
