#!/usr/bin/env bash
# Build ProtonDriveSync.app — see docs/research/gpui.md §6 for the rationale
# (menu-bar agent app: LSUIElement lives in the bundle; ad-hoc signature is
# enough for local builds).
#
# Usage:
#   packaging/macos/bundle.sh                 # build + assemble + ad-hoc sign
#   PDS_SKIP_SIDECAR_DEPS=1 packaging/macos/bundle.sh
#                                             # don't run `npm ci` inside the bundle
#
# Output: dist/ProtonDriveSync.app
#
# Sidecar note: node_modules is NOT vendored by default; the bundle contains
# sidecar sources + package.json/package-lock.json/patches only. Unless
# PDS_SKIP_SIDECAR_DEPS=1 is set, this script runs `npm ci` inside the bundled
# Resources/sidecar (requires Node >= 20 on the build machine). The app still
# needs a system Node runtime at launch — it starts the sidecar via
# `npx tsx <Resources>/sidecar/src/index.ts` (see the README Packaging section
# for the exact PDS_SIDECAR_CMD).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

APP_DIR="$REPO_ROOT/dist/ProtonDriveSync.app"
CONTENTS="$APP_DIR/Contents"

# --- Resolve the GPUI binary package ---------------------------------------
# crates/app is being built concurrently; until its Cargo.toml exists we
# assume the expected names (package `pds-app`, binary `proton-drive-sync`).
PKG_NAME="pds-app"
BIN_NAME="proton-drive-sync"
if [[ -f crates/app/Cargo.toml ]]; then
    PKG_NAME=$(sed -n 's/^name = "\(.*\)"/\1/p' crates/app/Cargo.toml | head -1)
    # Prefer an explicit [[bin]] name if present, else the package name.
    BIN_NAME=$(awk '/^\[\[bin\]\]/{f=1} f && /^name = /{gsub(/"/,"",$3); print $3; exit}' \
        crates/app/Cargo.toml)
    BIN_NAME=${BIN_NAME:-$PKG_NAME}
else
    echo "note: crates/app/Cargo.toml not found yet; assuming package '$PKG_NAME', binary '$BIN_NAME'"
fi

# --- Build -------------------------------------------------------------------
echo "==> cargo build --release -p $PKG_NAME"
cargo build --release -p "$PKG_NAME"

BIN_SRC="target/release/$BIN_NAME"
[[ -f "$BIN_SRC" ]] || { echo "error: expected binary $BIN_SRC not found" >&2; exit 1; }

# --- Assemble the bundle -----------------------------------------------------
echo "==> assembling $APP_DIR"
rm -rf "$APP_DIR"
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources/sidecar"

cp "$BIN_SRC" "$CONTENTS/MacOS/proton-drive-sync"
cp packaging/macos/Info.plist "$CONTENTS/Info.plist"

# Sidecar: sources + manifests only (node_modules excluded on purpose).
cp sidecar/package.json sidecar/package-lock.json sidecar/tsconfig.json \
    "$CONTENTS/Resources/sidecar/"
cp -R sidecar/src "$CONTENTS/Resources/sidecar/src"
cp -R sidecar/patches "$CONTENTS/Resources/sidecar/patches"   # postinstall (patch-package)

# Drop an icon here later: cp packaging/macos/AppIcon.icns "$CONTENTS/Resources/"

# --- Sidecar dependencies ----------------------------------------------------
if [[ "${PDS_SKIP_SIDECAR_DEPS:-0}" != "1" ]]; then
    if command -v npm >/dev/null 2>&1; then
        echo "==> npm ci (sidecar deps, inside the bundle)"
        npm ci --prefix "$CONTENTS/Resources/sidecar"
    else
        echo "warning: npm not found; run 'npm ci' in $CONTENTS/Resources/sidecar before launching" >&2
    fi
fi

# --- Ad-hoc codesign (last, so it covers the assembled bundle) ---------------
echo "==> codesign (ad-hoc)"
codesign --force --sign - "$APP_DIR"

echo
echo "Built $APP_DIR"
echo "Sidecar runtime: Node >= 20 required. Launch with:"
echo "  PDS_SIDECAR_CMD=\"npx tsx $CONTENTS/Resources/sidecar/src/index.ts\" \\"
echo "    open $APP_DIR"
echo "(If you modify anything inside the .app afterwards, re-sign:"
echo "  codesign --force --sign - $APP_DIR )"
