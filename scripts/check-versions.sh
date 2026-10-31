#!/usr/bin/env bash
# Verify that every place carrying the product version agrees.
#
# The single source of truth is `version` in [workspace.package] (Cargo.toml).
# Kept in sync by `scripts/release.sh`; this script fails loudly if a bump
# missed one of the mirrored sites (run by CI on every push/PR).
#
# Checked sites:
#   Cargo.toml                 [workspace.package] version   (source of truth)
#   crates/*/Cargo.toml        version.workspace = true      (inherit, no drift)
#   sidecar/package.json       "version"
#   sidecar/src/config.ts      APP_VERSION (x-pm-appversion: external-drive-
#                              proton_drive_sync@<semver>-<channel>, Proton
#                              compliance format — see AGENT.md hard rule 2)
#   nix/package.nix            version
#   nix/sidecar.nix            version

set -euo pipefail

cd "$(dirname "$0")/.."

fail() { echo "check-versions: $*" >&2; exit 1; }

version="$(sed -n '/^\[workspace\.package\]/,/^\[/ s/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n1)"
[ -n "$version" ] || fail "could not read version from [workspace.package] in Cargo.toml"

# crate manifests must inherit the workspace version instead of pinning one
stray="$(grep -l '^version = ' crates/*/Cargo.toml || true)"
if [ -n "$stray" ]; then
  fail "crates must inherit the workspace version; found a pinned version in:
$stray"
fi
grep -q '^version.workspace = true$' crates/app/Cargo.toml crates/cli/Cargo.toml \
  crates/core/Cargo.toml crates/sidecar-client/Cargo.toml crates/vfs-linux/Cargo.toml \
  crates/xdg/Cargo.toml \
  || fail "expected 'version.workspace = true' in every crate manifest"

check() { # <description> <file> <expected-line-content>
  local desc="$1" file="$2" want="$3"
  grep -qF -- "$want" "$file" \
    || fail "$desc in $file is out of sync (want: $want)"
}

check "sidecar/package.json version" sidecar/package.json "\"version\": \"$version\""

appversion="$(sed -n 's/^export const APP_VERSION = "external-drive-proton_drive_sync@\(.*\)";$/\1/p' sidecar/src/config.ts | head -n1)"
[ -n "$appversion" ] || fail "could not read APP_VERSION from sidecar/src/config.ts"
# Proton expects exactly one `-<channel>` suffix after the semver; the version
# itself must be plain (a prerelease like 0.2.0-rc.1 would produce two dashes).
case "$version" in
  *-*) fail "workspace version '$version' contains a prerelease suffix; \
APP_VERSION allows only one -<channel> (e.g. ${version%-*}-alpha)" ;;
esac
[ "$appversion" = "$version-alpha" ] \
  || fail "APP_VERSION is '$appversion', want '$version-alpha' (bump the channel only when distribution changes)"

check "nix/package.nix version" nix/package.nix 'version = "'"$version"'";'
check "nix/sidecar.nix version" nix/sidecar.nix 'version = "'"$version"'";'

echo "check-versions: all version sites agree on $version"
