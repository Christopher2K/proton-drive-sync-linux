#!/usr/bin/env bash
# Verify that every place carrying the product version agrees.
#
# The single source of truth is `version` in sidecar/package.json (the
# Rust workspace that used to hold it was removed in the Go migration).
# Kept in sync by `scripts/release.sh`; this script fails loudly if a
# bump missed one of the mirrored sites (run by CI on every push/PR).
#
# Checked sites:
#   sidecar/package.json       "version"                        (source of truth)
#   sidecar/src/config.ts      APP_VERSION (x-pm-appversion: external-drive-
#                              proton_drive_sync@<semver>-<channel>, Proton
#                              compliance format — see AGENT.md hard rule 2)
#   mygo-shell/mygo.json       "version" (shown in the desktop app)
#   mygo-shell/version.go      fallback shown when MyGo metadata is unavailable
#   nix/package.nix            version
#   nix/sidecar.nix            version

set -euo pipefail

cd "$(dirname "$0")/.."

fail() { echo "check-versions: $*" >&2; exit 1; }

version="$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' sidecar/package.json | head -n1)"
[ -n "$version" ] || fail "could not read version from sidecar/package.json"

check() { # <description> <file> <expected-line-content>
  local desc="$1" file="$2" want="$3"
  grep -qF -- "$want" "$file" \
    || fail "$desc in $file is out of sync (want: $want)"
}

appversion="$(sed -n 's/^export const APP_VERSION = "external-drive-proton_drive_sync@\(.*\)";$/\1/p' sidecar/src/config.ts | head -n1)"
[ -n "$appversion" ] || fail "could not read APP_VERSION from sidecar/src/config.ts"
# Proton expects exactly one `-<channel>` suffix after the semver; the version
# itself must be plain (a prerelease like 0.2.0-rc.1 would produce two dashes).
case "$version" in
  *-*) fail "product version '$version' contains a prerelease suffix; \
APP_VERSION allows only one -<channel> (e.g. ${version%-*}-alpha)" ;;
esac
[ "$appversion" = "$version-alpha" ] \
  || fail "APP_VERSION is '$appversion', want '$version-alpha' (bump the channel only when distribution changes)"

check "mygo-shell/mygo.json version" mygo-shell/mygo.json '"version": "'"$version"'",'
check "mygo-shell/version.go fallback" mygo-shell/version.go 'return "'"$version"'"'
check "nix/package.nix version" nix/package.nix 'version = "'"$version"'";'
check "nix/sidecar.nix version" nix/sidecar.nix 'version = "'"$version"'";'

echo "check-versions: all version sites agree on $version"
