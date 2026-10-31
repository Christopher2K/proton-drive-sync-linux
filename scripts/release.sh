#!/usr/bin/env bash
# Cut a release: bump the version everywhere, generate the changelog, tag.
#
# Usage:
#   scripts/release.sh <major|minor|patch>   (from the repository root)
#
# What it does:
#   1. Verifies a clean tree on `main`, in sync with `origin/main`.
#   2. Computes the next version and rewrites every version site
#      (workspace Cargo.toml, sidecar package.json + APP_VERSION, Nix).
#   3. Regenerates CHANGELOG.md with git-cliff from conventional commits.
#   4. Commits `chore(release: vX.Y.Z)` and tags `vX.Y.Z`; you push:
#      `git push origin main vX.Y.Z`. The tag push triggers the release
#      workflow (.github/workflows/release.yml).
#
# Requires: git, git-cliff + cargo (both in devenv).
set -euo pipefail

cd "$(dirname "$0")/.."

BUMP="${1:-}"
case "$BUMP" in
  major|minor|patch) ;;
  *) echo "usage: scripts/release.sh <major|minor|patch>" >&2; exit 2 ;;
esac

branch="$(git branch --show-current)"
[ "$branch" = "main" ] || { echo "release: must run on main (on: $branch)" >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "release: tree must be clean" >&2; exit 1; }
git fetch origin main --quiet
# Local main may be ahead (release.sh does not push), but never behind.
git merge-base --is-ancestor origin/main HEAD \
  || { echo "release: main is behind origin/main (pull first)" >&2; exit 1; }

# --- compute the next version ------------------------------------------------
version="$(sed -n '/^\[workspace\.package\]/,/^\[/ s/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n1)"
[ -n "$version" ] || { echo "release: could not read workspace version" >&2; exit 1; }

major=${version%%.*}
rest=${version#*.}
minor=${rest%%.*}
patch=${rest#*.}
case "$BUMP" in
  major) next="$((major + 1)).0.0" ;;
  minor) next="$major.$((minor + 1)).0" ;;
  patch) next="$major.$minor.$((patch + 1))" ;;
esac
git rev-parse -q --verify "refs/tags/v$next" >/dev/null \
  && { echo "release: tag v$next already exists" >&2; exit 1; }

echo "release: $version -> $next"

# --- rewrite every version site ----------------------------------------------
# sed -i is not portable across BSD/GNU; edit via a temp copy instead.
rewrite() { # <file> <sed-expression>
  sed "$2" "$1" > "$1.tmp" && mv "$1.tmp" "$1"
}

# 1. Workspace version (the five crates inherit via version.workspace = true).
rewrite Cargo.toml "s/^version = \"$version\"/version = \"$next\"/"

# 2. Sidecar package.json + the Proton-compliance APP_VERSION string
#    (x-pm-appversion, `external-drive-{name}@{semver}-{channel}`).
rewrite sidecar/package.json "s/\"version\": \"$version\"/\"version\": \"$next\"/"
rewrite sidecar/src/config.ts \
  "s|external-drive-proton_drive_sync@$version-alpha|external-drive-proton_drive_sync@$next-alpha|"

# 3. Nix derivations.
rewrite nix/package.nix "s/^  version = \"$version\"/  version = \"$next\"/"
rewrite nix/sidecar.nix "s/^  version = \"$version\"/  version = \"$next\"/"

# 4. Refresh Cargo.lock (the workspace crates carry the version).
cargo update -w >/dev/null

# 5. Verify nothing drifted, then generate the changelog. The release commit
#    is not tagged yet, so label the unreleased section with the new tag.
#    --prepend (not -o) is required: `-o` truncates the file and `--unreleased`
#    only renders commits since the last tag, so every earlier release section
#    would be dropped. --prepend inserts the new section after the header and
#    keeps the existing history intact.
./scripts/check-versions.sh
# Guard: never let a changelog regeneration erase an earlier release section.
previous_heading="$(grep -m1 '^## v' CHANGELOG.md || true)"
git-cliff --config cliff.toml --unreleased --tag "v$next" --prepend CHANGELOG.md
if [ -n "$previous_heading" ] \
  && ! grep -qF -- "$previous_heading" CHANGELOG.md; then
  echo "release: changelog lost '$previous_heading' — refusing to continue" >&2
  exit 1
fi

# --- commit + tag -------------------------------------------------------------
git add Cargo.toml Cargo.lock sidecar/package.json sidecar/src/config.ts \
  nix/package.nix nix/sidecar.nix CHANGELOG.md
git commit -m "chore(release: v$next): bump version and changelog" --quiet
git tag -a "v$next" -m "v$next"

cat <<EOF

release: done. Review the diff, then push:

    git push origin main v$next

The v$next tag triggers the release workflow
(.github/workflows/release.yml), which builds and publishes the tarball,
.deb and .rpm artifacts and creates the GitHub Release.
EOF
