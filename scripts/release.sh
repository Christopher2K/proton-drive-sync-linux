#!/usr/bin/env bash
# Cut a release: bump the version everywhere, generate the changelog, open a
# release PR. Tagging happens after the PR merges (see tag-release.yml).
#
# Usage:
#   scripts/release.sh <major|minor|patch>   (from the repository root)
#
# What it does:
#   1. Verifies a clean tree on `main`, in sync with `origin/main`.
#   2. Computes the next version and rewrites every version site
#      (workspace Cargo.toml, sidecar package.json + APP_VERSION, Nix).
#   3. Regenerates CHANGELOG.md with git-cliff from conventional commits.
#   4. Commits `chore(release: vX.Y.Z)` on `release/vX.Y.Z`, pushes the branch
#      and opens the PR (squash-merge it like any other change).
#   5. After the merge, .github/workflows/tag-release.yml sees the new
#      workspace version on main, tags `vX.Y.Z` and dispatches the release
#      build (.github/workflows/release.yml: tarball, .deb, .rpm, GitHub
#      Release).
#
# Requires: git, cargo, git-cliff, gh (all in devenv; gh must be authenticated
# — `gh auth status`). Without gh the branch is still pushed and the script
# prints the URL for opening the PR by hand.
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

release_branch="release/v$next"
git ls-remote --exit-code --heads origin "$release_branch" >/dev/null 2>&1 \
  && { echo "release: branch $release_branch already exists on origin — delete it first" >&2; exit 1; }

echo "release: $version -> $next ($release_branch)"

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

# --- commit on a release branch ----------------------------------------------
git switch -c "$release_branch"
git add Cargo.toml Cargo.lock sidecar/package.json sidecar/src/config.ts \
  nix/package.nix nix/sidecar.nix CHANGELOG.md
git commit -m "chore(release: v$next): bump version and changelog" --quiet

# --- push + open the PR -------------------------------------------------------
git push -u origin "$release_branch" --quiet

# The PR body is the changelog section git-cliff just wrote.
release_notes="$(awk -v head="## v$next " '
  index($0, head) == 1 { inside = 1; next }
  inside && /^## v/ { exit }
  inside { print }
' CHANGELOG.md)"
# Drop the section's leading blank lines so the PR body starts cleanly.
release_notes="$(printf '%s\n' "$release_notes" | sed -e '/./,$!d')"

pr_body="Release v$next.

${release_notes}

Merging this PR bumps the workspace version on \`main\`; the Tag Release
workflow then tags \`v$next\` and dispatches the release build."

if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
  pr_url="$(gh pr create --base main --head "$release_branch" \
    --title "chore(release: v$next)" --body "$pr_body")"
  echo
  echo "release: PR opened — $pr_url"
  echo "Squash-merge it. Tagging + the release build run automatically afterwards."
else
  repo_url="$(git remote get-url origin | sed -e 's|^git@[^:]*:|https://github.com/|' -e 's|\.git$||')"
  echo
  echo "release: branch pushed, but gh is unavailable or not authenticated."
  echo "Open the PR yourself (base: main):"
  echo
  echo "    ${repo_url}/compare/main...${release_branch}?expand=1"
  echo
  echo "PR title: chore(release: v$next)"
fi

cat <<EOF

release: waiting on you to merge the PR. After it merges, tag-release.yml
tags v$next and dispatches release.yml, which publishes the tarball, .deb,
.rpm and the GitHub Release.

If you need to back out before merging:

    git switch main && git branch -D $release_branch
    git push origin --delete $release_branch
EOF
