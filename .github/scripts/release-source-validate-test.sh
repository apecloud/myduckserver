#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/release-source-validate.sh"

expect_fail() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "expected failure: $label" >&2
    exit 1
  fi
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

git_dir="$tmpdir/repo"
mkdir -p "$git_dir"
git -C "$git_dir" init -q
git -C "$git_dir" config user.name test
git -C "$git_dir" config user.email test@example.com
git -C "$git_dir" checkout -q -b main

echo one >"$git_dir/file"
git -C "$git_dir" add file
git -C "$git_dir" commit -q -m c1
c1="$(git -C "$git_dir" rev-parse HEAD)"

echo two >>"$git_dir/file"
git -C "$git_dir" commit -q -am c2
c2="$(git -C "$git_dir" rev-parse HEAD)"

git -C "$git_dir" branch release/0.2.1 "$c1"
git -C "$git_dir" checkout -q release/0.2.1
echo rel >>"$git_dir/file"
git -C "$git_dir" commit -q -am c3
c3="$(git -C "$git_dir" rev-parse HEAD)"
git -C "$git_dir" checkout -q main

git -C "$git_dir" update-ref refs/remotes/origin/main "$c2"
git -C "$git_dir" update-ref refs/remotes/origin/release/0.2.1 "$c3"

missing_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
output_file="$tmpdir/out"

# Original main path: ancestor of resolved main tip.
: >"$output_file"
release_source_validate \
  refs/heads/main \
  main \
  "$c1" \
  v0.1.0-dev.20260822.1 \
  "$git_dir" \
  "$output_file"
grep -Fxq "source_branch=main" "$output_file"
grep -Fxq "source_branch_sha=$c2" "$output_file"
grep -Fxq "sha_tag=sha-${c1:0:8}" "$output_file"

release_source_validate \
  refs/heads/main \
  main \
  "$c2" \
  v0.1.0-rc.1 \
  "$git_dir"

# Main-source cross-dev-version promotion remains allowed.
STABLE_VERSION=v0.2.0 release_source_validate \
  refs/heads/main \
  main \
  "$c1" \
  v0.1.0-dev.20260822.3 \
  "$git_dir"

# Legal release source.
: >"$output_file"
release_source_validate \
  refs/heads/main \
  release/0.2.1 \
  "$c3" \
  v0.2.1-dev.20260906.1 \
  "$git_dir" \
  "$output_file"
grep -Fxq "source_branch=release/0.2.1" "$output_file"
grep -Fxq "source_branch_sha=$c3" "$output_file"

STABLE_VERSION=v0.2.1 release_source_validate \
  refs/heads/main \
  release/0.2.1 \
  "$c1" \
  v0.2.1-rc.1 \
  "$git_dir"

# Reject wrong branch names.
expect_fail wrong-branch release_source_validate \
  refs/heads/main develop "$c2" v0.1.0-dev.20260822.1 "$git_dir"
expect_fail incomplete-release release_source_validate \
  refs/heads/main release/0.2 "$c3" v0.2.0-dev.20260822.1 "$git_dir"
expect_fail release-prefix release_source_validate \
  refs/heads/main release/v0.2.1 "$c3" v0.2.1-dev.20260906.1 "$git_dir"
expect_fail not-from-main release_source_validate \
  refs/heads/release/0.2.1 release/0.2.1 "$c3" v0.2.1-dev.20260906.1 "$git_dir"

# Reject wrong version correspondence on a release branch.
expect_fail wrong-release-version release_source_validate \
  refs/heads/main release/0.2.1 "$c3" v0.3.0-dev.20260906.1 "$git_dir"
if STABLE_VERSION=v0.3.0 release_source_validate \
  refs/heads/main release/0.2.1 "$c3" v0.2.1-dev.20260906.1 "$git_dir" \
  >/dev/null 2>&1; then
  echo "expected failure: wrong-stable" >&2
  exit 1
fi

# Reject unknown ref and commits that are not on the selected branch.
expect_fail unknown-ref release_source_validate \
  refs/heads/main release/9.9.9 "$c3" v9.9.9-dev.20260906.1 "$git_dir"
expect_fail main-missing-release-commit release_source_validate \
  refs/heads/main main "$c3" v0.1.0-dev.20260822.1 "$git_dir"
expect_fail release-missing-main-commit release_source_validate \
  refs/heads/main release/0.2.1 "$c2" v0.2.1-dev.20260906.1 "$git_dir"
expect_fail missing-sha release_source_validate \
  refs/heads/main main "$missing_sha" v0.1.0-dev.20260822.1 "$git_dir"

echo "release source validation tests passed"
