#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/promote-image-validate.sh"

source_sha=0123456789abcdef0123456789abcdef01234567
source_digest=sha256:fddbbf0e7fff3013202d054216c829b20ff3d34f6e20b93e64f24fa6d6a7de13
output_file="$(mktemp)"
trap 'rm -f "$output_file"' EXIT

promote_image_validate \
  refs/heads/main \
  "$source_sha" \
  v0.1.0-dev.20260822.3 \
  "$source_digest" \
  v0.2.0 \
  task-32-acceptance \
  "$output_file"
grep -Fxq "sha_tag=sha-01234567" "$output_file"

promote_image_validate \
  refs/heads/main \
  "$source_sha" \
  v0.2.0-rc.1 \
  "$source_digest" \
  v0.2.0 \
  task-32-acceptance

expect_invalid() {
  if promote_image_validate "$@" 2>/dev/null; then
    echo "expected promotion input validation to fail" >&2
    exit 1
  fi
}

expect_invalid refs/heads/release "$source_sha" v0.1.0-dev.20260822.3 "$source_digest" v0.2.0 task-32-acceptance
expect_invalid refs/heads/main "$source_sha" v0.1.0 "$source_digest" v0.2.0 task-32-acceptance
expect_invalid refs/heads/main "$source_sha" v0.1.0-dev.20260822.3 "$source_digest" v0.2 task-32-acceptance
expect_invalid refs/heads/main "$source_sha" v0.1.0-dev.20260822.3 "$source_digest" v0.2.0 $'task-32\nacceptance'
