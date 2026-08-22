#!/usr/bin/env bash

promote_image_validate() {
  if (( $# < 6 || $# > 7 )); then
    echo "usage: promote_image_validate GITHUB_REF SOURCE_SHA SOURCE_VERSION SOURCE_DIGEST STABLE_VERSION ACCEPTANCE_REFERENCE [GITHUB_OUTPUT]" >&2
    return 2
  fi

  local github_ref="$1"
  local source_sha="$2"
  local source_version="$3"
  local source_digest="$4"
  local stable_version="$5"
  local acceptance_reference="$6"
  local output_file="${7:-}"

  if [[ "$github_ref" != "refs/heads/main" ]]; then
    echo "promotion workflow must be dispatched from the main branch" >&2
    return 1
  fi
  if [[ ! "$source_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "source_sha must be a lowercase, full 40-character commit SHA" >&2
    return 1
  fi
  if [[ ! "$source_version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-(dev\.[0-9]{8}\.[1-9][0-9]*|rc\.[1-9][0-9]*)$ ]]; then
    echo "source_version must be an immutable dev or release-candidate version" >&2
    return 1
  fi
  if [[ ! "$stable_version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo "stable_version must match vX.Y.Z" >&2
    return 1
  fi
  if [[ ! "$source_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "source_digest must be a full sha256 digest" >&2
    return 1
  fi
  if [[ -z "${acceptance_reference//[[:space:]]/}" ]]; then
    echo "acceptance_reference must contain a non-whitespace value" >&2
    return 1
  fi
  if [[ "$acceptance_reference" == *$'\n'* || "$acceptance_reference" == *$'\r'* ]]; then
    echo "acceptance_reference must be a single line" >&2
    return 1
  fi

  if [[ -n "$output_file" ]]; then
    printf 'sha_tag=sha-%s\n' "${source_sha:0:8}" >> "$output_file"
  fi
}
