#!/usr/bin/env bash

# Shared source-branch checks for Release Image and Promote Image.
# Tooling still runs from a fixed main commit. Application source must belong
# to main or an explicit release/X.Y.Z branch; arbitrary SHAs are rejected.

release_source_branch_ok() {
  local branch=$1
  [[ "$branch" == "main" ]] && return 0
  [[ "$branch" =~ ^release/(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

release_prerelease_ok() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-(dev\.[0-9]{8}\.[1-9][0-9]*|rc\.[1-9][0-9]*)$ ]]
}

release_stable_ok() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

release_source_validate_inputs() {
  if (( $# != 4 )); then
    echo "usage: release_source_validate_inputs GITHUB_REF SOURCE_BRANCH SOURCE_SHA VERSION" >&2
    return 2
  fi

  local github_ref="$1"
  local source_branch="$2"
  local source_sha="$3"
  local version="$4"
  local stable_version="${STABLE_VERSION:-}"

  if [[ "$github_ref" != "refs/heads/main" ]]; then
    echo "release workflow must be dispatched from the main branch" >&2
    return 1
  fi
  if ! release_source_branch_ok "$source_branch"; then
    echo "source_branch must be main or release/X.Y.Z" >&2
    return 1
  fi
  if [[ ! "$source_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "source_sha must be a lowercase, full 40-character commit SHA" >&2
    return 1
  fi
  if ! release_prerelease_ok "$version"; then
    echo "version must match vX.Y.Z-dev.YYYYMMDD.N or vX.Y.Z-rc.N" >&2
    return 1
  fi
  if [[ -n "$stable_version" ]] && ! release_stable_ok "$stable_version"; then
    echo "stable_version must match vX.Y.Z" >&2
    return 1
  fi

  if [[ "$source_branch" != "main" ]]; then
    local rel="${source_branch#release/}"
    local rel_re="${rel//./\\.}"
    if [[ ! "$version" =~ ^v${rel_re}-(dev\.[0-9]{8}\.[1-9][0-9]*|rc\.[1-9][0-9]*)$ ]]; then
      echo "version $version does not correspond to $source_branch" >&2
      return 1
    fi
    if [[ -n "$stable_version" && "$stable_version" != "v${rel}" ]]; then
      echo "stable_version $stable_version does not correspond to $source_branch" >&2
      return 1
    fi
  fi
}

release_source_resolve() {
  if (( $# < 3 || $# > 4 )); then
    echo "usage: release_source_resolve SOURCE_BRANCH SOURCE_SHA GIT_DIR [OUTPUT_FILE]" >&2
    return 2
  fi

  local source_branch="$1"
  local source_sha="$2"
  local git_dir="$3"
  local output_file="${4:-}"
  local branch_sha=""

  if [[ ! -d "$git_dir" ]]; then
    echo "git directory is missing: $git_dir" >&2
    return 1
  fi
  if ! release_source_branch_ok "$source_branch"; then
    echo "source_branch must be main or release/X.Y.Z" >&2
    return 1
  fi
  if [[ ! "$source_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "source_sha must be a lowercase, full 40-character commit SHA" >&2
    return 1
  fi

  if branch_sha="$(git -C "$git_dir" rev-parse --verify --quiet "refs/remotes/origin/${source_branch}^{commit}")"; then
    :
  elif branch_sha="$(git -C "$git_dir" rev-parse --verify --quiet "refs/heads/${source_branch}^{commit}")"; then
    :
  else
    echo "unknown source branch ref: $source_branch" >&2
    return 1
  fi

  if ! git -C "$git_dir" cat-file -e "${source_sha}^{commit}" 2>/dev/null; then
    echo "source commit $source_sha is missing from selected branch history" >&2
    return 1
  fi
  if ! git -C "$git_dir" merge-base --is-ancestor "$source_sha" "$branch_sha"; then
    echo "source_sha must be reachable from $source_branch at $branch_sha" >&2
    return 1
  fi

  if [[ -n "$output_file" ]]; then
    {
      printf 'source_branch=%s\n' "$source_branch"
      printf 'source_branch_sha=%s\n' "$branch_sha"
      printf 'sha_tag=sha-%s\n' "${source_sha:0:8}"
    } >> "$output_file"
  fi
}

release_source_validate() {
  if (( $# < 5 || $# > 6 )); then
    echo "usage: release_source_validate GITHUB_REF SOURCE_BRANCH SOURCE_SHA VERSION GIT_DIR [OUTPUT_FILE]" >&2
    return 2
  fi
  release_source_validate_inputs "$1" "$2" "$3" "$4" || return
  release_source_resolve "$2" "$3" "$5" "${6:-}"
}

release_source_fetch() {
  if (( $# != 2 )); then
    echo "usage: release_source_fetch GIT_DIR SOURCE_BRANCH" >&2
    return 2
  fi
  local git_dir="$1"
  local source_branch="$2"
  if ! release_source_branch_ok "$source_branch"; then
    echo "source_branch must be main or release/X.Y.Z" >&2
    return 1
  fi
  if ! git -C "$git_dir" fetch --no-tags origin \
    "+refs/heads/${source_branch}:refs/remotes/origin/${source_branch}"; then
    echo "unknown source branch ref: $source_branch" >&2
    return 1
  fi
}
