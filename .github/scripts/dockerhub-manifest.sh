#!/usr/bin/env bash

dockerhub_pull_token() {
  local repository="$1"
  local response

  if ! response="$(
    curl --fail --silent --show-error --get \
      --data-urlencode "service=registry.docker.io" \
      --data-urlencode "scope=repository:${repository}:pull" \
      https://auth.docker.io/token
  )"; then
    echo "failed to obtain a Docker Hub pull token for $repository" >&2
    return 2
  fi

  if ! jq -er '.token | select(type == "string" and length > 0)' <<<"$response"; then
    echo "Docker Hub returned an invalid pull token response for $repository" >&2
    return 2
  fi
}

# Prints a validated digest for HTTP 200, returns 1 for an explicit 404, and
# returns 2 for every other registry, authentication, or transport failure.
dockerhub_manifest_digest() {
  local repository="$1"
  local reference="$2"
  local token="$3"
  local response
  local status
  local headers
  local digest

  if ! response="$(
    curl --silent --show-error --head \
      --header "Authorization: Bearer $token" \
      --header "Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
      --write-out '\n%{http_code}' \
      "https://registry-1.docker.io/v2/${repository}/manifests/${reference}"
  )"; then
    echo "failed to query Docker Hub manifest $repository:$reference" >&2
    return 2
  fi

  status="${response##*$'\n'}"
  headers="${response%$'\n'*}"

  case "$status" in
    200)
      digest="$(
        awk 'tolower($1) == "docker-content-digest:" { gsub("\\r", "", $2); value = $2 } END { print value }' \
          <<<"$headers"
      )"
      if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
        echo "Docker Hub returned an invalid digest for $repository:$reference" >&2
        return 2
      fi
      printf '%s\n' "$digest"
      ;;
    404)
      return 1
      ;;
    *)
      echo "Docker Hub returned unexpected HTTP status $status for $repository:$reference" >&2
      return 2
      ;;
  esac
}
