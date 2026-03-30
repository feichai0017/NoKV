#!/usr/bin/env bash

if [[ -n "${NOKV_COMMON_SH_LOADED:-}" ]]; then
  return 0
fi
NOKV_COMMON_SH_LOADED=1

NOKV_LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NOKV_SCRIPTS_DIR=$(cd "$NOKV_LIB_DIR/.." && pwd)
NOKV_ROOT_DIR=$(cd "$NOKV_SCRIPTS_DIR/.." && pwd)

nokv_die() {
  echo "$*" >&2
  exit 1
}

nokv_resolve_path() {
  local base_dir=$1
  local value=$2
  if [[ "$value" == /* ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "$base_dir/$value"
  fi
}

nokv_split_addr() {
  local addr=$1
  local host=${addr%:*}
  local port=${addr##*:}
  if [[ -z "$host" || -z "$port" || "$host" == "$addr" ]]; then
    return 1
  fi
  printf '%s\n%s\n' "$host" "$port"
}

nokv_wait_for_tcp() {
  local addr=$1
  local timeout_s=${2:-30}
  local parsed host port deadline
  if ! parsed=$(nokv_split_addr "$addr"); then
    nokv_die "invalid address for readiness check: $addr"
  fi
  host=$(echo "$parsed" | sed -n '1p')
  port=$(echo "$parsed" | sed -n '2p')
  deadline=$((SECONDS + timeout_s))
  while (( SECONDS < deadline )); do
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  nokv_die "timed out waiting for $addr"
}

nokv_ensure_build_dir() {
  mkdir -p "$NOKV_ROOT_DIR/build"
}

nokv_build_cli_binaries() {
  nokv_ensure_build_dir
  go build -o "$NOKV_ROOT_DIR/build/nokv" "$NOKV_ROOT_DIR/cmd/nokv"
  go build -o "$NOKV_ROOT_DIR/build/nokv-config" "$NOKV_ROOT_DIR/cmd/nokv-config"
}

nokv_prepend_build_path() {
  PATH="$NOKV_ROOT_DIR/build:$PATH"
}
