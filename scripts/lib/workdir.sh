#!/usr/bin/env bash

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ -n "${NOKV_WORKDIR_SH_LOADED:-}" ]]; then
  return 0
fi
NOKV_WORKDIR_SH_LOADED=1

nokv_workdir_has_unexpected_entries() {
  local dir=$1
  find "$dir" -mindepth 1 -maxdepth 1 ! -name 'LOCK' -print -quit | grep -q .
}

nokv_remove_stale_lock_if_present() {
  local dir=$1
  local lock_path="$dir/LOCK"
  if [[ -f "$lock_path" ]]; then
    rm -f "$lock_path"
  fi
}

nokv_assert_fresh_workdir() {
  local dir=$1
  local context=$2
  mkdir -p "$dir"
  nokv_remove_stale_lock_if_present "$dir"
  if nokv_workdir_has_unexpected_entries "$dir"; then
    nokv_die "$context: $dir"
  fi
}
