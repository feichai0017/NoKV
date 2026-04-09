#!/usr/bin/env bash

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ -n "${NOKV_CONFIG_SH_LOADED:-}" ]]; then
  return 0
fi
NOKV_CONFIG_SH_LOADED=1

nokv_config_dir() {
  local config_path=$1
  cd "$(dirname "$config_path")" && pwd
}

nokv_config_coordinator_addr() {
  local config_path=$1
  local scope=${2:-host}
  nokv-config coordinator --config "$config_path" --scope "$scope" --format simple 2>/dev/null | tr -d '\r' | sed -n '1p'
}

nokv_config_coordinator_workdir() {
  local config_path=$1
  nokv-config coordinator --config "$config_path" --scope host --format simple --field workdir 2>/dev/null | tr -d '\r' | sed -n '1p'
}

nokv_config_store_lines() {
  local config_path=$1
  nokv-config stores --config "$config_path" --format simple
}

nokv_config_region_lines() {
  local config_path=$1
  nokv-config regions --config "$config_path" --format simple
}
