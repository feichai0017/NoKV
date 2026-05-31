#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-rust-store.sh --config <config> --store-id <id> (--region-id <id>|--all-regions) [options]

Options:
  --scope <local|docker>   Select which addresses to use (default: local)
  --region-id <id>         Host one configured region on this store.
  --all-regions            Host every configured region that includes this store.
  --workdir <dir>          Required Holt workdir for this store-region process
  --coordinator-addr <addr>
                           Optional Coordinator gRPC endpoint list.
  --extra <args...>        Additional arguments passed to nokv-raftstore-server

Notes:
  - This is the Rust raftstore parity launcher for config-driven store processes.
  - --all-regions maps the config region catalog into NOKV_RUST_RAFTSTORE_REGIONS
    and NOKV_RUST_RAFTSTORE_REGION_RANGES for one multi-region Rust process.
USAGE
}

CONFIG=""
STORE_ID=""
REGION_ID=""
ALL_REGIONS=0
WORKDIR=""
SCOPE="local"
COORDINATOR_ADDR=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG=$2
      shift 2
      ;;
    --store-id)
      STORE_ID=$2
      shift 2
      ;;
    --region-id)
      REGION_ID=$2
      shift 2
      ;;
    --all-regions)
      ALL_REGIONS=1
      shift
      ;;
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --scope)
      SCOPE=$2
      shift 2
      ;;
    --coordinator-addr)
      COORDINATOR_ADDR=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --extra)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$CONFIG" || -z "$STORE_ID" ]]; then
  nokv_die "serve-rust-store.sh: --config and --store-id are required"
fi

if [[ "$ALL_REGIONS" -eq 1 && -n "$REGION_ID" ]]; then
  nokv_die "serve-rust-store.sh: --all-regions and --region-id are mutually exclusive"
fi

if [[ "$ALL_REGIONS" -eq 0 && -z "$REGION_ID" ]]; then
  nokv_die "serve-rust-store.sh: either --region-id or --all-regions is required"
fi

if [[ "$SCOPE" != "local" && "$SCOPE" != "docker" ]]; then
  nokv_die "serve-rust-store.sh: --scope must be local or docker"
fi

if [[ -z "$WORKDIR" ]]; then
  nokv_die "serve-rust-store.sh: --workdir is required"
fi

if ! command -v nokv-config >/dev/null 2>&1; then
  nokv_die "serve-rust-store.sh: nokv-config binary not found in PATH"
fi

if ! command -v nokv-raftstore-server >/dev/null 2>&1; then
  nokv_die "serve-rust-store.sh: nokv-raftstore-server binary not found in PATH"
fi

declare -A STORE_LISTEN_ADDRS=()
declare -A STORE_CLIENT_ADDRS=()
while IFS= read -r store_line; do
  read -r store_id host_listen host_addr docker_listen docker_addr _ <<<"$store_line"
  if [[ "$SCOPE" == "docker" ]]; then
    STORE_LISTEN_ADDRS[$store_id]=$docker_listen
    STORE_CLIENT_ADDRS[$store_id]=$docker_addr
  else
    STORE_LISTEN_ADDRS[$store_id]=$host_listen
    STORE_CLIENT_ADDRS[$store_id]=$host_addr
  fi
done < <(nokv_config_store_lines "$CONFIG")

listen_addr=${STORE_LISTEN_ADDRS[$STORE_ID]:-}
if [[ -z "$listen_addr" ]]; then
  nokv_die "serve-rust-store.sh: store $STORE_ID not found in $CONFIG"
fi

region_specs=()
region_ranges=()
peer_endpoint_ids=()
declare -A PEER_ENDPOINTS_BY_ID=()

config_key_to_rust_hex() {
  local key=$1
  if [[ "$key" == "-" ]]; then
    printf ''
    return
  fi
  if [[ "$key" == hex:* ]]; then
    printf '%s' "${key#hex:}"
    return
  fi
  printf '%s' "$key"
}

record_peer_endpoint() {
  local peer_id=$1
  local endpoint=$2
  local existing=${PEER_ENDPOINTS_BY_ID[$peer_id]:-}
  if [[ -n "$existing" && "$existing" != "$endpoint" ]]; then
    nokv_die "serve-rust-store.sh: peer $peer_id maps to both $existing and $endpoint"
  fi
  if [[ -z "$existing" ]]; then
    peer_endpoint_ids+=("$peer_id")
  fi
  PEER_ENDPOINTS_BY_ID[$peer_id]=$endpoint
}

append_region_from_config_line() {
  local region_line=$1
  local region_id start_key end_key epoch_ver epoch_conf peer_str leader_store_id
  read -r region_id start_key end_key epoch_ver epoch_conf peer_str leader_store_id _ <<<"$region_line"
  if [[ -z "$region_id" ]]; then
    return
  fi
  if [[ "$ALL_REGIONS" -eq 0 && "$region_id" != "$REGION_ID" ]]; then
    return
  fi

  local peer_id=""
  IFS=',' read -ra peers <<<"$peer_str"
  for peer in "${peers[@]}"; do
    [[ -z "$peer" ]] && continue
    local peer_store_id=${peer%%:*}
    local peer_peer_id=${peer##*:}
    local peer_addr=${STORE_CLIENT_ADDRS[$peer_store_id]:-}
    if [[ -z "$peer_addr" ]]; then
      nokv_die "serve-rust-store.sh: store $peer_store_id for region $region_id has no configured address"
    fi
    if [[ "$peer_store_id" == "$STORE_ID" ]]; then
      peer_id=$peer_peer_id
    fi
    record_peer_endpoint "$peer_peer_id" "$peer_addr"
  done

  if [[ -z "$peer_id" ]]; then
    if [[ "$ALL_REGIONS" -eq 1 ]]; then
      return
    fi
    nokv_die "serve-rust-store.sh: store $STORE_ID is not a peer of region $REGION_ID"
  fi

  local bootstrap=false
  if [[ "$STORE_ID" == "$leader_store_id" ]]; then
    bootstrap=true
  fi
  region_specs+=("${region_id}:${STORE_ID}:${peer_id}:${bootstrap}")
  region_ranges+=("${region_id}=$(config_key_to_rust_hex "$start_key"):$(config_key_to_rust_hex "$end_key")")
}

while IFS= read -r region_line; do
  append_region_from_config_line "$region_line"
done < <(nokv_config_region_lines "$CONFIG")

if [[ "${#region_specs[@]}" -eq 0 ]]; then
  if [[ "$ALL_REGIONS" -eq 1 ]]; then
    nokv_die "serve-rust-store.sh: store $STORE_ID is not a peer of any configured region"
  fi
  nokv_die "serve-rust-store.sh: region $REGION_ID not found in $CONFIG"
fi

peer_endpoints=()
for peer_id in "${peer_endpoint_ids[@]}"; do
  peer_endpoints+=("${peer_id}=${PEER_ENDPOINTS_BY_ID[$peer_id]}")
done

if [[ -z "$COORDINATOR_ADDR" ]]; then
  coord_scope="host"
  if [[ "$SCOPE" == "docker" ]]; then
    coord_scope="docker"
  fi
  COORDINATOR_ADDR=$(nokv_config_coordinator_addr "$CONFIG" "$coord_scope")
fi
mkdir -p "$WORKDIR"

export NOKV_RUST_RAFTSTORE_ADDR="$listen_addr"
if [[ "$ALL_REGIONS" -eq 1 ]]; then
  export NOKV_RUST_RAFTSTORE_REGIONS
  export NOKV_RUST_RAFTSTORE_REGION_RANGES
  NOKV_RUST_RAFTSTORE_REGIONS=$(IFS=,; echo "${region_specs[*]}")
  NOKV_RUST_RAFTSTORE_REGION_RANGES=$(IFS=,; echo "${region_ranges[*]}")
else
  IFS=':' read -r _single_region _single_store peer_id bootstrap <<<"${region_specs[0]}"
  export NOKV_RUST_RAFTSTORE_REGION_ID="$REGION_ID"
  export NOKV_RUST_RAFTSTORE_STORE_ID="$STORE_ID"
  export NOKV_RUST_RAFTSTORE_PEER_ID="$peer_id"
  export NOKV_RUST_RAFTSTORE_BOOTSTRAP="$bootstrap"
fi
export NOKV_RUST_RAFTSTORE_HOLT_DIR="$WORKDIR/holt"
export NOKV_RUST_RAFTSTORE_LOG_DIR="$WORKDIR/raftlog"
export NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS
NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=$(IFS=,; echo "${peer_endpoints[*]}")

if [[ -n "$COORDINATOR_ADDR" ]]; then
  export NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR="$COORDINATOR_ADDR"
fi

exec nokv-raftstore-server "${EXTRA_ARGS[@]}"
