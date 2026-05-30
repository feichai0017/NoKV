#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-rust-store.sh --config <config> --store-id <id> --region-id <id> [options]

Options:
  --scope <local|docker>   Select which addresses to use (default: local)
  --workdir <dir>          Required Holt workdir for this store-region process
  --coordinator-addr <addr>
                           Optional Coordinator gRPC endpoint list.
  --extra <args...>        Additional arguments passed to nokv-raftstore-server

Notes:
  - This is the Rust raftstore parity launcher for one store-region process.
  - Rust raftstore still hosts one region per process; the default compose path
    remains the Go store until multi-region Rust hosting is implemented.
USAGE
}

CONFIG=""
STORE_ID=""
REGION_ID=""
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

if [[ -z "$CONFIG" || -z "$STORE_ID" || -z "$REGION_ID" ]]; then
  nokv_die "serve-rust-store.sh: --config, --store-id, and --region-id are required"
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

target_region=""
while IFS= read -r region_line; do
  read -r region_id _start_key _end_key _epoch_ver _epoch_conf _peer_str _leader_store_id _ <<<"$region_line"
  if [[ "$region_id" == "$REGION_ID" ]]; then
    target_region="$region_line"
    break
  fi
done < <(nokv_config_region_lines "$CONFIG")

if [[ -z "$target_region" ]]; then
  nokv_die "serve-rust-store.sh: region $REGION_ID not found in $CONFIG"
fi

read -r _region_id _start_key _end_key _epoch_ver _epoch_conf peer_str leader_store_id _ <<<"$target_region"
peer_id=""
peer_endpoints=()
IFS=',' read -ra peers <<<"$peer_str"
for peer in "${peers[@]}"; do
  [[ -z "$peer" ]] && continue
  peer_store_id=${peer%%:*}
  peer_peer_id=${peer##*:}
  peer_addr=${STORE_CLIENT_ADDRS[$peer_store_id]:-}
  if [[ -z "$peer_addr" ]]; then
    nokv_die "serve-rust-store.sh: store $peer_store_id for region $REGION_ID has no configured address"
  fi
  if [[ "$peer_store_id" == "$STORE_ID" ]]; then
    peer_id=$peer_peer_id
  fi
  peer_endpoints+=("${peer_peer_id}=${peer_addr}")
done

if [[ -z "$peer_id" ]]; then
  nokv_die "serve-rust-store.sh: store $STORE_ID is not a peer of region $REGION_ID"
fi

bootstrap=false
if [[ "$STORE_ID" == "$leader_store_id" ]]; then
  bootstrap=true
fi

if [[ -z "$COORDINATOR_ADDR" ]]; then
  coord_scope="host"
  if [[ "$SCOPE" == "docker" ]]; then
    coord_scope="docker"
  fi
  COORDINATOR_ADDR=$(nokv_config_coordinator_addr "$CONFIG" "$coord_scope")
fi
mkdir -p "$WORKDIR"

export NOKV_RUST_RAFTSTORE_ADDR="$listen_addr"
export NOKV_RUST_RAFTSTORE_REGION_ID="$REGION_ID"
export NOKV_RUST_RAFTSTORE_STORE_ID="$STORE_ID"
export NOKV_RUST_RAFTSTORE_PEER_ID="$peer_id"
export NOKV_RUST_RAFTSTORE_BOOTSTRAP="$bootstrap"
export NOKV_RUST_RAFTSTORE_HOLT_DIR="$WORKDIR/holt"
export NOKV_RUST_RAFTSTORE_LOG_DIR="$WORKDIR/raftlog"
export NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS
NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=$(IFS=,; echo "${peer_endpoints[*]}")

if [[ -n "$COORDINATOR_ADDR" ]]; then
  export NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR="$COORDINATOR_ADDR"
fi

exec nokv-raftstore-server "${EXTRA_ARGS[@]}"
