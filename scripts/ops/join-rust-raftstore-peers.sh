#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/join-rust-raftstore-peers.sh --config <config> [options]

Options:
  --scope <local|docker>   Select which addresses to use (default: local)
  --region-id <id>         Join peers only for one region
  --timeout <duration>     Per-RPC timeout passed to nokv raft-admin (default: 5s)
  --attempts <n>           Retry each AddPeer RPC up to n times (default: 60)
  --sleep <duration>       Sleep between attempts (default: 1s)

Notes:
  - The script assumes each region leader was started with
    NOKV_RUST_RAFTSTORE_BOOTSTRAP=true and peer endpoints configured.
  - It does not seed storage; it only drives the existing RaftAdmin AddPeer RPC.
USAGE
}

CONFIG=""
SCOPE="local"
REGION_FILTER=""
TIMEOUT="5s"
ATTEMPTS=60
SLEEP_FOR="1s"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG=$2
      shift 2
      ;;
    --scope)
      SCOPE=$2
      shift 2
      ;;
    --region-id)
      REGION_FILTER=$2
      shift 2
      ;;
    --timeout)
      TIMEOUT=$2
      shift 2
      ;;
    --attempts)
      ATTEMPTS=$2
      shift 2
      ;;
    --sleep)
      SLEEP_FOR=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "join-rust-raftstore-peers.sh: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG" ]]; then
  nokv_die "join-rust-raftstore-peers.sh: --config is required"
fi

if [[ "$SCOPE" != "local" && "$SCOPE" != "docker" ]]; then
  nokv_die "join-rust-raftstore-peers.sh: --scope must be local or docker"
fi

if ! [[ "$ATTEMPTS" =~ ^[0-9]+$ ]] || [[ "$ATTEMPTS" -eq 0 ]]; then
  nokv_die "join-rust-raftstore-peers.sh: --attempts must be a positive integer"
fi

if ! command -v nokv >/dev/null 2>&1; then
  nokv_die "join-rust-raftstore-peers.sh: nokv binary not found in PATH"
fi

declare -A STORE_ADDRS=()
while IFS= read -r store_line; do
  read -r store_id _host_listen host_addr _docker_listen docker_addr _ <<<"$store_line"
  if [[ "$SCOPE" == "docker" ]]; then
    STORE_ADDRS[$store_id]=$docker_addr
  else
    STORE_ADDRS[$store_id]=$host_addr
  fi
done < <(nokv_config_store_lines "$CONFIG")

join_peer_with_retry() {
  local leader_addr=$1
  local region_id=$2
  local store_id=$3
  local peer_id=$4
  local attempt=1
  while (( attempt <= ATTEMPTS )); do
    if nokv raft-admin add-peer \
      --addr "$leader_addr" \
      --timeout "$TIMEOUT" \
      --region "$region_id" \
      --store "$store_id" \
      --peer "$peer_id" >/dev/null; then
      echo "join-rust-raftstore-peers.sh: added region=$region_id store=$store_id peer=$peer_id via $leader_addr"
      return 0
    fi
    sleep "$SLEEP_FOR"
    attempt=$((attempt + 1))
  done
  nokv_die "join-rust-raftstore-peers.sh: failed to add region=$region_id store=$store_id peer=$peer_id via $leader_addr after $ATTEMPTS attempts"
}

joined=0
while IFS= read -r region_line; do
  read -r region_id _start_key _end_key _epoch_ver _epoch_conf peer_str leader_store_id _ <<<"$region_line"
  if [[ -n "$REGION_FILTER" && "$REGION_FILTER" != "$region_id" ]]; then
    continue
  fi
  leader_addr=${STORE_ADDRS[$leader_store_id]:-}
  if [[ -z "$leader_addr" ]]; then
    nokv_die "join-rust-raftstore-peers.sh: leader store $leader_store_id for region $region_id has no configured address"
  fi
  IFS=',' read -ra peers <<<"$peer_str"
  for peer in "${peers[@]}"; do
    [[ -z "$peer" ]] && continue
    store_id=${peer%%:*}
    peer_id=${peer##*:}
    if [[ "$store_id" == "$leader_store_id" ]]; then
      continue
    fi
    join_peer_with_retry "$leader_addr" "$region_id" "$store_id" "$peer_id"
    joined=$((joined + 1))
  done
done < <(nokv_config_region_lines "$CONFIG")

if [[ "$joined" -eq 0 ]]; then
  echo "join-rust-raftstore-peers.sh: no peer joins were needed"
fi
