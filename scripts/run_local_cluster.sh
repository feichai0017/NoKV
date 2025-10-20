#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_local_cluster.sh [options]

Options:
  --nodes N         Number of stores to launch (default: 3)
  --base-port PORT  First gRPC port to use (default: 20170)
  --workdir DIR     Base directory for cluster data (default: ./artifacts/cluster)
  --bin PATH        Path to nokv binary (default: build into ./build/nokv)
  --tso-port PORT   Optional TSO HTTP port (example: 9494) to launch alongside stores
USAGE
}

NODES=3
BASE_PORT=20170
WORKDIR=""
BIN=""
TSO_PORT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)
      NODES=$2
      shift 2
      ;;
    --base-port)
      BASE_PORT=$2
      shift 2
      ;;
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --bin)
      BIN=$2
      shift 2
      ;;
    --tso-port)
      TSO_PORT=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ "$NODES" =~ ^[0-9]+$ ]] || [ "$NODES" -lt 1 ]; then
  echo "--nodes must be a positive integer" >&2
  exit 1
fi

if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]] || [ "$BASE_PORT" -lt 1 ]; then
  echo "--base-port must be a positive integer" >&2
  exit 1
fi

if [ -n "$TSO_PORT" ]; then
  if ! [[ "$TSO_PORT" =~ ^[0-9]+$ ]] || [ "$TSO_PORT" -lt 1 ]; then
    echo "--tso-port must be a positive integer" >&2
    exit 1
  fi
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if [ -z "$WORKDIR" ]; then
  WORKDIR="$ROOT_DIR/artifacts/cluster"
fi
mkdir -p "$WORKDIR"

BUILD_DIR="$ROOT_DIR/build"
mkdir -p "$BUILD_DIR"

if [ -z "$BIN" ]; then
  BIN="$BUILD_DIR/nokv"
  go build -o "$BIN" "$ROOT_DIR/cmd/nokv"
fi

MANIFEST_BIN="$BUILD_DIR/nokv-manifest"
go build -o "$MANIFEST_BIN" "$ROOT_DIR/scripts/manifestctl"

declare -a STORE_IDS PEER_IDS ADDRS WORKDIRS
for i in $(seq 1 "$NODES"); do
  STORE_IDS+=("$i")
  PEER_IDS+=($((100 + i)))
  WORKDIRS+=("$WORKDIR/store-$i")
  ADDRS+=("127.0.0.1:$((BASE_PORT + i - 1))")
done

PEER_LIST=()
for i in "${!STORE_IDS[@]}"; do
  PEER_LIST+=("${STORE_IDS[$i]}:${PEER_IDS[$i]}")
done

for i in "${!STORE_IDS[@]}"; do
  mkdir -p "${WORKDIRS[$i]}"
  args=(
    --workdir "${WORKDIRS[$i]}"
    --region-id 1
  )
  for peerEntry in "${PEER_LIST[@]}"; do
    args+=(--peer "$peerEntry")
  done
  "$MANIFEST_BIN" "${args[@]}"
done

PIDS=()

if [ -n "$TSO_PORT" ]; then
  TSO_BIN="$BUILD_DIR/nokv-tso"
  go build -o "$TSO_BIN" "$ROOT_DIR/scripts/tso"
  echo "Starting TSO allocator on 127.0.0.1:${TSO_PORT}"
  "$TSO_BIN" --addr "127.0.0.1:${TSO_PORT}" --start 100 &
  PIDS+=($!)
fi

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

for i in "${!STORE_IDS[@]}"; do
  storeID=${STORE_IDS[$i]}
  addr=${ADDRS[$i]}
  dir=${WORKDIRS[$i]}
  cmd=("$BIN" serve --workdir "$dir" --store-id "$storeID" --addr "$addr")
  for j in "${!STORE_IDS[@]}"; do
    if [ "$i" -eq "$j" ]; then
      continue
    fi
    cmd+=(--peer "${STORE_IDS[$j]}=${ADDRS[$j]}")
  done
  echo "Starting store ${storeID} at ${addr} (workdir=${dir})"
  "${cmd[@]}" &
  PIDS+=($!)
done

if [ -n "$TSO_PORT" ]; then
  echo "Cluster running. TSO available at http://127.0.0.1:${TSO_PORT}/tso"
else
  echo "Cluster running. Press Ctrl+C to stop."
fi
wait
