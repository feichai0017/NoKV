#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_local_cluster.sh [options]

Options:
  --nodes N         Number of stores to launch (default: 2)
  --base-port PORT  First gRPC port to use (default: 20170)
  --workdir DIR     Base directory for cluster data (default: ./artifacts/cluster)
  --bin PATH        Path to nokv binary (default: build into ./bin/nokv)
USAGE
}

NODES=2
BASE_PORT=20170
WORKDIR=""
BIN=""

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

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if [ -z "$WORKDIR" ]; then
  WORKDIR="$ROOT_DIR/artifacts/cluster"
fi
mkdir -p "$WORKDIR"

if [ -z "$BIN" ]; then
  BIN="$ROOT_DIR/bin/nokv"
  mkdir -p "$(dirname "$BIN")"
  go build -o "$BIN" "$ROOT_DIR/cmd/nokv"
fi

TMP_BOOTSTRAP=$(mktemp -d)
cat >"$TMP_BOOTSTRAP/bootstrap.go" <<'EOF'
package main

import (
    "log"
    "os"
    "strconv"
    "strings"

    "github.com/feichai0017/NoKV/manifest"
)

func main() {
    if len(os.Args) != 4 {
        log.Fatalf("usage: bootstrap <workdir> <regionID> <storeID:peerID,...>")
    }
    workdir := os.Args[1]
    regionID, err := strconv.ParseUint(os.Args[2], 10, 64)
    if err != nil {
        log.Fatalf("invalid region id: %v", err)
    }
    peersArg := os.Args[3]
    peers := strings.Split(peersArg, ",")
    meta := manifest.RegionMeta{
        ID:    regionID,
        State: manifest.RegionStateRunning,
        Epoch: manifest.RegionEpoch{Version: 1, ConfVersion: uint64(len(peers))},
    }
    for _, entry := range peers {
        entry = strings.TrimSpace(entry)
        if entry == "" {
            continue
        }
        parts := strings.Split(entry, ":")
        if len(parts) != 2 {
            log.Fatalf("invalid peer entry %q", entry)
        }
        storeID, err := strconv.ParseUint(parts[0], 10, 64)
        if err != nil {
            log.Fatalf("invalid store id in %q: %v", entry, err)
        }
        peerID, err := strconv.ParseUint(parts[1], 10, 64)
        if err != nil {
            log.Fatalf("invalid peer id in %q: %v", entry, err)
        }
        meta.Peers = append(meta.Peers, manifest.PeerMeta{StoreID: storeID, PeerID: peerID})
    }
    mgr, err := manifest.Open(workdir)
    if err != nil {
        log.Fatalf("open manifest: %v", err)
    }
    defer mgr.Close()
    if err := mgr.LogRegionUpdate(meta); err != nil {
        log.Fatalf("log region: %v", err)
    }
}
EOF

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
PEER_CSV=$(IFS=,; echo "${PEER_LIST[*]}")

for i in "${!STORE_IDS[@]}"; do
  mkdir -p "${WORKDIRS[$i]}"
  go run "$TMP_BOOTSTRAP/bootstrap.go" "${WORKDIRS[$i]}" "1" "$PEER_CSV"
done

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMP_BOOTSTRAP"
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

echo "Cluster running. Press Ctrl+C to stop."
wait
