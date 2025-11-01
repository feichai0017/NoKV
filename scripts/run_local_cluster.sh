#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_local_cluster.sh [options]

Options:
  --config PATH         Raft configuration file (default: ./raft_config.example.json)
  --workdir DIR         Base directory for cluster data (default: ./artifacts/cluster)
  --raft-debug-log      Enable verbose raft debug logging (default: enabled)
  --no-raft-debug-log   Disable raft debug logging
USAGE
}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONFIG_PATH="$ROOT_DIR/raft_config.example.json"
WORKDIR=""
RAFT_DEBUG=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH=$2
      shift 2
      ;;
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --raft-debug-log)
      RAFT_DEBUG=1
      shift
      ;;
    --no-raft-debug-log)
      RAFT_DEBUG=0
      shift
      ;;
    --help|-h)
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

if ! [[ -f "$CONFIG_PATH" ]]; then
  echo "configuration file not found: $CONFIG_PATH" >&2
  exit 1
fi

if [ -z "$WORKDIR" ]; then
  WORKDIR="$ROOT_DIR/artifacts/cluster"
fi
mkdir -p "$WORKDIR"

BUILD_DIR="$ROOT_DIR/build"
mkdir -p "$BUILD_DIR"

go build -o "$BUILD_DIR/nokv" "$ROOT_DIR/cmd/nokv"
go build -o "$BUILD_DIR/nokv-config" "$ROOT_DIR/cmd/nokv-config"
go build -o "$BUILD_DIR/nokv-tso" "$ROOT_DIR/scripts/tso"

cleaned=0
STORE_PIDS=()
TSO_PID=""

cleanup() {
  if [[ $cleaned -eq 1 ]]; then
    return
  fi
  cleaned=1

  for pid in "${STORE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done

  if [[ -n "${TSO_PID:-}" ]] && kill -0 "$TSO_PID" 2>/dev/null; then
    kill -INT "$TSO_PID" 2>/dev/null || true
  fi

  for pid in "${STORE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
    fi
  done

  if [[ -n "${TSO_PID:-}" ]]; then
    wait "$TSO_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

PATH="$BUILD_DIR:$PATH"

mapfile -t STORE_LINES < <(nokv-config stores --config "$CONFIG_PATH" --format simple)
if [ "${#STORE_LINES[@]}" -eq 0 ]; then
  echo "no stores defined in $CONFIG_PATH" >&2
  exit 1
fi

declare -a STORE_IDS STORE_WORKDIRS
for line in "${STORE_LINES[@]}"; do
  read -r store_id listen_addr advertise_addr docker_listen docker_addr <<<"$line"
  STORE_IDS+=("$store_id")
  STORE_WORKDIRS+=("$WORKDIR/store-$store_id")
  store_dir="$WORKDIR/store-$store_id"
  mkdir -p "$store_dir"
  lock_path="$store_dir/LOCK"
  if [[ -f "$lock_path" ]]; then
    echo "Removing stale lock file $lock_path (previous run exited uncleanly)"
    rm -f "$lock_path"
  fi
done

mapfile -t REGION_LINES < <(nokv-config regions --config "$CONFIG_PATH" --format simple)
if [ "${#REGION_LINES[@]}" -eq 0 ]; then
  echo "no regions defined in $CONFIG_PATH" >&2
  exit 1
fi

for idx in "${!STORE_IDS[@]}"; do
  store_dir="${STORE_WORKDIRS[$idx]}"
  if [[ -f "$store_dir/CURRENT" ]]; then
    echo "Store ${STORE_IDS[$idx]} already bootstrapped; skipping manifest seeding"
    continue
  fi
  for region_line in "${REGION_LINES[@]}"; do
    read -r region_id start_key end_key epoch_ver epoch_conf peer_str leader_store <<<"$region_line"
    args=(--workdir "$store_dir" --region-id "$region_id" --epoch-version "$epoch_ver" --epoch-conf-version "$epoch_conf")
    if [[ "$start_key" != "-" ]]; then
      args+=(--start-key "$start_key")
    fi
    if [[ "$end_key" != "-" ]]; then
      args+=(--end-key "$end_key")
    fi
    IFS=',' read -ra peers <<<"$peer_str"
    for peer in "${peers[@]}"; do
      if [[ -n "$peer" ]]; then
        args+=(--peer "$peer")
      fi
    done
    nokv-config manifest "${args[@]}"
  done
done

read -r TSO_LISTEN TSO_URL < <(nokv-config tso --config "$CONFIG_PATH" --format simple 2>/dev/null || echo "- -")

if [[ -n "${TSO_LISTEN:-}" && "$TSO_LISTEN" != "-" ]]; then
  echo "Starting TSO allocator on ${TSO_LISTEN}"
  nokv-tso --addr "$TSO_LISTEN" --start 100 >"$WORKDIR/tso.log" 2>&1 &
  TSO_PID=$!
fi

serve_debug_args=()
if [[ $RAFT_DEBUG -eq 1 ]]; then
  serve_debug_args=(--raft-debug-log)
fi

for idx in "${!STORE_IDS[@]}"; do
  store_id="${STORE_IDS[$idx]}"
  store_dir="${STORE_WORKDIRS[$idx]}"
  echo "Starting store ${store_id} (workdir=${store_dir})"
  scripts/serve_from_config.sh \
    --config "$CONFIG_PATH" \
    --store-id "$store_id" \
    --workdir "$store_dir" \
    "${serve_debug_args[@]}" >"$store_dir/server.log" 2>&1 &
  STORE_PIDS+=($!)
done

if [[ -n "${TSO_URL:-}" && "$TSO_URL" != "-" ]]; then
  echo "Cluster running. TSO available at ${TSO_URL}/tso"
else
  echo "Cluster running (TSO disabled). Press Ctrl+C to stop."
fi
wait
