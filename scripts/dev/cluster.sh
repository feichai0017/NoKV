#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"
source "$SCRIPT_DIR/../lib/workdir.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/dev/cluster.sh [options]

Options:
  --config PATH         Raft configuration file (default: ./raft_config.example.json)
  --workdir DIR         Base directory for cluster data (default: ./artifacts/cluster)
  --coordinator-listen ADDR      Coordinator gRPC listen address override (default: config.coordinator.addr or 127.0.0.1:2379)
  --raft-debug-log      Enable verbose raft debug logging (default: enabled)
  --no-raft-debug-log   Disable raft debug logging
USAGE
}

ROOT_DIR=$NOKV_ROOT_DIR
CONFIG_PATH="$ROOT_DIR/raft_config.example.json"
WORKDIR=""
WORKDIR_SET=0
COORDINATOR_LISTEN=""
COORDINATOR_LISTEN_SET=0
RAFT_DEBUG=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH=$2
      shift 2
      ;;
    --workdir)
      WORKDIR=$2
      WORKDIR_SET=1
      shift 2
      ;;
    --coordinator-listen)
      COORDINATOR_LISTEN=$2
      COORDINATOR_LISTEN_SET=1
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
      echo "cluster.sh: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ -f "$CONFIG_PATH" ]]; then
  nokv_die "cluster.sh: configuration file not found: $CONFIG_PATH"
fi
CONFIG_DIR=$(nokv_config_dir "$CONFIG_PATH")

if [ -z "$WORKDIR" ]; then
  WORKDIR="$ROOT_DIR/artifacts/cluster"
fi
mkdir -p "$WORKDIR"

nokv_build_cli_binaries

start_with_logs() {
  local __pid_var=$1
  local prefix=$2
  local logfile=$3
  shift 3

  "$@" > >(sed -u "s/^/[$prefix] /" | tee "$logfile") 2>&1 &
  printf -v "$__pid_var" '%s' "$!"
}

cleaned=0
STORE_PIDS=()
COORDINATOR_PID=""

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

  if [[ -n "${COORDINATOR_PID:-}" ]] && kill -0 "$COORDINATOR_PID" 2>/dev/null; then
    kill -INT "$COORDINATOR_PID" 2>/dev/null || true
  fi

  for pid in "${STORE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
    fi
  done

  if [[ -n "${COORDINATOR_PID:-}" ]]; then
    wait "$COORDINATOR_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

nokv_prepend_build_path

if [[ $COORDINATOR_LISTEN_SET -eq 0 ]]; then
  COORDINATOR_LISTEN=$(nokv_config_coordinator_addr "$CONFIG_PATH" host)
fi
if [[ -z "$COORDINATOR_LISTEN" ]]; then
  COORDINATOR_LISTEN="127.0.0.1:2379"
fi

COORDINATOR_WORKDIR=$(nokv_config_coordinator_workdir "$CONFIG_PATH")
if [[ -z "$COORDINATOR_WORKDIR" ]]; then
  COORDINATOR_WORKDIR="$WORKDIR/coordinator"
else
  COORDINATOR_WORKDIR=$(nokv_resolve_path "$CONFIG_DIR" "$COORDINATOR_WORKDIR")
fi
mkdir -p "$COORDINATOR_WORKDIR"

STORE_LINES=()
while IFS= read -r _line; do STORE_LINES+=("$_line"); done < <(nokv_config_store_lines "$CONFIG_PATH")
if [ "${#STORE_LINES[@]}" -eq 0 ]; then
  nokv_die "cluster.sh: no stores defined in $CONFIG_PATH"
fi

declare -a STORE_IDS STORE_WORKDIRS
for line in "${STORE_LINES[@]}"; do
  read -r store_id listen_addr advertise_addr docker_listen docker_addr store_workdir docker_workdir <<<"$line"
  STORE_IDS+=("$store_id")
  store_dir=""
  if [[ $WORKDIR_SET -eq 1 ]]; then
    store_dir="$WORKDIR/store-$store_id"
  elif [[ -n "${store_workdir:-}" && "$store_workdir" != "-" ]]; then
    store_dir=$(nokv_resolve_path "$CONFIG_DIR" "$store_workdir")
  fi
  if [[ -z "$store_dir" ]]; then
    store_dir="$WORKDIR/store-$store_id"
  fi
  STORE_WORKDIRS+=("$store_dir")
  mkdir -p "$store_dir"
  lock_path="$store_dir/LOCK"
  if [[ -f "$lock_path" ]]; then
    echo "Removing stale lock file $lock_path (previous run exited uncleanly)"
    rm -f "$lock_path"
  fi
done

REGION_LINES=()
while IFS= read -r _line; do REGION_LINES+=("$_line"); done < <(nokv_config_region_lines "$CONFIG_PATH")
if [ "${#REGION_LINES[@]}" -eq 0 ]; then
  nokv_die "cluster.sh: no regions defined in $CONFIG_PATH"
fi

for idx in "${!STORE_IDS[@]}"; do
  store_dir="${STORE_WORKDIRS[$idx]}"
  if [[ -f "$store_dir/CURRENT" ]]; then
    echo "Store ${STORE_IDS[$idx]} already bootstrapped; skipping manifest seeding"
    continue
  fi
  nokv_assert_fresh_workdir "$store_dir" "cluster.sh: store ${STORE_IDS[$idx]} has stale files; refusing to seed into dirty directory"
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
    nokv-config catalog "${args[@]}"
  done
done

echo "Starting Coordinator service on ${COORDINATOR_LISTEN}"
start_with_logs COORDINATOR_PID "coordinator" "$WORKDIR/coordinator.log" \
  nokv coordinator --addr "$COORDINATOR_LISTEN" --id-start 1 --ts-start 100 --workdir "$COORDINATOR_WORKDIR"

serve_debug_args=()
if [[ $RAFT_DEBUG -eq 1 ]]; then
  serve_debug_args=(--raft-debug-log)
fi

for idx in "${!STORE_IDS[@]}"; do
  store_id="${STORE_IDS[$idx]}"
  store_dir="${STORE_WORKDIRS[$idx]}"
  echo "Starting store ${store_id} (workdir=${store_dir})"
  serve_args=(
    --config "$CONFIG_PATH"
    --store-id "$store_id"
    --workdir "$store_dir"
    --coordinator-addr "$COORDINATOR_LISTEN"
    "${serve_debug_args[@]}"
  )
  start_with_logs store_pid "store-${store_id}" "$store_dir/server.log" \
    "$ROOT_DIR/scripts/dev/serve-store.sh" "${serve_args[@]}"
  STORE_PIDS+=("$store_pid")
done

echo "Cluster running. Coordinator available at ${COORDINATOR_LISTEN}"
echo "Logs are streaming to this terminal and saved under ${WORKDIR}"
wait
