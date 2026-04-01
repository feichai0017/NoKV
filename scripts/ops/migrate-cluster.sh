#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"
source "$SCRIPT_DIR/../lib/workdir.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/migrate-cluster.sh [options]

Options:
  --config PATH            Raft configuration file (default: ./raft_config.example.json)
  --workdir DIR            Existing standalone seed workdir (required)
  --seed-store ID          Store ID for the seed node (required)
  --seed-region ID         Region ID used during migrate init (required)
  --seed-peer ID           Peer ID used during migrate init (required)
  --target SPEC            Target peer rollout in <store>:<peer>[@addr] form; may be repeated
  --pd-listen ADDR         PD gRPC listen/address override
  --wait DURATION          Wait timeout passed to migrate commands (default: 30s)
  --poll-interval DURATION Poll interval passed to migrate commands (default: 200ms)
  --transfer-leader PEER   Optional peer ID to transfer leadership to after expansion
  --remove-peer PEER       Optional peer ID to remove after expansion/leader transfer; may be repeated
  --dry-run                Print the planned commands without executing them
  --raft-debug-log         Enable verbose raft debug logging (default: enabled)
  --no-raft-debug-log      Disable verbose raft debug logging
  --help, -h               Show this help

Notes:
  - The seed workdir must already contain standalone data.
  - Target stores must use fresh workdirs; this script refuses to reuse existing stores.
  - The script starts PD and store processes locally, runs the migration flow, then keeps
    the cluster running in the foreground until interrupted.
USAGE
}

ROOT_DIR=$NOKV_ROOT_DIR
CONFIG_PATH="$ROOT_DIR/raft_config.example.json"
WORKDIR=""
SEED_STORE_ID=""
SEED_REGION_ID=""
SEED_PEER_ID=""
PD_LISTEN=""
PD_LISTEN_SET=0
WAIT_TIMEOUT="30s"
POLL_INTERVAL="200ms"
DRY_RUN=0
RAFT_DEBUG=1
TRANSFER_LEADER_PEER=""
declare -a TARGET_SPECS=()
declare -a REMOVE_PEERS=()
CURRENT_STAGE="bootstrap"
REPORT_FILE=""
REPORT_JSON_FILE=""

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
    --seed-store)
      SEED_STORE_ID=$2
      shift 2
      ;;
    --seed-region)
      SEED_REGION_ID=$2
      shift 2
      ;;
    --seed-peer)
      SEED_PEER_ID=$2
      shift 2
      ;;
    --target)
      TARGET_SPECS+=("$2")
      shift 2
      ;;
    --pd-listen)
      PD_LISTEN=$2
      PD_LISTEN_SET=1
      shift 2
      ;;
    --wait)
      WAIT_TIMEOUT=$2
      shift 2
      ;;
    --poll-interval)
      POLL_INTERVAL=$2
      shift 2
      ;;
    --transfer-leader)
      TRANSFER_LEADER_PEER=$2
      shift 2
      ;;
    --remove-peer)
      REMOVE_PEERS+=("$2")
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
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
      echo "migrate-cluster.sh: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$WORKDIR" || -z "$SEED_STORE_ID" || -z "$SEED_REGION_ID" || -z "$SEED_PEER_ID" ]]; then
  nokv_die "migrate-cluster.sh: --workdir, --seed-store, --seed-region, and --seed-peer are required"
fi
if [[ ${#TARGET_SPECS[@]} -eq 0 ]]; then
  nokv_die "migrate-cluster.sh: at least one --target <store>:<peer>[@addr] is required"
fi
if [[ ! -f "$CONFIG_PATH" ]]; then
  nokv_die "migrate-cluster.sh: configuration file not found: $CONFIG_PATH"
fi
if [[ ! -d "$WORKDIR" ]]; then
  nokv_die "migrate-cluster.sh: seed workdir not found: $WORKDIR"
fi
if [[ ! -f "$WORKDIR/CURRENT" ]]; then
  nokv_die "migrate-cluster.sh: seed workdir does not look like a standalone NoKV directory (missing CURRENT): $WORKDIR"
fi

CONFIG_DIR=$(nokv_config_dir "$CONFIG_PATH")
nokv_build_cli_binaries
nokv_prepend_build_path

if [[ $PD_LISTEN_SET -eq 0 ]]; then
  PD_LISTEN=$(nokv_config_pd_addr "$CONFIG_PATH" host)
fi
if [[ -z "$PD_LISTEN" ]]; then
  PD_LISTEN="127.0.0.1:2379"
fi

PD_WORKDIR=$(nokv_config_pd_workdir "$CONFIG_PATH")
if [[ -z "$PD_WORKDIR" ]]; then
  PD_WORKDIR="$ROOT_DIR/artifacts/migration/pd"
else
  PD_WORKDIR=$(nokv_resolve_path "$CONFIG_DIR" "$PD_WORKDIR")
fi
mkdir -p "$PD_WORKDIR"

run_cmd() {
  echo "+ $*"
  if [[ $DRY_RUN -eq 0 ]]; then
    "$@"
  fi
}

stage() {
  local title=$1
  CURRENT_STAGE=$title
  echo
  echo "==> $title"
}

info() {
  echo "[info] $*"
}

warn() {
  echo "[warn] $*" >&2
}

show_local_status() {
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "+ nokv migrate status --workdir $WORKDIR"
    return
  fi
  nokv migrate status --workdir "$WORKDIR" | sed 's/^/    /'
}

write_report() {
  local report_dir="$ROOT_DIR/artifacts/migration"
  REPORT_FILE="$report_dir/summary.txt"
  REPORT_JSON_FILE="$report_dir/summary.json"
  mkdir -p "$report_dir"
  {
    echo "NoKV migration summary"
    echo "======================"
    echo "Config:            $CONFIG_PATH"
    echo "Seed workdir:      $WORKDIR"
    echo "Seed store:        $SEED_STORE_ID"
    echo "Seed region:       $SEED_REGION_ID"
    echo "Seed peer:         $SEED_PEER_ID"
    echo "PD address:        $PD_LISTEN"
    echo "Leader admin:      $leader_admin_addr"
    echo "Wait timeout:      $WAIT_TIMEOUT"
    echo "Poll interval:     $POLL_INTERVAL"
    echo "Targets:"
    for target in "${EXPAND_TARGETS[@]}"; do
      echo "  - $target"
    done
    if [[ -n "$TRANSFER_LEADER_PEER" ]]; then
      echo "Transfer leader:   $TRANSFER_LEADER_PEER"
    fi
    if [[ ${#REMOVE_PEERS[@]} -gt 0 ]]; then
      echo "Removed peers:"
      for peer in "${REMOVE_PEERS[@]}"; do
        echo "  - $peer"
      done
    fi
    echo
    echo "Migration report:"
    if [[ $DRY_RUN -eq 0 ]]; then
      nokv migrate report --workdir "$WORKDIR" --addr "$leader_admin_addr" --region "$SEED_REGION_ID" | sed 's/^/  /'
    else
      echo "  dry-run: report not collected"
    fi
  } >"$REPORT_FILE"
  if [[ $DRY_RUN -eq 0 ]]; then
    nokv migrate report --workdir "$WORKDIR" --addr "$leader_admin_addr" --region "$SEED_REGION_ID" --json >"$REPORT_JSON_FILE"
  else
    cat >"$REPORT_JSON_FILE" <<'EOF'
{
  "dry_run": true
}
EOF
  fi
}

on_error() {
  local line=$1
  warn "migration failed during stage: $CURRENT_STAGE (line $line)"
  warn "inspect logs under $ROOT_DIR/artifacts/migration and target workdirs"
  if [[ -n "$REPORT_FILE" ]]; then
    warn "last summary report: $REPORT_FILE"
  fi
  if [[ -n "$REPORT_JSON_FILE" ]]; then
    warn "last machine-readable report: $REPORT_JSON_FILE"
  fi
}

trap 'on_error $LINENO' ERR

start_with_logs() {
  local __pid_var=$1
  local prefix=$2
  local logfile=$3
  shift 3
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "+ $* > $logfile"
    printf -v "$__pid_var" '%s' ""
    return
  fi
  "$@" > >(sed -u "s/^/[$prefix] /" | tee "$logfile") 2>&1 &
  printf -v "$__pid_var" '%s' "$!"
}

declare -A STORE_LISTEN_ADDR
declare -A STORE_ADMIN_ADDR
declare -A STORE_WORKDIR
STORE_LINES=()
while IFS= read -r _line; do STORE_LINES+=("$_line"); done < <(nokv_config_store_lines "$CONFIG_PATH")
if [[ ${#STORE_LINES[@]} -eq 0 ]]; then
  nokv_die "migrate-cluster.sh: no stores defined in $CONFIG_PATH"
fi
for line in "${STORE_LINES[@]}"; do
  read -r store_id listen_addr advertise_addr docker_listen docker_addr store_workdir docker_workdir <<<"$line"
  if [[ -z "$store_id" ]]; then
    continue
  fi
  STORE_LISTEN_ADDR["$store_id"]="$listen_addr"
  STORE_ADMIN_ADDR["$store_id"]="$advertise_addr"
  if [[ -n "${store_workdir:-}" && "$store_workdir" != "-" ]]; then
    STORE_WORKDIR["$store_id"]="$(nokv_resolve_path "$CONFIG_DIR" "$store_workdir")"
  fi
done

if [[ -z "${STORE_ADMIN_ADDR[$SEED_STORE_ID]:-}" || -z "${STORE_LISTEN_ADDR[$SEED_STORE_ID]:-}" ]]; then
  nokv_die "migrate-cluster.sh: seed store $SEED_STORE_ID not found in config"
fi

parse_target() {
  local value=$1
  local addr=""
  local base="$value"
  if [[ "$value" == *"@"* ]]; then
    addr=${value#*@}
    base=${value%@*}
  fi
  local store=${base%%:*}
  local peer=${base##*:}
  if [[ -z "$store" || -z "$peer" || "$store" == "$base" ]]; then
    nokv_die "migrate-cluster.sh: invalid target $value, want <store>:<peer>[@addr]"
  fi
  printf '%s\n%s\n%s\n' "$store" "$peer" "$addr"
}

declare -a EXPAND_TARGETS=()
declare -a TARGET_STORE_IDS=()
declare -A TARGET_ADMIN_ADDR_BY_STORE

declare -A PEER_TO_ADMIN_ADDR
PEER_TO_ADMIN_ADDR["$SEED_PEER_ID"]="${STORE_ADMIN_ADDR[$SEED_STORE_ID]}"
for spec in "${TARGET_SPECS[@]}"; do
  parsed=$(parse_target "$spec")
  target_store=$(echo "$parsed" | sed -n '1p')
  target_peer=$(echo "$parsed" | sed -n '2p')
  target_addr=$(echo "$parsed" | sed -n '3p')
  if [[ "$target_store" == "$SEED_STORE_ID" ]]; then
    nokv_die "migrate-cluster.sh: target store $target_store matches seed store"
  fi
  if [[ -z "${STORE_ADMIN_ADDR[$target_store]:-}" || -z "${STORE_LISTEN_ADDR[$target_store]:-}" ]]; then
    nokv_die "migrate-cluster.sh: target store $target_store not found in config"
  fi
  if [[ -z "$target_addr" ]]; then
    target_addr=${STORE_ADMIN_ADDR[$target_store]}
  fi
  target_dir=${STORE_WORKDIR[$target_store]:-}
  if [[ -z "$target_dir" ]]; then
    target_dir="$ROOT_DIR/artifacts/cluster/store-$target_store"
  fi
  if [[ "$target_dir" == "$WORKDIR" ]]; then
    nokv_die "migrate-cluster.sh: target store $target_store reuses the seed workdir: $target_dir"
  fi
  nokv_assert_fresh_workdir "$target_dir" "migrate-cluster.sh: target store $target_store workdir is not empty enough for fresh peer bootstrap"
  TARGET_STORE_IDS+=("$target_store")
  TARGET_ADMIN_ADDR_BY_STORE["$target_store"]="$target_addr"
  PEER_TO_ADMIN_ADDR["$target_peer"]="$target_addr"
  EXPAND_TARGETS+=("${target_store}:${target_peer}@${target_addr}")
done

leader_admin_addr=${STORE_ADMIN_ADDR[$SEED_STORE_ID]}
seed_lock_path="$WORKDIR/LOCK"
if [[ -f "$seed_lock_path" ]]; then
  rm -f "$seed_lock_path"
fi

cleaned=0
STORE_PIDS=()
PD_PID=""
cleanup() {
  if [[ $cleaned -eq 1 ]]; then
    return
  fi
  cleaned=1
  for pid in "${STORE_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "${PD_PID:-}" ]] && kill -0 "$PD_PID" 2>/dev/null; then
    kill -INT "$PD_PID" 2>/dev/null || true
  fi
  for pid in "${STORE_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
    fi
  done
  if [[ -n "${PD_PID:-}" ]]; then
    wait "$PD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

serve_debug_args=()
if [[ $RAFT_DEBUG -eq 1 ]]; then
  serve_debug_args=(--raft-debug-log)
fi

mkdir -p "$ROOT_DIR/artifacts/migration"

stage "Preflight"
info "seed workdir: $WORKDIR"
info "targets: ${EXPAND_TARGETS[*]}"
run_cmd nokv migrate plan --workdir "$WORKDIR"

stage "Promote Standalone Workdir"
run_cmd nokv migrate init --workdir "$WORKDIR" --store "$SEED_STORE_ID" --region "$SEED_REGION_ID" --peer "$SEED_PEER_ID"
info "local status after init:"
show_local_status

stage "Start Control Plane"
info "starting PD service on $PD_LISTEN"
start_with_logs PD_PID "pd" "$ROOT_DIR/artifacts/migration/pd.log" \
  nokv pd --addr "$PD_LISTEN" --id-start 1 --ts-start 100 --workdir "$PD_WORKDIR"

seed_log="$WORKDIR/server.log"
stage "Start Seed Store"
info "starting seed store ${SEED_STORE_ID} (workdir=${WORKDIR})"
start_with_logs seed_pid "store-${SEED_STORE_ID}" "$seed_log" \
  "$ROOT_DIR/scripts/dev/serve-store.sh" \
  --config "$CONFIG_PATH" \
  --store-id "$SEED_STORE_ID" \
  --workdir "$WORKDIR" \
  --pd-addr "$PD_LISTEN" \
  "${serve_debug_args[@]}"
STORE_PIDS+=("$seed_pid")

stage "Start Target Stores"
for target_store in "${TARGET_STORE_IDS[@]}"; do
  target_dir=${STORE_WORKDIR[$target_store]:-"$ROOT_DIR/artifacts/cluster/store-$target_store"}
  info "starting target store ${target_store} (workdir=${target_dir})"
  start_with_logs store_pid "store-${target_store}" "$target_dir/server.log" \
    "$ROOT_DIR/scripts/dev/serve-store.sh" \
    --config "$CONFIG_PATH" \
    --store-id "$target_store" \
    --workdir "$target_dir" \
    --pd-addr "$PD_LISTEN" \
    "${serve_debug_args[@]}"
  STORE_PIDS+=("$store_pid")
done

if [[ $DRY_RUN -eq 0 ]]; then
  stage "Wait For Services"
  nokv_wait_for_tcp "$PD_LISTEN" 30
  nokv_wait_for_tcp "$leader_admin_addr" 30
  for target_store in "${TARGET_STORE_IDS[@]}"; do
    nokv_wait_for_tcp "${TARGET_ADMIN_ADDR_BY_STORE[$target_store]}" 30
  done
fi

stage "Expand Seed Region"
expand_cmd=(nokv migrate expand --workdir "$WORKDIR" --addr "$leader_admin_addr" --region "$SEED_REGION_ID" --wait "$WAIT_TIMEOUT" --poll-interval "$POLL_INTERVAL")
for target in "${EXPAND_TARGETS[@]}"; do
  expand_cmd+=(--target "$target")
done
run_cmd "${expand_cmd[@]}"

if [[ -n "$TRANSFER_LEADER_PEER" ]]; then
  stage "Transfer Leader"
  transfer_target_addr=${PEER_TO_ADMIN_ADDR[$TRANSFER_LEADER_PEER]:-}
  transfer_cmd=(nokv migrate transfer-leader
    --workdir "$WORKDIR"
    --addr "$leader_admin_addr"
    --region "$SEED_REGION_ID"
    --peer "$TRANSFER_LEADER_PEER"
    --wait "$WAIT_TIMEOUT"
    --poll-interval "$POLL_INTERVAL"
  )
  if [[ -n "$transfer_target_addr" ]]; then
    transfer_cmd+=(--target-addr "$transfer_target_addr")
  fi
  run_cmd "${transfer_cmd[@]}"
  if [[ -n "$transfer_target_addr" ]]; then
    leader_admin_addr="$transfer_target_addr"
  fi
fi

for remove_peer in "${REMOVE_PEERS[@]}"; do
  stage "Remove Peer $remove_peer"
  remove_target_addr=${PEER_TO_ADMIN_ADDR[$remove_peer]:-}
  if [[ -n "$remove_target_addr" ]]; then
    run_cmd nokv migrate remove-peer \
      --workdir "$WORKDIR" \
      --addr "$leader_admin_addr" \
      --target-addr "$remove_target_addr" \
      --region "$SEED_REGION_ID" \
      --peer "$remove_peer" \
      --wait "$WAIT_TIMEOUT" \
      --poll-interval "$POLL_INTERVAL"
  else
    run_cmd nokv migrate remove-peer \
      --workdir "$WORKDIR" \
      --addr "$leader_admin_addr" \
      --region "$SEED_REGION_ID" \
      --peer "$remove_peer" \
      --wait "$WAIT_TIMEOUT" \
      --poll-interval "$POLL_INTERVAL"
  fi
done

stage "Migration Summary"
write_report
info "final local status:"
show_local_status
info "summary report written to $REPORT_FILE"
info "machine-readable report written to $REPORT_JSON_FILE"
echo "Migration flow completed. Cluster logs are streaming; press Ctrl+C to stop all spawned processes."
wait
