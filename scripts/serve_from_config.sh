#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: serve_from_config.sh --config <config> --store-id <id> --workdir <dir> [options]

Options:
  --scope <local|docker>   Select which addresses to use (default: local)
  --pd-addr <addr>         Optional PD gRPC endpoint override passed to "nokv serve"
  --raft-debug-log         Enable verbose etcd/raft debug logging
  --no-raft-debug-log      Disable verbose etcd/raft debug logging
  --extra <args...>        Additional arguments passed to "nokv serve"
USAGE
}

CONFIG=""
STORE_ID=""
WORKDIR=""
SCOPE="local"
PD_ADDR=""
RAFT_DEBUG=0
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
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --scope)
      SCOPE=$2
      shift 2
      ;;
    --pd-addr)
      PD_ADDR=$2
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

if [[ -z "$CONFIG" || -z "$STORE_ID" || -z "$WORKDIR" ]]; then
  echo "serve_from_config: --config, --store-id, and --workdir are required" >&2
  exit 1
fi

if [[ "$SCOPE" != "local" && "$SCOPE" != "docker" ]]; then
  echo "serve_from_config: --scope must be local or docker" >&2
  exit 1
fi

if [[ -z "$PD_ADDR" ]]; then
  pd_scope="host"
  if [[ "$SCOPE" == "docker" ]]; then
    pd_scope="docker"
  fi
  if pd_from_config=$(nokv-config pd --config "$CONFIG" --scope "$pd_scope" --format simple 2>/dev/null); then
    PD_ADDR=$(echo "$pd_from_config" | tr -d '\r' | sed -n '1p')
  fi
fi

mapfile -t STORE_LINES < <(nokv-config stores --config "$CONFIG" --format simple)
if [[ "${#STORE_LINES[@]}" -eq 0 ]]; then
  echo "serve_from_config: no stores defined in $CONFIG" >&2
  exit 1
fi

TARGET_LISTEN=""
peer_args=()
declare -A STORE_PEER_ADDR
declare -A REMOTE_PEERS

for line in "${STORE_LINES[@]}"; do
  read -r pid listen addr docker_listen docker_addr _store_workdir _docker_workdir <<<"$line"
  if [[ -z "$pid" ]]; then
    continue
  fi
  selected_listen="$listen"
  if [[ "$SCOPE" == "docker" && "$docker_listen" != "-" ]]; then
    selected_listen="$docker_listen"
  fi
  STORE_PEER_ADDR["$pid"]="$addr"
  if [[ "$SCOPE" == "docker" && "$docker_addr" != "-" ]]; then
    STORE_PEER_ADDR["$pid"]="$docker_addr"
  fi
  if [[ "$pid" == "$STORE_ID" ]]; then
    TARGET_LISTEN="$selected_listen"
  fi
done

if [[ -z "$TARGET_LISTEN" ]]; then
  echo "serve_from_config: store $STORE_ID not found in $CONFIG" >&2
  exit 1
fi

mapfile -t REGION_LINES < <(nokv-config regions --config "$CONFIG" --format simple)
for region_line in "${REGION_LINES[@]}"; do
  read -r _ start_key end_key _ _ peer_str _ <<<"$region_line"
  IFS=',' read -ra peers <<<"$peer_str"
  for entry in "${peers[@]}"; do
    if [[ -z "$entry" ]]; then
      continue
    fi
    IFS=':' read -r peer_store peer_id <<<"$entry"
    if [[ -z "$peer_store" || -z "$peer_id" ]]; then
      continue
    fi
    if [[ "$peer_store" == "$STORE_ID" ]]; then
      continue
    fi
    addr="${STORE_PEER_ADDR[$peer_store]}"
    if [[ -z "$addr" ]]; then
      continue
    fi
    REMOTE_PEERS["$peer_id"]="$addr"
  done
done

for peer_id in "${!REMOTE_PEERS[@]}"; do
  peer_args+=(--peer "${peer_id}=${REMOTE_PEERS[$peer_id]}")
done

cmd=(nokv serve
  --workdir "$WORKDIR"
  --store-id "$STORE_ID"
  --addr "$TARGET_LISTEN"
)

if [[ $RAFT_DEBUG -eq 1 ]]; then
  cmd+=(--raft-debug-log)
fi

if [[ -n "$PD_ADDR" ]]; then
  cmd+=(--pd-addr "$PD_ADDR")
fi

cmd+=("${peer_args[@]}")
cmd+=("${EXTRA_ARGS[@]}")

child=""
cleanup() {
  if [[ -n "${child:-}" ]] && kill -0 "$child" 2>/dev/null; then
    kill -INT "$child" 2>/dev/null || true
    wait "$child" || true
  fi
}

trap cleanup EXIT INT TERM

"${cmd[@]}" &
child=$!
wait "$child"
status=$?
child=""
trap - EXIT INT TERM
exit $status
