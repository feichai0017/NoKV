#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-store.sh --config <config> --store-id <id> [options]

Options:
  --scope <local|docker>   Select which addresses to use (default: local)
  --workdir <dir>          Optional workdir override; otherwise resolved from config
  --coordinator-addr <addr>
                           Optional Coordinator gRPC endpoint override passed to "nokv serve"
  --raft-debug-log         Enable verbose etcd/raft debug logging
  --no-raft-debug-log      Disable verbose etcd/raft debug logging
  --extra <args...>        Additional arguments passed to "nokv serve"
USAGE
}

CONFIG=""
STORE_ID=""
WORKDIR=""
SCOPE="local"
COORDINATOR_ADDR=""
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
    --coordinator-addr)
      COORDINATOR_ADDR=$2
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

if [[ -z "$CONFIG" || -z "$STORE_ID" ]]; then
  nokv_die "serve-store.sh: --config and --store-id are required"
fi

if [[ "$SCOPE" != "local" && "$SCOPE" != "docker" ]]; then
  nokv_die "serve-store.sh: --scope must be local or docker"
fi

SERVE_SCOPE="host"
if [[ "$SCOPE" == "docker" ]]; then
  SERVE_SCOPE="docker"
fi

if [[ -z "$COORDINATOR_ADDR" ]]; then
  COORDINATOR_ADDR=$(nokv_config_coordinator_addr "$CONFIG" "$SERVE_SCOPE")
fi

cmd=(nokv serve
  --config "$CONFIG"
  --scope "$SERVE_SCOPE"
  --store-id "$STORE_ID"
)

if [[ -n "$WORKDIR" ]]; then
  cmd+=(--workdir "$WORKDIR")
fi

if [[ $RAFT_DEBUG -eq 1 ]]; then
  cmd+=(--raft-debug-log)
fi

if [[ -n "$COORDINATOR_ADDR" ]]; then
  cmd+=(--coordinator-addr "$COORDINATOR_ADDR")
fi

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
