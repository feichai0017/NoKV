#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-meta-root.sh [options]

Options:
  --addr <addr>              Metadata root gRPC listen address (default: 127.0.0.1:2380)
  --mode <local|replicated>  Metadata root mode (default: local)
  --metrics-addr <addr>      Optional expvar endpoint for metadata root
  --workdir <dir>            Metadata root workdir (required)
  --node-id <id>             Replicated mode local node id
  --transport-addr <addr>    Replicated mode raft transport address
  --peer <id=addr>           Replicated mode peer mapping; may be repeated
  --extra <args...>          Additional arguments passed to "nokv meta-root"
USAGE
}

ADDR="127.0.0.1:2380"
MODE="local"
METRICS_ADDR=""
WORKDIR=""
NODE_ID=""
TRANSPORT_ADDR=""
PEERS=()
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --addr)
      ADDR=$2
      shift 2
      ;;
    --mode)
      MODE=$2
      shift 2
      ;;
    --metrics-addr)
      METRICS_ADDR=$2
      shift 2
      ;;
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --node-id)
      NODE_ID=$2
      shift 2
      ;;
    --transport-addr)
      TRANSPORT_ADDR=$2
      shift 2
      ;;
    --peer)
      PEERS+=("$2")
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

if [[ -z "$WORKDIR" ]]; then
  nokv_die "serve-meta-root.sh: --workdir is required"
fi

MODE=$(echo "$MODE" | tr '[:upper:]' '[:lower:]')
if [[ "$MODE" != "local" && "$MODE" != "replicated" ]]; then
  nokv_die "serve-meta-root.sh: --mode must be local or replicated"
fi

cmd=(nokv meta-root
  --addr "$ADDR"
  --mode "$MODE"
  --workdir "$WORKDIR"
)

if [[ -n "$METRICS_ADDR" ]]; then
  cmd+=(--metrics-addr "$METRICS_ADDR")
fi

if [[ "$MODE" == "replicated" ]]; then
  if [[ -z "$NODE_ID" || -z "$TRANSPORT_ADDR" ]]; then
    nokv_die "serve-meta-root.sh: replicated mode requires --node-id and --transport-addr"
  fi
  if [[ "${#PEERS[@]}" -ne 3 ]]; then
    nokv_die "serve-meta-root.sh: replicated mode requires exactly 3 --peer values"
  fi
  cmd+=(--node-id "$NODE_ID" --transport-addr "$TRANSPORT_ADDR")
  for peer in "${PEERS[@]}"; do
    cmd+=(--peer "$peer")
  done
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
