#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-meta-root.sh [options]

Launches one meta-root peer. NoKV only ships the replicated 3-peer topology,
so --node-id, --transport-addr, and exactly 3 --peer values are required.

Options:
  --addr <addr>              Metadata root gRPC listen address (default: 127.0.0.1:2380)
  --workdir <dir>            Metadata root workdir (required)
  --node-id <id>             Local node id (required, must be > 0)
  --transport-addr <addr>    Raft transport address (required)
  --peer <id=addr>           Peer mapping; repeatable (exactly 3)
  --extra <args...>          Additional arguments passed to "nokv meta-root"
USAGE
}

ADDR="127.0.0.1:2380"
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
if [[ -z "$NODE_ID" ]]; then
  nokv_die "serve-meta-root.sh: --node-id is required"
fi
if [[ -z "$TRANSPORT_ADDR" ]]; then
  nokv_die "serve-meta-root.sh: --transport-addr is required"
fi
if [[ "${#PEERS[@]}" -ne 3 ]]; then
  nokv_die "serve-meta-root.sh: requires exactly 3 --peer values"
fi

cmd=(nokv meta-root
  --addr "$ADDR"
  --workdir "$WORKDIR"
  --node-id "$NODE_ID"
  --transport-addr "$TRANSPORT_ADDR"
)
for peer in "${PEERS[@]}"; do
  cmd+=(--peer "$peer")
done

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
