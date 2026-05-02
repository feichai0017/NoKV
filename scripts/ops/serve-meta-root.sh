#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-meta-root.sh --config <file> --node-id <id> [options]

Launches one meta-root peer. The raft configuration is the only topology
source; the script does not accept an alternate peer-list mode.

Options:
  --config <file>            Raft config file (meta_root.peers drives peer list)
  --scope <host|docker>      Address scope for config resolution (default: host)
  --addr <addr>              Metadata root gRPC listen address (default: 127.0.0.1:2380)
  --workdir <dir>            Optional metadata root workdir override
  --node-id <id>             Local node id (required, must be > 0)
  --extra <args...>          Additional arguments passed to "nokv meta-root"
USAGE
}

CONFIG=""
SCOPE="host"
ADDR="127.0.0.1:2380"
WORKDIR=""
NODE_ID=""
EXTRA_ARGS=()

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

if [[ -z "$CONFIG" ]]; then
  nokv_die "serve-meta-root.sh: --config is required"
fi
if [[ -z "$NODE_ID" ]]; then
  nokv_die "serve-meta-root.sh: --node-id is required"
fi

cmd=(nokv meta-root
  --config "$CONFIG"
  --scope "$SCOPE"
  --node-id "$NODE_ID"
  --addr "$ADDR"
)
if [[ -n "$WORKDIR" ]]; then
  cmd+=(--workdir "$WORKDIR")
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
