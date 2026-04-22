#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-meta-root.sh [options]

Launches one meta-root peer. NoKV only ships the replicated 3-peer topology.

There are two ways to describe the cluster:

  1. --config <file> + --node-id <id>:
     The meta_root.peers section of the config file provides the peer list,
     transport address, and workdir. This is the recommended path since it
     keeps topology in one place.

  2. Explicit flags (--workdir, --transport-addr, --peer x3):
     Use when no config file is available.

Options:
  --config <file>            Raft config file (meta_root.peers drives peer list)
  --scope <host|docker>      Address scope for config resolution (default: host)
  --addr <addr>              Metadata root gRPC listen address (default: 127.0.0.1:2380)
  --workdir <dir>            Metadata root workdir (required unless in --config)
  --node-id <id>             Local node id (required, must be > 0)
  --transport-addr <addr>    Raft transport address (required unless in --config)
  --peer <id=addr>           Peer mapping; repeatable (exactly 3 unless in --config)
  --extra <args...>          Additional arguments passed to "nokv meta-root"
USAGE
}

CONFIG=""
SCOPE="host"
ADDR="127.0.0.1:2380"
WORKDIR=""
NODE_ID=""
TRANSPORT_ADDR=""
PEERS=()
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

if [[ -z "$NODE_ID" ]]; then
  nokv_die "serve-meta-root.sh: --node-id is required"
fi

# When --config is given, let `nokv meta-root` itself resolve peer/transport/
# workdir from the config file. Only pass the explicitly-set flags through.
if [[ -n "$CONFIG" ]]; then
  cmd=(nokv meta-root
    --config "$CONFIG"
    --scope "$SCOPE"
    --node-id "$NODE_ID"
    --addr "$ADDR"
  )
  if [[ -n "$WORKDIR" ]]; then
    cmd+=(--workdir "$WORKDIR")
  fi
  if [[ -n "$TRANSPORT_ADDR" ]]; then
    cmd+=(--transport-addr "$TRANSPORT_ADDR")
  fi
  for peer in "${PEERS[@]}"; do
    cmd+=(--peer "$peer")
  done
else
  # Legacy path: all addresses via flags.
  if [[ -z "$WORKDIR" ]]; then
    nokv_die "serve-meta-root.sh: --workdir is required (or use --config)"
  fi
  if [[ -z "$TRANSPORT_ADDR" ]]; then
    nokv_die "serve-meta-root.sh: --transport-addr is required (or use --config)"
  fi
  if [[ "${#PEERS[@]}" -ne 3 ]]; then
    nokv_die "serve-meta-root.sh: requires exactly 3 --peer values (or use --config)"
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
