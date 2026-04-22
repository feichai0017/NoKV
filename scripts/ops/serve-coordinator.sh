#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/serve-coordinator.sh [options]

Launches a single coordinator process wired to an external 3-peer meta-root
cluster. This is the only deployment topology NoKV supports.

Options:
  --config <file>           Raft config file (meta_root.peers drives --root-peer)
  --scope <host|docker>     Address scope for config resolution (default: host)
  --addr <addr>             Coordinator gRPC listen address (default: 127.0.0.1:2379)
  --metrics-addr <addr>     Optional /debug/vars expvar listen address
  --coordinator-id <id>     Stable coordinator lease owner id (required)
  --root-peer <id=addr>     Meta-root gRPC peer mapping; repeatable (exactly 3 unless --config supplies them)
  --root-refresh <dur>      Rooted refresh interval (default: 200ms)
  --lease-ttl <dur>         Coordinator lease ttl (default: 10s)
  --lease-renew-before <dur> Renew window before lease expiry (default: 3s)
  --extra <args...>         Additional arguments passed to "nokv coordinator"
USAGE
}

CONFIG=""
SCOPE="host"
ADDR="127.0.0.1:2379"
METRICS_ADDR=""
COORDINATOR_ID=""
ROOT_REFRESH=""
LEASE_TTL=""
LEASE_RENEW_BEFORE=""
ROOT_PEERS=()
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
    --metrics-addr)
      METRICS_ADDR=$2
      shift 2
      ;;
    --coordinator-id)
      COORDINATOR_ID=$2
      shift 2
      ;;
    --root-refresh)
      ROOT_REFRESH=$2
      shift 2
      ;;
    --lease-ttl)
      LEASE_TTL=$2
      shift 2
      ;;
    --lease-renew-before)
      LEASE_RENEW_BEFORE=$2
      shift 2
      ;;
    --root-peer)
      ROOT_PEERS+=("$2")
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

if [[ -z "$COORDINATOR_ID" ]]; then
  nokv_die "serve-coordinator.sh: --coordinator-id is required"
fi
# When --config is given, peers come from the config file; otherwise require
# exactly 3 --root-peer flags.
if [[ -z "$CONFIG" && "${#ROOT_PEERS[@]}" -ne 3 ]]; then
  nokv_die "serve-coordinator.sh: exactly 3 --root-peer values required (or use --config)"
fi

cmd=(nokv coordinator
  --addr "$ADDR"
  --coordinator-id "$COORDINATOR_ID"
)

if [[ -n "$CONFIG" ]]; then
  cmd+=(--config "$CONFIG" --scope "$SCOPE")
fi

if [[ -n "$METRICS_ADDR" ]]; then
  cmd+=(--metrics-addr "$METRICS_ADDR")
fi
if [[ -n "$ROOT_REFRESH" ]]; then
  cmd+=(--root-refresh "$ROOT_REFRESH")
fi
if [[ -n "$LEASE_TTL" ]]; then
  cmd+=(--lease-ttl "$LEASE_TTL")
fi
if [[ -n "$LEASE_RENEW_BEFORE" ]]; then
  cmd+=(--lease-renew-before "$LEASE_RENEW_BEFORE")
fi

for peer in "${ROOT_PEERS[@]}"; do
  cmd+=(--root-peer "$peer")
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
