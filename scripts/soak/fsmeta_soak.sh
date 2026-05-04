#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/common.sh"

ADDR="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
MOUNT="${NOKV_FSMETA_MOUNT:-default}"
DURATION="${NOKV_SOAK_DURATION:-24h}"
STEPS="${NOKV_SOAK_STEPS:-80}"
BATCH="${NOKV_SOAK_BATCH:-3}"
SEED_START="${NOKV_SOAK_SEED_START:-1}"
RESTART_INTERVAL="${NOKV_SOAK_RESTART_INTERVAL:-10m}"
READY_TIMEOUT="${NOKV_SOAK_READY_TIMEOUT:-180}"
TOOLS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-soak.XXXXXX")"
chaos_pid=""

dump_cluster_state() {
  if [[ "${NOKV_SOAK_DUMP_LOGS:-1}" != "1" ]]; then
    return
  fi
  docker compose ps || true
  docker compose logs --no-color --tail="${NOKV_SOAK_LOG_TAIL:-200}" || true
}

stop_chaos() {
  if [[ -n "$chaos_pid" ]] && kill -0 "$chaos_pid" 2>/dev/null; then
    kill "$chaos_pid" 2>/dev/null || true
    wait "$chaos_pid" 2>/dev/null || true
  fi
  chaos_pid=""
}

cleanup() {
  local status="${1:-0}"
  if [[ "$status" != "0" ]]; then
    dump_cluster_state
  fi
  stop_chaos
  rm -rf "$TOOLS_DIR"
  if [[ "${NOKV_SOAK_DOWN:-0}" == "1" ]]; then
    docker compose down -v
  fi
}
trap 'status=$?; cleanup "$status"; exit "$status"' EXIT
trap 'trap - EXIT; cleanup 130; exit 130' INT TERM

go build -o "$TOOLS_DIR/nokv-fsmeta-history" ./cmd/nokv-fsmeta-history
go build -o "$TOOLS_DIR/nokv-fsmeta-soak" ./cmd/nokv-fsmeta-soak
go build -o "$TOOLS_DIR/nokv-fsmeta-scrub" ./cmd/nokv-fsmeta-scrub

if [[ "${NOKV_SOAK_BUILD:-1}" == "1" ]]; then
  docker compose up -d --build
else
  docker compose up -d
fi

wait_fsmeta() {
  nokv_wait_for_tcp "$ADDR" "$READY_TIMEOUT"
}

rolling_restart_loop() {
  local services=(coordinator-1 coordinator-2 coordinator-3 store-1 store-2 store-3 meta-root-1 meta-root-2 meta-root-3 fsmeta)
  local i=0
  while true; do
    sleep "$RESTART_INTERVAL"
    local svc="${services[$((i % ${#services[@]}))]}"
    docker compose restart "$svc"
    wait_fsmeta
    i=$((i + 1))
  done
}

wait_fsmeta
if [[ "${NOKV_SOAK_ROLLING_RESTARTS:-1}" == "1" ]]; then
  rolling_restart_loop &
  chaos_pid="$!"
fi

"$TOOLS_DIR/nokv-fsmeta-soak" \
  --addr "$ADDR" \
  --mount "$MOUNT" \
  --duration "$DURATION" \
  --steps "$STEPS" \
  --batch "$BATCH" \
  --seed-start "$SEED_START"

stop_chaos
wait_fsmeta
if [[ "${NOKV_SOAK_SCRUB:-1}" == "1" ]]; then
  "$TOOLS_DIR/nokv-fsmeta-scrub" \
    --addr "$ADDR" \
    --mount "$MOUNT" \
    --timeout "${NOKV_SOAK_SCRUB_TIMEOUT:-60s}" \
    --max-issues "${NOKV_SOAK_SCRUB_MAX_ISSUES:-64}"
fi
