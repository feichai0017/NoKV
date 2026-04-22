#!/usr/bin/env bash
# Recreate the full NoKV 333 demo cluster from scratch.
#
# This is intentionally a one-shot reset script:
#   docker compose down -v --remove-orphans
#   docker compose up -d --build
#   wait until redis gateway answers PING
#   optionally restart the local dashboard proxy
#
# For periodic resets on a server, schedule this script with cron/systemd
# rather than running an infinite loop in-process.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT_DIR"

INTERVAL=""
DASHBOARD=0
DASHBOARD_PORT=18080
BUILD=1
TIMEOUT=120
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/demo/recycle-demo.sh [options]

One-shot reset:
  - stop and remove the demo stack
  - wipe docker volumes (-v)
  - rebuild/restart the stack
  - wait for redis gateway readiness
  - optionally restart the local dashboard proxy

Options:
  --dashboard             restart dashboard_server.py after the cluster is ready
  --dashboard-port PORT   dashboard port (default: 18080)
  --interval SECONDS      repeat forever, sleeping this many seconds between cycles
  --no-build              skip docker compose --build on startup
  --timeout SECONDS       readiness timeout waiting for redis ping (default: 120)
  --dry-run               print actions without executing them
  -h, --help              show this help

Examples:
  scripts/demo/recycle-demo.sh
  scripts/demo/recycle-demo.sh --dashboard
  scripts/demo/recycle-demo.sh --interval 21600 --dashboard
EOF
}

log() {
  printf '[recycle-demo] %s\n' "$*"
}

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] '
    printf '%q ' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dashboard)
      DASHBOARD=1
      shift
      ;;
    --dashboard-port)
      DASHBOARD_PORT="${2:-}"
      shift 2
      ;;
    --interval)
      INTERVAL="${2:-}"
      shift 2
      ;;
    --no-build)
      BUILD=0
      shift
      ;;
    --timeout)
      TIMEOUT="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "$INTERVAL" ]] && ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "--interval must be an integer number of seconds" >&2
  exit 1
fi
if ! [[ "$DASHBOARD_PORT" =~ ^[0-9]+$ ]]; then
  echo "--dashboard-port must be numeric" >&2
  exit 1
fi
if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]]; then
  echo "--timeout must be an integer number of seconds" >&2
  exit 1
fi

compose_up() {
  if [[ "$BUILD" == "1" ]]; then
    run docker compose up -d --build
  else
    run docker compose up -d
  fi
}

wait_for_gateway() {
  local deadline=$((SECONDS + TIMEOUT))
  while (( SECONDS < deadline )); do
    if [[ "$DRY_RUN" == "1" ]]; then
      log "would wait for redis gateway on :6380"
      return 0
    fi
    if redis-cli -p 6380 ping >/dev/null 2>&1; then
      log "gateway is ready on :6380"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for redis gateway readiness on :6380" >&2
  return 1
}

restart_dashboard() {
  log "restarting local dashboard on :$DASHBOARD_PORT"
  run pkill -f "dashboard_server.py $DASHBOARD_PORT"
  run pkill -f "serve-dashboard.sh $DASHBOARD_PORT"
  if [[ "$DRY_RUN" == "1" ]]; then
    log "would start scripts/demo/dashboard_server.py $DASHBOARD_PORT"
    return 0
  fi
  nohup python3 "$ROOT_DIR/scripts/demo/dashboard_server.py" "$DASHBOARD_PORT" \
    >"/tmp/nokv-dashboard-${DASHBOARD_PORT}.log" 2>&1 </dev/null &
}

run_cycle() {
  log "stopping demo stack and wiping volumes"
  run docker compose down -v --remove-orphans
  log "starting demo stack"
  compose_up
  wait_for_gateway
  if [[ "$DASHBOARD" == "1" ]]; then
    restart_dashboard
  fi
}

if [[ -z "$INTERVAL" ]]; then
  run_cycle
  exit 0
fi

while true; do
  run_cycle
  log "sleeping ${INTERVAL}s before the next recycle"
  if [[ "$DRY_RUN" == "1" ]]; then
    exit 0
  fi
  sleep "$INTERVAL"
done
