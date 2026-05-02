#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/common.sh"

ADDR="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
MOUNT="${NOKV_FSMETA_MOUNT:-default}"
SEEDS="${NOKV_DOCKER_CHAOS_SEEDS:-3}"
STEPS="${NOKV_DOCKER_CHAOS_STEPS:-48}"
BATCH="${NOKV_DOCKER_CHAOS_BATCH:-3}"
TIMEOUT="${NOKV_DOCKER_CHAOS_TIMEOUT:-60s}"
READY_TIMEOUT="${NOKV_DOCKER_CHAOS_READY_TIMEOUT:-180}"
TOOLS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-chaos.XXXXXX")"

cleanup() {
  rm -rf "$TOOLS_DIR"
  if [[ "${NOKV_DOCKER_CHAOS_DOWN:-0}" == "1" ]]; then
    docker compose down -v
  fi
}
trap cleanup EXIT
trap 'trap - EXIT; cleanup; exit 130' INT TERM

go build -o "$TOOLS_DIR/nokv-fsmeta-history" ./cmd/nokv-fsmeta-history

if [[ "${NOKV_DOCKER_CHAOS_BUILD:-1}" == "1" ]]; then
  docker compose up -d --build
else
  docker compose up -d
fi

wait_fsmeta() {
  nokv_wait_for_tcp "$ADDR" "$READY_TIMEOUT"
}

inject_fault() {
  local seed="$1"
  case $((seed % 6)) in
    0)
      docker compose restart fsmeta
      ;;
    1)
      docker compose restart coordinator-1
      ;;
    2)
      docker compose kill -s SIGKILL store-2
      docker compose up -d store-2
      ;;
    3)
      docker compose restart meta-root-3
      ;;
    4)
      docker compose kill -s SIGKILL coordinator-2
      docker compose up -d coordinator-2
      ;;
    *)
      docker compose restart store-3
      ;;
  esac
}

wait_fsmeta
for seed in $(seq 1 "$SEEDS"); do
  inject_fault "$seed"
  wait_fsmeta
  "$TOOLS_DIR/nokv-fsmeta-history" \
    --addr "$ADDR" \
    --mount "$MOUNT" \
    --seed-start "$seed" \
    --seeds 1 \
    --steps "$STEPS" \
    --batch "$BATCH" \
    --timeout "$TIMEOUT"
done
