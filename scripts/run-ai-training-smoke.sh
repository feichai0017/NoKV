#!/usr/bin/env bash
#
# Run the NoKV-FS AI-training smoke gate against a disposable RustFS endpoint.
#
# This is the fast evidence gate for the current product goal: Holt-native
# metadata reads, object-backed checkpoint publish, DLIO-style generated data,
# and shared-log metadata HA/fault recovery. Each workload gets its own
# temporary RustFS instance through scripts/run-rustfs-e2e.sh so failures are
# isolated and logs stay easy to inspect.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILE="${NOKV_AI_SMOKE_PROFILE:-smoke}"
OBJECT_CONCURRENCY="${NOKV_AI_SMOKE_OBJECT_CONCURRENCY:-8}"
READ_REPEATS="${NOKV_AI_SMOKE_READ_REPEATS:-1}"
BLOCK_CACHE="${NOKV_AI_SMOKE_BLOCK_CACHE:-on}"

DEFAULT_WORKLOADS=(
    metadata-concurrent-read
    checkpoint-publish
    mlperf-dlio
    metadata-ha-smoke
    metadata-ha-fault-smoke
)

usage() {
    cat <<EOF
Usage: scripts/run-ai-training-smoke.sh [workload...]

Runs the standard AI-training smoke gate. With no workload arguments, runs:
  ${DEFAULT_WORKLOADS[*]}

Environment:
  NOKV_AI_SMOKE_PROFILE              smoke|standard|long (default: smoke)
  NOKV_AI_SMOKE_OBJECT_CONCURRENCY   object PUT/GET concurrency (default: 8)
  NOKV_AI_SMOKE_READ_REPEATS         read repeats for read workloads (default: 1)
  NOKV_AI_SMOKE_BLOCK_CACHE          on|off (default: on)

All NOKV_E2E_* and RustFS override variables accepted by run-rustfs-e2e.sh
still apply. Workload-specific command-line arguments can be passed through
NOKV_AI_SMOKE_EXTRA_ARGS.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

workloads=("$@")
if [[ "${#workloads[@]}" -eq 0 ]]; then
    workloads=("${DEFAULT_WORKLOADS[@]}")
fi

extra_args=()
if [[ -n "${NOKV_AI_SMOKE_EXTRA_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    extra_args=(${NOKV_AI_SMOKE_EXTRA_ARGS})
fi

for workload in "${workloads[@]}"; do
    echo "==> NoKV-FS smoke workload: $workload"
    NOKV_E2E_PROFILE="$PROFILE" \
        NOKV_E2E_WORKLOAD="$workload" \
        NOKV_E2E_OBJECT_CONCURRENCY="$OBJECT_CONCURRENCY" \
        NOKV_E2E_READ_REPEATS="$READ_REPEATS" \
        NOKV_E2E_BLOCK_CACHE="$BLOCK_CACHE" \
        "$ROOT_DIR/scripts/run-rustfs-e2e.sh" "${extra_args[@]}"
done
