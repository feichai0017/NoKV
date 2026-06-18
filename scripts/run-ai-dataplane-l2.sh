#!/usr/bin/env bash
#
# Focused mounted NoKV-vs-JuiceFS AI data-plane comparison.
#
# This is the quick L2 gate for training-facing data movement. It reuses the
# canonical mounted benchmark runner and narrows the workload set to checkpoint
# publish and training reads, so the output is directly comparable between NoKV
# FUSE and JuiceFS FUSE without pulling in fio/mdtest/juicefs-bench rows.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="${NOKV_AI_L2_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
HOST_LABEL="${NOKV_AI_L2_HOST_LABEL:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo host)}"
OUT_DIR="${NOKV_AI_L2_RESULT_DIR:-$ROOT_DIR/bench/results}"
PROFILE="${NOKV_AI_L2_PROFILE:-smoke}"
REPEATS="${NOKV_AI_L2_REPEATS:-1}"
CONCURRENCY="${NOKV_AI_L2_CONCURRENCY:-1}"
WORKLOADS="${NOKV_AI_L2_WORKLOADS:-checkpoint,training_read,ai_shard_range_read}"
RANGE_STRIDE="${NOKV_AI_L2_RANGE_STRIDE:-${NOKV_BENCH_RANGE_STRIDE:-2}}"
RANGE_COALESCE_GAP_BYTES="${NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES:-${NOKV_BENCH_RANGE_COALESCE_GAP_BYTES:-512}}"
CACHE_STATES="${NOKV_AI_L2_CACHE_STATES:-${NOKV_BENCH_CACHE_STATES:-cold,warm}}"
DECOMPOSE="${NOKV_AI_L2_DECOMPOSE:-1}"

usage() {
    cat <<EOF
Usage: scripts/run-ai-dataplane-l2.sh [extra run-fs-benchmark args...]

Runs the mounted L2 NoKV-vs-JuiceFS AI data-plane comparison.

Environment:
  NOKV_AI_L2_PROFILE       smoke|standard|long (default: smoke)
  NOKV_AI_L2_REPEATS       repeat count (default: 1)
  NOKV_AI_L2_CONCURRENCY   concurrency sweep passed to L2 runner (default: 1)
  NOKV_AI_L2_WORKLOADS     product workloads (default: checkpoint,training_read,ai_shard_range_read)
  NOKV_AI_L2_RANGE_STRIDE  shard-read sample stride (default: 2)
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES
                            shard-read POSIX range merge gap (default: 512)
  NOKV_AI_L2_CACHE_STATES  read cache states to emit: cold,warm,hot (default: cold,warm)
  NOKV_AI_L2_RESULT_DIR    artifact directory (default: bench/results)
  NOKV_AI_L2_DECOMPOSE     write NoKV stats decompose sidecar, 1|0 (default: 1)
  NOKV_AI_L2_STAMP         artifact timestamp override
  NOKV_AI_L2_HOST_LABEL    artifact host label override

The wrapper skips fio/mdtest/juicefs-bench by default. Pass extra
run-fs-benchmark flags after the wrapper arguments to override defaults.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$OUT_DIR"

RAW_CSV="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${STAMP}.raw.csv"
AGGREGATE_CSV="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${STAMP}.aggregate.csv"
ENV_JSON="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${STAMP}.env.json"
DECOMPOSE_CSV="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${STAMP}.decompose.csv"

export NOKV_BENCH_PROFILE="$PROFILE"
export NOKV_BENCH_RANGE_STRIDE="$RANGE_STRIDE"
export NOKV_BENCH_RANGE_COALESCE_GAP_BYTES="$RANGE_COALESCE_GAP_BYTES"
export NOKV_BENCH_CACHE_STATES="$CACHE_STATES"

args=(
    --quick
    --repeats "$REPEATS"
    --concurrency "$CONCURRENCY"
    --product-workloads "$WORKLOADS"
    --primitive-workloads none
    --skip-real-tools
    --result-csv "$RAW_CSV"
    --aggregate-csv "$AGGREGATE_CSV"
    --env-json "$ENV_JSON"
)

if [[ "$DECOMPOSE" == "1" ]]; then
    args+=(--decompose-csv "$DECOMPOSE_CSV")
fi

exec "$ROOT_DIR/scripts/run-fs-benchmark.sh" "${args[@]}" "$@"
