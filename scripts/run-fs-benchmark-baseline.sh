#!/usr/bin/env bash
#
# Opinionated clean NoKV-FS baseline run.
#
# Produces raw, median/p95 aggregate, environment, and NoKV /stats decompose
# files under bench/results by default. This is the command to rerun after a
# benchmark-framework refactor before promoting a new local baseline.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="${NOKV_BENCH_BASELINE_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
HOST_LABEL="${NOKV_BENCH_HOST_LABEL:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo host)}"
OUT_DIR="${NOKV_BENCH_RESULT_DIR:-$ROOT_DIR/bench/results}"
PROFILE="${NOKV_BENCH_PROFILE:-standard}"
REPEATS="${NOKV_BENCH_REPEATS:-3}"
CONCURRENCY_SWEEP="${NOKV_BENCH_CONCURRENCY_SWEEP:-1 4 16}"
NOKV_TIERS="${NOKV_BENCH_NOKV_TIERS:-local}"

mkdir -p "$OUT_DIR"

RAW_CSV="$OUT_DIR/fs-baseline-${HOST_LABEL}-${STAMP}.raw.csv"
AGGREGATE_CSV="$OUT_DIR/fs-baseline-${HOST_LABEL}-${STAMP}.aggregate.csv"
ENV_JSON="$OUT_DIR/fs-baseline-${HOST_LABEL}-${STAMP}.env.json"
DECOMPOSE_CSV="$OUT_DIR/fs-baseline-${HOST_LABEL}-${STAMP}.decompose.csv"

export NOKV_BENCH_PROFILE="$PROFILE"

exec "$ROOT_DIR/scripts/run-fs-benchmark.sh" \
    --matrix \
    --repeats "$REPEATS" \
    --concurrency "$CONCURRENCY_SWEEP" \
    --tiers "$NOKV_TIERS" \
    --result-csv "$RAW_CSV" \
    --aggregate-csv "$AGGREGATE_CSV" \
    --env-json "$ENV_JSON" \
    --decompose-csv "$DECOMPOSE_CSV" \
    "$@"
