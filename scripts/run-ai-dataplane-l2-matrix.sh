#!/usr/bin/env bash
#
# Focused mounted AI data-plane validation matrix.
#
# This wrapper runs scripts/run-ai-dataplane-l2.sh across seed-fsync and
# cache-state variants, keeping each cold/warm stats-decomposition row isolated.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

STAMP="${NOKV_AI_L2_MATRIX_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
HOST_LABEL="${NOKV_AI_L2_MATRIX_HOST_LABEL:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo host)}"
OUT_DIR="${NOKV_AI_L2_MATRIX_RESULT_DIR:-$ROOT_DIR/bench/results/ai-dataplane-l2-matrix-${HOST_LABEL}-${STAMP}}"
PROFILE="${NOKV_AI_L2_MATRIX_PROFILE:-smoke}"
REPEATS="${NOKV_AI_L2_MATRIX_REPEATS:-1}"
CONCURRENCY="${NOKV_AI_L2_MATRIX_CONCURRENCY:-1 4}"
WORKLOADS="${NOKV_AI_L2_MATRIX_WORKLOADS:-ai_shard_range_read}"
CACHE_STATES="${NOKV_AI_L2_MATRIX_CACHE_STATES:-cold warm}"
FSYNC_MODES="${NOKV_AI_L2_MATRIX_FSYNC:-0 1}"
DECOMPOSE="${NOKV_AI_L2_MATRIX_DECOMPOSE:-1}"

COMBINED_RAW="${NOKV_AI_L2_MATRIX_RAW_CSV:-$OUT_DIR/matrix.raw.csv}"
COMBINED_AGGREGATE="${NOKV_AI_L2_MATRIX_AGGREGATE_CSV:-$OUT_DIR/matrix.aggregate.csv}"

usage() {
    cat <<EOF
Usage: scripts/run-ai-dataplane-l2-matrix.sh [extra run-fs-benchmark args...]

Runs mounted NoKV-vs-JuiceFS AI data-plane rows across benchmark seed-fsync and
cache-state variants. Each variant delegates to scripts/run-ai-dataplane-l2.sh
and writes ordinary raw/aggregate/env/decompose artifacts plus combined matrix
CSVs.

Environment:
  NOKV_AI_L2_MATRIX_PROFILE       smoke|standard|long (default: smoke)
  NOKV_AI_L2_MATRIX_REPEATS       repeat count per variant (default: 1)
  NOKV_AI_L2_MATRIX_CONCURRENCY   concurrency sweep (default: "1 4")
  NOKV_AI_L2_MATRIX_WORKLOADS     product workloads (default: ai_shard_range_read)
  NOKV_AI_L2_MATRIX_CACHE_STATES  space-separated cold/warm/hot variants (default: "cold warm")
  NOKV_AI_L2_MATRIX_FSYNC         space-separated seed fsync 0/1 variants (default: "0 1")
  NOKV_AI_L2_MATRIX_RESULT_DIR    output directory
  NOKV_AI_L2_MATRIX_DECOMPOSE     write NoKV decompose sidecars, 1|0 (default: 1)
  NOKV_AI_L2_MATRIX_STAMP         artifact timestamp override
  NOKV_AI_L2_MATRIX_HOST_LABEL    artifact host label override
  NOKV_AI_L2_MATRIX_RAW_CSV       combined raw CSV path
  NOKV_AI_L2_MATRIX_AGGREGATE_CSV combined aggregate CSV path

Extra arguments are forwarded to scripts/run-ai-dataplane-l2.sh and then to
scripts/run-fs-benchmark.sh.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$OUT_DIR" "$(dirname "$COMBINED_RAW")" "$(dirname "$COMBINED_AGGREGATE")"
: >"$COMBINED_RAW"
: >"$COMBINED_AGGREGATE"

append_matrix_csv() {
    local input="$1"
    local output="$2"
    local matrix_case="$3"
    local fsync_mode="$4"
    local cache_state="$5"

    "$PYTHON_BIN" - "$input" "$output" "$matrix_case" "$fsync_mode" "$cache_state" <<'PY'
import csv
import os
import sys

input_path, output_path, matrix_case, fsync_mode, cache_state = sys.argv[1:]
emit_header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
with open(input_path, newline="") as src, open(output_path, "a", newline="") as dst:
    reader = csv.reader(src)
    writer = csv.writer(dst, lineterminator="\n")
    try:
        header = next(reader)
    except StopIteration:
        raise SystemExit(f"empty CSV: {input_path}")
    if emit_header:
        writer.writerow(["matrix_case", "fsync", "cache_state_scope", *header])
    for row in reader:
        writer.writerow([matrix_case, fsync_mode, cache_state, *row])
PY
}

validate_fsync_mode() {
    case "$1" in
        0|1) ;;
        *)
            echo "error: fsync mode must be 0 or 1, got '$1'" >&2
            exit 2
            ;;
    esac
}

validate_cache_state() {
    case "$1" in
        cold|warm|hot) ;;
        *)
            echo "error: cache state must be cold, warm, or hot, got '$1'" >&2
            exit 2
            ;;
    esac
}

echo "NoKV AI L2 matrix output directory: $OUT_DIR"

for fsync_mode in $FSYNC_MODES; do
    validate_fsync_mode "$fsync_mode"
    for cache_state in $CACHE_STATES; do
        validate_cache_state "$cache_state"
        matrix_case="fsync${fsync_mode}-${cache_state}"
        case_stamp="${STAMP}-${matrix_case}"
        raw_csv="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${case_stamp}.raw.csv"
        aggregate_csv="$OUT_DIR/ai-dataplane-l2-${HOST_LABEL}-${case_stamp}.aggregate.csv"

        echo "==> AI L2 matrix case: $matrix_case"
        NOKV_BENCH_FSYNC="$fsync_mode" \
        NOKV_AI_L2_PROFILE="$PROFILE" \
        NOKV_AI_L2_REPEATS="$REPEATS" \
        NOKV_AI_L2_CONCURRENCY="$CONCURRENCY" \
        NOKV_AI_L2_WORKLOADS="$WORKLOADS" \
        NOKV_AI_L2_CACHE_STATES="$cache_state" \
        NOKV_AI_L2_DECOMPOSE="$DECOMPOSE" \
        NOKV_AI_L2_STAMP="$case_stamp" \
        NOKV_AI_L2_HOST_LABEL="$HOST_LABEL" \
        NOKV_AI_L2_RESULT_DIR="$OUT_DIR" \
            "$ROOT_DIR/scripts/run-ai-dataplane-l2.sh" "$@"

        append_matrix_csv "$raw_csv" "$COMBINED_RAW" "$matrix_case" "$fsync_mode" "$cache_state"
        append_matrix_csv "$aggregate_csv" "$COMBINED_AGGREGATE" "$matrix_case" "$fsync_mode" "$cache_state"
    done
done

echo "Combined raw matrix CSV: $COMBINED_RAW"
echo "Combined aggregate matrix CSV: $COMBINED_AGGREGATE"
