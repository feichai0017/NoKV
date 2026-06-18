#!/usr/bin/env bash
#
# Run the layered AI data-plane matrix:
#   - Rust SDK L1 (`nokv-bench` through run-rustfs-e2e.sh)
#   - Python/fsspec L1 (`nokv.fsspec` range batch shapes)
#   - optional mounted L2 NoKV FUSE vs JuiceFS FUSE

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

STAMP="${NOKV_AI_LAYERED_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
HOST_LABEL="${NOKV_AI_LAYERED_HOST_LABEL:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo host)}"
OUT_DIR="${NOKV_AI_LAYERED_RESULT_DIR:-$ROOT_DIR/bench/results/ai-dataplane-layered-${HOST_LABEL}-${STAMP}}"
PROFILE="${NOKV_AI_LAYERED_PROFILE:-smoke}"
CASES_RAW="${NOKV_AI_LAYERED_CASES:-sparse-exact sparse-coalesced}"

RUN_RUST_L1="${NOKV_AI_LAYERED_RUN_RUST_L1:-1}"
RUN_PYTHON_L1="${NOKV_AI_LAYERED_RUN_PYTHON_L1:-1}"
RUN_L2="${NOKV_AI_LAYERED_RUN_L2:-1}"

RUST_OBJECT_CONCURRENCY="${NOKV_AI_LAYERED_RUST_OBJECT_CONCURRENCY:-4}"
RUST_READ_REPEATS="${NOKV_AI_LAYERED_RUST_READ_REPEATS:-1}"
RUST_BLOCK_CACHE="${NOKV_AI_LAYERED_RUST_BLOCK_CACHE:-on}"
PYTHON_CONCURRENCY="${NOKV_AI_LAYERED_PYTHON_CONCURRENCY:-4}"
PYTHON_CACHE_STATES="${NOKV_AI_LAYERED_PYTHON_CACHE_STATES:-cold,warm}"
PYTHON_READ_SHAPE="${NOKV_AI_LAYERED_PYTHON_READ_SHAPE:-ranges}"
PYTHON_READ_BUFFER_MEMORY_KIND="${NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND:-system}"
L2_CONCURRENCY="${NOKV_AI_LAYERED_L2_CONCURRENCY:-1 4}"
L2_CACHE_STATES="${NOKV_AI_LAYERED_L2_CACHE_STATES:-cold warm}"
L2_FSYNC_MODES="${NOKV_AI_LAYERED_L2_FSYNC:-0}"
L2_REPEATS="${NOKV_AI_LAYERED_L2_REPEATS:-1}"
L2_DECOMPOSE="${NOKV_AI_LAYERED_L2_DECOMPOSE:-1}"

CARGO_TARGET_DIR_OVERRIDE="${NOKV_AI_LAYERED_CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"
RUSTFS_BASE_PORT="${NOKV_AI_LAYERED_RUSTFS_BASE_PORT:-9070}"
PYTHON_RUSTFS_BASE_PORT="${NOKV_AI_LAYERED_PYTHON_RUSTFS_BASE_PORT:-9080}"
PYTHON_SERVER_BASE_PORT="${NOKV_AI_LAYERED_PYTHON_SERVER_BASE_PORT:-7790}"

COMBINED_RAW="${NOKV_AI_LAYERED_RAW_CSV:-$OUT_DIR/layered.raw.csv}"
COMBINED_AGGREGATE="${NOKV_AI_LAYERED_AGGREGATE_CSV:-$OUT_DIR/layered.aggregate.csv}"

usage() {
    cat <<EOF
Usage: scripts/run-ai-dataplane-layered-matrix.sh [case...]

Runs layered AI data-plane evidence. With no positional cases, runs:
  $CASES_RAW

Cases:
  sparse-exact       every second sample, exact semantic windows
  sparse-coalesced   every second sample, coalesce across one skipped sample
  large-window       1 MiB samples, sparse MB windows, L2 skipped

Environment:
  NOKV_AI_LAYERED_PROFILE                 smoke|standard|long (default: smoke)
  NOKV_AI_LAYERED_CASES                   default case list
  NOKV_AI_LAYERED_RUN_RUST_L1             1|0, run Rust SDK L1 (default: 1)
  NOKV_AI_LAYERED_RUN_PYTHON_L1           1|0, run Python/fsspec L1 (default: 1)
  NOKV_AI_LAYERED_RUN_L2                  1|0, run mounted NoKV/JuiceFS L2 (default: 1)
  NOKV_AI_LAYERED_RUST_OBJECT_CONCURRENCY Rust L1 object concurrency (default: 4)
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY      Python shard batch size (default: 4)
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE       ranges|packed|into|buffer|planned_buffer|batch_reader|epoch_reader Python return shape (default: ranges)
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND system|page_locked for Python buffer shapes (default: system)
  NOKV_AI_LAYERED_L2_CONCURRENCY          L2 concurrency sweep (default: "1 4")
  NOKV_AI_LAYERED_L2_CACHE_STATES         L2 cache-state variants (default: "cold warm")
  NOKV_AI_LAYERED_L2_FSYNC                L2 seed fsync variants (default: "0")
  NOKV_AI_LAYERED_RESULT_DIR              output directory
  NOKV_AI_LAYERED_RAW_CSV                 combined raw CSV path
  NOKV_AI_LAYERED_AGGREGATE_CSV           combined aggregate CSV path

The combined CSVs keep benchmark_layer/source_script/layer_case columns so L1
SDK rows are never silently compared as mounted L2 rows.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

case "$PROFILE" in
    smoke)
        BASE_SHARD_COUNT=8
        BASE_FILES_PER_DIR=64
        BASE_SAMPLE_BYTES=512
        ;;
    standard)
        BASE_SHARD_COUNT=32
        BASE_FILES_PER_DIR=256
        BASE_SAMPLE_BYTES=$((16 * 1024))
        ;;
    long)
        BASE_SHARD_COUNT=64
        BASE_FILES_PER_DIR=1024
        BASE_SAMPLE_BYTES=$((256 * 1024))
        ;;
    *)
        echo "error: NOKV_AI_LAYERED_PROFILE must be smoke|standard|long" >&2
        exit 2
        ;;
esac

cases=("$@")
if [[ "${#cases[@]}" -eq 0 ]]; then
    # shellcheck disable=SC2206
    cases=($CASES_RAW)
fi

case_spec() {
    local case_name="$1"
    case "$case_name" in
        sparse-exact)
            CASE_RANGE_STRIDE=2
            CASE_COALESCE_GAP_BYTES=0
            CASE_SAMPLE_BYTES="$BASE_SAMPLE_BYTES"
            ;;
        sparse-coalesced)
            CASE_RANGE_STRIDE=2
            CASE_COALESCE_GAP_BYTES="$BASE_SAMPLE_BYTES"
            CASE_SAMPLE_BYTES="$BASE_SAMPLE_BYTES"
            ;;
        large-window)
            CASE_RANGE_STRIDE=32
            CASE_COALESCE_GAP_BYTES=0
            CASE_SAMPLE_BYTES=$((1024 * 1024))
            ;;
        *)
            echo "error: unknown layered AI data-plane case: $case_name" >&2
            exit 2
            ;;
    esac
}

extract_csv_rows() {
    local log_path="$1"
    local csv_path="$2"
    "$PYTHON_BIN" - "$log_path" "$csv_path" <<'PY'
import sys

log_path, csv_path = sys.argv[1:]
header = None
rows = []
with open(log_path) as handle:
    for raw in handle:
        line = raw.rstrip("\n")
        if line.startswith("boundary,"):
            header = line
        elif line.startswith(("L0,", "L1,", "L2,")):
            rows.append(line)
if header is None or not rows:
    raise SystemExit(f"no benchmark CSV rows found in {log_path}")
with open(csv_path, "w") as out:
    out.write(header + "\n")
    for row in rows:
        out.write(row + "\n")
PY
}

add_input() {
    local layer="$1"
    local source="$2"
    local case_name="$3"
    local path="$4"
    MERGE_INPUTS+=("--input" "${layer}:${source}:${case_name}:${path}")
}

mkdir -p "$OUT_DIR"
MERGE_INPUTS=()

echo "NoKV layered AI data-plane output directory: $OUT_DIR"

case_index=0
for case_name in "${cases[@]}"; do
    case_spec "$case_name"
    case_dir="$OUT_DIR/$case_name"
    mkdir -p "$case_dir"

    echo "==> Layered AI data-plane case: $case_name"

    if [[ "$RUN_RUST_L1" == "1" ]]; then
        rust_log="$case_dir/rust-l1.log"
        rust_csv="$case_dir/rust-l1.raw.csv"
        rust_port=$((RUSTFS_BASE_PORT + case_index * 10))
        rust_console_port=$((rust_port + 1))
        rust_env=(
            NOKV_E2E_PROFILE="$PROFILE"
            NOKV_E2E_WORKLOAD="ai-shard-range-read"
            NOKV_E2E_OBJECT_CONCURRENCY="$RUST_OBJECT_CONCURRENCY"
            NOKV_E2E_READ_REPEATS="$RUST_READ_REPEATS"
            NOKV_E2E_BLOCK_CACHE="$RUST_BLOCK_CACHE"
            NOKV_E2E_RUSTFS_ADDRESS="127.0.0.1:${rust_port}"
            NOKV_E2E_RUSTFS_CONSOLE_ADDRESS="127.0.0.1:${rust_console_port}"
        )
        if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
            rust_env+=(NOKV_E2E_CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE")
        fi
        env "${rust_env[@]}" "$ROOT_DIR/scripts/run-rustfs-e2e.sh" \
            --sample-bytes "$CASE_SAMPLE_BYTES" \
            --range-stride "$CASE_RANGE_STRIDE" \
            --range-coalesce-gap-bytes "$CASE_COALESCE_GAP_BYTES" \
            2>&1 | tee "$rust_log"
        extract_csv_rows "$rust_log" "$rust_csv"
        add_input "L1" "rust-sdk" "$case_name" "$rust_csv"
    fi

    if [[ "$RUN_PYTHON_L1" == "1" ]]; then
        python_log="$case_dir/python-fsspec-l1.log"
        python_csv="$case_dir/python-fsspec-l1.raw.csv"
        python_work="$case_dir/python-work"
        python_hot="$case_dir/python-hot"
        python_rustfs_port=$((PYTHON_RUSTFS_BASE_PORT + case_index * 10))
        python_console_port=$((python_rustfs_port + 1))
        python_server_port=$((PYTHON_SERVER_BASE_PORT + case_index))
        python_env=(
            NOKV_PYTHON_SMOKE_PROFILE="$PROFILE"
            NOKV_PYTHON_SMOKE_SHARD_COUNT="$BASE_SHARD_COUNT"
            NOKV_PYTHON_SMOKE_FILES_PER_DIR="$BASE_FILES_PER_DIR"
            NOKV_PYTHON_SMOKE_SAMPLE_BYTES="$CASE_SAMPLE_BYTES"
            NOKV_PYTHON_SMOKE_RANGE_STRIDE="$CASE_RANGE_STRIDE"
            NOKV_PYTHON_SMOKE_RANGE_COALESCE_GAP_BYTES="$CASE_COALESCE_GAP_BYTES"
            NOKV_PYTHON_SMOKE_CONCURRENCY="$PYTHON_CONCURRENCY"
            NOKV_PYTHON_SMOKE_CACHE_STATES="$PYTHON_CACHE_STATES"
            NOKV_PYTHON_SMOKE_READ_SHAPE="$PYTHON_READ_SHAPE"
            NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND="$PYTHON_READ_BUFFER_MEMORY_KIND"
            NOKV_PYTHON_SMOKE_RESULT_CSV="$python_csv"
            NOKV_PYTHON_SMOKE_WORKDIR="$python_work"
            NOKV_PYTHON_SMOKE_HOT_OBJECT_ROOT="$python_hot"
            NOKV_PYTHON_SMOKE_OBJECT_BACKEND="rustfs+local-hot+put=cold-then-hot"
            NOKV_PYTHON_SMOKE_METADATA_TIER="nokv-l1-service"
            NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS="127.0.0.1:${python_rustfs_port}"
            NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS="127.0.0.1:${python_console_port}"
            NOKV_PYTHON_SMOKE_SERVER_ADDRESS="127.0.0.1:${python_server_port}"
        )
        if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
            python_env+=(NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE")
        fi
        env "${python_env[@]}" "$ROOT_DIR/scripts/run-python-fsspec-smoke.sh" 2>&1 | tee "$python_log"
        add_input "L1" "python-fsspec" "$case_name" "$python_csv"
    fi

    if [[ "$RUN_L2" == "1" ]]; then
        if [[ "$CASE_SAMPLE_BYTES" != "$BASE_SAMPLE_BYTES" ]]; then
            echo "Skipping L2 for $case_name: mounted L2 profile sample_bytes=$BASE_SAMPLE_BYTES, case sample_bytes=$CASE_SAMPLE_BYTES" >&2
        else
            l2_dir="$case_dir/l2"
            l2_log="$case_dir/l2-fuse-juicefs.log"
            l2_raw="$l2_dir/matrix.raw.csv"
            l2_aggregate="$l2_dir/matrix.aggregate.csv"
            mkdir -p "$l2_dir"
            NOKV_AI_L2_MATRIX_PROFILE="$PROFILE" \
            NOKV_AI_L2_MATRIX_REPEATS="$L2_REPEATS" \
            NOKV_AI_L2_MATRIX_CONCURRENCY="$L2_CONCURRENCY" \
            NOKV_AI_L2_MATRIX_WORKLOADS="ai_shard_range_read" \
            NOKV_AI_L2_MATRIX_CACHE_STATES="$L2_CACHE_STATES" \
            NOKV_AI_L2_MATRIX_FSYNC="$L2_FSYNC_MODES" \
            NOKV_AI_L2_MATRIX_RESULT_DIR="$l2_dir" \
            NOKV_AI_L2_MATRIX_RAW_CSV="$l2_raw" \
            NOKV_AI_L2_MATRIX_AGGREGATE_CSV="$l2_aggregate" \
            NOKV_AI_L2_MATRIX_DECOMPOSE="$L2_DECOMPOSE" \
            NOKV_AI_L2_RANGE_STRIDE="$CASE_RANGE_STRIDE" \
            NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES="$CASE_COALESCE_GAP_BYTES" \
                "$ROOT_DIR/scripts/run-ai-dataplane-l2-matrix.sh" 2>&1 | tee "$l2_log"
            add_input "L2" "fuse-vs-juicefs" "$case_name" "$l2_raw"
        fi
    fi

    case_index=$((case_index + 1))
done

if [[ "${#MERGE_INPUTS[@]}" -eq 0 ]]; then
    echo "error: no benchmark layers were run" >&2
    exit 1
fi

"$PYTHON_BIN" "$ROOT_DIR/scripts/merge-layered-benchmark-csv.py" \
    "${MERGE_INPUTS[@]}" \
    --raw-out "$COMBINED_RAW" \
    --aggregate-out "$COMBINED_AGGREGATE"

echo "Combined layered raw CSV: $COMBINED_RAW"
echo "Combined layered aggregate CSV: $COMBINED_AGGREGATE"
