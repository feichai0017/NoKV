#!/usr/bin/env bash
#
# Run the NoKV AI shard range-read RustFS matrix.
#
# This gate keeps the packed-shard data path evidence reproducible: small sparse
# exact reads, small sparse gap-coalesced reads, and admitted MB-scale
# read-ahead windows. Each case gets a disposable RustFS instance through
# scripts/run-rustfs-e2e.sh so cold/warm rows stay isolated.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILE="${NOKV_AI_SHARD_MATRIX_PROFILE:-smoke}"
OBJECT_CONCURRENCY="${NOKV_AI_SHARD_MATRIX_OBJECT_CONCURRENCY:-4}"
READ_REPEATS="${NOKV_AI_SHARD_MATRIX_READ_REPEATS:-1}"
BLOCK_CACHE="${NOKV_AI_SHARD_MATRIX_BLOCK_CACHE:-on}"
CARGO_TARGET_DIR_OVERRIDE="${NOKV_AI_SHARD_MATRIX_CARGO_TARGET_DIR:-${NOKV_E2E_CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}}"
MATRIX_OUTPUT_DIR="${NOKV_AI_SHARD_MATRIX_OUTPUT_DIR:-}"

DEFAULT_CASES=(
    small-exact
    small-gap
    large-window
)

usage() {
    cat <<EOF
Usage: scripts/run-ai-shard-range-matrix.sh [case...]

Runs the RustFS-backed NoKV AI shard range-read matrix. With no case arguments,
runs:
  ${DEFAULT_CASES[*]}

Cases:
  small-exact       512-byte sparse samples, exact windows
  small-gap         512-byte sparse samples, 512-byte gap coalescing
  large-window      1 MiB sparse windows, read-ahead admission smoke

Environment:
  NOKV_AI_SHARD_MATRIX_PROFILE              smoke|standard|long (default: smoke)
  NOKV_AI_SHARD_MATRIX_OBJECT_CONCURRENCY   object GET concurrency (default: 4)
  NOKV_AI_SHARD_MATRIX_READ_REPEATS         read repeats (default: 1)
  NOKV_AI_SHARD_MATRIX_BLOCK_CACHE          on|off (default: on)
  NOKV_AI_SHARD_MATRIX_CARGO_TARGET_DIR     cargo target directory
  NOKV_AI_SHARD_MATRIX_OUTPUT_DIR           output directory for logs and CSV
  NOKV_AI_SHARD_MATRIX_CSV                  combined CSV output path

All NOKV_E2E_* and RustFS override variables accepted by run-rustfs-e2e.sh
still apply.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

cases=("$@")
if [[ "${#cases[@]}" -eq 0 ]]; then
    cases=("${DEFAULT_CASES[@]}")
fi

if [[ -z "$MATRIX_OUTPUT_DIR" ]]; then
    MATRIX_OUTPUT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-ai-shard-range-matrix.XXXXXX")"
else
    mkdir -p "$MATRIX_OUTPUT_DIR"
fi

COMBINED_CSV="${NOKV_AI_SHARD_MATRIX_CSV:-$MATRIX_OUTPUT_DIR/matrix.csv}"
mkdir -p "$(dirname "$COMBINED_CSV")"
: >"$COMBINED_CSV"
wrote_header=0

case_args() {
    case "$1" in
        small-exact)
            echo "--range-stride" "2" "--range-coalesce-gap-bytes" "0"
            ;;
        small-gap)
            echo "--range-stride" "2" "--range-coalesce-gap-bytes" "512"
            ;;
        large-window)
            echo "--sample-bytes" "1048576" "--range-stride" "32" "--range-coalesce-gap-bytes" "0"
            ;;
        *)
            echo "error: unknown AI shard range matrix case: $1" >&2
            exit 2
            ;;
    esac
}

append_case_csv() {
    local case_name="$1"
    local case_log="$2"

    while IFS= read -r line; do
        if [[ "$line" == boundary,* ]]; then
            if [[ "$wrote_header" -eq 0 ]]; then
                printf 'matrix_case,%s\n' "$line" >>"$COMBINED_CSV"
                wrote_header=1
            fi
        elif [[ "$line" == L1,* || "$line" == L2,* ]]; then
            printf '%s,%s\n' "$case_name" "$line" >>"$COMBINED_CSV"
        fi
    done <"$case_log"
}

echo "NoKV AI shard range matrix output directory: $MATRIX_OUTPUT_DIR"

for case_name in "${cases[@]}"; do
    # shellcheck disable=SC2207
    args=($(case_args "$case_name"))
    case_log="$MATRIX_OUTPUT_DIR/$case_name.log"
    env_args=(
        NOKV_E2E_PROFILE="$PROFILE"
        NOKV_E2E_WORKLOAD="ai-shard-range-read"
        NOKV_E2E_OBJECT_CONCURRENCY="$OBJECT_CONCURRENCY"
        NOKV_E2E_READ_REPEATS="$READ_REPEATS"
        NOKV_E2E_BLOCK_CACHE="$BLOCK_CACHE"
    )
    if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
        env_args+=(NOKV_E2E_CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE")
    fi

    echo "==> NoKV AI shard range matrix case: $case_name"
    env "${env_args[@]}" "$ROOT_DIR/scripts/run-rustfs-e2e.sh" "${args[@]}" 2>&1 | tee "$case_log"
    append_case_csv "$case_name" "$case_log"
done

if [[ "$wrote_header" -eq 0 ]]; then
    echo "error: no benchmark CSV rows found in matrix output" >&2
    exit 1
fi

echo "Combined NoKV AI shard range matrix CSV: $COMBINED_CSV"
