#!/usr/bin/env bash
#
# Run a local RustFS-backed NoKV-FS end-to-end benchmark.
#
# The script starts a temporary RustFS process, creates the configured S3
# bucket, runs the deployable single-node NoKV-FS benchmark harness against
# that endpoint, and then stops RustFS.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"

RUSTFS_ADDRESS="${NOKV_E2E_RUSTFS_ADDRESS:-127.0.0.1:9000}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_E2E_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9001}"
RUSTFS_ENDPOINT="${NOKV_E2E_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_E2E_RUSTFS_BUCKET:-nokv}"
RUSTFS_ACCESS_KEY="${NOKV_E2E_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_E2E_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_E2E_RUSTFS_BUFFER_PROFILE:-AiTraining}"

PROFILE="${NOKV_E2E_PROFILE:-smoke}"
WORKLOAD="${NOKV_E2E_WORKLOAD:-mlperf-dlio}"
OBJECT_CONCURRENCY="${NOKV_E2E_OBJECT_CONCURRENCY:-8}"
READ_REPEATS="${NOKV_E2E_READ_REPEATS:-1}"
BLOCK_CACHE="${NOKV_E2E_BLOCK_CACHE:-on}"
BENCH_KEEP="${NOKV_E2E_BENCH_KEEP:-0}"

RUSTFS_DATA_DIR="${NOKV_E2E_RUSTFS_DATA_DIR:-}"
RUSTFS_LOG="${NOKV_E2E_RUSTFS_LOG:-}"

RUSTFS_PID=""
OWN_DATA_DIR=0

usage() {
    cat <<EOF
Usage: scripts/run-rustfs-e2e.sh [extra nokv-fs-bench args...]

Environment:
  NOKV_E2E_PROFILE                  smoke|standard|long (default: smoke)
  NOKV_E2E_WORKLOAD                 benchmark workload (default: mlperf-dlio)
  NOKV_E2E_OBJECT_CONCURRENCY       object PUT/GET concurrency (default: 8)
  NOKV_E2E_READ_REPEATS             training-read repeat count (default: 1)
  NOKV_E2E_BLOCK_CACHE              on|off (default: on)
  NOKV_E2E_RUSTFS_ADDRESS           RustFS listen address (default: 127.0.0.1:9000)
  NOKV_E2E_RUSTFS_CONSOLE_ADDRESS   RustFS console address (default: 127.0.0.1:9001)
  NOKV_E2E_RUSTFS_BUCKET            bucket name (default: nokv)
  NOKV_E2E_RUSTFS_DATA_DIR          keep/use a specific RustFS data directory
  NOKV_E2E_RUSTFS_LOG               keep/use a specific RustFS log file
  NOKV_E2E_KEEP_RUSTFS_DATA=1       keep temporary RustFS data directory
  NOKV_E2E_BENCH_KEEP=1             keep benchmark metad workdir

Examples:
  scripts/run-rustfs-e2e.sh
  NOKV_E2E_PROFILE=standard NOKV_E2E_WORKLOAD=checkpoint-publish scripts/run-rustfs-e2e.sh
  scripts/run-rustfs-e2e.sh --checkpoint-bytes 1048576 --sample-bytes 65536
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command not found: $cmd" >&2
        exit 127
    fi
}

cleanup() {
    local status=$?
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$RUSTFS_LOG" && -f "$RUSTFS_LOG" ]]; then
        echo "---- RustFS log tail ----" >&2
        tail -80 "$RUSTFS_LOG" >&2 || true
        echo "-------------------------" >&2
    fi
    if [[ "$OWN_DATA_DIR" -eq 1 && "${NOKV_E2E_KEEP_RUSTFS_DATA:-0}" != "1" ]]; then
        rm -rf "$RUSTFS_DATA_DIR"
    elif [[ -n "$RUSTFS_DATA_DIR" ]]; then
        echo "RustFS data directory: $RUSTFS_DATA_DIR" >&2
    fi
    exit "$status"
}

wait_for_rustfs() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if ! kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
            echo "error: RustFS exited before becoming ready" >&2
            return 1
        fi
        if curl -fsS --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1; then
            return 0
        fi
        # RustFS returns 501 for GET / on some versions; any HTTP response means
        # the S3 listener is up enough for bucket creation retries.
        if curl -sS -I --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for RustFS at $RUSTFS_ENDPOINT" >&2
    return 1
}

create_bucket() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api create-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
            return 0
        fi
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api head-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo "error: failed to create or find bucket '$RUSTFS_BUCKET' at $RUSTFS_ENDPOINT" >&2
    return 1
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd curl

if [[ -z "$RUSTFS_DATA_DIR" ]]; then
    RUSTFS_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-rustfs-e2e.XXXXXX")"
    OWN_DATA_DIR=1
else
    mkdir -p "$RUSTFS_DATA_DIR"
fi

if [[ -z "$RUSTFS_LOG" ]]; then
    RUSTFS_LOG="$RUSTFS_DATA_DIR/rustfs.log"
fi

trap cleanup EXIT INT TERM

echo "Starting RustFS at $RUSTFS_ENDPOINT"
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server \
    --address "$RUSTFS_ADDRESS" \
    --console-enable \
    --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
    "$RUSTFS_DATA_DIR" >"$RUSTFS_LOG" 2>&1 &
RUSTFS_PID=$!

wait_for_rustfs
create_bucket

bench_args=(
    --profile "$PROFILE"
    --workload "$WORKLOAD"
    --object-backend rustfs
    --s3-bucket "$RUSTFS_BUCKET"
    --s3-endpoint "$RUSTFS_ENDPOINT"
    --s3-access-key-id "$RUSTFS_ACCESS_KEY"
    --s3-secret-access-key "$RUSTFS_SECRET_KEY"
    --object-concurrency "$OBJECT_CONCURRENCY"
    --read-repeats "$READ_REPEATS"
    --block-cache "$BLOCK_CACHE"
)

if [[ "$BENCH_KEEP" == "1" ]]; then
    bench_args+=(--keep)
fi
if [[ "$#" -gt 0 ]]; then
    bench_args+=("$@")
fi

echo "Running NoKV-FS E2E benchmark: workload=$WORKLOAD profile=$PROFILE object_concurrency=$OBJECT_CONCURRENCY"
(
    cd "$ROOT_DIR"
    cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- "${bench_args[@]}"
)
