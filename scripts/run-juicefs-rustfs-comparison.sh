#!/usr/bin/env bash
#
# Run a fair local NoKV-FS vs JuiceFS comparison against one disposable RustFS
# endpoint. The script starts RustFS, Redis, formats and mounts JuiceFS, then
# runs scripts/run-training-comparison.sh with isolated buckets.
#
# This is a local engineering baseline, not an official MLPerf result.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
REDIS_BIN="${NOKV_REDIS_BIN:-redis-server}"
JUICEFS_BIN="${NOKV_JUICEFS_BIN:-juicefs}"

RUSTFS_ADDRESS="${NOKV_COMPARE_RUSTFS_ADDRESS:-127.0.0.1:9000}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_COMPARE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9001}"
RUSTFS_ENDPOINT="${NOKV_COMPARE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_ACCESS_KEY="${NOKV_COMPARE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_COMPARE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_COMPARE_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NOKV_BUCKET="${NOKV_COMPARE_RUSTFS_BUCKET:-nokv}"
JUICEFS_BUCKET="${NOKV_COMPARE_JUICEFS_BUCKET:-juicefs}"
REDIS_PORT="${NOKV_COMPARE_REDIS_PORT:-16379}"
META_URL="redis://127.0.0.1:${REDIS_PORT}/1"

WORKDIR="${NOKV_COMPARE_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_COMPARE_KEEP_WORKDIR:-0}"
RUSTFS_PID=""
REDIS_PID=""
OWN_WORKDIR=0
JUICEFS_MOUNT=""

usage() {
    cat <<EOF
Usage: scripts/run-juicefs-rustfs-comparison.sh

Starts disposable RustFS + Redis + JuiceFS and runs the same-shape NoKV-FS and
JuiceFS comparison workload against the same RustFS endpoint.

Environment:
  NOKV_RUSTFS_BIN                  RustFS binary path/name (default: rustfs)
  NOKV_AWS_BIN                     AWS CLI path/name (default: aws)
  NOKV_REDIS_BIN                   Redis server binary path/name (default: redis-server)
  NOKV_JUICEFS_BIN                 JuiceFS binary path/name (default: juicefs)
  NOKV_COMPARE_PROFILE             smoke|standard|long (default: smoke)
  NOKV_COMPARE_WORKLOAD            NoKV benchmark workload (default: mlperf-dlio)
  NOKV_COMPARE_RUSTFS_ADDRESS      RustFS S3 listen address (default: 127.0.0.1:9000)
  NOKV_COMPARE_REDIS_PORT          Redis port (default: 16379)
  NOKV_COMPARE_RUSTFS_BUCKET       NoKV bucket (default: nokv)
  NOKV_COMPARE_JUICEFS_BUCKET      JuiceFS bucket (default: juicefs)
  NOKV_COMPARE_KEEP_WORKDIR=1      keep temp data for inspection

Required tools are never installed automatically. Install examples:
  brew tap rustfs/homebrew-tap && brew install rustfs
  brew install awscli redis juicefs
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

require_cmd() {
    local cmd="$1"
    local install="$2"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command not found: $cmd" >&2
        echo "install: $install" >&2
        exit 127
    fi
}

cleanup() {
    local status=$?
    if [[ -n "$JUICEFS_MOUNT" && -d "$JUICEFS_MOUNT" ]]; then
        if command -v "$JUICEFS_BIN" >/dev/null 2>&1; then
            "$JUICEFS_BIN" umount "$JUICEFS_MOUNT" >/dev/null 2>&1 || true
        fi
        umount "$JUICEFS_MOUNT" >/dev/null 2>&1 || true
        diskutil unmount "$JUICEFS_MOUNT" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$REDIS_PID" ]] && kill -0 "$REDIS_PID" >/dev/null 2>&1; then
        kill "$REDIS_PID" >/dev/null 2>&1 || true
        wait "$REDIS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORKDIR" ]]; then
        for log in "$WORKDIR"/rustfs.log "$WORKDIR"/redis.log "$WORKDIR"/juicefs.log; do
            if [[ -f "$log" ]]; then
                echo "---- $(basename "$log") tail ----" >&2
                tail -80 "$log" >&2 || true
                echo "------------------------------" >&2
            fi
        done
    fi
    if [[ "$OWN_WORKDIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORKDIR"
    elif [[ -n "$WORKDIR" ]]; then
        echo "comparison workdir: $WORKDIR" >&2
    fi
    exit "$status"
}

wait_for_tcp() {
    local host="$1"
    local port="$2"
    local name="$3"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $host:$port" >&2
    return 1
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
        if curl -sS -I --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for RustFS at $RUSTFS_ENDPOINT" >&2
    return 1
}

create_bucket() {
    local bucket="$1"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api create-bucket --bucket "$bucket" >/dev/null 2>&1; then
            return 0
        fi
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo "error: failed to create or find bucket '$bucket' at $RUSTFS_ENDPOINT" >&2
    return 1
}

require_cmd "$RUSTFS_BIN" "brew tap rustfs/homebrew-tap && brew install rustfs"
require_cmd "$AWS_BIN" "brew install awscli"
require_cmd "$REDIS_BIN" "brew install redis"
require_cmd "$JUICEFS_BIN" "brew install juicefs"
require_cmd curl "brew install curl"
require_cmd cargo "install Rust from https://rustup.rs/"
require_cmd python3 "brew install python"

if [[ -z "$WORKDIR" ]]; then
    WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-juicefs-rustfs.XXXXXX")"
    OWN_WORKDIR=1
else
    mkdir -p "$WORKDIR"
fi
trap cleanup EXIT INT TERM

RUSTFS_DATA_DIR="$WORKDIR/rustfs-data"
REDIS_DIR="$WORKDIR/redis"
JUICEFS_MOUNT="$WORKDIR/juicefs-mount"
mkdir -p "$RUSTFS_DATA_DIR" "$REDIS_DIR" "$JUICEFS_MOUNT"

echo "Starting RustFS endpoint=$RUSTFS_ENDPOINT buckets=$NOKV_BUCKET,$JUICEFS_BUCKET"
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server \
    --address "$RUSTFS_ADDRESS" \
    --console-enable \
    --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
    "$RUSTFS_DATA_DIR" >"$WORKDIR/rustfs.log" 2>&1 &
RUSTFS_PID=$!

echo "Starting Redis metadata backend port=$REDIS_PORT"
"$REDIS_BIN" \
    --bind 127.0.0.1 \
    --port "$REDIS_PORT" \
    --dir "$REDIS_DIR" \
    --save "" \
    --appendonly no >"$WORKDIR/redis.log" 2>&1 &
REDIS_PID=$!

wait_for_rustfs
wait_for_tcp 127.0.0.1 "$REDIS_PORT" Redis
create_bucket "$NOKV_BUCKET"
create_bucket "$JUICEFS_BUCKET"

echo "Formatting JuiceFS bucket=$JUICEFS_BUCKET metadata=$META_URL"
"$JUICEFS_BIN" format \
    --storage s3 \
    --bucket "$RUSTFS_ENDPOINT/$JUICEFS_BUCKET" \
    --access-key "$RUSTFS_ACCESS_KEY" \
    --secret-key "$RUSTFS_SECRET_KEY" \
    --trash-days 0 \
    "$META_URL" nokv-juicefs-compare >"$WORKDIR/juicefs.log" 2>&1

echo "Mounting JuiceFS at $JUICEFS_MOUNT"
"$JUICEFS_BIN" mount -d "$META_URL" "$JUICEFS_MOUNT" >>"$WORKDIR/juicefs.log" 2>&1

python3 - "$JUICEFS_MOUNT" <<'PY'
import os
import sys
import time

mount = sys.argv[1]
deadline = time.time() + 30
while time.time() < deadline:
    if os.path.ismount(mount):
        sys.exit(0)
    time.sleep(0.25)
print(f"error: timed out waiting for JuiceFS mount at {mount}", file=sys.stderr)
sys.exit(1)
PY

echo "Running fair comparison profile=${NOKV_COMPARE_PROFILE:-smoke}"
NOKV_COMPARE_RUSTFS_ENDPOINT="$RUSTFS_ENDPOINT" \
NOKV_COMPARE_RUSTFS_BUCKET="$NOKV_BUCKET" \
NOKV_COMPARE_RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
NOKV_COMPARE_RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
NOKV_COMPARE_JUICEFS_BUCKET="$JUICEFS_BUCKET" \
JUICEFS_MOUNT="$JUICEFS_MOUNT" \
"$ROOT_DIR/scripts/run-training-comparison.sh"
