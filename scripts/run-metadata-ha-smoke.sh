#!/usr/bin/env bash
#
# Metadata HA smoke test against local RustFS plus etcd.
#
# This proves the deployable control-plane path: a first nokv server owns the
# shard through etcd, archives a checkpoint and sync shared-log segment to the
# object store, then a replacement server acquires the next epoch after the
# first owner dies and verifies checkpoint+log recovery.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
ETCD_BIN="${NOKV_ETCD_BIN:-etcd}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
CURL_BIN="${NOKV_CURL_BIN:-curl}"

RUSTFS_ADDRESS="${NOKV_HA_RUSTFS_ADDRESS:-127.0.0.1:9030}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_HA_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9031}"
RUSTFS_ENDPOINT="${NOKV_HA_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_HA_RUSTFS_BUCKET:-nokv-ha-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_HA_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_HA_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_HA_RUSTFS_BUFFER_PROFILE:-AiTraining}"

ETCD_CLIENT_ADDRESS="${NOKV_HA_ETCD_CLIENT_ADDRESS:-127.0.0.1:12379}"
ETCD_PEER_ADDRESS="${NOKV_HA_ETCD_PEER_ADDRESS:-127.0.0.1:12380}"
ETCD_ENDPOINTS="${NOKV_HA_ETCD_ENDPOINTS:-http://${ETCD_CLIENT_ADDRESS}}"
ETCD_TTL_SECONDS="${NOKV_HA_ETCD_LEASE_TTL_SECONDS:-3}"

SERVER_BIND="${NOKV_HA_SERVER_BIND:-127.0.0.1:7730}"
SHARD_ID="${NOKV_HA_SHARD_ID:-mount-1:/}"
HA_CARGO_TARGET_DIR="${NOKV_HA_CARGO_TARGET_DIR:-$ROOT_DIR/target}"
STALE_OWNER_CHAOS="${NOKV_HA_STALE_OWNER_CHAOS:-0}"
KEEP_WORKDIR="${NOKV_HA_KEEP_WORKDIR:-0}"

WORK_DIR=""
RUSTFS_PID=""
ETCD_PID=""
SERVER_A_PID=""
SERVER_B_PID=""
OWN_ETCD=0
OWNER_A_READY_MS=0
OWNER_A_KILL_MS=0
OWNER_A_RESUME_MS=0
OWNER_B_START_MS=0
OWNER_B_READY_MS=0
VERIFY_DONE_MS=0
STALE_OWNER_DETECT_MS=0
STALE_OWNER_FENCE_MS=0
PRE_INODE=0
POST_INODE=0
AFTER_FAILOVER_INODE=0
CHECKPOINT_COMMIT_VERSION=0

usage() {
    cat <<EOF
Usage: scripts/run-metadata-ha-smoke.sh

Environment:
  NOKV_HA_RUSTFS_ADDRESS              RustFS S3 address (default: 127.0.0.1:9030)
  NOKV_HA_RUSTFS_CONSOLE_ADDRESS      RustFS console address (default: 127.0.0.1:9031)
  NOKV_HA_RUSTFS_BUCKET               bucket name (default: nokv-ha-smoke)
  NOKV_HA_ETCD_ENDPOINTS              external etcd endpoints; when unset, start local etcd
  NOKV_HA_ETCD_CLIENT_ADDRESS         local etcd client address (default: 127.0.0.1:12379)
  NOKV_HA_ETCD_PEER_ADDRESS           local etcd peer address (default: 127.0.0.1:12380)
  NOKV_HA_ETCD_LEASE_TTL_SECONDS      owner lease TTL (default: 3)
  NOKV_HA_SERVER_BIND                 nokv server address (default: 127.0.0.1:7730)
  NOKV_HA_STALE_OWNER_CHAOS=1         pause owner A, fail over owner B on a second bind, and verify stale-owner fencing
  NOKV_HA_OWNER_A_BIND                owner A bind in stale-owner chaos mode (default: NOKV_HA_SERVER_BIND)
  NOKV_HA_OWNER_B_BIND                owner B bind in stale-owner chaos mode (default: 127.0.0.1:7731)
  NOKV_HA_METRICS_JSON                optional path for machine-readable timing output
  NOKV_HA_KEEP_WORKDIR=1              keep temporary logs and state

Requires rustfs, aws, curl, and either etcd or NOKV_HA_ETCD_ENDPOINTS.
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

now_ms() {
    if command -v python3 >/dev/null 2>&1; then
        python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
    else
        echo "$(($(date +%s) * 1000))"
    fi
}

extract_inode() {
    sed -n 's/.*inode=\([0-9][0-9]*\).*/\1/p'
}

extract_json_number() {
    local field="$1"
    sed -n "s/.*\"${field}\":\([0-9][0-9]*\).*/\1/p"
}

cleanup() {
    local status=$?
    if [[ -n "$SERVER_B_PID" ]] && kill -0 "$SERVER_B_PID" >/dev/null 2>&1; then
        kill "$SERVER_B_PID" >/dev/null 2>&1 || true
        wait "$SERVER_B_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_A_PID" ]] && kill -0 "$SERVER_A_PID" >/dev/null 2>&1; then
        kill -CONT "$SERVER_A_PID" >/dev/null 2>&1 || true
        kill "$SERVER_A_PID" >/dev/null 2>&1 || true
        wait "$SERVER_A_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$OWN_ETCD" -eq 1 && -n "$ETCD_PID" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then
        kill "$ETCD_PID" >/dev/null 2>&1 || true
        wait "$ETCD_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORK_DIR" ]]; then
        for log in rustfs.log etcd.log server-a.log server-b.log; do
            if [[ -f "$WORK_DIR/$log" ]]; then
                echo "---- $log tail ----" >&2
                tail -80 "$WORK_DIR/$log" >&2 || true
            fi
        done
    fi
    if [[ -n "$WORK_DIR" && "$KEEP_WORKDIR" == "1" ]]; then
        echo "HA smoke workdir: $WORK_DIR" >&2
    elif [[ -n "$WORK_DIR" ]]; then
        rm -rf "$WORK_DIR"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

wait_for_url() {
    local url="$1" name="$2" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        "$CURL_BIN" -fsS --max-time 2 "$url" >/dev/null 2>&1 && return 0
        "$CURL_BIN" -sS -I --max-time 2 "$url" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $url" >&2
    return 1
}

wait_for_server() {
    local bind="$1" pid="$2" name="$3" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: $name exited before becoming ready" >&2
            return 1
        fi
        "$CURL_BIN" -fsS --max-time 2 "http://${bind}/readyz" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $bind" >&2
    return 1
}

wait_for_stale_owner_fence() {
    local bind="$1" pid="$2" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: stale owner exited before observing the new epoch" >&2
            return 1
        fi
        local stats
        stats="$("$CURL_BIN" -fsS --max-time 2 "http://${bind}/stats" 2>/dev/null || true)"
        if echo "$stats" | grep -q '"last_error":"lease holder does not own shard'; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for stale owner to observe the new epoch" >&2
    return 1
}

create_bucket() {
    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
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

if ! [[ "$ETCD_TTL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: NOKV_HA_ETCD_LEASE_TTL_SECONDS must be a positive integer" >&2
    exit 2
fi
if [[ "$STALE_OWNER_CHAOS" != "0" && "$STALE_OWNER_CHAOS" != "1" ]]; then
    echo "error: NOKV_HA_STALE_OWNER_CHAOS must be 0 or 1" >&2
    exit 2
fi

OWNER_A_BIND="${NOKV_HA_OWNER_A_BIND:-$SERVER_BIND}"
OWNER_B_BIND="$SERVER_BIND"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    OWNER_B_BIND="${NOKV_HA_OWNER_B_BIND:-127.0.0.1:7731}"
fi

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd "$CURL_BIN"
if [[ -z "${NOKV_HA_ETCD_ENDPOINTS:-}" ]]; then
    require_cmd "$ETCD_BIN"
    OWN_ETCD=1
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-ha-smoke.XXXXXX")"
mkdir -p "$WORK_DIR/rustfs" "$WORK_DIR/etcd" "$WORK_DIR/meta-a" "$WORK_DIR/meta-b"

UNIQUE="$(date +%s)-$$"
ETCD_PREFIX="${NOKV_HA_ETCD_PREFIX:-/nokv/ha-smoke/${UNIQUE}}"
CHECKPOINT_PREFIX="${NOKV_HA_CHECKPOINT_PREFIX:-metadata/ha-smoke/${UNIQUE}/checkpoints}"
SHARED_LOG_PREFIX="${NOKV_HA_SHARED_LOG_PREFIX:-metadata/ha-smoke/${UNIQUE}/shared-log}"

S3_ARGS=(
    --object-backend rustfs
    --s3-bucket "$RUSTFS_BUCKET"
    --s3-endpoint "$RUSTFS_ENDPOINT"
    --s3-access-key-id "$RUSTFS_ACCESS_KEY"
    --s3-secret-access-key "$RUSTFS_SECRET_KEY"
)
CONTROL_ARGS=(
    --control-backend etcd
    --control-etcd-endpoints "$ETCD_ENDPOINTS"
    --control-etcd-prefix "$ETCD_PREFIX"
    --control-etcd-lease-ttl-seconds "$ETCD_TTL_SECONDS"
    --shard-id "$SHARD_ID"
    --shard-owner-renewal-interval-ms 500
    --metadata-shared-log-prefix "$SHARED_LOG_PREFIX"
    --metadata-checkpoint-archive-prefix "$CHECKPOINT_PREFIX"
)
CLIENT_A=(--server-bind "$OWNER_A_BIND" "${S3_ARGS[@]}")
CLIENT_B=(--server-bind "$OWNER_B_BIND" "${S3_ARGS[@]}")

echo "==> building nokv with etcd feature"
(
    cd "$ROOT_DIR"
    CARGO_TARGET_DIR="$HA_CARGO_TARGET_DIR" cargo build -p nokv --features etcd >/dev/null
)
NOKV="$HA_CARGO_TARGET_DIR/debug/nokv"

echo "==> starting RustFS at $RUSTFS_ENDPOINT"
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server \
    --address "$RUSTFS_ADDRESS" \
    --console-enable \
    --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
    "$WORK_DIR/rustfs" >"$WORK_DIR/rustfs.log" 2>&1 &
RUSTFS_PID=$!
wait_for_url "$RUSTFS_ENDPOINT" RustFS
create_bucket

if [[ "$OWN_ETCD" -eq 1 ]]; then
    echo "==> starting etcd at $ETCD_ENDPOINTS"
    "$ETCD_BIN" \
        --name nokv-ha-smoke \
        --data-dir "$WORK_DIR/etcd" \
        --listen-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --advertise-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --listen-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-advertise-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster "nokv-ha-smoke=http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster-state new \
        --initial-cluster-token "nokv-ha-smoke-${UNIQUE}" \
        >"$WORK_DIR/etcd.log" 2>&1 &
    ETCD_PID=$!
fi
FIRST_ETCD_ENDPOINT="${ETCD_ENDPOINTS%%,*}"
wait_for_url "${FIRST_ETCD_ENDPOINT%/}/health" etcd

echo "==> starting owner A with etcd lease and sync shared-log"
"$NOKV" \
    --meta "$WORK_DIR/meta-a" \
    --server-bind "$OWNER_A_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_ARGS[@]}" \
    --node-id node-a \
    serve >"$WORK_DIR/server-a.log" 2>&1 &
SERVER_A_PID=$!
wait_for_server "$OWNER_A_BIND" "$SERVER_A_PID" "nokv owner A"
OWNER_A_READY_MS="$(now_ms)"

printf 'ha-smoke-pre-checkpoint' >"$WORK_DIR/pre.bin"
printf 'ha-smoke-post-checkpoint' >"$WORK_DIR/post.bin"

echo "==> writing data before checkpoint"
"$NOKV" "${CLIENT_A[@]}" mkdir /runs
PRE_OUT="$("$NOKV" "${CLIENT_A[@]}" put-artifact /runs/pre.bin "$WORK_DIR/pre.bin")"
echo "$PRE_OUT"
PRE_INODE="$(printf '%s\n' "$PRE_OUT" | extract_inode)"
"$NOKV" "${CLIENT_A[@]}" ls /runs | grep -q "pre.bin"

echo "==> publishing checkpoint ref through owner A"
BACKUP_JSON="$("$NOKV" "${CLIENT_A[@]}" backup)"
echo "    $BACKUP_JSON"
echo "$BACKUP_JSON" | grep -q '"checkpoint_key"'
CHECKPOINT_COMMIT_VERSION="$(printf '%s\n' "$BACKUP_JSON" | extract_json_number commit_version)"

echo "==> writing data after checkpoint so failover must replay shared log"
POST_OUT="$("$NOKV" "${CLIENT_A[@]}" put-artifact /runs/post.bin "$WORK_DIR/post.bin")"
echo "$POST_OUT"
POST_INODE="$(printf '%s\n' "$POST_OUT" | extract_inode)"
"$NOKV" "${CLIENT_A[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "==> pausing owner A and waiting for etcd lease expiry"
else
    echo "==> killing owner A and waiting for etcd lease expiry"
fi
OWNER_A_KILL_MS="$(now_ms)"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    kill -STOP "$SERVER_A_PID" >/dev/null 2>&1
else
    kill "$SERVER_A_PID" >/dev/null 2>&1 || true
    wait "$SERVER_A_PID" >/dev/null 2>&1 || true
    SERVER_A_PID=""
fi
sleep "$((ETCD_TTL_SECONDS + 2))"

echo "==> starting owner B as failover from epoch 1"
OWNER_B_START_MS="$(now_ms)"
"$NOKV" \
    --meta "$WORK_DIR/meta-b" \
    --server-bind "$OWNER_B_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_ARGS[@]}" \
    --node-id node-b \
    --failover-from-epoch 1 \
    serve >"$WORK_DIR/server-b.log" 2>&1 &
SERVER_B_PID=$!
wait_for_server "$OWNER_B_BIND" "$SERVER_B_PID" "nokv owner B"
OWNER_B_READY_MS="$(now_ms)"

STATS="$("$CURL_BIN" -fsS "http://${OWNER_B_BIND}/stats")"
echo "$STATS" | grep -q '"node_id":"node-b"'
echo "$STATS" | grep -q '"epoch":2'

echo "==> verifying checkpoint restore and shared-log replay"
"$NOKV" "${CLIENT_B[@]}" cat /runs/pre.bin | grep -q "ha-smoke-pre-checkpoint"
"$NOKV" "${CLIENT_B[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

echo "==> verifying owner B accepts new writes without clobbering replayed data"
AFTER_OUT="$("$NOKV" "${CLIENT_B[@]}" mkdir /after-failover)"
echo "$AFTER_OUT"
AFTER_FAILOVER_INODE="$(printf '%s\n' "$AFTER_OUT" | extract_inode)"
if [[ -n "$POST_INODE" && -n "$AFTER_FAILOVER_INODE" && "$AFTER_FAILOVER_INODE" -le "$POST_INODE" ]]; then
    echo "error: post-failover inode $AFTER_FAILOVER_INODE did not advance past replayed inode $POST_INODE" >&2
    exit 1
fi
"$NOKV" "${CLIENT_B[@]}" ls / | grep -q "after-failover"
"$NOKV" "${CLIENT_B[@]}" ls /runs | grep -q "post.bin"
"$NOKV" "${CLIENT_B[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

echo "==> running fsck after failover"
FSCK_JSON="$("$NOKV" "${CLIENT_B[@]}" fsck)"
echo "    $FSCK_JSON"
echo "$FSCK_JSON" | grep -q '"dangling_count":0'

if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "==> resuming owner A and verifying stale-owner fencing"
    OWNER_A_RESUME_MS="$(now_ms)"
    kill -CONT "$SERVER_A_PID" >/dev/null 2>&1
    wait_for_stale_owner_fence "$OWNER_A_BIND" "$SERVER_A_PID"
    STALE_OWNER_DETECT_MS="$(now_ms)"
    set +e
    STALE_WRITE_OUT="$("$NOKV" "${CLIENT_A[@]}" mkdir /stale-owner-write 2>&1)"
    STALE_WRITE_STATUS=$?
    set -e
    if [[ "$STALE_WRITE_STATUS" -eq 0 ]]; then
        echo "error: stale owner accepted a metadata write after epoch-2 failover" >&2
        echo "$STALE_WRITE_OUT" >&2
        exit 1
    fi
    echo "$STALE_WRITE_OUT" | grep -q "owner epoch 1 is stale; required owner epoch is 2"
    STALE_OWNER_FENCE_MS="$(now_ms)"
fi
VERIFY_DONE_MS="$(now_ms)"

FAILOVER_OBSERVED_MS="$((OWNER_B_READY_MS - OWNER_A_KILL_MS))"
LEASE_WAIT_MS="$((OWNER_B_START_MS - OWNER_A_KILL_MS))"
OWNER_B_STARTUP_MS="$((OWNER_B_READY_MS - OWNER_B_START_MS))"
VERIFY_AFTER_READY_MS="$((VERIFY_DONE_MS - OWNER_B_READY_MS))"
STALE_OWNER_DETECT_AFTER_RESUME_MS=0
STALE_OWNER_FENCE_AFTER_DETECT_MS=0
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    STALE_OWNER_DETECT_AFTER_RESUME_MS="$((STALE_OWNER_DETECT_MS - OWNER_A_RESUME_MS))"
    STALE_OWNER_FENCE_AFTER_DETECT_MS="$((STALE_OWNER_FENCE_MS - STALE_OWNER_DETECT_MS))"
fi
METRICS_JSON="{\"lease_ttl_seconds\":${ETCD_TTL_SECONDS},\"stale_owner_chaos\":${STALE_OWNER_CHAOS},\"owner_a_ready_ms\":${OWNER_A_READY_MS},\"owner_a_kill_ms\":${OWNER_A_KILL_MS},\"owner_a_resume_ms\":${OWNER_A_RESUME_MS},\"owner_b_start_ms\":${OWNER_B_START_MS},\"owner_b_ready_ms\":${OWNER_B_READY_MS},\"failover_observed_ms\":${FAILOVER_OBSERVED_MS},\"lease_wait_ms\":${LEASE_WAIT_MS},\"owner_b_startup_ms\":${OWNER_B_STARTUP_MS},\"verify_after_ready_ms\":${VERIFY_AFTER_READY_MS},\"stale_owner_detect_after_resume_ms\":${STALE_OWNER_DETECT_AFTER_RESUME_MS},\"stale_owner_fence_after_detect_ms\":${STALE_OWNER_FENCE_AFTER_DETECT_MS},\"checkpoint_commit_version\":${CHECKPOINT_COMMIT_VERSION:-0},\"pre_inode\":${PRE_INODE:-0},\"post_checkpoint_inode\":${POST_INODE:-0},\"after_failover_inode\":${AFTER_FAILOVER_INODE:-0}}"
if [[ -n "${NOKV_HA_METRICS_JSON:-}" ]]; then
    printf '%s\n' "$METRICS_JSON" >"$NOKV_HA_METRICS_JSON"
fi

echo
echo "HA_SMOKE_METRICS $METRICS_JSON"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "HA_STALE_OWNER_OK: resumed epoch-1 owner observed epoch 2 and rejected a stale write"
fi
echo "HA_SMOKE_OK: etcd owner failover restored checkpoint, replayed shared log, and served epoch 2"
