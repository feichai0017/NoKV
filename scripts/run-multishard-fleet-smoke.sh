#!/usr/bin/env bash
#
# Multi-shard fleet HA smoke test against local RustFS plus etcd.
#
# This is the DEPLOY GATE for the multi-shard fleet: it proves that two server
# PROCESSES, each owning a different metadata shard of one mount through etcd, are
# routed correctly by a single fleet client, and that when one shard's owner dies
# a replacement process acquires the next epoch, restores that shard from its
# object-store checkpoint + shared log, and the fleet client transparently
# re-resolves to the new owner.
#
# It complements scripts/run-metadata-ha-smoke.sh (which gates single-shard owner
# failover) by exercising the cross-shard routing + per-shard failover path.
#
# Requires etcd + RustFS, so it does NOT run in CI sandboxes; it is gated the same
# way as run-metadata-ha-smoke.sh (env-driven, external binaries required).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
ETCD_BIN="${NOKV_ETCD_BIN:-etcd}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
CURL_BIN="${NOKV_CURL_BIN:-curl}"

RUSTFS_ADDRESS="${NOKV_FLEET_RUSTFS_ADDRESS:-127.0.0.1:9040}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_FLEET_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9041}"
RUSTFS_ENDPOINT="${NOKV_FLEET_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_FLEET_RUSTFS_BUCKET:-nokv-fleet-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_FLEET_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_FLEET_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_FLEET_RUSTFS_BUFFER_PROFILE:-AiTraining}"

ETCD_CLIENT_ADDRESS="${NOKV_FLEET_ETCD_CLIENT_ADDRESS:-127.0.0.1:12389}"
ETCD_PEER_ADDRESS="${NOKV_FLEET_ETCD_PEER_ADDRESS:-127.0.0.1:12390}"
ETCD_ENDPOINTS="${NOKV_FLEET_ETCD_ENDPOINTS:-http://${ETCD_CLIENT_ADDRESS}}"
ETCD_TTL_SECONDS="${NOKV_FLEET_ETCD_LEASE_TTL_SECONDS:-3}"

# Two shards of one mount: the default shard (index 0, prefix "/") owned by
# server A, and the /dataset subtree (index 1) owned by server B (then B').
MOUNT="${NOKV_FLEET_MOUNT:-1}"
DEFAULT_SHARD_ID="${NOKV_FLEET_DEFAULT_SHARD_ID:-mount-${MOUNT}:/}"
DATASET_SHARD_ID="${NOKV_FLEET_DATASET_SHARD_ID:-mount-${MOUNT}:/dataset}"
DEFAULT_SHARD_INDEX="${NOKV_FLEET_DEFAULT_SHARD_INDEX:-0}"
DATASET_SHARD_INDEX="${NOKV_FLEET_DATASET_SHARD_INDEX:-1}"

SERVER_A_BIND="${NOKV_FLEET_SERVER_A_BIND:-127.0.0.1:7740}"
SERVER_B_BIND="${NOKV_FLEET_SERVER_B_BIND:-127.0.0.1:7741}"
# B' (the dataset shard's replacement owner) binds a fresh port so the fleet
# client's cached endpoint for shard 1 is genuinely stale after the handoff.
SERVER_B2_BIND="${NOKV_FLEET_SERVER_B2_BIND:-127.0.0.1:7742}"

FLEET_CARGO_TARGET_DIR="${NOKV_FLEET_CARGO_TARGET_DIR:-$ROOT_DIR/target}"
KEEP_WORKDIR="${NOKV_FLEET_KEEP_WORKDIR:-0}"

WORK_DIR=""
RUSTFS_PID=""
ETCD_PID=""
SERVER_A_PID=""
SERVER_B_PID=""
SERVER_B2_PID=""
OWN_ETCD=0
DATASET_PRE_INODE=0
OTHER_INODE=0
DATASET_POST_INODE=0
AFTER_FAILOVER_INODE=0

usage() {
    cat <<EOF
Usage: scripts/run-multishard-fleet-smoke.sh

Multi-shard fleet HA gate: two shard-owner processes + a fleet client, with a
per-shard failover. Requires rustfs, aws, curl, and either etcd or
NOKV_FLEET_ETCD_ENDPOINTS.

Environment:
  NOKV_FLEET_RUSTFS_ADDRESS           RustFS S3 address (default: 127.0.0.1:9040)
  NOKV_FLEET_RUSTFS_CONSOLE_ADDRESS   RustFS console address (default: 127.0.0.1:9041)
  NOKV_FLEET_RUSTFS_BUCKET            bucket name (default: nokv-fleet-smoke)
  NOKV_FLEET_ETCD_ENDPOINTS           external etcd endpoints; when unset, start local etcd
  NOKV_FLEET_ETCD_CLIENT_ADDRESS      local etcd client address (default: 127.0.0.1:12389)
  NOKV_FLEET_ETCD_PEER_ADDRESS        local etcd peer address (default: 127.0.0.1:12390)
  NOKV_FLEET_ETCD_LEASE_TTL_SECONDS   owner lease TTL (default: 3)
  NOKV_FLEET_SERVER_A_BIND            default-shard owner bind (default: 127.0.0.1:7740)
  NOKV_FLEET_SERVER_B_BIND            dataset-shard owner bind (default: 127.0.0.1:7741)
  NOKV_FLEET_SERVER_B2_BIND           dataset-shard failover bind (default: 127.0.0.1:7742)
  NOKV_FLEET_MOUNT                    mount id (default: 1)
  NOKV_FLEET_METRICS_JSON             optional path for machine-readable timing output
  NOKV_FLEET_KEEP_WORKDIR=1           keep temporary logs and state
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

cleanup() {
    local status=$?
    for pid in "$SERVER_B2_PID" "$SERVER_B_PID" "$SERVER_A_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    if [[ "$OWN_ETCD" -eq 1 && -n "$ETCD_PID" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then
        kill "$ETCD_PID" >/dev/null 2>&1 || true
        wait "$ETCD_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORK_DIR" ]]; then
        for log in rustfs.log etcd.log server-a.log server-b.log server-b2.log; do
            if [[ -f "$WORK_DIR/$log" ]]; then
                echo "---- $log tail ----" >&2
                tail -80 "$WORK_DIR/$log" >&2 || true
            fi
        done
    fi
    if [[ -n "$WORK_DIR" && "$KEEP_WORKDIR" == "1" ]]; then
        echo "fleet smoke workdir: $WORK_DIR" >&2
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
    echo "error: NOKV_FLEET_ETCD_LEASE_TTL_SECONDS must be a positive integer" >&2
    exit 2
fi

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd "$CURL_BIN"
if [[ -z "${NOKV_FLEET_ETCD_ENDPOINTS:-}" ]]; then
    require_cmd "$ETCD_BIN"
    OWN_ETCD=1
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fleet-smoke.XXXXXX")"
mkdir -p "$WORK_DIR/rustfs" "$WORK_DIR/etcd" \
    "$WORK_DIR/meta-a" "$WORK_DIR/meta-b" "$WORK_DIR/meta-b2"

UNIQUE="$(date +%s)-$$"
ETCD_PREFIX="${NOKV_FLEET_ETCD_PREFIX:-/nokv/fleet-smoke/${UNIQUE}}"
CHECKPOINT_PREFIX="${NOKV_FLEET_CHECKPOINT_PREFIX:-metadata/fleet-smoke/${UNIQUE}/checkpoints}"
SHARED_LOG_PREFIX="${NOKV_FLEET_SHARED_LOG_PREFIX:-metadata/fleet-smoke/${UNIQUE}/shared-log}"

S3_ARGS=(
    --object-backend rustfs
    --s3-bucket "$RUSTFS_BUCKET"
    --s3-endpoint "$RUSTFS_ENDPOINT"
    --s3-access-key-id "$RUSTFS_ACCESS_KEY"
    --s3-secret-access-key "$RUSTFS_SECRET_KEY"
)
# Control args shared by every server process and by the fleet client. The client
# uses the SAME etcd control plane to resolve which shard each path lives on.
CONTROL_COMMON=(
    --mount "$MOUNT"
    --control-backend etcd
    --control-etcd-endpoints "$ETCD_ENDPOINTS"
    --control-etcd-prefix "$ETCD_PREFIX"
    --control-etcd-lease-ttl-seconds "$ETCD_TTL_SECONDS"
    --metadata-shared-log-prefix "$SHARED_LOG_PREFIX"
    --metadata-checkpoint-archive-prefix "$CHECKPOINT_PREFIX"
)
# The fleet client points at the control plane (not a single server) and routes
# per request. `--server-bind` is unused for routing but kept as a harmless
# default; every request resolves its owning shard's endpoint from etcd.
FLEET_CLIENT=(--mount "$MOUNT" "${S3_ARGS[@]}"
    --control-backend etcd
    --control-etcd-endpoints "$ETCD_ENDPOINTS"
    --control-etcd-prefix "$ETCD_PREFIX"
    --control-etcd-lease-ttl-seconds "$ETCD_TTL_SECONDS")

echo "==> building nokv with etcd feature"
(
    cd "$ROOT_DIR"
    CARGO_TARGET_DIR="$FLEET_CARGO_TARGET_DIR" cargo build -p nokv --features etcd >/dev/null
)
NOKV="$FLEET_CARGO_TARGET_DIR/debug/nokv"

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
        --name nokv-fleet-smoke \
        --data-dir "$WORK_DIR/etcd" \
        --listen-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --advertise-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --listen-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-advertise-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster "nokv-fleet-smoke=http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster-state new \
        --initial-cluster-token "nokv-fleet-smoke-${UNIQUE}" \
        >"$WORK_DIR/etcd.log" 2>&1 &
    ETCD_PID=$!
fi
FIRST_ETCD_ENDPOINT="${ETCD_ENDPOINTS%%,*}"
wait_for_url "${FIRST_ETCD_ENDPOINT%/}/health" etcd

echo "==> starting server A: default shard ${DEFAULT_SHARD_ID} (index ${DEFAULT_SHARD_INDEX})"
"$NOKV" \
    --meta "$WORK_DIR/meta-a" \
    --server-bind "$SERVER_A_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_COMMON[@]}" \
    --shard-id "$DEFAULT_SHARD_ID" \
    --shard-index "$DEFAULT_SHARD_INDEX" \
    --node-id "$SERVER_A_BIND" \
    serve >"$WORK_DIR/server-a.log" 2>&1 &
SERVER_A_PID=$!
wait_for_server "$SERVER_A_BIND" "$SERVER_A_PID" "nokv server A"

echo "==> starting server B: dataset shard ${DATASET_SHARD_ID} (index ${DATASET_SHARD_INDEX})"
"$NOKV" \
    --meta "$WORK_DIR/meta-b" \
    --server-bind "$SERVER_B_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_COMMON[@]}" \
    --shard-id "$DATASET_SHARD_ID" \
    --shard-index "$DATASET_SHARD_INDEX" \
    --node-id "$SERVER_B_BIND" \
    serve >"$WORK_DIR/server-b.log" 2>&1 &
SERVER_B_PID=$!
wait_for_server "$SERVER_B_BIND" "$SERVER_B_PID" "nokv server B"

echo "==> fleet client: cross-shard writes (dataset -> B, other -> A)"
printf 'fleet-dataset-pre' >"$WORK_DIR/dataset-pre.bin"
printf 'fleet-other' >"$WORK_DIR/other.bin"
# /dataset is shard 1 (server B). Create the shard's own /dataset directory first,
# then a file under it.
"$NOKV" "${FLEET_CLIENT[@]}" mkdir /dataset
DATASET_PRE_OUT="$("$NOKV" "${FLEET_CLIENT[@]}" put-artifact /dataset/pre.bin "$WORK_DIR/dataset-pre.bin")"
echo "$DATASET_PRE_OUT"
DATASET_PRE_INODE="$(printf '%s\n' "$DATASET_PRE_OUT" | extract_inode)"
# /other is shard 0 (server A).
"$NOKV" "${FLEET_CLIENT[@]}" mkdir /other
OTHER_OUT="$("$NOKV" "${FLEET_CLIENT[@]}" put-artifact /other/file.bin "$WORK_DIR/other.bin")"
echo "$OTHER_OUT"
OTHER_INODE="$(printf '%s\n' "$OTHER_OUT" | extract_inode)"

echo "==> fleet client: cross-shard reads route to the right shard"
"$NOKV" "${FLEET_CLIENT[@]}" cat /dataset/pre.bin | grep -q "fleet-dataset-pre"
"$NOKV" "${FLEET_CLIENT[@]}" cat /other/file.bin | grep -q "fleet-other"
"$NOKV" "${FLEET_CLIENT[@]}" ls /dataset | grep -q "pre.bin"
"$NOKV" "${FLEET_CLIENT[@]}" ls /other | grep -q "file.bin"

# The two shards mint inodes from disjoint high-bit subspaces, so a /dataset inode
# and an /other inode can never collide; that they differ confirms two shards
# actually served the two paths.
if [[ -n "$DATASET_PRE_INODE" && -n "$OTHER_INODE" && "$DATASET_PRE_INODE" == "$OTHER_INODE" ]]; then
    echo "error: dataset and other inodes collided ($DATASET_PRE_INODE); paths were not sharded" >&2
    exit 1
fi

echo "==> writing more dataset data after a checkpoint so failover must replay the log"
BACKUP_JSON="$("$CURL_BIN" -fsS "http://${SERVER_B_BIND}/backup")"
echo "    $BACKUP_JSON"
echo "$BACKUP_JSON" | grep -q '"checkpoint_key"'
printf 'fleet-dataset-post' >"$WORK_DIR/dataset-post.bin"
DATASET_POST_OUT="$("$NOKV" "${FLEET_CLIENT[@]}" put-artifact /dataset/post.bin "$WORK_DIR/dataset-post.bin")"
echo "$DATASET_POST_OUT"
DATASET_POST_INODE="$(printf '%s\n' "$DATASET_POST_OUT" | extract_inode)"

echo "==> killing dataset-shard owner B and waiting for its etcd lease to expire"
OWNER_B_KILL_MS="$(now_ms)"
kill "$SERVER_B_PID" >/dev/null 2>&1 || true
wait "$SERVER_B_PID" >/dev/null 2>&1 || true
SERVER_B_PID=""
sleep "$((ETCD_TTL_SECONDS + 2))"

echo "==> starting B' as the dataset shard's failover owner (epoch 1 -> 2) on a new port"
OWNER_B2_START_MS="$(now_ms)"
"$NOKV" \
    --meta "$WORK_DIR/meta-b2" \
    --server-bind "$SERVER_B2_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_COMMON[@]}" \
    --shard-id "$DATASET_SHARD_ID" \
    --shard-index "$DATASET_SHARD_INDEX" \
    --node-id "$SERVER_B2_BIND" \
    --failover-from-epoch 1 \
    serve >"$WORK_DIR/server-b2.log" 2>&1 &
SERVER_B2_PID=$!
wait_for_server "$SERVER_B2_BIND" "$SERVER_B2_PID" "nokv server B'"
OWNER_B2_READY_MS="$(now_ms)"

STATS="$("$CURL_BIN" -fsS "http://${SERVER_B2_BIND}/stats")"
echo "$STATS" | grep -q "\"node_id\":\"${SERVER_B2_BIND}\""
echo "$STATS" | grep -q '"epoch":2'

echo "==> fleet client transparently re-resolves the dataset shard to B' and keeps serving"
# The client still has B cached for shard 1; its first /dataset request gets a
# typed handoff error, refreshes the shard map from etcd, and retries against B'.
# B' restored the shard from its checkpoint image + replayed shared log, so BOTH
# the pre- and post-checkpoint files are present.
"$NOKV" "${FLEET_CLIENT[@]}" cat /dataset/pre.bin | grep -q "fleet-dataset-pre"
"$NOKV" "${FLEET_CLIENT[@]}" cat /dataset/post.bin | grep -q "fleet-dataset-post"
"$NOKV" "${FLEET_CLIENT[@]}" ls /dataset | grep -q "post.bin"

# The default shard (server A) was never disturbed by the dataset-shard failover.
"$NOKV" "${FLEET_CLIENT[@]}" cat /other/file.bin | grep -q "fleet-other"

echo "==> fleet client accepts new dataset writes through B' without clobbering replayed data"
AFTER_OUT="$("$NOKV" "${FLEET_CLIENT[@]}" mkdir /dataset/after-failover)"
echo "$AFTER_OUT"
AFTER_FAILOVER_INODE="$(printf '%s\n' "$AFTER_OUT" | extract_inode)"
"$NOKV" "${FLEET_CLIENT[@]}" ls /dataset | grep -q "after-failover"

echo "==> running fsck on both shards after failover"
for bind in "$SERVER_A_BIND" "$SERVER_B2_BIND"; do
    FSCK_JSON="$("$CURL_BIN" -fsS "http://${bind}/fsck")"
    echo "    ${bind}: $FSCK_JSON"
    echo "$FSCK_JSON" | grep -q '"dangling_count":0'
done

FAILOVER_OBSERVED_MS="$((OWNER_B2_READY_MS - OWNER_B_KILL_MS))"
B2_STARTUP_MS="$((OWNER_B2_READY_MS - OWNER_B2_START_MS))"
METRICS_JSON="{\"lease_ttl_seconds\":${ETCD_TTL_SECONDS},\"owner_b_kill_ms\":${OWNER_B_KILL_MS},\"owner_b2_start_ms\":${OWNER_B2_START_MS},\"owner_b2_ready_ms\":${OWNER_B2_READY_MS},\"failover_observed_ms\":${FAILOVER_OBSERVED_MS},\"owner_b2_startup_ms\":${B2_STARTUP_MS},\"dataset_pre_inode\":${DATASET_PRE_INODE:-0},\"other_inode\":${OTHER_INODE:-0},\"dataset_post_inode\":${DATASET_POST_INODE:-0},\"after_failover_inode\":${AFTER_FAILOVER_INODE:-0}}"
if [[ -n "${NOKV_FLEET_METRICS_JSON:-}" ]]; then
    printf '%s\n' "$METRICS_JSON" >"$NOKV_FLEET_METRICS_JSON"
fi

echo
echo "FLEET_SMOKE_METRICS $METRICS_JSON"
echo "FLEET_SMOKE_OK: two-shard fleet routed cross-shard reads, failed the dataset shard over to a new owner, and the fleet client re-resolved and kept serving both shards"
