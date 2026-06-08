#!/usr/bin/env bash
#
# Metadata disaster-recovery smoke test against a local RustFS.
#
# Proves the end-to-end server-level DR flow against a real S3-compatible object
# store: start a server, build a namespace, archive the metadata checkpoint to
# the object store, destroy the metadata node entirely, restore onto a fresh
# node from the archive, and confirm the namespace (and file bodies) survived.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"

RUSTFS_ADDRESS="${NOKV_DR_RUSTFS_ADDRESS:-127.0.0.1:9020}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_DR_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9021}"
RUSTFS_ENDPOINT="${NOKV_DR_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_DR_RUSTFS_BUCKET:-nokv-dr-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_DR_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_DR_RUSTFS_SECRET_KEY:-rustfsadmin}"
SERVER_BIND="${NOKV_DR_SERVER_BIND:-127.0.0.1:7720}"

RUSTFS_PID=""
SERVER_PID=""
WORK_DIR=""

cleanup() {
    local status=$?
    [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" >/dev/null 2>&1 || true
    [[ -n "$RUSTFS_PID" ]] && kill "$RUSTFS_PID" >/dev/null 2>&1 || true
    wait >/dev/null 2>&1 || true
    if [[ "$status" -ne 0 && -n "$WORK_DIR" && -f "$WORK_DIR/rustfs.log" ]]; then
        echo "---- RustFS log tail ----" >&2
        tail -40 "$WORK_DIR/rustfs.log" >&2 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORK_DIR" && -f "$WORK_DIR/server.log" ]]; then
        echo "---- nokv server log tail ----" >&2
        tail -40 "$WORK_DIR/server.log" >&2 || true
    fi
    [[ -n "$WORK_DIR" ]] && rm -rf "$WORK_DIR"
    exit "$status"
}
trap cleanup EXIT INT TERM

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || { echo "error: required command not found: $1" >&2; exit 127; }
}

wait_for_http() {
    local url="$1" deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        curl -s -o /dev/null --max-time 2 "$url" 2>/dev/null && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $url" >&2
    return 1
}

wait_for_server() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null && {
            echo "error: nokv server exited early" >&2; return 1; }
        if curl -s -o /dev/null --max-time 2 "http://${SERVER_BIND}/readyz" 2>/dev/null; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for nokv server at $SERVER_BIND" >&2
    return 1
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd curl

echo "==> building nokv (debug)"
( cd "$ROOT_DIR" && cargo build -p nokv >/dev/null 2>&1 )
NOKV="$ROOT_DIR/target/debug/nokv"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-dr-smoke.XXXXXX")"
META_A="$WORK_DIR/meta-a"      # original metadata node
META_B="$WORK_DIR/meta-b"      # replacement node after "loss"
RUSTFS_DATA="$WORK_DIR/rustfs"
mkdir -p "$META_A" "$META_B" "$RUSTFS_DATA"

S3_ARGS=(
    --object-backend rustfs
    --s3-bucket "$RUSTFS_BUCKET"
    --s3-endpoint "$RUSTFS_ENDPOINT"
    --s3-access-key-id "$RUSTFS_ACCESS_KEY"
    --s3-secret-access-key "$RUSTFS_SECRET_KEY"
)
CLIENT=(--server-bind "$SERVER_BIND" "${S3_ARGS[@]}")

echo "==> starting RustFS at $RUSTFS_ENDPOINT"
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server --address "$RUSTFS_ADDRESS" \
    --console-enable --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    "$RUSTFS_DATA" >"$WORK_DIR/rustfs.log" 2>&1 &
RUSTFS_PID=$!
wait_for_http "$RUSTFS_ENDPOINT"
AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
    "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" s3api create-bucket --bucket "$RUSTFS_BUCKET" \
    >/dev/null 2>&1 || true

echo "==> starting original server (meta-a), DR archive on by default"
"$NOKV" --meta "$META_A" --server-bind "$SERVER_BIND" "${S3_ARGS[@]}" serve \
    >"$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
wait_for_server

echo "==> building a namespace"
printf 'disaster-recovery-payload-v1' > "$WORK_DIR/a.bin"
"$NOKV" "${CLIENT[@]}" mkdir /runs
"$NOKV" "${CLIENT[@]}" put-artifact /runs/a.bin "$WORK_DIR/a.bin"
"$NOKV" "${CLIENT[@]}" ls /runs | grep -q "a.bin" \
    || { echo "FAIL: /runs/a.bin not visible before backup" >&2; exit 1; }
echo "    namespace ok: /runs/a.bin present"

echo "==> archiving metadata checkpoint to the object store"
BACKUP_JSON="$("$NOKV" "${CLIENT[@]}" backup)"
echo "    $BACKUP_JSON"
echo "$BACKUP_JSON" | grep -q '"checkpoint_key"' \
    || { echo "FAIL: backup did not report a checkpoint_key" >&2; exit 1; }

echo "==> destroying the metadata node (stop server, wipe meta-a)"
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
SERVER_PID=""
rm -rf "$META_A"
echo "    meta-a wiped; objects + archive remain in RustFS"

echo "==> restoring onto a fresh node (meta-b) from the archive"
RESTORE_JSON="$("$NOKV" --meta "$META_B" "${S3_ARGS[@]}" restore)"
echo "    $RESTORE_JSON"
echo "$RESTORE_JSON" | grep -q '"restored":true' \
    || { echo "FAIL: restore did not report success" >&2; exit 1; }

echo "==> starting replacement server (meta-b)"
"$NOKV" --meta "$META_B" --server-bind "$SERVER_BIND" "${S3_ARGS[@]}" serve \
    >>"$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
wait_for_server

echo "==> verifying the namespace + body survived the loss"
"$NOKV" "${CLIENT[@]}" ls /runs | grep -q "a.bin" \
    || { echo "FAIL: /runs/a.bin missing after restore" >&2; exit 1; }
GOT="$("$NOKV" "${CLIENT[@]}" cat /runs/a.bin)"
[[ "$GOT" == "disaster-recovery-payload-v1" ]] \
    || { echo "FAIL: body mismatch after restore: got '$GOT'" >&2; exit 1; }
echo "    recovered: /runs/a.bin present and body byte-identical"

echo "==> verifying the recovered node accepts new writes"
"$NOKV" "${CLIENT[@]}" mkdir /runs2
"$NOKV" "${CLIENT[@]}" ls / | grep -q "runs2" \
    || { echo "FAIL: recovered node rejected a new write" >&2; exit 1; }
echo "    recovered node is writable"

echo "==> running fsck on the recovered node"
FSCK_JSON="$("$NOKV" "${CLIENT[@]}" fsck)"
echo "    $FSCK_JSON"
echo "$FSCK_JSON" | grep -q '"dangling_count":0' \
    || { echo "FAIL: fsck found dangling block references after restore" >&2; exit 1; }
echo "    fsck clean: every live block reference resolves to an object"

echo
echo "DR_SMOKE_OK: namespace + bodies recovered from object-store archive after total metadata loss"
