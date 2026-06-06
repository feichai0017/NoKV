#!/usr/bin/env bash
#
# Run a local RustFS-backed 3-voter OpenRaft metadata smoke test.
#
# The script starts RustFS, launches three NoKV metadata service processes in
# one OpenRaft metadata group, writes an artifact through the leader endpoint,
# and reads it back through the other voters. It is intentionally limited to
# the current production path: OpenRaft log durability plus an in-memory Holt
# state machine rebuilt from committed log entries.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_METADATA_RAFT_SMOKE_AWS_BIN:-aws}"
NOKV_FS_BIN="${NOKV_METADATA_RAFT_SMOKE_NOKV_FS_BIN:-}"
SKIP_BUILD="${NOKV_METADATA_RAFT_SMOKE_SKIP_BUILD:-0}"

RUSTFS_ADDRESS="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_ADDRESS:-127.0.0.1:9020}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9021}"
RUSTFS_ENDPOINT="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_BUCKET:-nokv-metadata-raft-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_METADATA_RAFT_SMOKE_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NODE1_ADDRESS="${NOKV_METADATA_RAFT_SMOKE_NODE1:-127.0.0.1:7791}"
NODE2_ADDRESS="${NOKV_METADATA_RAFT_SMOKE_NODE2:-127.0.0.1:7792}"
NODE3_ADDRESS="${NOKV_METADATA_RAFT_SMOKE_NODE3:-127.0.0.1:7793}"
WORK_DIR="${NOKV_METADATA_RAFT_SMOKE_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_METADATA_RAFT_SMOKE_KEEP:-0}"

RUSTFS_PID=""
NODE_PIDS=()
OWN_WORK_DIR=0

usage() {
    cat <<EOF
Usage: scripts/run-metadata-raft-smoke.sh

Environment:
  NOKV_METADATA_RAFT_SMOKE_WORKDIR        keep/use a specific work directory
  NOKV_METADATA_RAFT_SMOKE_KEEP=1         keep the temporary work directory
  NOKV_METADATA_RAFT_SMOKE_NOKV_FS_BIN    use an existing nokv binary
  NOKV_METADATA_RAFT_SMOKE_SKIP_BUILD=1   do not build nokv when a binary is set
  NOKV_METADATA_RAFT_SMOKE_RUSTFS_ADDRESS RustFS listen address (default: 127.0.0.1:9020)
  NOKV_METADATA_RAFT_SMOKE_NODE1          voter address (default: 127.0.0.1:7791)
  NOKV_METADATA_RAFT_SMOKE_NODE2          voter address (default: 127.0.0.1:7792)
  NOKV_METADATA_RAFT_SMOKE_NODE3          voter address (default: 127.0.0.1:7793)
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
    for pid in "${NODE_PIDS[@]}"; do
        if kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
        fi
    done
    for pid in "${NODE_PIDS[@]}"; do
        wait "$pid" >/dev/null 2>&1 || true
    done
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORK_DIR" ]]; then
        for log in "$WORK_DIR"/node*.log "$WORK_DIR"/rustfs.log; do
            if [[ -f "$log" ]]; then
                echo "---- $(basename "$log") tail ----" >&2
                tail -80 "$log" >&2 || true
                echo "-----------------------------" >&2
            fi
        done
    fi
    if [[ "$OWN_WORK_DIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORK_DIR"
    elif [[ -n "$WORK_DIR" ]]; then
        echo "NoKV OpenRaft metadata smoke workdir: $WORK_DIR" >&2
    fi
    exit "$status"
}

wait_for_http() {
    local url="$1"
    local name="$2"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $url" >&2
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

server_args() {
    local node="$1"
    local bind="$2"
    local dir="$WORK_DIR/node-$node"
    mkdir -p "$dir"
    printf '%s\0' \
        --server-bind "$bind" \
        --meta "$dir/meta" \
        --metadata-raft-node "$node" \
        --metadata-raft-voters 1,2,3 \
        --metadata-raft-peer "1=$NODE1_ADDRESS" \
        --metadata-raft-peer "2=$NODE2_ADDRESS" \
        --metadata-raft-peer "3=$NODE3_ADDRESS" \
        --metadata-raft-log-sync none \
        --object-backend rustfs \
        --s3-bucket "$RUSTFS_BUCKET" \
        --s3-endpoint "$RUSTFS_ENDPOINT" \
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
        --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
        serve
}

start_node() {
    local node="$1"
    local bind="$2"
    local log="$WORK_DIR/node-$node.log"
    local args=()
    while IFS= read -r -d '' arg; do
        args+=("$arg")
    done < <(server_args "$node" "$bind")
    echo "Starting NoKV metadata node $node at $bind"
    "$NOKV_FS_BIN" "${args[@]}" >"$log" 2>&1 &
    NODE_PIDS+=("$!")
}

run_client() {
    "$NOKV_FS_BIN" \
        --server-bind "$1" \
        --object-backend rustfs \
        --s3-bucket "$RUSTFS_BUCKET" \
        --s3-endpoint "$RUSTFS_ENDPOINT" \
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
        --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
        "${@:2}"
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd curl

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-metadata-raft-smoke.XXXXXX")"
    OWN_WORK_DIR=1
else
    mkdir -p "$WORK_DIR"
fi

trap cleanup EXIT INT TERM

if [[ -z "$NOKV_FS_BIN" ]]; then
    if [[ "$SKIP_BUILD" == "1" ]]; then
        NOKV_FS_BIN="$ROOT_DIR/target/release/nokv"
    else
        echo "Building nokv"
        (cd "$ROOT_DIR" && cargo build --release -p nokv --bin nokv)
        NOKV_FS_BIN="$ROOT_DIR/target/release/nokv"
    fi
fi

if [[ ! -x "$NOKV_FS_BIN" ]]; then
    echo "error: nokv binary is not executable: $NOKV_FS_BIN" >&2
    exit 127
fi

echo "Starting RustFS at $RUSTFS_ENDPOINT"
mkdir -p "$WORK_DIR/rustfs-data"
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server \
    --address "$RUSTFS_ADDRESS" \
    --console-enable \
    --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
    "$WORK_DIR/rustfs-data" >"$WORK_DIR/rustfs.log" 2>&1 &
RUSTFS_PID=$!

wait_for_rustfs
create_bucket

start_node 2 "$NODE2_ADDRESS"
wait_for_http "http://$NODE2_ADDRESS/healthz" "NoKV node 2"
start_node 3 "$NODE3_ADDRESS"
wait_for_http "http://$NODE3_ADDRESS/healthz" "NoKV node 3"
start_node 1 "$NODE1_ADDRESS"
wait_for_http "http://$NODE1_ADDRESS/healthz" "NoKV node 1"

payload="$WORK_DIR/payload.bin"
restored2="$WORK_DIR/restored-node2.bin"
restored3="$WORK_DIR/restored-node3.bin"
printf 'nokv metadata raft smoke\n' >"$payload"

run_client "$NODE1_ADDRESS" mkdir /runs
run_client "$NODE1_ADDRESS" mkdir /runs/1
run_client "$NODE1_ADDRESS" put-artifact /runs/1/checkpoint.txt "$payload"

echo "Reading artifact through metadata node 2"
run_client "$NODE2_ADDRESS" cat /runs/1/checkpoint.txt >"$restored2"

echo "Reading artifact through metadata node 3"
run_client "$NODE3_ADDRESS" cat /runs/1/checkpoint.txt >"$restored3"

cmp "$payload" "$restored2"
cmp "$payload" "$restored3"

echo "NoKV OpenRaft metadata smoke passed: all three voters served a leader-published artifact."
