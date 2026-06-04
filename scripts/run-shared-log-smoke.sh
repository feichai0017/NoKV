#!/usr/bin/env bash
#
# Run a local RustFS-backed 3-voter + 1-learner metadata shared-log smoke test.
#
# The script starts RustFS, launches three NoKV metadata service processes that
# share one metadata-log membership, writes an artifact through the leader, and
# reads it back through both followers and a learner. It exercises real framed
# RPC append between metadata voters and learner checkpoint bootstrap rather
# than only the in-process fake quorum tests.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_SHARED_LOG_SMOKE_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_SHARED_LOG_SMOKE_AWS_BIN:-aws}"
NOKV_FS_BIN="${NOKV_SHARED_LOG_SMOKE_NOKV_FS_BIN:-}"
SKIP_BUILD="${NOKV_SHARED_LOG_SMOKE_SKIP_BUILD:-0}"

RUSTFS_ADDRESS="${NOKV_SHARED_LOG_SMOKE_RUSTFS_ADDRESS:-127.0.0.1:9020}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_SHARED_LOG_SMOKE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9021}"
RUSTFS_ENDPOINT="${NOKV_SHARED_LOG_SMOKE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_SHARED_LOG_SMOKE_RUSTFS_BUCKET:-nokv-shared-log-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_SHARED_LOG_SMOKE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_SHARED_LOG_SMOKE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_SHARED_LOG_SMOKE_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NODE1_ADDRESS="${NOKV_SHARED_LOG_SMOKE_NODE1:-127.0.0.1:7791}"
NODE2_ADDRESS="${NOKV_SHARED_LOG_SMOKE_NODE2:-127.0.0.1:7792}"
NODE3_ADDRESS="${NOKV_SHARED_LOG_SMOKE_NODE3:-127.0.0.1:7793}"
NODE4_ADDRESS="${NOKV_SHARED_LOG_SMOKE_NODE4:-127.0.0.1:7794}"
WORK_DIR="${NOKV_SHARED_LOG_SMOKE_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_SHARED_LOG_SMOKE_KEEP:-0}"

RUSTFS_PID=""
NODE_PIDS=()
OWN_WORK_DIR=0

usage() {
    cat <<EOF
Usage: scripts/run-shared-log-smoke.sh

Environment:
  NOKV_SHARED_LOG_SMOKE_WORKDIR        keep/use a specific work directory
  NOKV_SHARED_LOG_SMOKE_KEEP=1         keep the temporary work directory
  NOKV_SHARED_LOG_SMOKE_NOKV_FS_BIN    use an existing nokv-fs binary
  NOKV_SHARED_LOG_SMOKE_SKIP_BUILD=1   do not build nokv-fs when a binary is set
  NOKV_SHARED_LOG_SMOKE_RUSTFS_ADDRESS RustFS listen address (default: 127.0.0.1:9020)
  NOKV_SHARED_LOG_SMOKE_NODE1          leader address (default: 127.0.0.1:7791)
  NOKV_SHARED_LOG_SMOKE_NODE2          follower voter address (default: 127.0.0.1:7792)
  NOKV_SHARED_LOG_SMOKE_NODE3          follower voter address (default: 127.0.0.1:7793)
  NOKV_SHARED_LOG_SMOKE_NODE4          learner address (default: 127.0.0.1:7794)
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
        echo "NoKV shared-log smoke workdir: $WORK_DIR" >&2
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
    shift 2
    mkdir -p "$dir"
    printf '%s\0' \
        --server-bind "$bind" \
        --meta "$dir/meta" \
        --metadata-log "$dir/metadata.log" \
        --metadata-log-node "$node" \
        --metadata-log-leader 1 \
        --metadata-log-term 1 \
        --metadata-log-voters 1,2,3 \
        --metadata-log-learners 4 \
        --metadata-log-sync none \
        --object-backend rustfs \
        --s3-bucket "$RUSTFS_BUCKET" \
        --s3-endpoint "$RUSTFS_ENDPOINT" \
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
        --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
        "$@" \
        serve
}

start_node() {
    local node="$1"
    local bind="$2"
    local log="$WORK_DIR/node-$node.log"
    shift 2
    local args=()
    while IFS= read -r -d '' arg; do
        args+=("$arg")
    done < <(server_args "$node" "$bind" "$@")
    echo "Starting NoKV metadata node $node at $bind"
    "$NOKV_FS_BIN" "${args[@]}" >"$log" 2>&1 &
    NODE_PIDS+=("$!")
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd curl

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-shared-log-smoke.XXXXXX")"
    OWN_WORK_DIR=1
else
    mkdir -p "$WORK_DIR"
fi

trap cleanup EXIT INT TERM

if [[ -z "$NOKV_FS_BIN" ]]; then
    if [[ "$SKIP_BUILD" == "1" ]]; then
        NOKV_FS_BIN="$ROOT_DIR/target/release/nokv-fs"
    else
        echo "Building nokv-fs"
        (cd "$ROOT_DIR" && cargo build --release -p nokvfs-cli --bin nokv-fs)
        NOKV_FS_BIN="$ROOT_DIR/target/release/nokv-fs"
    fi
fi

if [[ ! -x "$NOKV_FS_BIN" ]]; then
    echo "error: nokv-fs binary is not executable: $NOKV_FS_BIN" >&2
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

start_node 1 "$NODE1_ADDRESS" \
    --metadata-log-peer "2=$NODE2_ADDRESS" \
    --metadata-log-peer "3=$NODE3_ADDRESS" \
    --metadata-log-peer "4=$NODE4_ADDRESS"
wait_for_http "http://$NODE1_ADDRESS/healthz" "NoKV node 1"

payload="$WORK_DIR/payload.bin"
restored2="$WORK_DIR/restored-node2.bin"
restored3="$WORK_DIR/restored-node3.bin"
restored4="$WORK_DIR/restored-node4.bin"
printf 'nokv shared log smoke\n' >"$payload"

"$NOKV_FS_BIN" \
    --server-bind "$NODE1_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /runs

"$NOKV_FS_BIN" \
    --server-bind "$NODE1_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /runs/1

"$NOKV_FS_BIN" \
    --server-bind "$NODE1_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    put-artifact /runs/1/checkpoint.txt "$payload"

curl -fsS --max-time 10 "http://$NODE1_ADDRESS/gc?limit=128" >/dev/null

# Bring the third voter and fourth learner online only after the leader has
# published a metadata checkpoint and compacted the initial log prefix. The
# next leader append must automatically install the checkpoint and append the
# current entry before both late nodes can serve reads.
start_node 3 "$NODE3_ADDRESS"
wait_for_http "http://$NODE3_ADDRESS/healthz" "NoKV node 3"
start_node 4 "$NODE4_ADDRESS"
wait_for_http "http://$NODE4_ADDRESS/healthz" "NoKV node 4"

"$NOKV_FS_BIN" \
    --server-bind "$NODE1_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /runs/1/after-checkpoint

"$NOKV_FS_BIN" \
    --server-bind "$NODE1_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /runs/1/learner-after-checkpoint

echo "Reading artifact through metadata node 2"
"$NOKV_FS_BIN" \
    --server-bind "$NODE2_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    cat /runs/1/checkpoint.txt >"$restored2"

echo "Reading artifact through metadata node 3"
"$NOKV_FS_BIN" \
    --server-bind "$NODE3_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    cat /runs/1/checkpoint.txt >"$restored3"

echo "Reading artifact through metadata node 4 learner"
"$NOKV_FS_BIN" \
    --server-bind "$NODE4_ADDRESS" \
    --object-backend rustfs \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    cat /runs/1/checkpoint.txt >"$restored4"

cmp "$payload" "$restored2"
cmp "$payload" "$restored3"
cmp "$payload" "$restored4"

echo "NoKV metadata shared-log smoke passed: leader compacted, late voter and learner auto-bootstrapped from checkpoint, and all replicas read the artifact."
