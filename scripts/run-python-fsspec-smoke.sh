#!/usr/bin/env bash
#
# Run a live RustFS-backed smoke for the NoKV Python/fsspec batch range path.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
PYTHON_BIN="${NOKV_PYTHON_BIN:-python3}"

RUSTFS_ADDRESS="${NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS:-127.0.0.1:9060}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9061}"
RUSTFS_ENDPOINT="${NOKV_PYTHON_SMOKE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_PYTHON_SMOKE_RUSTFS_BUCKET:-nokv-python-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_PYTHON_SMOKE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_PYTHON_SMOKE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_PYTHON_SMOKE_RUSTFS_BUFFER_PROFILE:-AiTraining}"
SERVER_ADDRESS="${NOKV_PYTHON_SMOKE_SERVER_ADDRESS:-127.0.0.1:7782}"

PROFILE="${NOKV_PYTHON_SMOKE_PROFILE:-smoke}"
SHARD_COUNT="${NOKV_PYTHON_SMOKE_SHARD_COUNT:-8}"
FILES_PER_DIR="${NOKV_PYTHON_SMOKE_FILES_PER_DIR:-64}"
SAMPLE_BYTES="${NOKV_PYTHON_SMOKE_SAMPLE_BYTES:-4096}"
RANGE_STRIDE="${NOKV_PYTHON_SMOKE_RANGE_STRIDE:-2}"
RANGE_COALESCE_GAP_BYTES="${NOKV_PYTHON_SMOKE_RANGE_COALESCE_GAP_BYTES:-512}"
READ_CONCURRENCY="${NOKV_PYTHON_SMOKE_CONCURRENCY:-4}"
CACHE_STATES="${NOKV_PYTHON_SMOKE_CACHE_STATES:-cold,warm}"
READ_SHAPE="${NOKV_PYTHON_SMOKE_READ_SHAPE:-ranges}"
READ_BUFFER_MEMORY_KIND="${NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND:-system}"
RESULT_CSV="${NOKV_PYTHON_SMOKE_RESULT_CSV:-}"
METADATA_TIER="${NOKV_PYTHON_SMOKE_METADATA_TIER:-nokv-l1-service}"
OBJECT_BACKEND_LABEL="${NOKV_PYTHON_SMOKE_OBJECT_BACKEND:-rustfs}"
HOT_OBJECT_ROOT="${NOKV_PYTHON_SMOKE_HOT_OBJECT_ROOT:-}"
if [[ -z "${NOKV_PYTHON_SMOKE_OBJECT_BACKEND:-}" && -n "$HOT_OBJECT_ROOT" ]]; then
    OBJECT_BACKEND_LABEL="rustfs+local-hot+put=cold-then-hot"
fi

NOKV_BIN="${NOKV_PYTHON_SMOKE_NOKV_BIN:-}"
SKIP_BUILD="${NOKV_PYTHON_SMOKE_SKIP_BUILD:-0}"
KEEP_WORKDIR="${NOKV_PYTHON_SMOKE_KEEP:-0}"
WORK_DIR="${NOKV_PYTHON_SMOKE_WORKDIR:-}"
CARGO_TARGET_DIR_OVERRIDE="${NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"
RUSTFS_DATA_DIR_OVERRIDE="${NOKV_PYTHON_SMOKE_RUSTFS_DATA_DIR:-}"

RUSTFS_PID=""
SERVER_PID=""
OWN_WORK_DIR=0
OWN_RUSTFS_DATA_DIR=0
META_DIR=""
RUSTFS_DATA_DIR=""
RUSTFS_LOG=""
SERVER_LOG=""
VENV_DIR=""

usage() {
    cat <<EOF
Usage: scripts/run-python-fsspec-smoke.sh

Environment:
  NOKV_PYTHON_SMOKE_WORKDIR              keep/use a specific work directory
  NOKV_PYTHON_SMOKE_KEEP=1               keep temporary work directory
  NOKV_PYTHON_SMOKE_NOKV_BIN             use an existing nokv binary
  NOKV_PYTHON_SMOKE_SKIP_BUILD=1         do not build nokv when a binary is set
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR     cargo target directory for nokv/maturin builds
  NOKV_PYTHON_SMOKE_RUSTFS_DATA_DIR      optional RustFS data dir; defaults to a no-space temp dir when workdir contains spaces
  NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS       RustFS listen address (default: 127.0.0.1:9060)
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS       NoKV metadata server address (default: 127.0.0.1:7782)
  NOKV_PYTHON_SMOKE_SHARD_COUNT          packed shard count (default: 8)
  NOKV_PYTHON_SMOKE_FILES_PER_DIR        samples per shard (default: 64)
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES         bytes per sample (default: 4096)
  NOKV_PYTHON_SMOKE_RANGE_STRIDE         selected sample stride (default: 2)
  NOKV_PYTHON_SMOKE_CONCURRENCY          shard requests per native batch (default: 4)
  NOKV_PYTHON_SMOKE_READ_SHAPE           ranges|packed|into|buffer|planned_buffer|batch_reader|epoch_reader benchmark return shape (default: ranges)
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND system|page_locked for buffer shapes (default: system)
  NOKV_PYTHON_SMOKE_CACHE_STATES         cold,warm,hot subset (default: cold,warm)
  NOKV_PYTHON_SMOKE_RESULT_CSV           optional benchmark CSV output path
  NOKV_PYTHON_SMOKE_METADATA_TIER        result metadata_tier label (default: nokv-l1-service)
  NOKV_PYTHON_SMOKE_OBJECT_BACKEND       result object_backend label (default: rustfs, or rustfs+local-hot+put=cold-then-hot with hot root)
  NOKV_PYTHON_SMOKE_HOT_OBJECT_ROOT      optional local hot object root for Python reads; default tiered writes are cold-then-hot

The smoke builds the Python extension with maturin, writes a packed shard
dataset through the NoKV CLI, checks sample ranges through
nokv.fsspec.NoKVFileSystem range-batch APIs, and emits L1 canonical benchmark
rows from bench/drivers/native_fsspec_bench.py.
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
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 ]]; then
        if [[ -n "$SERVER_LOG" && -f "$SERVER_LOG" ]]; then
            echo "---- NoKV server log tail ----" >&2
            tail -80 "$SERVER_LOG" >&2 || true
            echo "---------------------------------" >&2
        fi
        if [[ -n "$RUSTFS_LOG" && -f "$RUSTFS_LOG" ]]; then
            echo "---- RustFS log tail ----" >&2
            tail -80 "$RUSTFS_LOG" >&2 || true
            echo "-------------------------" >&2
        fi
    fi
    if [[ "$OWN_WORK_DIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORK_DIR"
    elif [[ -n "$WORK_DIR" ]]; then
        echo "NoKV Python smoke workdir: $WORK_DIR" >&2
    fi
    if [[ "$OWN_RUSTFS_DATA_DIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$RUSTFS_DATA_DIR"
    elif [[ "$OWN_RUSTFS_DATA_DIR" -eq 1 && -n "$RUSTFS_DATA_DIR" ]]; then
        echo "NoKV Python smoke RustFS data dir: $RUSTFS_DATA_DIR" >&2
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

wait_for_metadata_server() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
            echo "error: nokv serve exited before becoming ready" >&2
            return 1
        fi
        if curl -fsS --max-time 2 "http://${SERVER_ADDRESS}/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for NoKV metadata server at $SERVER_ADDRESS" >&2
    return 1
}

build_nokv_binary() {
    if [[ -z "$NOKV_BIN" ]]; then
        if [[ "$SKIP_BUILD" == "1" ]]; then
            echo "error: NOKV_PYTHON_SMOKE_NOKV_BIN is required when NOKV_PYTHON_SMOKE_SKIP_BUILD=1" >&2
            exit 2
        fi
        if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
            (cd "$ROOT_DIR" && CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE" cargo build -p nokv --bin nokv)
            NOKV_BIN="$CARGO_TARGET_DIR_OVERRIDE/debug/nokv"
        else
            (cd "$ROOT_DIR" && cargo build -p nokv --bin nokv)
            NOKV_BIN="$ROOT_DIR/target/debug/nokv"
        fi
    elif [[ "$SKIP_BUILD" != "1" ]]; then
        if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
            (cd "$ROOT_DIR" && CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE" cargo build -p nokv --bin nokv)
        else
            (cd "$ROOT_DIR" && cargo build -p nokv --bin nokv)
        fi
    fi
}

build_python_package() {
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    "$VENV_DIR/bin/python" -m pip install --upgrade pip >/dev/null
    "$VENV_DIR/bin/python" -m pip install 'maturin>=1,<2' 'fsspec>=2024.0.0' >/dev/null
    (
        cd "$ROOT_DIR/crates/nokv-python"
        if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
            CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE" \
                VIRTUAL_ENV="$VENV_DIR" \
                PATH="$VENV_DIR/bin:$PATH" \
                "$VENV_DIR/bin/python" -m maturin develop --release
        else
            VIRTUAL_ENV="$VENV_DIR" \
                PATH="$VENV_DIR/bin:$PATH" \
                "$VENV_DIR/bin/python" -m maturin develop --release
        fi
    )
}

write_shard_payload() {
    local shard="$1"
    local path="$2"
    "$VENV_DIR/bin/python" - "$path" "$shard" "$FILES_PER_DIR" "$SAMPLE_BYTES" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
shard = int(sys.argv[2])
files_per_dir = int(sys.argv[3])
sample_bytes = int(sys.argv[4])
samples = []
for sample in range(files_per_dir):
    seed = shard * 31 + sample * 17
    samples.append(bytes(((seed + offset) % 251 for offset in range(sample_bytes))))
path.write_bytes(b"".join(samples))
PY
}

run_python_smoke() {
    local hot_root_arg="${HOT_OBJECT_ROOT:-__NONE__}"
    "$VENV_DIR/bin/python" - \
        "$SERVER_ADDRESS" "$RUSTFS_BUCKET" "$RUSTFS_ENDPOINT" \
        "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" \
        "$FILES_PER_DIR" "$SAMPLE_BYTES" "$hot_root_arg" <<'PY'
import gc
import sys

from nokv.fsspec import NoKVFileSystem

server, bucket, endpoint, access_key, secret_key = sys.argv[1:6]
files_per_dir = int(sys.argv[6])
sample_bytes = int(sys.argv[7])
hot_object_root = sys.argv[8]
if hot_object_root == "__NONE__":
    hot_object_root = None

def sample_payload(sample, length):
    seed = sample * 17
    return bytes(((seed + offset) % 251 for offset in range(length)))

fs = NoKVFileSystem(
    metadata_addr=server,
    bucket=bucket,
    endpoint=endpoint,
    access_key_id=access_key,
    secret_access_key=secret_key,
    region="auto",
    hot_object_root=hot_object_root,
)
sample_indexes = sorted({0, min(files_per_dir - 1, 3), min(files_per_dir - 1, 7)})
reads = fs.read_ranges_batch([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
])
assert len(reads) == 1, reads
assert len(reads[0]) == len(sample_indexes), reads
for sample, data in zip(sample_indexes, reads[0]):
    assert data == sample_payload(sample, sample_bytes)
packed = fs.read_ranges_batch_packed([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
])
assert packed == [b"".join(sample_payload(sample, sample_bytes) for sample in sample_indexes)]
expected_packed = b"".join(sample_payload(sample, sample_bytes) for sample in sample_indexes)
into_buffer = bytearray(len(expected_packed))
returned_buffer, layout = fs.read_ranges_batch_into([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
], into_buffer)
assert returned_buffer is into_buffer
assert layout == [(0, len(expected_packed))]
assert bytes(into_buffer) == expected_packed
read_buffer, layout = fs.read_ranges_batch_buffer([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
])
assert layout == [(0, len(expected_packed))]
assert len(read_buffer) == len(expected_packed)
assert read_buffer.memory_kind() == "system"
assert read_buffer.to_bytes() == expected_packed
assert read_buffer[0] == expected_packed[0]
assert read_buffer[-1] == expected_packed[-1]
exported = read_buffer.export()
assert len(exported) == len(expected_packed)
assert exported[0] == expected_packed[0]
assert exported[-1] == expected_packed[-1]
assert exported.to_bytes() == expected_packed
prefix_len = min(8, len(expected_packed))
assert exported.slice(0, prefix_len) == expected_packed[:prefix_len]
assert read_buffer.export_count() == 1
try:
    read_buffer.clear()
    raise AssertionError("active ReadBuffer export should block clear")
except (BufferError, RuntimeError):
    pass
try:
    fs.read_ranges_batch_buffer([
        (
            "/dataset/shards/shard-0000.bin",
            [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
            None,
            2 * sample_bytes,
        )
    ], read_buffer)
    raise AssertionError("active ReadBuffer export should block refill")
except (BufferError, RuntimeError):
    pass
del exported
gc.collect()
assert read_buffer.export_count() == 0
reused_buffer = fs.new_read_buffer(len(expected_packed))
assert reused_buffer.memory_kind() == "system"
returned_buffer, layout = fs.read_ranges_batch_buffer([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
], reused_buffer)
assert returned_buffer is reused_buffer
assert layout == [(0, len(expected_packed))]
assert reused_buffer.to_bytes() == expected_packed
range_plan = fs.prepare_range_batch([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
])
assert len(range_plan) == 1
assert range_plan.request_count() == 1
assert range_plan.range_count() == len(sample_indexes)
assert range_plan.output_len() == len(expected_packed)
assert range_plan.layout() == [(0, len(expected_packed))]
planned_buffer = fs.new_read_buffer(range_plan.output_len())
returned_buffer, layout = fs.read_range_batch_plan_buffer(range_plan, planned_buffer)
assert returned_buffer is planned_buffer
assert layout == [(0, len(expected_packed))]
assert planned_buffer.to_bytes() == expected_packed
range_reader = fs.prepare_range_batch_reader([
    (
        "/dataset/shards/shard-0000.bin",
        [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
        None,
        2 * sample_bytes,
    )
])
assert len(range_reader) == 1
assert range_reader.request_count() == 1
assert range_reader.range_count() == len(sample_indexes)
assert range_reader.output_len() == len(expected_packed)
assert range_reader.layout() == [(0, len(expected_packed))]
assert range_reader.memory_kind() == "system"
reader_buffer = range_reader.buffer()
range_reader.read()
assert len(reader_buffer) == len(expected_packed)
assert reader_buffer.to_bytes() == expected_packed
reader_export = reader_buffer.export()
try:
    range_reader.read()
    raise AssertionError("active RangeBatchReader buffer export should block refill")
except (BufferError, RuntimeError):
    pass
del reader_export
gc.collect()
range_reader.read()
assert reader_buffer.to_bytes() == expected_packed
epoch_reader = fs.prepare_range_batch_epoch([
    [
        (
            "/dataset/shards/shard-0000.bin",
            [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
            None,
            2 * sample_bytes,
        )
    ],
    [
        (
            "/dataset/shards/shard-0000.bin",
            [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
            None,
            2 * sample_bytes,
        )
    ],
])
assert len(epoch_reader) == 2
assert epoch_reader.batch_count() == 2
assert epoch_reader.worker_count() == 2
assert epoch_reader.output_len(0) == len(expected_packed)
assert epoch_reader.output_len(1) == len(expected_packed)
assert epoch_reader.layout(0) == [(0, len(expected_packed))]
assert epoch_reader.layout(1) == [(0, len(expected_packed))]
assert epoch_reader.memory_kind(0) == "system"
assert epoch_reader.memory_kind(1) == "system"
epoch_buffer0 = epoch_reader.buffer(0)
epoch_buffer1 = epoch_reader.buffer(1)
assert epoch_reader.read_next() == 0
assert epoch_buffer0.to_bytes() == expected_packed
assert epoch_reader.read_next() == 1
assert epoch_buffer1.to_bytes() == expected_packed
assert epoch_reader.read_next() == 0
assert epoch_buffer0.to_bytes() == expected_packed
epoch_reader.reset()
assert epoch_reader.read_all() == [0, 1]
assert epoch_buffer0.to_bytes() == expected_packed
assert epoch_buffer1.to_bytes() == expected_packed
epoch_reader.reset()
assert epoch_reader.read_next() == 0
assert epoch_buffer0.to_bytes() == expected_packed
epoch_reader.reset()
epoch_export = epoch_buffer0.export()
try:
    epoch_reader.read_all()
    raise AssertionError("active RangeBatchEpochReader buffer export should block read_all refill")
except (BufferError, RuntimeError):
    pass
del epoch_export
gc.collect()
epoch_reader.reset()
assert epoch_reader.read_all() == [0, 1]
assert epoch_buffer0.to_bytes() == expected_packed
assert epoch_buffer1.to_bytes() == expected_packed
try:
    page_locked_buffer = fs.new_read_buffer(len(expected_packed), memory_kind="page_locked")
except RuntimeError as err:
    print(f"NoKV page_locked ReadBuffer smoke skipped: {err}")
else:
    assert page_locked_buffer.memory_kind() == "page_locked"
    returned_buffer, layout = fs.read_ranges_batch_buffer([
        (
            "/dataset/shards/shard-0000.bin",
            [(sample * sample_bytes, sample_bytes) for sample in sample_indexes],
            None,
            2 * sample_bytes,
        )
    ], page_locked_buffer)
    assert returned_buffer is page_locked_buffer
    assert layout == [(0, len(expected_packed))]
    assert page_locked_buffer.to_bytes() == expected_packed
    print("NoKV page_locked ReadBuffer smoke passed")
try:
    fs.new_read_buffer(0, memory_kind="cuda_pinned")
    raise AssertionError("invalid ReadBuffer memory_kind should fail")
except ValueError:
    pass
cat_sample = min(files_per_dir - 1, 5)
cat_len = min(32, sample_bytes)
assert fs.cat_file(
    "/dataset/shards/shard-0000.bin",
    start=cat_sample * sample_bytes,
    end=cat_sample * sample_bytes + cat_len,
) == sample_payload(cat_sample, cat_len)
stats = fs.stats()
for field in (
    "object_gets",
    "cache_hits",
    "read_plan_cache_hits",
    "read_plan_cache_misses",
    "data_fabric_planned_blocks",
    "data_fabric_object_fallbacks",
):
    assert field in stats, field
assert stats["object_gets"] + stats["cache_hits"] > 0, stats
print("NoKV Python fsspec live smoke passed")
PY
}

run_python_benchmark() {
    local args=(
        "$VENV_DIR/bin/python" "$ROOT_DIR/bench/drivers/native_fsspec_bench.py"
        --metadata-addr "$SERVER_ADDRESS"
        --bucket "$RUSTFS_BUCKET"
        --endpoint "$RUSTFS_ENDPOINT"
        --access-key-id "$RUSTFS_ACCESS_KEY"
        --secret-access-key "$RUSTFS_SECRET_KEY"
        --region auto
        --metadata-tier "$METADATA_TIER"
        --object-backend "$OBJECT_BACKEND_LABEL"
        --profile "$PROFILE"
        --dataset-root /dataset/shards
        --shard-count "$SHARD_COUNT"
        --files-per-dir "$FILES_PER_DIR"
        --sample-bytes "$SAMPLE_BYTES"
        --range-stride "$RANGE_STRIDE"
        --range-coalesce-gap-bytes "$RANGE_COALESCE_GAP_BYTES"
        --concurrency "$READ_CONCURRENCY"
        --read-shape "$READ_SHAPE"
        --read-buffer-memory-kind "$READ_BUFFER_MEMORY_KIND"
        --cache-states "$CACHE_STATES"
    )
    if [[ -n "$HOT_OBJECT_ROOT" ]]; then
        mkdir -p "$HOT_OBJECT_ROOT"
        args+=(--hot-object-root "$HOT_OBJECT_ROOT")
    fi
    if [[ -n "$RESULT_CSV" ]]; then
        mkdir -p "$(dirname "$RESULT_CSV")"
        "${args[@]}" | tee "$RESULT_CSV"
    else
        "${args[@]}"
    fi
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd "$PYTHON_BIN"
require_cmd curl

if (( SHARD_COUNT <= 0 || FILES_PER_DIR <= 0 || SAMPLE_BYTES <= 0 || READ_CONCURRENCY <= 0 )); then
    echo "error: shard/files/sample/concurrency values must be positive" >&2
    exit 2
fi

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-python-smoke.XXXXXX")"
    OWN_WORK_DIR=1
else
    mkdir -p "$WORK_DIR"
fi

META_DIR="$WORK_DIR/meta"
if [[ -n "$RUSTFS_DATA_DIR_OVERRIDE" ]]; then
    RUSTFS_DATA_DIR="$RUSTFS_DATA_DIR_OVERRIDE"
elif [[ "$WORK_DIR" == *[[:space:]]* ]]; then
    RUSTFS_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-python-rustfs.XXXXXX")"
    OWN_RUSTFS_DATA_DIR=1
else
    RUSTFS_DATA_DIR="$WORK_DIR/rustfs-data"
fi
RUSTFS_LOG="$WORK_DIR/rustfs.log"
SERVER_LOG="$WORK_DIR/nokv-server.log"
VENV_DIR="$WORK_DIR/venv"
mkdir -p "$META_DIR" "$RUSTFS_DATA_DIR"

trap cleanup EXIT INT TERM

build_nokv_binary
build_python_package

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

echo "Starting NoKV metadata server at $SERVER_ADDRESS"
"$NOKV_BIN" \
    --server-bind "$SERVER_ADDRESS" \
    --meta "$META_DIR" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    --uid "$(id -u)" \
    --gid "$(id -g)" \
    serve >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

wait_for_metadata_server

"$NOKV_BIN" \
    --server-bind "$SERVER_ADDRESS" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /dataset >/dev/null
"$NOKV_BIN" \
    --server-bind "$SERVER_ADDRESS" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    mkdir /dataset/shards >/dev/null
for ((shard = 0; shard < SHARD_COUNT; shard++)); do
    shard_name="$(printf 'shard-%04d.bin' "$shard")"
    shard_path="$WORK_DIR/$shard_name"
    write_shard_payload "$shard" "$shard_path"
    "$NOKV_BIN" \
        --server-bind "$SERVER_ADDRESS" \
        --object-backend rustfs \
        --s3-endpoint "$RUSTFS_ENDPOINT" \
        --s3-bucket "$RUSTFS_BUCKET" \
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
        --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
        put-artifact "/dataset/shards/$shard_name" "$shard_path" >/dev/null
done

run_python_smoke
run_python_benchmark
