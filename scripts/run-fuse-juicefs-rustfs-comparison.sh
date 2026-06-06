#!/usr/bin/env bash
#
# Run a local FUSE-vs-FUSE NoKV and JuiceFS comparison against one
# disposable RustFS endpoint.
#
# This is an engineering baseline, not an official MLPerf result. It keeps the
# object backend and generated workload shape fixed, then measures the mounted
# filesystem path for both systems.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
REDIS_BIN="${NOKV_REDIS_BIN:-redis-server}"
JUICEFS_BIN="${NOKV_JUICEFS_BIN:-juicefs}"
PYTHON_BIN="${NOKV_PYTHON_BIN:-python3}"
NOKV_FS_BIN="${NOKV_COMPARE_NOKV_FS_BIN:-}"

PROFILE="${NOKV_COMPARE_PROFILE:-smoke}"
READ_REPEATS="${NOKV_COMPARE_READ_REPEATS:-1}"
FSYNC="${NOKV_COMPARE_FSYNC:-0}"
SYNC_MODE="${NOKV_COMPARE_METADATA_RAFT_SYNC:-none}"
BUILD_RELEASE="${NOKV_COMPARE_BUILD_RELEASE:-1}"
RESULT_CSV="${NOKV_COMPARE_RESULT_CSV:-}"
SEQUENTIAL_MOUNTS="${NOKV_COMPARE_SEQUENTIAL_MOUNTS:-1}"

RUSTFS_ADDRESS="${NOKV_COMPARE_RUSTFS_ADDRESS:-127.0.0.1:9030}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_COMPARE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9031}"
RUSTFS_ENDPOINT="${NOKV_COMPARE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_ACCESS_KEY="${NOKV_COMPARE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_COMPARE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_COMPARE_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NOKV_BUCKET="${NOKV_COMPARE_RUSTFS_BUCKET:-nokv-fuse}"
JUICEFS_BUCKET="${NOKV_COMPARE_JUICEFS_BUCKET:-juicefs-fuse}"
REDIS_PORT="${NOKV_COMPARE_REDIS_PORT:-16430}"
SERVER_ADDRESS="${NOKV_COMPARE_SERVER_ADDRESS:-127.0.0.1:7831}"
META_URL="redis://127.0.0.1:${REDIS_PORT}/1"

WORKDIR="${NOKV_COMPARE_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_COMPARE_KEEP_WORKDIR:-0}"

case "$(uname -s)" in
Darwin)
    DEFAULT_JUICEFS_MOUNT_OPTIONS="noappledouble,noapplexattr"
    ;;
*)
    DEFAULT_JUICEFS_MOUNT_OPTIONS=""
    ;;
esac
JUICEFS_MOUNT_OPTIONS="${NOKV_COMPARE_JUICEFS_MOUNT_OPTIONS:-$DEFAULT_JUICEFS_MOUNT_OPTIONS}"
NOKV_MOUNT_OPTIONS="${NOKV_COMPARE_NOKV_MOUNT_OPTIONS:-}"

case "$PROFILE" in
smoke)
    DEFAULT_DATASET_DIRS=8
    DEFAULT_FILES_PER_DIR=64
    DEFAULT_SAMPLE_BYTES=512
    DEFAULT_CHECKPOINT_BYTES=4096
    DEFAULT_CHECKPOINT_STEPS=32
    ;;
standard)
    DEFAULT_DATASET_DIRS=32
    DEFAULT_FILES_PER_DIR=256
    DEFAULT_SAMPLE_BYTES=$((16 * 1024))
    DEFAULT_CHECKPOINT_BYTES=$((1024 * 1024))
    DEFAULT_CHECKPOINT_STEPS=256
    ;;
long)
    DEFAULT_DATASET_DIRS=64
    DEFAULT_FILES_PER_DIR=1024
    DEFAULT_SAMPLE_BYTES=$((256 * 1024))
    DEFAULT_CHECKPOINT_BYTES=$((8 * 1024 * 1024))
    DEFAULT_CHECKPOINT_STEPS=1024
    ;;
*)
    echo "error: NOKV_COMPARE_PROFILE must be smoke, standard, or long" >&2
    exit 2
    ;;
esac

DATASET_DIRS="${NOKV_COMPARE_DATASET_DIRS:-$DEFAULT_DATASET_DIRS}"
FILES_PER_DIR="${NOKV_COMPARE_FILES_PER_DIR:-$DEFAULT_FILES_PER_DIR}"
SAMPLE_BYTES="${NOKV_COMPARE_SAMPLE_BYTES:-$DEFAULT_SAMPLE_BYTES}"
CHECKPOINT_BYTES="${NOKV_COMPARE_CHECKPOINT_BYTES:-$DEFAULT_CHECKPOINT_BYTES}"
CHECKPOINT_STEPS="${NOKV_COMPARE_CHECKPOINT_STEPS:-$DEFAULT_CHECKPOINT_STEPS}"

RUSTFS_PID=""
REDIS_PID=""
SERVER_PID=""
NOKV_MOUNT_PID=""
OWN_WORKDIR=0
NOKV_MOUNT=""
JUICEFS_MOUNT=""
RUSTFS_LOG=""
REDIS_LOG=""
SERVER_LOG=""
NOKV_MOUNT_LOG=""
JUICEFS_LOG=""

usage() {
    cat <<EOF
Usage: scripts/run-fuse-juicefs-rustfs-comparison.sh

Starts disposable RustFS + Redis, mounts NoKV and JuiceFS, and runs the same
generated dataset/checkpoint workload through both FUSE mountpoints.

Environment:
  NOKV_RUSTFS_BIN                         RustFS binary path/name (default: rustfs)
  NOKV_AWS_BIN                            AWS CLI path/name (default: aws)
  NOKV_REDIS_BIN                          Redis server binary path/name (default: redis-server)
  NOKV_JUICEFS_BIN                        JuiceFS binary path/name (default: juicefs)
  NOKV_COMPARE_NOKV_FS_BIN                nokv binary path (default: target/release/nokv)
  NOKV_COMPARE_BUILD_RELEASE=0            skip cargo release build
  NOKV_COMPARE_PROFILE                    smoke|standard|long (default: smoke)
  NOKV_COMPARE_FSYNC=1                    fsync checkpoint writes in both mounts
  NOKV_COMPARE_METADATA_RAFT_SYNC         NoKV metadata Raft log sync data|none (default: none)
  NOKV_COMPARE_RUSTFS_ADDRESS             RustFS listen address (default: 127.0.0.1:9030)
  NOKV_COMPARE_REDIS_PORT                 Redis port (default: 16430)
  NOKV_COMPARE_SERVER_ADDRESS             NoKV metadata server address (default: 127.0.0.1:7831)
  NOKV_COMPARE_RUSTFS_BUCKET              NoKV bucket (default: nokv-fuse)
  NOKV_COMPARE_JUICEFS_BUCKET             JuiceFS bucket (default: juicefs-fuse)
  NOKV_COMPARE_JUICEFS_MOUNT_OPTIONS      JuiceFS FUSE options; macOS default disables AppleDouble
  NOKV_COMPARE_NOKV_MOUNT_OPTIONS         extra nokv mount args, e.g. "--no-kernel-cache"
  NOKV_COMPARE_RESULT_CSV                 optional path to write the comparison CSV
  NOKV_COMPARE_SEQUENTIAL_MOUNTS=0        keep NoKV and JuiceFS mounted together
  NOKV_COMPARE_KEEP_WORKDIR=1             keep temp data and logs

Shape overrides:
  NOKV_COMPARE_DATASET_DIRS
  NOKV_COMPARE_FILES_PER_DIR
  NOKV_COMPARE_SAMPLE_BYTES
  NOKV_COMPARE_CHECKPOINT_BYTES
  NOKV_COMPARE_CHECKPOINT_STEPS
  NOKV_COMPARE_READ_REPEATS

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

is_mounted() {
    local mountpoint="$1"
    "$PYTHON_BIN" - "$mountpoint" <<'PY'
import os
import sys
sys.exit(0 if os.path.ismount(sys.argv[1]) else 1)
PY
}

unmount_path() {
    local mountpoint="$1"
    if [[ -z "$mountpoint" || ! -d "$mountpoint" ]]; then
        return 0
    fi
    if ! is_mounted "$mountpoint"; then
        return 0
    fi
    if command -v "$JUICEFS_BIN" >/dev/null 2>&1; then
        "$JUICEFS_BIN" umount "$mountpoint" >/dev/null 2>&1 && return 0
    fi
    if command -v fusermount3 >/dev/null 2>&1; then
        fusermount3 -u "$mountpoint" >/dev/null 2>&1 && return 0
    fi
    if command -v fusermount >/dev/null 2>&1; then
        fusermount -u "$mountpoint" >/dev/null 2>&1 && return 0
    fi
    if command -v diskutil >/dev/null 2>&1; then
        diskutil unmount "$mountpoint" >/dev/null 2>&1 && return 0
    fi
    umount "$mountpoint" >/dev/null 2>&1 || true
}

cleanup() {
    local status=$?
    unmount_path "$NOKV_MOUNT" || true
    unmount_path "$JUICEFS_MOUNT" || true
    if [[ -n "$NOKV_MOUNT_PID" ]] && kill -0 "$NOKV_MOUNT_PID" >/dev/null 2>&1; then
        kill "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
        wait "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$REDIS_PID" ]] && kill -0 "$REDIS_PID" >/dev/null 2>&1; then
        kill "$REDIS_PID" >/dev/null 2>&1 || true
        wait "$REDIS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 ]]; then
        for log in "$RUSTFS_LOG" "$REDIS_LOG" "$SERVER_LOG" "$NOKV_MOUNT_LOG" "$JUICEFS_LOG"; do
            if [[ -n "$log" && -f "$log" ]]; then
                echo "---- $(basename "$log") tail ----" >&2
                tail -80 "$log" >&2 || true
                echo "------------------------------" >&2
            fi
        done
    fi
    if [[ "$OWN_WORKDIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORKDIR"
    elif [[ -n "$WORKDIR" ]]; then
        echo "FUSE comparison workdir: $WORKDIR" >&2
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

wait_for_mount() {
    local mountpoint="$1"
    local name="$2"
    local pid="${3:-}"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: $name process exited before mount became ready" >&2
            return 1
        fi
        if is_mounted "$mountpoint"; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for $name mount at $mountpoint" >&2
    return 1
}

run_mount_workload() {
    local system_filter="${1:-both}"
    local emit_header="${2:-1}"
    "$PYTHON_BIN" - \
        "$NOKV_MOUNT" \
        "$JUICEFS_MOUNT" \
        "$PROFILE" \
        "$RUSTFS_ENDPOINT" \
        "$NOKV_BUCKET" \
        "$JUICEFS_BUCKET" \
        "$FSYNC" \
        "$DATASET_DIRS" \
        "$FILES_PER_DIR" \
        "$SAMPLE_BYTES" \
        "$CHECKPOINT_BYTES" \
        "$CHECKPOINT_STEPS" \
        "$READ_REPEATS" \
        "$system_filter" \
        "$emit_header" <<'PY'
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

nokv_mount = Path(sys.argv[1]).resolve()
juicefs_mount = Path(sys.argv[2]).resolve()
profile = sys.argv[3]
endpoint = sys.argv[4]
nokv_bucket = sys.argv[5]
juicefs_bucket = sys.argv[6]
do_fsync = sys.argv[7] == "1"
dataset_dirs = int(sys.argv[8])
files_per_dir = int(sys.argv[9])
sample_bytes = int(sys.argv[10])
checkpoint_bytes = int(sys.argv[11])
checkpoint_steps = int(sys.argv[12])
read_repeats = int(sys.argv[13])
system_filter = sys.argv[14]
emit_header = sys.argv[15] == "1"

def payload(seed: int, length: int) -> bytes:
    return bytes(((seed + offset) % 251 for offset in range(length)))

def write_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)
        if do_fsync:
            handle.flush()
            os.fsync(handle.fileno())

def visible_entries(path: Path):
    return sorted(entry for entry in path.iterdir() if not entry.name.startswith("._"))

def count_appledouble(path: Path) -> int:
    count = 0
    for _, _, files in os.walk(path):
        count += sum(1 for name in files if name.startswith("._"))
    return count

def row(system: str, workload: str, bucket: str, operations: int, seconds: float, bytes_total: int, samples: int, seed_seconds: float, checksum: int, sidecars: int, shape: str) -> str:
    mib = bytes_total / 1024 / 1024
    caveat = (
        "local engineering FUSE comparison; same RustFS endpoint and generated shape; "
        "not an official MLPerf result"
    )
    return (
        f"{system},{profile},{workload},{endpoint},{bucket},"
        f"{'on' if do_fsync else 'off'},"
        f"{operations},{seconds:.6f},{operations / seconds if seconds > 0 else 0:.2f},"
        f"{mib / seconds if seconds > 0 else 0:.2f},"
        f"{samples / seconds if seconds > 0 else 0:.2f},{seed_seconds:.6f},"
        f"{checksum},{sidecars},\"{shape}\",\"{caveat}\""
    )

def run_metadata_create_list(system: str, root: Path, bucket: str) -> str:
    metadata_root = root / "metadata"
    start = time.perf_counter()
    metadata_root.mkdir()
    created = 1
    checksum = 0
    for shard in range(dataset_dirs):
        shard_dir = metadata_root / f"dir-{shard:04d}"
        shard_dir.mkdir()
        created += 1
        for file_index in range(files_per_dir):
            path = shard_dir / f"file-{file_index:05d}.bin"
            with path.open("wb"):
                pass
            created += 1
        entries = visible_entries(shard_dir)
        checksum += len(entries)
    seconds = time.perf_counter() - start
    shape = (
        f"dataset_dirs={dataset_dirs} files_per_dir={files_per_dir} "
        "file_body=metadata-only"
    )
    return row(
        system,
        "metadata_create_list",
        bucket,
        created + dataset_dirs,
        seconds,
        0,
        0,
        0.0,
        checksum,
        count_appledouble(metadata_root),
        shape,
    )

def run_checkpoint_write(system: str, root: Path, bucket: str) -> str:
    checkpoints = root / "checkpoints"
    checkpoints.mkdir()
    write_file(checkpoints / "latest.ckpt", payload(0, checkpoint_bytes))
    start = time.perf_counter()
    checksum = 0
    for step in range(checkpoint_steps):
        stage = checkpoints / f".stage-{step:06d}"
        data = payload(step, checkpoint_bytes)
        write_file(stage, data)
        os.replace(stage, checkpoints / "latest.ckpt")
        checksum += data[0] if data else 0
    seconds = time.perf_counter() - start
    shape = (
        f"checkpoint_steps={checkpoint_steps} checkpoint_bytes={checkpoint_bytes}"
    )
    return row(
        system,
        "checkpoint_write",
        bucket,
        checkpoint_steps * 2,
        seconds,
        checkpoint_steps * checkpoint_bytes,
        0,
        0.0,
        checksum,
        count_appledouble(checkpoints),
        shape,
    )

def seed_training_dataset(dataset: Path) -> float:
    start = time.perf_counter()
    dataset.mkdir()
    for shard in range(dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        shard_dir.mkdir()
        for file_index in range(files_per_dir):
            write_file(
                shard_dir / f"sample-{file_index:05d}.bin",
                payload(shard * 31 + file_index * 17, sample_bytes),
            )
    return time.perf_counter() - start

def run_training_read(system: str, root: Path, bucket: str) -> str:
    dataset = root / "dataset"
    seed_seconds = seed_training_dataset(dataset)
    start = time.perf_counter()
    checksum = 0
    samples = 0
    for _ in range(read_repeats):
        for shard in range(dataset_dirs):
            shard_dir = dataset / f"shard-{shard:04d}"
            for entry in visible_entries(shard_dir):
                try:
                    data = entry.read_bytes()
                except OSError as err:
                    raise RuntimeError(f"{system} training read failed for {entry}: {err}") from err
                checksum += len(data)
                samples += 1
    seconds = time.perf_counter() - start
    shape = (
        f"dataset_dirs={dataset_dirs} files_per_dir={files_per_dir} "
        f"sample_bytes={sample_bytes} read_repeats={read_repeats}"
    )
    return row(
        system,
        "training_read",
        bucket,
        samples,
        seconds,
        samples * sample_bytes,
        samples,
        seed_seconds,
        checksum,
        count_appledouble(dataset),
        shape,
    )

def run_one(system: str, mount: Path, bucket: str) -> None:
    root = Path(tempfile.mkdtemp(prefix=f"nokv-fuse-compare-{system}-", dir=mount))
    try:
        print(run_metadata_create_list(system, root, bucket), flush=True)
        print(run_checkpoint_write(system, root, bucket), flush=True)
        print(run_training_read(system, root, bucket), flush=True)
    finally:
        shutil.rmtree(root, ignore_errors=True)

if emit_header:
    print(
        "system,profile,workload,endpoint,bucket,fsync,operations,seconds,"
        "ops_per_second,MiB_per_second,samples_per_second,seed_seconds,checksum,"
        "sidecar_files,shape,caveat"
    )
if system_filter in ("both", "nokv-fuse"):
    run_one("nokv-fuse", nokv_mount, nokv_bucket)
if system_filter in ("both", "juicefs-fuse"):
    run_one("juicefs-fuse", juicefs_mount, juicefs_bucket)
PY
}

require_cmd "$RUSTFS_BIN" "brew tap rustfs/homebrew-tap && brew install rustfs"
require_cmd "$AWS_BIN" "brew install awscli"
require_cmd "$REDIS_BIN" "brew install redis"
require_cmd "$JUICEFS_BIN" "brew install juicefs"
require_cmd "$PYTHON_BIN" "brew install python"
require_cmd curl "brew install curl"

if [[ -z "$NOKV_FS_BIN" ]]; then
    NOKV_FS_BIN="$ROOT_DIR/target/release/nokv"
fi
if [[ "$BUILD_RELEASE" == "1" || ! -x "$NOKV_FS_BIN" ]]; then
    require_cmd cargo "install Rust from https://rustup.rs/"
    (cd "$ROOT_DIR" && cargo build --release -p nokv --bin nokv)
fi
if [[ ! -x "$NOKV_FS_BIN" ]]; then
    echo "error: nokv binary is not executable: $NOKV_FS_BIN" >&2
    exit 2
fi

if [[ -z "$WORKDIR" ]]; then
    WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fuse-juicefs.XXXXXX")"
    OWN_WORKDIR=1
else
    mkdir -p "$WORKDIR"
fi

RUSTFS_DATA_DIR="$WORKDIR/rustfs-data"
REDIS_DIR="$WORKDIR/redis"
NOKV_META_DIR="$WORKDIR/nokv-meta"
NOKV_MOUNT="$WORKDIR/nokv-mount"
JUICEFS_MOUNT="$WORKDIR/juicefs-mount"
RUSTFS_LOG="$WORKDIR/rustfs.log"
REDIS_LOG="$WORKDIR/redis.log"
SERVER_LOG="$WORKDIR/nokv-server.log"
NOKV_MOUNT_LOG="$WORKDIR/nokv-mount.log"
JUICEFS_LOG="$WORKDIR/juicefs.log"
mkdir -p "$RUSTFS_DATA_DIR" "$REDIS_DIR" "$NOKV_META_DIR" "$NOKV_MOUNT" "$JUICEFS_MOUNT"

trap cleanup EXIT INT TERM

echo "Starting RustFS endpoint=$RUSTFS_ENDPOINT buckets=$NOKV_BUCKET,$JUICEFS_BUCKET" >&2
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BIN" server \
    --address "$RUSTFS_ADDRESS" \
    --console-enable \
    --console-address "$RUSTFS_CONSOLE_ADDRESS" \
    --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
    "$RUSTFS_DATA_DIR" >"$RUSTFS_LOG" 2>&1 &
RUSTFS_PID=$!

echo "Starting Redis metadata backend port=$REDIS_PORT" >&2
"$REDIS_BIN" \
    --bind 127.0.0.1 \
    --port "$REDIS_PORT" \
    --dir "$REDIS_DIR" \
    --save "" \
    --appendonly no >"$REDIS_LOG" 2>&1 &
REDIS_PID=$!

wait_for_rustfs
wait_for_tcp 127.0.0.1 "$REDIS_PORT" Redis
create_bucket "$NOKV_BUCKET"
create_bucket "$JUICEFS_BUCKET"

echo "Starting NoKV metadata server at $SERVER_ADDRESS sync=$SYNC_MODE" >&2
"$NOKV_FS_BIN" \
    --server-bind "$SERVER_ADDRESS" \
    --meta "$NOKV_META_DIR" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$NOKV_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    --metadata-raft-log-sync "$SYNC_MODE" \
    --uid "$(id -u)" \
    --gid "$(id -g)" \
    serve >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_for_metadata_server

echo "Mounting NoKV at $NOKV_MOUNT" >&2
NOKV_MOUNT_ARGS=()
if [[ -n "$NOKV_MOUNT_OPTIONS" ]]; then
    read -r -a NOKV_MOUNT_ARGS <<<"$NOKV_MOUNT_OPTIONS"
fi
"$NOKV_FS_BIN" \
    --server-bind "$SERVER_ADDRESS" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$NOKV_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    --uid "$(id -u)" \
    --gid "$(id -g)" \
    mount "${NOKV_MOUNT_ARGS[@]}" "$NOKV_MOUNT" >"$NOKV_MOUNT_LOG" 2>&1 &
NOKV_MOUNT_PID=$!
wait_for_mount "$NOKV_MOUNT" NoKV "$NOKV_MOUNT_PID"

emit_summary() {
    local csv="$1"
    "$PYTHON_BIN" - "$csv" >&2 <<'PY'
import csv
import sys

path = sys.argv[1]
with open(path, newline="") as handle:
    rows = list(csv.DictReader(handle))

if rows:
    print("FUSE comparison summary:")
    for row in rows:
        print(
            f"  {row['workload']} {row['system']}: "
            f"{row['ops_per_second']} ops/s, "
            f"{row['MiB_per_second']} MiB/s, "
            f"{row['samples_per_second']} samples/s"
        )
PY
}

run_and_record() {
    local system_filter="$1"
    local emit_header="$2"
    if [[ -n "$RESULT_CSV" ]]; then
        mkdir -p "$(dirname "$RESULT_CSV")"
        if [[ "$emit_header" == "1" ]]; then
            run_mount_workload "$system_filter" "$emit_header" | tee "$RESULT_CSV"
        else
            run_mount_workload "$system_filter" "$emit_header" | tee -a "$RESULT_CSV"
        fi
    else
        run_mount_workload "$system_filter" "$emit_header"
    fi
}

echo "Running FUSE comparison profile=$PROFILE fsync=$FSYNC" >&2
if [[ "$SEQUENTIAL_MOUNTS" == "1" ]]; then
    run_and_record "nokv-fuse" "1"
    unmount_path "$NOKV_MOUNT" || true
    if [[ -n "$NOKV_MOUNT_PID" ]] && kill -0 "$NOKV_MOUNT_PID" >/dev/null 2>&1; then
        kill "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
        wait "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
    fi
    NOKV_MOUNT_PID=""

    echo "Formatting JuiceFS bucket=$JUICEFS_BUCKET metadata=$META_URL" >&2
    "$JUICEFS_BIN" format \
        --storage s3 \
        --bucket "$RUSTFS_ENDPOINT/$JUICEFS_BUCKET" \
        --access-key "$RUSTFS_ACCESS_KEY" \
        --secret-key "$RUSTFS_SECRET_KEY" \
        --trash-days 0 \
        "$META_URL" nokv-fuse-juicefs-compare >"$JUICEFS_LOG" 2>&1

    echo "Mounting JuiceFS at $JUICEFS_MOUNT" >&2
    if [[ -n "$JUICEFS_MOUNT_OPTIONS" ]]; then
        "$JUICEFS_BIN" mount -d -o "$JUICEFS_MOUNT_OPTIONS" \
            "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    else
        "$JUICEFS_BIN" mount -d "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    fi
    wait_for_mount "$JUICEFS_MOUNT" JuiceFS
    run_and_record "juicefs-fuse" "0"
else
    echo "Formatting JuiceFS bucket=$JUICEFS_BUCKET metadata=$META_URL" >&2
    "$JUICEFS_BIN" format \
        --storage s3 \
        --bucket "$RUSTFS_ENDPOINT/$JUICEFS_BUCKET" \
        --access-key "$RUSTFS_ACCESS_KEY" \
        --secret-key "$RUSTFS_SECRET_KEY" \
        --trash-days 0 \
        "$META_URL" nokv-fuse-juicefs-compare >"$JUICEFS_LOG" 2>&1

    echo "Mounting JuiceFS at $JUICEFS_MOUNT" >&2
    if [[ -n "$JUICEFS_MOUNT_OPTIONS" ]]; then
        "$JUICEFS_BIN" mount -d -o "$JUICEFS_MOUNT_OPTIONS" \
            "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    else
        "$JUICEFS_BIN" mount -d "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    fi
    wait_for_mount "$JUICEFS_MOUNT" JuiceFS
    run_and_record "both" "1"
fi

if [[ -n "$RESULT_CSV" ]]; then
    emit_summary "$RESULT_CSV"
fi
