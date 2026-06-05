#!/usr/bin/env bash
#
# Run a local NoKV-FS AI-training benchmark and, optionally, a same-shape
# filesystem comparison against an existing JuiceFS mount.
#
# This is not an MLCommons submission harness. It is a controlled local
# same-shape workload for comparing NoKV-FS and a mounted filesystem with the
# same generated dataset/checkpoint shape.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILE="${NOKV_COMPARE_PROFILE:-smoke}"
OBJECT_CONCURRENCY="${NOKV_COMPARE_OBJECT_CONCURRENCY:-8}"
READ_REPEATS="${NOKV_COMPARE_READ_REPEATS:-1}"
BLOCK_CACHE="${NOKV_COMPARE_BLOCK_CACHE:-on}"
WORKLOAD="${NOKV_COMPARE_WORKLOAD:-mlperf-dlio}"
SYNC_MODE="${NOKV_COMPARE_METADATA_RAFT_SYNC:-none}"
JUICEFS_MOUNT="${JUICEFS_MOUNT:-}"
JUICEFS_FSYNC="${JUICEFS_FSYNC:-0}"
RUSTFS_ENDPOINT="${NOKV_COMPARE_RUSTFS_ENDPOINT:-}"
RUSTFS_BUCKET="${NOKV_COMPARE_RUSTFS_BUCKET:-nokv}"
RUSTFS_ACCESS_KEY="${NOKV_COMPARE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_COMPARE_RUSTFS_SECRET_KEY:-rustfsadmin}"

case "$PROFILE" in
smoke)
    DEFAULT_DATASET_DIRS=8
    DEFAULT_FILES_PER_DIR=64
    DEFAULT_SAMPLE_BYTES=512
    DEFAULT_CHECKPOINT_BYTES=4096
    DEFAULT_CHECKPOINTS=128
    ;;
standard)
    DEFAULT_DATASET_DIRS=32
    DEFAULT_FILES_PER_DIR=256
    DEFAULT_SAMPLE_BYTES=$((16 * 1024))
    DEFAULT_CHECKPOINT_BYTES=$((1024 * 1024))
    DEFAULT_CHECKPOINTS=1024
    ;;
long)
    DEFAULT_DATASET_DIRS=64
    DEFAULT_FILES_PER_DIR=1024
    DEFAULT_SAMPLE_BYTES=$((256 * 1024))
    DEFAULT_CHECKPOINT_BYTES=$((8 * 1024 * 1024))
    DEFAULT_CHECKPOINTS=4096
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
CHECKPOINTS="${NOKV_COMPARE_CHECKPOINTS:-$DEFAULT_CHECKPOINTS}"

usage() {
    cat <<EOF
Usage: scripts/run-training-comparison.sh

Runs NoKV-FS through scripts/run-rustfs-e2e.sh. If JUICEFS_MOUNT is set, also
runs a same-shape generated dataset/checkpoint workload inside that mounted
filesystem.

Environment:
  NOKV_COMPARE_PROFILE              smoke|standard|long (default: smoke)
  NOKV_COMPARE_WORKLOAD             NoKV bench workload (default: mlperf-dlio)
  NOKV_COMPARE_OBJECT_CONCURRENCY   NoKV object concurrency (default: 8)
  NOKV_COMPARE_READ_REPEATS         read repeats for both paths (default: 1)
  NOKV_COMPARE_BLOCK_CACHE          NoKV block cache on|off (default: on)
  NOKV_COMPARE_METADATA_RAFT_SYNC   data|none (default: none)
  NOKV_COMPARE_RUSTFS_ENDPOINT      existing RustFS/S3 endpoint for NoKV-FS
  NOKV_COMPARE_RUSTFS_BUCKET        NoKV-FS bucket at that endpoint (default: nokv)
  NOKV_COMPARE_RUSTFS_ACCESS_KEY    access key for existing endpoint
  NOKV_COMPARE_RUSTFS_SECRET_KEY    secret key for existing endpoint
  NOKV_COMPARE_JUICEFS_BUCKET       JuiceFS bucket name for reporting
  JUICEFS_MOUNT                     existing JuiceFS mount path for comparison
  JUICEFS_FSYNC                     1 to fsync JuiceFS checkpoint writes

Shape overrides:
  NOKV_COMPARE_DATASET_DIRS
  NOKV_COMPARE_FILES_PER_DIR
  NOKV_COMPARE_SAMPLE_BYTES
  NOKV_COMPARE_CHECKPOINT_BYTES
  NOKV_COMPARE_CHECKPOINTS
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

echo "==> NoKV-FS $WORKLOAD profile=$PROFILE sync=$SYNC_MODE"
if [[ -n "$RUSTFS_ENDPOINT" ]]; then
    (
        cd "$ROOT_DIR"
        cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
            --profile "$PROFILE" \
            --workload "$WORKLOAD" \
            --object-backend rustfs \
            --s3-bucket "$RUSTFS_BUCKET" \
            --s3-endpoint "$RUSTFS_ENDPOINT" \
            --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
            --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
            --object-concurrency "$OBJECT_CONCURRENCY" \
            --read-repeats "$READ_REPEATS" \
            --block-cache "$BLOCK_CACHE" \
            --metadata-raft-log-sync "$SYNC_MODE" \
            --sample-bytes "$SAMPLE_BYTES" \
            --checkpoint-bytes "$CHECKPOINT_BYTES"
    )
else
    NOKV_E2E_PROFILE="$PROFILE" \
        NOKV_E2E_WORKLOAD="$WORKLOAD" \
        NOKV_E2E_OBJECT_CONCURRENCY="$OBJECT_CONCURRENCY" \
        NOKV_E2E_READ_REPEATS="$READ_REPEATS" \
        NOKV_E2E_BLOCK_CACHE="$BLOCK_CACHE" \
        "$ROOT_DIR/scripts/run-rustfs-e2e.sh" \
        --metadata-raft-log-sync "$SYNC_MODE" \
        --sample-bytes "$SAMPLE_BYTES" \
        --checkpoint-bytes "$CHECKPOINT_BYTES"
fi

if [[ -z "$JUICEFS_MOUNT" ]]; then
    echo "JUICEFS_MOUNT is not set; skipped JuiceFS same-shape comparison." >&2
    exit 0
fi

require_cmd python3

if [[ ! -d "$JUICEFS_MOUNT" ]]; then
    echo "error: JUICEFS_MOUNT is not a directory: $JUICEFS_MOUNT" >&2
    exit 2
fi

echo "==> JuiceFS same-shape workload mount=$JUICEFS_MOUNT profile=$PROFILE"
NOKV_COMPARE_PROFILE="$PROFILE" \
NOKV_COMPARE_RUSTFS_ENDPOINT="${RUSTFS_ENDPOINT:-existing-mount}" \
NOKV_COMPARE_JUICEFS_BUCKET="${NOKV_COMPARE_JUICEFS_BUCKET:-existing-mount}" \
JUICEFS_MOUNT="$JUICEFS_MOUNT" \
JUICEFS_FSYNC="$JUICEFS_FSYNC" \
DATASET_DIRS="$DATASET_DIRS" \
FILES_PER_DIR="$FILES_PER_DIR" \
SAMPLE_BYTES="$SAMPLE_BYTES" \
CHECKPOINT_BYTES="$CHECKPOINT_BYTES" \
CHECKPOINTS="$CHECKPOINTS" \
READ_REPEATS="$READ_REPEATS" \
python3 <<'PY'
import os
import shutil
import tempfile
import time
from pathlib import Path

mount = Path(os.environ["JUICEFS_MOUNT"]).resolve()
dataset_dirs = int(os.environ["DATASET_DIRS"])
files_per_dir = int(os.environ["FILES_PER_DIR"])
sample_bytes = int(os.environ["SAMPLE_BYTES"])
checkpoint_bytes = int(os.environ["CHECKPOINT_BYTES"])
checkpoints = int(os.environ["CHECKPOINTS"])
read_repeats = int(os.environ["READ_REPEATS"])
do_fsync = os.environ.get("JUICEFS_FSYNC") == "1"

root = Path(tempfile.mkdtemp(prefix="nokv-juicefs-compare-", dir=mount))

def payload(seed: int, length: int) -> bytes:
    return bytes(((seed + offset) % 251 for offset in range(length)))

def write_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)
        if do_fsync:
            handle.flush()
            os.fsync(handle.fileno())

try:
    dataset = root / "dataset"
    checkpoints_dir = root / "checkpoints"
    dataset.mkdir()
    checkpoints_dir.mkdir()
    for shard in range(dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        shard_dir.mkdir()
        for file_index in range(files_per_dir):
            write_file(
                shard_dir / f"sample-{file_index:05d}.bin",
                payload(shard * 31 + file_index * 17, sample_bytes),
            )
    write_file(checkpoints_dir / "latest.ckpt", payload(0, checkpoint_bytes))

    checkpoint_steps = max(checkpoints, 1) // 4
    start = time.perf_counter()
    checksum = 0
    for shard in range(dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        entries = sorted(shard_dir.iterdir())
        checksum += len(entries)
        if entries:
            first = entries[0]
            for _ in range(read_repeats):
                checksum += len(first.read_bytes())
    for step in range(checkpoint_steps):
        stage = checkpoints_dir / f".stage-{step:06d}"
        write_file(stage, payload(step, checkpoint_bytes))
        os.replace(stage, checkpoints_dir / "latest.ckpt")
    seconds = time.perf_counter() - start
    bytes_total = dataset_dirs * sample_bytes * read_repeats + checkpoint_steps * checkpoint_bytes
    samples = dataset_dirs * read_repeats
    operations = dataset_dirs * (1 + read_repeats) + checkpoint_steps * 2
    mib = bytes_total / 1024 / 1024
    print("system,profile,workload,endpoint,bucket,fsync,operations,seconds,ops_per_second,mb_per_second,samples_per_second,object_puts,object_gets,checksum,shape,caveat")
    print(
        "juicefs,"
        f"{os.environ.get('NOKV_COMPARE_PROFILE', 'smoke')},same_shape_dlio,"
        f"{os.environ.get('NOKV_COMPARE_RUSTFS_ENDPOINT', 'existing-mount')},"
        f"{os.environ.get('NOKV_COMPARE_JUICEFS_BUCKET', 'existing-mount')},"
        f"{'on' if do_fsync else 'off'},"
        f"{operations},{seconds:.6f},{operations / seconds:.2f},{mib / seconds:.2f},"
        f"{samples / seconds:.2f},unknown,unknown,{checksum},"
        f"\"dataset_dirs={dataset_dirs} files_per_dir={files_per_dir} sample_bytes={sample_bytes} "
        f"checkpoint_steps={checkpoint_steps} checkpoint_bytes={checkpoint_bytes}\","
        f"\"existing JuiceFS mount, fsync={'on' if do_fsync else 'off'}, same generated shape as NoKV-FS local bench\""
    )
finally:
    shutil.rmtree(root, ignore_errors=True)
PY
