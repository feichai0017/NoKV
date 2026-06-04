#!/usr/bin/env bash
#
# Run a local RustFS-backed NoKV-FS FUSE smoke test.
#
# The script starts a temporary RustFS process, creates the configured S3
# bucket, mounts NoKV-FS, runs a small POSIX smoke suite through the mounted
# filesystem, and then unmounts and cleans up.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
PYTHON_BIN="${NOKV_PYTHON_BIN:-python3}"

RUSTFS_ADDRESS="${NOKV_FUSE_SMOKE_RUSTFS_ADDRESS:-127.0.0.1:9010}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_FUSE_SMOKE_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9011}"
RUSTFS_ENDPOINT="${NOKV_FUSE_SMOKE_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_FUSE_SMOKE_RUSTFS_BUCKET:-nokv-fuse-smoke}"
RUSTFS_ACCESS_KEY="${NOKV_FUSE_SMOKE_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_FUSE_SMOKE_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_FUSE_SMOKE_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NOKV_FS_BIN="${NOKV_FUSE_SMOKE_NOKV_FS_BIN:-}"
SKIP_BUILD="${NOKV_FUSE_SMOKE_SKIP_BUILD:-0}"
KEEP_WORKDIR="${NOKV_FUSE_SMOKE_KEEP:-0}"
WORK_DIR="${NOKV_FUSE_SMOKE_WORKDIR:-}"

RUSTFS_PID=""
MOUNT_PID=""
OWN_WORK_DIR=0
MOUNT_DIR=""
META_DIR=""
RUSTFS_DATA_DIR=""
RUSTFS_LOG=""
MOUNT_LOG=""

usage() {
    cat <<EOF
Usage: scripts/run-fuse-smoke.sh

Environment:
  NOKV_FUSE_SMOKE_WORKDIR              keep/use a specific work directory
  NOKV_FUSE_SMOKE_KEEP=1               keep the temporary work directory
  NOKV_FUSE_SMOKE_NOKV_FS_BIN          use an existing nokv-fs binary
  NOKV_FUSE_SMOKE_SKIP_BUILD=1         do not build nokv-fs when a binary is set
  NOKV_FUSE_SMOKE_RUSTFS_ADDRESS       RustFS listen address (default: 127.0.0.1:9010)
  NOKV_FUSE_SMOKE_RUSTFS_BUCKET        bucket name (default: nokv-fuse-smoke)

The smoke covers mkdir, file write/read, file fsync, directory fsync, rename,
readdir, truncate, symlink/readlink, xattr roundtrip, access(2), rm, and rmdir
through the mounted FUSE filesystem.
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

is_mounted() {
    "$PYTHON_BIN" - "$MOUNT_DIR" <<'PY'
import os
import sys
sys.exit(0 if os.path.ismount(sys.argv[1]) else 1)
PY
}

unmount_mountpoint() {
    if [[ -z "$MOUNT_DIR" || ! -d "$MOUNT_DIR" ]]; then
        return 0
    fi
    if ! is_mounted; then
        return 0
    fi
    if command -v fusermount3 >/dev/null 2>&1; then
        fusermount3 -u "$MOUNT_DIR" >/dev/null 2>&1 && return 0
    fi
    if command -v fusermount >/dev/null 2>&1; then
        fusermount -u "$MOUNT_DIR" >/dev/null 2>&1 && return 0
    fi
    if command -v diskutil >/dev/null 2>&1; then
        diskutil unmount "$MOUNT_DIR" >/dev/null 2>&1 && return 0
    fi
    umount "$MOUNT_DIR" >/dev/null 2>&1 || true
}

cleanup() {
    local status=$?
    unmount_mountpoint || true
    if [[ -n "$MOUNT_PID" ]] && kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
        kill "$MOUNT_PID" >/dev/null 2>&1 || true
        wait "$MOUNT_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 ]]; then
        if [[ -n "$MOUNT_LOG" && -f "$MOUNT_LOG" ]]; then
            echo "---- NoKV-FS mount log tail ----" >&2
            tail -80 "$MOUNT_LOG" >&2 || true
            echo "--------------------------------" >&2
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
        echo "NoKV-FS FUSE smoke workdir: $WORK_DIR" >&2
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

wait_for_mount() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if ! kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
            echo "error: nokv-fs mount process exited before mount became ready" >&2
            return 1
        fi
        if is_mounted; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for NoKV-FS mount at $MOUNT_DIR" >&2
    return 1
}

run_python_smoke() {
    "$PYTHON_BIN" - "$MOUNT_DIR" <<'PY'
import errno
import os
import shutil
import subprocess
import sys

root = sys.argv[1]
run_dir = os.path.join(root, "workspace", "run-1")
os.makedirs(run_dir, exist_ok=True)

checkpoint = os.path.join(run_dir, "checkpoint.bin")
with open(checkpoint, "wb") as fh:
    fh.write(b"hello nokv-fs\n")
    fh.flush()
    os.fsync(fh.fileno())

with open(checkpoint, "rb") as fh:
    assert fh.read() == b"hello nokv-fs\n"

dir_fd = os.open(run_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(dir_fd)
finally:
    os.close(dir_fd)

renamed_dir = os.path.join(root, "workspace", "renamed-run")
os.rename(run_dir, renamed_dir)
checkpoint = os.path.join(renamed_dir, "checkpoint.bin")
assert os.path.exists(checkpoint)

for index in range(24):
    with open(os.path.join(renamed_dir, f"sample-{index:04}.txt"), "wb") as fh:
        fh.write(f"sample-{index}\n".encode())

listed = sorted(os.listdir(renamed_dir))
assert "checkpoint.bin" in listed
assert "sample-0023.txt" in listed

with open(checkpoint, "r+b") as fh:
    fh.truncate(5)
    fh.flush()
    os.fsync(fh.fileno())
with open(checkpoint, "rb") as fh:
    assert fh.read() == b"hello"

link_path = os.path.join(renamed_dir, "latest")
os.symlink("checkpoint.bin", link_path)
assert os.readlink(link_path) == "checkpoint.bin"

if (
    hasattr(os, "setxattr")
    and hasattr(os, "getxattr")
    and hasattr(os, "listxattr")
    and hasattr(os, "removexattr")
):
    os.setxattr(checkpoint, "user.nokvfs-smoke", b"value")
    assert os.getxattr(checkpoint, "user.nokvfs-smoke") == b"value"
    assert "user.nokvfs-smoke" in os.listxattr(checkpoint)
    os.removexattr(checkpoint, "user.nokvfs-smoke")
    try:
        os.getxattr(checkpoint, "user.nokvfs-smoke")
    except OSError as err:
        if err.errno not in {getattr(errno, "ENODATA", 61), getattr(errno, "ENOATTR", 93)}:
            raise
    else:
        raise AssertionError("removed xattr was still readable")
elif shutil.which("xattr"):
    subprocess.run(["xattr", "-w", "user.nokvfs-smoke", "value", checkpoint], check=True)
    value = subprocess.check_output(["xattr", "-p", "user.nokvfs-smoke", checkpoint])
    assert value.rstrip(b"\n") == b"value"
    listed = subprocess.check_output(["xattr", checkpoint])
    assert b"user.nokvfs-smoke" in listed.splitlines()
    subprocess.run(["xattr", "-d", "user.nokvfs-smoke", checkpoint], check=True)
else:
    print("warning: python xattr APIs are unavailable; skipping xattr smoke", file=sys.stderr)

if os.geteuid() != 0:
    no_access = os.path.join(renamed_dir, "no-access.txt")
    with open(no_access, "wb") as fh:
        fh.write(b"private")
    os.chmod(no_access, 0)
    try:
        assert not os.access(no_access, os.R_OK)
    finally:
        os.chmod(no_access, 0o644)
        os.unlink(no_access)

os.unlink(link_path)
os.unlink(checkpoint)
for index in range(24):
    os.unlink(os.path.join(renamed_dir, f"sample-{index:04}.txt"))
os.rmdir(renamed_dir)
os.rmdir(os.path.join(root, "workspace"))
PY
}

require_cmd "$RUSTFS_BIN"
require_cmd "$AWS_BIN"
require_cmd "$PYTHON_BIN"
require_cmd curl

if [[ -z "$NOKV_FS_BIN" ]]; then
    if [[ "$SKIP_BUILD" == "1" ]]; then
        echo "error: NOKV_FUSE_SMOKE_NOKV_FS_BIN is required when NOKV_FUSE_SMOKE_SKIP_BUILD=1" >&2
        exit 2
    fi
    (cd "$ROOT_DIR" && cargo build -p nokvfs-cli --bin nokv-fs)
    NOKV_FS_BIN="$ROOT_DIR/target/debug/nokv-fs"
elif [[ "$SKIP_BUILD" != "1" ]]; then
    (cd "$ROOT_DIR" && cargo build -p nokvfs-cli --bin nokv-fs)
fi

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fuse-smoke.XXXXXX")"
    OWN_WORK_DIR=1
else
    mkdir -p "$WORK_DIR"
fi

MOUNT_DIR="$WORK_DIR/mnt"
META_DIR="$WORK_DIR/meta"
RUSTFS_DATA_DIR="$WORK_DIR/rustfs-data"
RUSTFS_LOG="$WORK_DIR/rustfs.log"
MOUNT_LOG="$WORK_DIR/nokv-fs-mount.log"
mkdir -p "$MOUNT_DIR" "$META_DIR" "$RUSTFS_DATA_DIR"

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

echo "Mounting NoKV-FS at $MOUNT_DIR"
"$NOKV_FS_BIN" \
    --meta "$META_DIR" \
    --object-backend rustfs \
    --s3-endpoint "$RUSTFS_ENDPOINT" \
    --s3-bucket "$RUSTFS_BUCKET" \
    --s3-access-key-id "$RUSTFS_ACCESS_KEY" \
    --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
    --uid "$(id -u)" \
    --gid "$(id -g)" \
    mount "$MOUNT_DIR" >"$MOUNT_LOG" 2>&1 &
MOUNT_PID=$!

wait_for_mount
run_python_smoke

echo "NoKV-FS FUSE smoke passed"
