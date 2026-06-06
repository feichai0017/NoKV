#!/usr/bin/env bash
#
# Run JuiceFS-style compatibility gates against an already mounted NoKV
# FUSE mount.
#
# This script intentionally does not start NoKV. It is for external POSIX
# suites that must exercise the mounted filesystem boundary, similar to the
# pjdfstest/LTP/random-test layers used by mature FUSE filesystems.

set -euo pipefail

PYTHON_BIN="${NOKV_PYTHON_BIN:-python3}"
MOUNT_DIR="${NOKV_FUSE_MOUNT:-}"
TESTS="${NOKV_FUSE_COMPAT_TESTS:-basic xattr}"
WORK_DIR="${NOKV_FUSE_COMPAT_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_FUSE_COMPAT_KEEP:-0}"
PJDFSTEST_DIR="${NOKV_PJDFSTEST_DIR:-}"
PJDFSTEST_REPO="${NOKV_PJDFSTEST_REPO:-https://github.com/pjd/pjdfstest.git}"
LTP_RUNLTP="${NOKV_LTP_RUNLTP:-}"

OWN_WORK_DIR=0
TARGET_PJDFSTEST_DIR=""

usage() {
    cat <<EOF
Usage: NOKV_FUSE_MOUNT=/mnt/nokv scripts/run-fuse-compat-gate.sh

Runs compatibility checks against an already mounted NoKV FUSE mount.

Environment:
  NOKV_FUSE_MOUNT                 required mountpoint
  NOKV_FUSE_COMPAT_TESTS          space-separated tests (default: "basic xattr")
                                  available: basic xattr pjdfstest ltp
  NOKV_FUSE_COMPAT_WORKDIR        temporary work directory
  NOKV_FUSE_COMPAT_KEEP=1         keep temporary work directory
  NOKV_PJDFSTEST_DIR              existing pjdfstest checkout
  NOKV_PJDFSTEST_REPO             pjdfstest repo to clone when needed
  NOKV_LTP_RUNLTP                 path to runltp, or command name if in PATH

The external pjdfstest and LTP suites are expected to expose unsupported POSIX
semantics until NoKV reaches the full POSIX gate. They are not part of the
default smoke suite.
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

ensure_mount() {
    if [[ -z "$MOUNT_DIR" ]]; then
        echo "error: NOKV_FUSE_MOUNT is required" >&2
        exit 2
    fi
    if [[ ! -d "$MOUNT_DIR" ]]; then
        echo "error: mountpoint does not exist: $MOUNT_DIR" >&2
        exit 2
    fi
    "$PYTHON_BIN" - "$MOUNT_DIR" <<'PY'
import os
import sys

mount = sys.argv[1]
if not os.path.ismount(mount):
    print(f"error: not a mounted filesystem: {mount}", file=sys.stderr)
    sys.exit(2)
PY
}

cleanup() {
    local status=$?
    if [[ "$OWN_WORK_DIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORK_DIR"
    elif [[ -n "$WORK_DIR" ]]; then
        echo "NoKV compatibility workdir: $WORK_DIR" >&2
    fi
    exit "$status"
}

prepare_workdir() {
    if [[ -z "$WORK_DIR" ]]; then
        WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fuse-compat.XXXXXX")"
        OWN_WORK_DIR=1
    else
        mkdir -p "$WORK_DIR"
    fi
}

run_basic() {
    echo "==> fuse-compat: basic namespace/data smoke"
    "$PYTHON_BIN" - "$MOUNT_DIR" <<'PY'
import os
import shutil
import sys

root = sys.argv[1]
base = os.path.join(root, "nokv-compat-basic")
shutil.rmtree(base, ignore_errors=True)
os.makedirs(base)

path = os.path.join(base, "file.bin")
with open(path, "wb") as fh:
    fh.write(b"nokv\n")
    fh.flush()
    os.fsync(fh.fileno())

with open(path, "rb") as fh:
    assert fh.read() == b"nokv\n"

renamed = os.path.join(base, "renamed.bin")
os.rename(path, renamed)
assert sorted(os.listdir(base)) == ["renamed.bin"]

with open(renamed, "r+b") as fh:
    fh.truncate(4)
    fh.flush()
    os.fsync(fh.fileno())
with open(renamed, "rb") as fh:
    assert fh.read() == b"nokv"

with open(renamed, "r+b") as fh:
    if hasattr(os, "SEEK_DATA") and hasattr(os, "SEEK_HOLE"):
        assert os.lseek(fh.fileno(), 0, os.SEEK_DATA) == 0
        assert os.lseek(fh.fileno(), 0, os.SEEK_HOLE) == 4
    if hasattr(os, "posix_fallocate"):
        os.posix_fallocate(fh.fileno(), 0, 4096)
        assert os.fstat(fh.fileno()).st_size == 4096
        fh.seek(4)
        assert fh.read(4) == b"\0\0\0\0"

if hasattr(os, "copy_file_range"):
    copied_path = os.path.join(base, "copied.bin")
    with open(renamed, "rb") as src, open(copied_path, "wb") as dst:
        copied = os.copy_file_range(src.fileno(), dst.fileno(), 4)
        assert copied == 4
        dst.flush()
        os.fsync(dst.fileno())
    with open(copied_path, "rb") as fh:
        assert fh.read() == b"nokv"
    os.unlink(copied_path)

link = os.path.join(base, "latest")
os.symlink("renamed.bin", link)
assert os.readlink(link) == "renamed.bin"

os.unlink(link)
os.unlink(renamed)
os.rmdir(base)
PY
}

run_xattr() {
    echo "==> fuse-compat: xattr smoke"
    "$PYTHON_BIN" - "$MOUNT_DIR" <<'PY'
import errno
import os
import shutil
import sys

root = sys.argv[1]
base = os.path.join(root, "nokv-compat-xattr")
shutil.rmtree(base, ignore_errors=True)
os.makedirs(base)
path = os.path.join(base, "file")
with open(path, "wb") as fh:
    fh.write(b"xattr")

if not all(hasattr(os, name) for name in ("setxattr", "getxattr", "listxattr", "removexattr")):
    print("warning: python xattr APIs are unavailable; skipping xattr smoke", file=sys.stderr)
    shutil.rmtree(base, ignore_errors=True)
    sys.exit(0)

os.setxattr(path, "user.nokv.compat", b"value")
assert os.getxattr(path, "user.nokv.compat") == b"value"
assert "user.nokv.compat" in os.listxattr(path)
os.removexattr(path, "user.nokv.compat")
try:
    os.getxattr(path, "user.nokv.compat")
except OSError as err:
    allowed = {
        getattr(errno, "ENODATA", 61),
        getattr(errno, "ENOATTR", 93),
    }
    if err.errno not in allowed:
        raise
else:
    raise AssertionError("removed xattr was still readable")

shutil.rmtree(base)
PY
}

prepare_pjdfstest() {
    TARGET_PJDFSTEST_DIR="$MOUNT_DIR/nokv-compat-pjdfstest"
    rm -rf "$TARGET_PJDFSTEST_DIR"
    if [[ -n "$PJDFSTEST_DIR" ]]; then
        cp -R "$PJDFSTEST_DIR" "$TARGET_PJDFSTEST_DIR"
        return 0
    fi
    require_cmd git
    git clone --depth=1 "$PJDFSTEST_REPO" "$TARGET_PJDFSTEST_DIR"
}

run_pjdfstest() {
    echo "==> fuse-compat: pjdfstest"
    require_cmd make
    require_cmd prove
    prepare_pjdfstest
    (
        cd "$TARGET_PJDFSTEST_DIR"
        if [[ ! -x ./pjdfstest ]]; then
            if [[ -x ./autogen.sh ]]; then
                ./autogen.sh
            elif [[ -f configure.ac ]]; then
                require_cmd autoreconf
                autoreconf -ifs
            fi
            if [[ -x ./configure ]]; then
                ./configure
            fi
            make
        fi
        prove -rv tests/
    )
}

run_ltp() {
    echo "==> fuse-compat: LTP filesystem suite"
    local runltp="$LTP_RUNLTP"
    if [[ -z "$runltp" ]]; then
        runltp="runltp"
    fi
    if ! command -v "$runltp" >/dev/null 2>&1 && [[ ! -x "$runltp" ]]; then
        echo "error: LTP runltp not found; set NOKV_LTP_RUNLTP=/path/to/runltp" >&2
        exit 127
    fi
    mkdir -p "$MOUNT_DIR/nokv-ltp"
    "$runltp" -d "$MOUNT_DIR/nokv-ltp" -f fs,fs_perms_simple,fsx,io,fcntl-locktests
}

ensure_mount
prepare_workdir
trap cleanup EXIT

for test_name in $TESTS; do
    case "$test_name" in
        basic) run_basic ;;
        xattr) run_xattr ;;
        pjdfstest) run_pjdfstest ;;
        ltp) run_ltp ;;
        "")
            ;;
        *)
            echo "error: unknown compatibility test: $test_name" >&2
            exit 2
            ;;
    esac
done
