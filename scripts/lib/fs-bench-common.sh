#!/usr/bin/env bash
#
# Shared orchestration for the NoKV-FS benchmark framework.
#
# Sourced by the benchmark entry scripts (run-fs-benchmark.sh and the quick
# single-shot comparison). It owns the disposable backend topology — RustFS
# object store, Redis (JuiceFS metadata), the NoKV metadata server, and the two
# FUSE mounts — plus the cache-control and driver-invocation helpers. Keeping
# this in one place is what lets the quick comparison and the full matrix share
# exactly one implementation.
#
# Contract: the entry script sets configuration variables (or relies on the
# defaults below), calls `bench_require_tools`, installs `trap bench_cleanup
# EXIT INT TERM`, then drives the bench_* lifecycle functions.

# --------------------------------------------------------------------------- #
# Configuration (override via environment before sourcing, or after).
# --------------------------------------------------------------------------- #
: "${ROOT_DIR:?fs-bench-common.sh requires ROOT_DIR (repo root) to be set}"

RUSTFS_BIN="${NOKV_BENCH_RUSTFS_BIN:-${NOKV_RUSTFS_BIN:-rustfs}}"
AWS_BIN="${NOKV_BENCH_AWS_BIN:-${NOKV_AWS_BIN:-aws}}"
REDIS_BIN="${NOKV_BENCH_REDIS_BIN:-${NOKV_REDIS_BIN:-redis-server}}"
JUICEFS_BIN="${NOKV_BENCH_JUICEFS_BIN:-${NOKV_JUICEFS_BIN:-juicefs}}"
PYTHON_BIN="${NOKV_BENCH_PYTHON_BIN:-${NOKV_PYTHON_BIN:-python3}}"
NOKV_FS_BIN="${NOKV_BENCH_NOKV_FS_BIN:-}"
if [[ -z "${NOKV_BENCH_MDTEST_BIN:-}" && -x "$ROOT_DIR/third_party/ior/_install/bin/mdtest" ]]; then
    export NOKV_BENCH_MDTEST_BIN="$ROOT_DIR/third_party/ior/_install/bin/mdtest"
fi

PROFILE="${NOKV_BENCH_PROFILE:-smoke}"
FSYNC="${NOKV_BENCH_FSYNC:-0}"
BUILD_RELEASE="${NOKV_BENCH_BUILD_RELEASE:-1}"

RUSTFS_ADDRESS="${NOKV_BENCH_RUSTFS_ADDRESS:-127.0.0.1:9040}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_BENCH_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9041}"
RUSTFS_ENDPOINT="${NOKV_BENCH_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_ACCESS_KEY="${NOKV_BENCH_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_BENCH_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_BENCH_RUSTFS_BUFFER_PROFILE:-AiTraining}"

NOKV_BUCKET="${NOKV_BENCH_NOKV_BUCKET:-nokv-fs-bench}"
JUICEFS_BUCKET="${NOKV_BENCH_JUICEFS_BUCKET:-juicefs-fs-bench}"
REDIS_PORT="${NOKV_BENCH_REDIS_PORT:-16440}"
SERVER_ADDRESS="${NOKV_BENCH_SERVER_ADDRESS:-127.0.0.1:7841}"
NOKV_MOUNT_STATS_ADDRESS="${NOKV_BENCH_MOUNT_STATS_ADDRESS:-127.0.0.1:7842}"
META_URL="redis://127.0.0.1:${REDIS_PORT}/1"

WORKDIR="${NOKV_BENCH_WORKDIR:-}"
KEEP_WORKDIR="${NOKV_BENCH_KEEP_WORKDIR:-0}"
OWN_WORKDIR=0

case "$(uname -s)" in
Darwin) DEFAULT_JUICEFS_MOUNT_OPTIONS="noappledouble,noapplexattr" ;;
*) DEFAULT_JUICEFS_MOUNT_OPTIONS="" ;;
esac
JUICEFS_MOUNT_OPTIONS="${NOKV_BENCH_JUICEFS_MOUNT_OPTIONS:-$DEFAULT_JUICEFS_MOUNT_OPTIONS}"
NOKV_MOUNT_OPTIONS="${NOKV_BENCH_NOKV_MOUNT_OPTIONS:-}"
NOKV_FUSE_THREADS="${NOKV_BENCH_NOKV_FUSE_THREADS:-}"
NOKV_HOT_OBJECT_ROOT="${NOKV_BENCH_HOT_OBJECT_ROOT:-auto}"
NOKV_HOT_OBJECT_MAX_BYTES="${NOKV_BENCH_HOT_OBJECT_MAX_BYTES:-}"

# Runtime state (set by the lifecycle functions).
RUSTFS_PID="" REDIS_PID="" SERVER_PID="" NOKV_MOUNT_PID="" JUICEFS_MOUNT_PID=""
NOKV_META_DIR="" NOKV_MOUNT="" JUICEFS_MOUNT="" JUICEFS_FORMATTED=0
RUSTFS_LOG="" REDIS_LOG="" SERVER_LOG="" NOKV_MOUNT_LOG="" JUICEFS_LOG=""

# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
bench_require_cmd() {
    local cmd="$1" install="$2"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command not found: $cmd" >&2
        echo "install: $install" >&2
        exit 127
    fi
}

bench_require_tools() {
    bench_require_cmd "$RUSTFS_BIN" "brew tap rustfs/homebrew-tap && brew install rustfs"
    bench_require_cmd "$AWS_BIN" "brew install awscli"
    bench_require_cmd "$REDIS_BIN" "brew install redis"
    bench_require_cmd "$JUICEFS_BIN" "brew install juicefs"
    bench_require_cmd "$PYTHON_BIN" "brew install python"
    bench_require_cmd curl "brew install curl"
}

bench_nokv_object_backend_label() {
    if [[ -n "$NOKV_HOT_OBJECT_ROOT" && "$NOKV_HOT_OBJECT_ROOT" != "none" ]]; then
        echo "rustfs+local-hot"
    else
        echo "rustfs"
    fi
}

bench_is_mounted() {
    "$PYTHON_BIN" - "$1" <<'PY'
import os, sys
sys.exit(0 if os.path.ismount(sys.argv[1]) else 1)
PY
}

bench_unmount_path() {
    local mountpoint="$1"
    [[ -z "$mountpoint" || ! -d "$mountpoint" ]] && return 0
    bench_is_mounted "$mountpoint" || return 0
    command -v "$JUICEFS_BIN" >/dev/null 2>&1 && "$JUICEFS_BIN" umount "$mountpoint" >/dev/null 2>&1 && return 0
    command -v fusermount3 >/dev/null 2>&1 && fusermount3 -u "$mountpoint" >/dev/null 2>&1 && return 0
    command -v fusermount >/dev/null 2>&1 && fusermount -u "$mountpoint" >/dev/null 2>&1 && return 0
    command -v diskutil >/dev/null 2>&1 && diskutil unmount "$mountpoint" >/dev/null 2>&1 && return 0
    umount "$mountpoint" >/dev/null 2>&1 || true
}

bench_wait_tcp() {
    local host="$1" port="$2" name="$3" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $host:$port" >&2
    return 1
}

# --------------------------------------------------------------------------- #
# Cache control
# --------------------------------------------------------------------------- #
# Best-effort OS page cache drop so a "cold" measurement actually reaches the
# filesystem. Portable fallback is the driver's own per-read bypass
# (posix_fadvise / F_NOCACHE), so this never being available is not fatal.
bench_drop_caches() {
    case "$(uname -s)" in
    Linux)
        sync
        if [[ -w /proc/sys/vm/drop_caches ]]; then
            echo 3 >/proc/sys/vm/drop_caches 2>/dev/null || true
        elif command -v sudo >/dev/null 2>&1; then
            sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || true
        fi
        ;;
    Darwin)
        sync
        command -v purge >/dev/null 2>&1 && purge 2>/dev/null || true
        ;;
    esac
}

# --------------------------------------------------------------------------- #
# Backends: RustFS + Redis
# --------------------------------------------------------------------------- #
bench_wait_rustfs() {
    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        kill -0 "$RUSTFS_PID" >/dev/null 2>&1 || { echo "error: RustFS exited early" >&2; return 1; }
        curl -fsS --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1 && return 0
        curl -sS -I --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for RustFS at $RUSTFS_ENDPOINT" >&2
    return 1
}

bench_create_bucket() {
    local bucket="$1" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" s3api create-bucket --bucket "$bucket" >/dev/null 2>&1 && return 0
        AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    echo "error: failed to create/find bucket '$bucket'" >&2
    return 1
}

bench_start_backends() {
    echo "Starting RustFS endpoint=$RUSTFS_ENDPOINT" >&2
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
        "$RUSTFS_BIN" server --address "$RUSTFS_ADDRESS" --console-enable \
        --console-address "$RUSTFS_CONSOLE_ADDRESS" --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
        "$WORKDIR/rustfs-data" >"$RUSTFS_LOG" 2>&1 &
    RUSTFS_PID=$!
    echo "Starting Redis port=$REDIS_PORT" >&2
    "$REDIS_BIN" --bind 127.0.0.1 --port "$REDIS_PORT" --dir "$WORKDIR/redis" \
        --save "" --appendonly no >"$REDIS_LOG" 2>&1 &
    REDIS_PID=$!
    bench_wait_rustfs
    bench_wait_tcp 127.0.0.1 "$REDIS_PORT" Redis
    bench_create_bucket "$NOKV_BUCKET"
    bench_create_bucket "$JUICEFS_BUCKET"
}

# --------------------------------------------------------------------------- #
# NoKV metadata server + mount, keyed by tier.
# --------------------------------------------------------------------------- #
# bench_tier_label <tier> <sync> -> canonical metadata_tier string
bench_tier_label() {
    local tier="$1" sync="$2"
    if [[ "$tier" != "local" ]]; then
        echo "error: current nokv CLI exposes only the local metadata server tier" >&2
        return 2
    fi
    echo "nokv-direct-wal-async"
}

bench_wait_metadata_server() {
    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        kill -0 "$SERVER_PID" >/dev/null 2>&1 || { echo "error: nokv serve exited early" >&2; return 1; }
        curl -fsS --max-time 2 "http://${SERVER_ADDRESS}/healthz" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for NoKV server at $SERVER_ADDRESS" >&2
    return 1
}

bench_wait_mount() {
    local mountpoint="$1" name="$2" pid="${3:-}" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: $name exited before mount became ready" >&2
            return 1
        fi
        bench_is_mounted "$mountpoint" && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name mount at $mountpoint" >&2
    return 1
}

# bench_start_nokv_server <tier local> <sync data|none>
bench_start_nokv_server() {
    local tier="$1" sync="${2:-none}"
    if [[ "$tier" != "local" ]]; then
        echo "error: current nokv CLI exposes only the local metadata server tier" >&2
        return 2
    fi
    NOKV_META_DIR="$WORKDIR/nokv-meta-${tier}"
    mkdir -p "$NOKV_META_DIR"
    echo "Starting NoKV metadata server tier=$tier sync=$sync" >&2
    local args=(
        --server-bind "$SERVER_ADDRESS" --meta "$NOKV_META_DIR"
        --object-backend rustfs --s3-endpoint "$RUSTFS_ENDPOINT" --s3-bucket "$NOKV_BUCKET"
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" --s3-secret-access-key "$RUSTFS_SECRET_KEY"
        --uid "$(id -u)" --gid "$(id -g)"
    )
    "$NOKV_FS_BIN" "${args[@]}" serve >"$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    bench_wait_metadata_server
}

bench_stop_nokv_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    SERVER_PID=""
}

# bench_mount_nokv <cache_state cold|warm>  (cold disables NoKV block cache and
# kernel page cache so reads reach the filesystem)
bench_mount_nokv() {
    local cache_state="${1:-warm}"
    NOKV_MOUNT="$WORKDIR/nokv-mount"
    mkdir -p "$NOKV_MOUNT"
    local opts=()
    [[ -n "$NOKV_MOUNT_OPTIONS" ]] && read -r -a opts <<<"$NOKV_MOUNT_OPTIONS"
    if [[ -n "$NOKV_FUSE_THREADS" && "$NOKV_FUSE_THREADS" != "none" ]]; then
        opts+=(--fuse-threads "$NOKV_FUSE_THREADS")
    fi
    if [[ "$cache_state" == "cold" ]]; then
        opts+=(--no-block-cache --direct-io)
    fi
    if [[ -n "$NOKV_MOUNT_STATS_ADDRESS" ]]; then
        opts+=(--stats-bind "$NOKV_MOUNT_STATS_ADDRESS")
    fi
    local object_args=()
    if [[ -n "$NOKV_HOT_OBJECT_ROOT" && "$NOKV_HOT_OBJECT_ROOT" != "none" ]]; then
        object_args+=(--hot-object-root "$NOKV_HOT_OBJECT_ROOT")
        if [[ -n "$NOKV_HOT_OBJECT_MAX_BYTES" ]]; then
            object_args+=(--hot-object-max-bytes "$NOKV_HOT_OBJECT_MAX_BYTES")
        fi
    fi
    echo "Mounting NoKV at $NOKV_MOUNT cache=$cache_state" >&2
    "$NOKV_FS_BIN" --server-bind "$SERVER_ADDRESS" --object-backend rustfs \
        --s3-endpoint "$RUSTFS_ENDPOINT" --s3-bucket "$NOKV_BUCKET" \
        --s3-access-key-id "$RUSTFS_ACCESS_KEY" --s3-secret-access-key "$RUSTFS_SECRET_KEY" \
        --uid "$(id -u)" --gid "$(id -g)" "${object_args[@]}" mount "${opts[@]}" "$NOKV_MOUNT" >"$NOKV_MOUNT_LOG" 2>&1 &
    NOKV_MOUNT_PID=$!
    bench_wait_mount "$NOKV_MOUNT" NoKV "$NOKV_MOUNT_PID"
    if [[ -n "$NOKV_MOUNT_STATS_ADDRESS" ]]; then
        bench_wait_tcp "${NOKV_MOUNT_STATS_ADDRESS%:*}" "${NOKV_MOUNT_STATS_ADDRESS##*:}" "NoKV mount stats"
    fi
}

bench_unmount_nokv() {
    bench_unmount_path "$NOKV_MOUNT" || true
    if [[ -n "$NOKV_MOUNT_PID" ]] && kill -0 "$NOKV_MOUNT_PID" >/dev/null 2>&1; then
        kill "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
        wait "$NOKV_MOUNT_PID" >/dev/null 2>&1 || true
    fi
    NOKV_MOUNT_PID=""
}

# --------------------------------------------------------------------------- #
# JuiceFS format + mount.
# --------------------------------------------------------------------------- #
bench_format_juicefs() {
    [[ "$JUICEFS_FORMATTED" == "1" ]] && return 0
    echo "Formatting JuiceFS bucket=$JUICEFS_BUCKET" >&2
    "$JUICEFS_BIN" format --storage s3 --bucket "$RUSTFS_ENDPOINT/$JUICEFS_BUCKET" \
        --access-key "$RUSTFS_ACCESS_KEY" --secret-key "$RUSTFS_SECRET_KEY" \
        --trash-days 0 "$META_URL" nokv-fs-bench-juicefs >"$JUICEFS_LOG" 2>&1
    JUICEFS_FORMATTED=1
}

bench_mount_juicefs() {
    JUICEFS_MOUNT="$WORKDIR/juicefs-mount"
    mkdir -p "$JUICEFS_MOUNT"
    bench_format_juicefs
    echo "Mounting JuiceFS at $JUICEFS_MOUNT" >&2
    if [[ -n "$JUICEFS_MOUNT_OPTIONS" ]]; then
        "$JUICEFS_BIN" mount -d -o "$JUICEFS_MOUNT_OPTIONS" "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    else
        "$JUICEFS_BIN" mount -d "$META_URL" "$JUICEFS_MOUNT" >>"$JUICEFS_LOG" 2>&1
    fi
    bench_wait_mount "$JUICEFS_MOUNT" JuiceFS
}

bench_unmount_juicefs() {
    bench_unmount_path "$JUICEFS_MOUNT" || true
}

# --------------------------------------------------------------------------- #
# Driver invocation (the two co-equal drivers).
# --------------------------------------------------------------------------- #
# bench_run_native <system> <mount> <tier> <concurrency> <emit_header> [workloads]
bench_run_native() {
    local system="$1" mount="$2" tier="$3" concurrency="$4" emit_header="$5"
    local workloads="${6:-metadata_create_list,checkpoint,training_read}"
    local object_backend="rustfs"
    [[ "$system" == "nokv" ]] && object_backend="$(bench_nokv_object_backend_label)"
    "$PYTHON_BIN" "$ROOT_DIR/bench/drivers/posix_bench.py" \
        --system "$system" --mount "$mount" --metadata-tier "$tier" --object-backend "$object_backend" \
        --profile "$PROFILE" --concurrency "$concurrency" --workloads "$workloads" \
        --dataset-dirs "${DATASET_DIRS:-8}" --files-per-dir "${FILES_PER_DIR:-64}" \
        --sample-bytes "${SAMPLE_BYTES:-512}" --checkpoint-bytes "${CHECKPOINT_BYTES:-4096}" \
        --checkpoint-steps "${CHECKPOINT_STEPS:-32}" \
        --fsync "$FSYNC" --emit-header "$emit_header"
}

# bench_run_real_tools <system> <mount> <tier> <concurrency> <tools> <emit_header>
bench_run_real_tools() {
    local system="$1" mount="$2" tier="$3" concurrency="$4" tools="$5" emit_header="$6"
    local object_backend="rustfs"
    [[ "$system" == "nokv" ]] && object_backend="$(bench_nokv_object_backend_label)"
    "$PYTHON_BIN" "$ROOT_DIR/bench/drivers/real_tools.py" \
        --system "$system" --mount "$mount" --metadata-tier "$tier" --object-backend "$object_backend" \
        --profile "$PROFILE" --concurrency "$concurrency" --tools "$tools" --emit-header "$emit_header"
}

# --------------------------------------------------------------------------- #
# Build + workspace + cleanup
# --------------------------------------------------------------------------- #
bench_build_nokv() {
    [[ -z "$NOKV_FS_BIN" ]] && NOKV_FS_BIN="$ROOT_DIR/target/release/nokv"
    if [[ "$BUILD_RELEASE" == "1" || ! -x "$NOKV_FS_BIN" ]]; then
        bench_require_cmd cargo "install Rust from https://rustup.rs/"
        (cd "$ROOT_DIR" && cargo build --release -p nokv --bin nokv)
    fi
    [[ -x "$NOKV_FS_BIN" ]] || { echo "error: nokv binary not executable: $NOKV_FS_BIN" >&2; exit 2; }
}

bench_make_workdir() {
    if [[ -z "$WORKDIR" ]]; then
        WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fs-bench.XXXXXX")"
        OWN_WORKDIR=1
    else
        mkdir -p "$WORKDIR"
    fi
    mkdir -p "$WORKDIR/rustfs-data" "$WORKDIR/redis"
    if [[ "$NOKV_HOT_OBJECT_ROOT" == "auto" ]]; then
        NOKV_HOT_OBJECT_ROOT="$WORKDIR/nokv-hot"
    fi
    if [[ -n "$NOKV_HOT_OBJECT_ROOT" && "$NOKV_HOT_OBJECT_ROOT" != "none" ]]; then
        mkdir -p "$NOKV_HOT_OBJECT_ROOT"
    fi
    RUSTFS_LOG="$WORKDIR/rustfs.log" REDIS_LOG="$WORKDIR/redis.log"
    SERVER_LOG="$WORKDIR/nokv-server.log" NOKV_MOUNT_LOG="$WORKDIR/nokv-mount.log"
    JUICEFS_LOG="$WORKDIR/juicefs.log"
}

bench_cleanup() {
    local status=$?
    bench_unmount_nokv || true
    bench_unmount_juicefs || true
    bench_stop_nokv_server || true
    for pid in "$RUSTFS_PID" "$REDIS_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    if [[ "$status" -ne 0 ]]; then
        for log in "$RUSTFS_LOG" "$REDIS_LOG" "$SERVER_LOG" "$NOKV_MOUNT_LOG" "$JUICEFS_LOG"; do
            if [[ -n "$log" && -f "$log" ]]; then
                echo "---- $(basename "$log") tail ----" >&2
                tail -60 "$log" >&2 || true
            fi
        done
    fi
    if [[ "$OWN_WORKDIR" -eq 1 && "$KEEP_WORKDIR" != "1" ]]; then
        rm -rf "$WORKDIR"
    elif [[ -n "$WORKDIR" ]]; then
        echo "benchmark workdir: $WORKDIR" >&2
    fi
    exit "$status"
}
