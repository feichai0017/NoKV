#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Helper to print errors in red when stdout is a TTY.
err() {
  if [ -t 2 ]; then
    printf '\e[31m%s\e[0m\n' "$*" >&2
  else
    printf '%s\n' "$*" >&2
  fi
}

die() {
  err "$*"
  exit 1
}

default_workloads="A,B,C,D,F"
default_engines="nokv,badger,rocksdb"
default_records=1000000
default_ops=1000000
default_conc=16

ycsb_engines="${YCSB_ENGINES:-$default_engines}"
ycsb_workloads="${YCSB_WORKLOADS:-$default_workloads}"
ycsb_records="${YCSB_RECORDS:-$default_records}"
ycsb_ops="${YCSB_OPS:-$default_ops}"
ycsb_conc="${YCSB_CONC:-$default_conc}"
ycsb_block_cache_mb="${YCSB_BLOCK_CACHE_MB:-512}"
ycsb_value_size="${YCSB_VALUE_SIZE:-256}"
ycsb_scan_len="${YCSB_SCAN_LEN:-100}"
ycsb_seed="${YCSB_SEED:-42}"
ycsb_sync="${YCSB_SYNC_WRITES:-false}"
ycsb_badger_comp="${YCSB_BADGER_COMPRESSION:-none}"
ycsb_rocks_comp="${YCSB_ROCKS_COMPRESSION:-none}"
benchdir="${YCSB_BENCHDIR:-benchmark_data}"
ycsb_warm_ops="${YCSB_WARM_OPS:-100000}"

export NOKV_RUN_BENCHMARKS=1

need_rocksdb=false
if [[ ",${ycsb_engines}," == *,rocksdb,* ]]; then
  need_rocksdb=true
fi

# Provide a single helper to append unique flags to an env var.
append_unique() {
  # $1 var name, $2 value to append
  local var="$1"
  local val="$2"
  # shellcheck disable=SC2086
  local current="${!var:-}"
  if [[ "${current}" == *"${val}"* ]]; then
    return 0
  fi
  if [[ -z "${current}" ]]; then
    export "${var}=${val}"
  else
    export "${var}=${current} ${val}"
  fi
}

if $need_rocksdb; then
  prefix="${REPO_ROOT}/third_party/rocksdb/dist"
  libdir="${prefix}/lib"
  if [[ ! -f "${libdir}/librocksdb.so" && ! -f "${libdir}/librocksdb.dylib" ]]; then
    die "RocksDB artifacts not found at ${prefix}. Run ./scripts/build_rocksdb.sh first."
  fi

  export CGO_CFLAGS="${CGO_CFLAGS:-} -I${prefix}/include"
  # Base rpath/ldflags for rocksdb and bundled codecs.
  rocks_ldflags="-L${libdir} -Wl,-rpath,${libdir} -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4"
  homebrew_lib="/opt/homebrew/lib"
  if [[ -d "${homebrew_lib}" ]]; then
    rocks_ldflags+=" -L${homebrew_lib} -Wl,-rpath,${homebrew_lib}"
  fi
  append_unique LD_LIBRARY_PATH "${libdir}"
  append_unique DYLD_LIBRARY_PATH "${libdir}"
  if [[ -d "${homebrew_lib}" ]]; then
    append_unique LD_LIBRARY_PATH "${homebrew_lib}"
    append_unique DYLD_LIBRARY_PATH "${homebrew_lib}"
  fi
  append_unique CGO_LDFLAGS "${rocks_ldflags}"
  build_tags="-tags benchmark_rocksdb"
else
  build_tags=""
fi

args=(
  -benchdir "${benchdir}"
  -seed "${ycsb_seed}"
  -sync="${ycsb_sync}"
  -value_threshold 32
  -badger_block_cache_mb "${ycsb_block_cache_mb}"
  -badger_index_cache_mb "${ycsb_block_cache_mb}"
  -badger_compression "${ycsb_badger_comp}"
  -ycsb_workloads "${ycsb_workloads}"
  -ycsb_engines "${ycsb_engines}"
  -ycsb_records "${ycsb_records}"
  -ycsb_ops "${ycsb_ops}"
  -ycsb_conc "${ycsb_conc}"
  -ycsb_scan_len "${ycsb_scan_len}"
  -ycsb_value_size "${ycsb_value_size}"
  -ycsb_rocks_compression "${ycsb_rocks_comp}"
  -ycsb_block_cache_mb "${ycsb_block_cache_mb}"
  -ycsb_warm_ops "${ycsb_warm_ops}"
)



if (( $# > 0 )); then
  args+=("$@")
fi

GOCACHE="${GOCACHE:-$REPO_ROOT/.gocache}"
GOMODCACHE="${GOMODCACHE:-$REPO_ROOT/.gomodcache}"
export GOCACHE GOMODCACHE

cmd=(go test)
if [[ -n "${build_tags}" ]]; then
  cmd+=(${build_tags})
fi
cmd+=(./benchmark -run TestBenchmarkYCSB -count=1 -args)
cmd+=("${args[@]}")

printf 'Running YCSB benchmark command: %s\n' "${cmd[*]}"
"${cmd[@]}"
