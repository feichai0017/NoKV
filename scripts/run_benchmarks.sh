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

default_workloads="A,B,C"
default_engines="nokv,badger,rocksdb"
default_records=100000
default_ops=500000
default_conc=8

ycsb_engines="${YCSB_ENGINES:-$default_engines}"
ycsb_workloads="${YCSB_WORKLOADS:-$default_workloads}"
ycsb_records="${YCSB_RECORDS:-$default_records}"
ycsb_ops="${YCSB_OPS:-$default_ops}"
ycsb_conc="${YCSB_CONC:-$default_conc}"
ycsb_block_cache_mb="${YCSB_BLOCK_CACHE_MB:-256}"
ycsb_value_size="${YCSB_VALUE_SIZE:-256}"
ycsb_scan_len="${YCSB_SCAN_LEN:-100}"
ycsb_seed="${YCSB_SEED:-42}"
ycsb_sync="${YCSB_SYNC_WRITES:-false}"
ycsb_badger_comp="${YCSB_BADGER_COMPRESSION:-none}"
ycsb_rocks_comp="${YCSB_ROCKS_COMPRESSION:-none}"
benchdir="${YCSB_BENCHDIR:-benchmark_data}"

export NOKV_RUN_BENCHMARKS=1

need_rocksdb=false
if [[ ",${ycsb_engines}," == *,rocksdb,* ]]; then
  need_rocksdb=true
fi

if $need_rocksdb; then
  prefix="${REPO_ROOT}/third_party/rocksdb/dist"
  if [[ ! -f "${prefix}/lib/librocksdb.so" ]]; then
    die "RocksDB artifacts not found at ${prefix}. Run ./scripts/build_rocksdb.sh first."
  fi
  export LD_LIBRARY_PATH="${prefix}/lib:${LD_LIBRARY_PATH:-}"
  export CGO_CFLAGS="${CGO_CFLAGS:-} -I${prefix}/include"
  export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${prefix}/lib -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4"
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
