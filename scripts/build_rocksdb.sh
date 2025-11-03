#!/usr/bin/env bash
# build_rocksdb.sh builds RocksDB and installs it into a local prefix so that the
# benchmark suite can link against it when comparing NoKV with RocksDB.
#
# Usage:
#   scripts/build_rocksdb.sh [--prefix DIR] [--branch <tag>] [--cmake-flags "<extra flags>"]
#   scripts/build_rocksdb.sh --clean   # remove the build directory
#
# The script clones RocksDB into ./third_party/rocksdb (unless already present),
# configures it with the minimal set of options required for the benchmark
# harness (shared library, compression codecs), and installs the artefacts
# into the chosen prefix. The prefix defaults to ./third_party/rocksdb/dist.
#
# After installation, the script prints the CGO related environment variables
# that need to be exported before running the benchmark with the
# `benchmark_rocksdb` build tag.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TP_DIR="${ROOT_DIR}/third_party"
ROCKS_DIR="${TP_DIR}/rocksdb"
PREFIX="${ROCKS_DIR}/dist"
BRANCH="v9.9.3"
JOBS="$(nproc || sysctl -n hw.ncpu || echo 4)"
CLEAN=0
CMAKE_FLAGS=("-DWITH_WARNINGS_AS_ERRORS=OFF" "-DCMAKE_CXX_FLAGS=-Wno-error=array-bounds")

usage() {
  grep '^#' "$0" | sed -e 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)
      PREFIX="$2"
      shift 2
      ;;
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --jobs)
      JOBS="$2"
      shift 2
      ;;
    --cmake-flags)
      CMAKE_FLAGS+=("$2")
      shift 2
      ;;
    --clean)
      CLEAN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown flag: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${CLEAN}" -eq 1 ]]; then
  echo "Removing RocksDB build directories under ${ROCKS_DIR}"
  rm -rf "${ROCKS_DIR}"
  exit 0
fi

mkdir -p "${TP_DIR}"

if [[ ! -d "${ROCKS_DIR}" ]]; then
  echo "Cloning RocksDB (${BRANCH}) into ${ROCKS_DIR}"
  git clone --depth 1 --branch "${BRANCH}" https://github.com/facebook/rocksdb.git "${ROCKS_DIR}"
else
  echo "RocksDB directory already exists; skipping clone."
fi

pushd "${ROCKS_DIR}" >/dev/null

echo "Configuring build (shared library, compression codecs)"
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
  -DROCKSDB_BUILD_SHARED=ON \
  -DROCKSDB_BUILD_STATIC=OFF \
  -DWITH_ZSTD=ON \
  -DWITH_LZ4=ON \
  -DWITH_ZLIB=ON \
  -DWITH_SNAPPY=ON \
  "${CMAKE_FLAGS[@]}"

echo "Building (jobs=${JOBS})"
cmake --build build --target install -j "${JOBS}"

popd >/dev/null

cat <<EOF

RocksDB installed to: ${PREFIX}

Export the following variables before running the benchmark with RocksDB support:

  export CGO_CFLAGS="-I${PREFIX}/include"
  export CGO_LDFLAGS="-L${PREFIX}/lib -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4"
  export LD_LIBRARY_PATH="${PREFIX}/lib:${LD_LIBRARY_PATH}"

Then run:

  go test -tags benchmark_rocksdb ./benchmark -run TestBenchmarkYCSB -count=1

EOF
