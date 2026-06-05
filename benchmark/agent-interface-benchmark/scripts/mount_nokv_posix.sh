#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
data_root="${YANEX_BENCH_DATA_ROOT:-${repo_root}/benchmark/data/yanex-demo}"
mountpoint="${1:-${data_root}/fuse}"
bucket="${YANEX_S3_BUCKET:-nokv-yanex-demo}"
endpoint="${YANEX_S3_ENDPOINT:-http://127.0.0.1:9000}"
access_key="${YANEX_S3_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${YANEX_S3_SECRET_ACCESS_KEY:-rustfsadmin}"

mkdir -p "${mountpoint}"

exec cargo run -p nokvfs-cli --bin nokv-fs -- \
  --meta "${data_root}/nokv/meta" \
  --object-backend rustfs \
  --s3-bucket "${bucket}" \
  --s3-endpoint "${endpoint}" \
  --s3-access-key-id "${access_key}" \
  --s3-secret-access-key "${secret_key}" \
  mount --read-only "${mountpoint}"
