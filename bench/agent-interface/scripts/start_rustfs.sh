#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
data_root="${YANEX_BENCH_DATA_ROOT:-${repo_root}/bench/data/yanex-demo}"
container="${YANEX_RUSTFS_CONTAINER:-yanex-demo-rustfs}"
image="${YANEX_RUSTFS_IMAGE:-rustfs/rustfs:latest}"
host="${YANEX_RUSTFS_HOST:-127.0.0.1}"
port="${YANEX_RUSTFS_PORT:-9000}"
console_port="${YANEX_RUSTFS_CONSOLE_PORT:-9001}"
access_key="${YANEX_S3_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${YANEX_S3_SECRET_ACCESS_KEY:-rustfsadmin}"
bucket="${YANEX_S3_BUCKET:-nokv-yanex-demo}"
endpoint="${YANEX_S3_ENDPOINT:-http://${host}:${port}}"
rustfs_data="${YANEX_RUSTFS_DATA_DIR:-${data_root}/rustfs}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to start RustFS" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required to create and verify the RustFS bucket" >&2
  exit 1
fi

mkdir -p "${rustfs_data}"

if docker container inspect "${container}" >/dev/null 2>&1; then
  if [ "$(docker inspect -f '{{.State.Running}}' "${container}")" != "true" ]; then
    docker start "${container}" >/dev/null
  fi
else
  docker run -d \
    --name "${container}" \
    -p "${host}:${port}:9000" \
    -p "${host}:${console_port}:9001" \
    -e "RUSTFS_ACCESS_KEY=${access_key}" \
    -e "RUSTFS_SECRET_KEY=${secret_key}" \
    -e "RUSTFS_CONSOLE_ENABLE=true" \
    -v "${rustfs_data}:/data" \
    "${image}" \
    --address :9000 \
    --console-enable \
    --access-key "${access_key}" \
    --secret-key "${secret_key}" \
    /data >/dev/null
fi

export AWS_ACCESS_KEY_ID="${access_key}"
export AWS_SECRET_ACCESS_KEY="${secret_key}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

ready=0
for _ in $(seq 1 60); do
  if aws --endpoint-url "${endpoint}" s3api list-buckets >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "${ready}" != "1" ]; then
  echo "RustFS did not become ready at ${endpoint}" >&2
  docker logs --tail 80 "${container}" >&2 || true
  exit 1
fi

if ! aws --endpoint-url "${endpoint}" s3api head-bucket --bucket "${bucket}" >/dev/null 2>&1; then
  aws --endpoint-url "${endpoint}" s3api create-bucket --bucket "${bucket}" >/dev/null
fi

cat <<EOF
RustFS ready
  container: ${container}
  endpoint:  ${endpoint}
  bucket:    ${bucket}
  data:      ${rustfs_data}
EOF
