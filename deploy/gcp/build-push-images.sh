#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
require_cmd gcloud docker git awk

"$SCRIPT_DIR/create-artifact-registry.sh"

tag="${NOKV_IMAGE_TAG:-$(git_tag_default)}"
runtime_image="${ARTIFACT_REPO}/nokv:${tag}"
bench_image="${ARTIFACT_REPO}/nokv-bench:${tag}"

gcloud auth configure-docker "$ARTIFACT_HOST" --quiet

if ! docker buildx inspect >/dev/null 2>&1; then
  docker buildx create --use >/dev/null
fi

docker buildx build \
  --platform linux/amd64 \
  --tag "$runtime_image" \
  --push \
  "$REPO_ROOT"

docker buildx build \
  --platform linux/amd64 \
  --file "$SCRIPT_DIR/Dockerfile.benchmark" \
  --tag "$bench_image" \
  --push \
  "$REPO_ROOT"

runtime_digest="$(docker buildx imagetools inspect "$runtime_image" | awk '/Digest:/ {print $2; exit}')"
bench_digest="$(docker buildx imagetools inspect "$bench_image" | awk '/Digest:/ {print $2; exit}')"

if [[ -z "$runtime_digest" || -z "$bench_digest" ]]; then
  die "failed to resolve image digests"
fi

runtime_pinned="${ARTIFACT_REPO}/nokv@${runtime_digest}"
bench_pinned="${ARTIFACT_REPO}/nokv-bench@${bench_digest}"
write_last_images "$tag" "$runtime_pinned" "$bench_pinned"

cat <<EOF
Runtime image:   $runtime_pinned
Benchmark image: $bench_pinned
Wrote:           $SCRIPT_DIR/.last-image.env
EOF
