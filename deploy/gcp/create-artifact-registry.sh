#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
require_cmd gcloud

if gcloud artifacts repositories describe "$GCP_REPOSITORY" \
  --location="$GCP_REGION" \
  --project="$GCP_PROJECT" >/dev/null 2>&1; then
  echo "Artifact Registry repository already exists: ${ARTIFACT_REPO}"
  exit 0
fi

gcloud artifacts repositories create "$GCP_REPOSITORY" \
  --repository-format=docker \
  --location="$GCP_REGION" \
  --description="NoKV lab-grade benchmark images" \
  --project="$GCP_PROJECT" \
  --quiet

echo "Created Artifact Registry repository: ${ARTIFACT_REPO}"

