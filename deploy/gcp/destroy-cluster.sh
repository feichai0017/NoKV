#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
require_cmd gcloud

delete_infra=0
if [[ "${1:-}" == "--delete-infra" || "${2:-}" == "--delete-infra" ]]; then
  delete_infra=1
fi

cat <<EOF | while IFS= read -r instance; do
${GCP_CLUSTER_NAME}-meta-root-1
${GCP_CLUSTER_NAME}-meta-root-2
${GCP_CLUSTER_NAME}-meta-root-3
${GCP_CLUSTER_NAME}-coordinator-1
${GCP_CLUSTER_NAME}-coordinator-2
${GCP_CLUSTER_NAME}-coordinator-3
${GCP_CLUSTER_NAME}-store-1
${GCP_CLUSTER_NAME}-store-2
${GCP_CLUSTER_NAME}-store-3
${GCP_CLUSTER_NAME}-gateway-1
${GCP_CLUSTER_NAME}-loadgen-1
EOF
  [[ -n "$instance" ]] || continue
  gcloud compute instances delete "$instance" \
    --zone="$GCP_ZONE" \
    --project="$GCP_PROJECT" \
    --quiet || true
done

if [[ "$delete_infra" -eq 1 ]]; then
  for firewall in \
    "${GCP_CLUSTER_NAME}-allow-internal" \
    "${GCP_CLUSTER_NAME}-allow-ssh" \
    "${GCP_CLUSTER_NAME}-allow-iap-ssh"; do
    if gcloud compute firewall-rules describe "$firewall" \
      --project="$GCP_PROJECT" >/dev/null 2>&1; then
      gcloud compute firewall-rules delete "$firewall" \
        --project="$GCP_PROJECT" \
        --quiet || true
    fi
  done
  gcloud compute routers nats delete "$GCP_NAT" \
    --router="$GCP_ROUTER" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" \
    --quiet || true
  gcloud compute routers delete "$GCP_ROUTER" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" \
    --quiet || true
  if gcloud compute resource-policies describe "$GCP_RESOURCE_POLICY" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute resource-policies delete "$GCP_RESOURCE_POLICY" \
      --region="$GCP_REGION" \
      --project="$GCP_PROJECT" \
      --quiet || true
  fi
  gcloud compute networks subnets delete "$GCP_SUBNET" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" \
    --quiet || true
  gcloud compute networks delete "$GCP_NETWORK" \
    --project="$GCP_PROJECT" \
    --quiet || true
fi

echo "Deleted NoKV benchmark cluster instances."
