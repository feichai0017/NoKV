#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
require_cmd gcloud curl

gcloud services enable compute.googleapis.com artifactregistry.googleapis.com iam.googleapis.com iap.googleapis.com \
  --project="$GCP_PROJECT" \
  --quiet

"$SCRIPT_DIR/create-artifact-registry.sh"

if ! gcloud compute networks describe "$GCP_NETWORK" --project="$GCP_PROJECT" >/dev/null 2>&1; then
  gcloud compute networks create "$GCP_NETWORK" \
    --subnet-mode=custom \
    --project="$GCP_PROJECT" \
    --quiet
fi

if ! gcloud compute networks subnets describe "$GCP_SUBNET" \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" >/dev/null 2>&1; then
  gcloud compute networks subnets create "$GCP_SUBNET" \
    --network="$GCP_NETWORK" \
    --range="$GCP_SUBNET_RANGE" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" \
    --quiet
fi

internal_fw="${GCP_CLUSTER_NAME}-allow-internal"
if ! gcloud compute firewall-rules describe "$internal_fw" --project="$GCP_PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules create "$internal_fw" \
    --network="$GCP_NETWORK" \
    --allow=tcp,udp,icmp \
    --source-ranges="$GCP_SUBNET_RANGE" \
    --target-tags="$GCP_CLUSTER_NAME" \
    --project="$GCP_PROJECT" \
    --quiet
fi

if is_truthy "$GCP_ASSIGN_EXTERNAL_IPS"; then
  ssh_ranges="${GCP_SSH_SOURCE_RANGES:-}"
  if [[ -z "$ssh_ranges" ]]; then
    ssh_ranges="$(current_public_ip_cidr)" || die "set GCP_SSH_SOURCE_RANGES; could not detect public IP"
  fi
  ssh_fw="${GCP_CLUSTER_NAME}-allow-ssh"
  if ! gcloud compute firewall-rules describe "$ssh_fw" --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "$ssh_fw" \
      --network="$GCP_NETWORK" \
      --allow=tcp:22 \
      --source-ranges="$ssh_ranges" \
      --target-tags="$GCP_CLUSTER_NAME" \
      --project="$GCP_PROJECT" \
      --quiet
  fi
else
  iap_fw="${GCP_CLUSTER_NAME}-allow-iap-ssh"
  if ! gcloud compute firewall-rules describe "$iap_fw" --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "$iap_fw" \
      --network="$GCP_NETWORK" \
      --allow=tcp:22 \
      --source-ranges=35.235.240.0/20 \
      --target-tags="$GCP_CLUSTER_NAME" \
      --project="$GCP_PROJECT" \
      --quiet
  fi

  if ! gcloud compute routers describe "$GCP_ROUTER" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute routers create "$GCP_ROUTER" \
      --network="$GCP_NETWORK" \
      --region="$GCP_REGION" \
      --project="$GCP_PROJECT" \
      --quiet
  fi

  if ! gcloud compute routers nats describe "$GCP_NAT" \
    --router="$GCP_ROUTER" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute routers nats create "$GCP_NAT" \
      --router="$GCP_ROUTER" \
      --region="$GCP_REGION" \
      --nat-all-subnet-ip-ranges \
      --auto-allocate-nat-external-ips \
      --project="$GCP_PROJECT" \
      --quiet
  fi
fi

if is_truthy "$GCP_USE_COMPACT_PLACEMENT"; then
  if ! gcloud compute resource-policies describe "$GCP_RESOURCE_POLICY" \
    --region="$GCP_REGION" \
    --project="$GCP_PROJECT" >/dev/null 2>&1; then
    gcloud compute resource-policies create group-placement "$GCP_RESOURCE_POLICY" \
      --region="$GCP_REGION" \
      --collocation=collocated \
      --vm-count=11 \
      --project="$GCP_PROJECT" \
      --quiet
  fi
fi

compute_sa="$(default_compute_service_account)"
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member="serviceAccount:${compute_sa}" \
  --role=roles/artifactregistry.reader \
  --condition=None \
  --quiet >/dev/null

echo "GCP benchmark infra is ready in ${GCP_PROJECT}/${GCP_REGION}."
