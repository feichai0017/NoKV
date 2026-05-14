#!/usr/bin/env bash
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$DEPLOY_DIR/../.." && pwd)"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  local missing=0
  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      printf 'missing required command: %s\n' "$cmd" >&2
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

load_config() {
  local config_file="${1:-$DEPLOY_DIR/config.env}"
  if [[ -f "$config_file" ]]; then
    # shellcheck disable=SC1090
    source "$config_file"
  fi

  : "${GCP_PROJECT:=nokv-benchmark}"
  : "${GCP_REGION:=australia-southeast2}"
  : "${GCP_ZONE:=australia-southeast2-b}"
  : "${GCP_REPOSITORY:=nokv-lab}"
  : "${GCP_CLUSTER_NAME:=nokv-bench}"
  : "${GCP_NETWORK:=nokv-bench-net}"
  : "${GCP_SUBNET:=nokv-bench-subnet}"
  : "${GCP_SUBNET_RANGE:=10.42.0.0/24}"
  : "${GCP_RESOURCE_POLICY:=nokv-bench-compact}"
  : "${GCP_ROUTER:=${GCP_CLUSTER_NAME}-router}"
  : "${GCP_NAT:=${GCP_CLUSTER_NAME}-nat}"
  : "${GCP_USE_COMPACT_PLACEMENT:=false}"
  : "${GCP_ASSIGN_EXTERNAL_IPS:=false}"
  : "${GCP_USE_IAP:=true}"
  : "${GCP_IMAGE_FAMILY:=debian-12}"
  : "${GCP_IMAGE_PROJECT:=debian-cloud}"
  : "${GCP_BOOT_DISK_SIZE_GB:=30}"
  : "${GCP_META_ROOT_MACHINE_TYPE:=c4-standard-2}"
  : "${GCP_COORDINATOR_MACHINE_TYPE:=c4-standard-2}"
  : "${GCP_STORE_MACHINE_TYPE:=c4-standard-4-lssd}"
  : "${GCP_GATEWAY_MACHINE_TYPE:=c4-standard-4}"
  : "${GCP_LOADGEN_MACHINE_TYPE:=c4-standard-4}"
  : "${GCP_BENCHMARK_START_GRACE_SECONDS:=60}"
  : "${GCP_TRANSPORT_RETRIES:=30}"
  : "${GCP_TRANSPORT_RETRY_DELAY_SECONDS:=10}"
  : "${GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS:=600}"
  : "${GCP_SERVICE_READY_TIMEOUT_SECONDS:=900}"
  : "${GCP_SERVICE_READY_RETRY_DELAY_SECONDS:=2}"
  : "${NOKV_META_ROOT_TICK_INTERVAL:=1000ms}"
  : "${NOKV_FSMETA_PROFILE:=median}"
  : "${NOKV_FSMETA_WORKLOADS:=multi-workspace-autoscale,mixed,durable-snapshot,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup}"
  : "${NOKV_PERAS_WITNESS:=true}"
  : "${NOKV_PERAS_WITNESS_DURABILITY:=fsync-batched}"
  : "${NOKV_FSMETA_PERAS_HOLDER_ID:=fsmeta-holder-1}"
  : "${NOKV_FSMETA_PERAS_AUTHORITY_TTL:=5m}"
  : "${NOKV_FSMETA_PERAS_VISIBLE_COMMIT:=true}"
  : "${NOKV_FSMETA_PERAS_WITNESS_STORES:=1,2,3}"
  : "${NOKV_FSMETA_PERAS_WITNESS_QUORUM:=2}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_WITNESS_RETRIES:=50}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_WITNESS_RETRY_BACKOFF:=20ms}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_BATCH_SIZE:=512}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_MAX_REPLAY_MUTATIONS:=4096}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_INSTALL_PARALLELISM:=0}"
  : "${NOKV_FSMETA_PERAS_SEGMENT_FLUSH_EVERY:=20ms}"
  : "${NOKV_FSMETA_PERAS_BACKGROUND_FLUSH_TIMEOUT:=30s}"
  : "${NOKV_FSMETA_PERAS_BACKGROUND_ERROR_BACKOFF:=1s}"

  ARTIFACT_HOST="${GCP_REGION}-docker.pkg.dev"
  ARTIFACT_REPO="${ARTIFACT_HOST}/${GCP_PROJECT}/${GCP_REPOSITORY}"
}

is_truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

git_tag_default() {
  local sha
  sha="$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD 2>/dev/null || true)"
  if [[ -z "$sha" ]]; then
    sha="nogit"
  fi
  printf '%s-%s\n' "$sha" "$(date -u +%Y%m%dT%H%M%SZ)"
}

load_last_images() {
  local file="$DEPLOY_DIR/.last-image.env"
  if [[ -f "$file" ]]; then
    # shellcheck disable=SC1090
    source "$file"
  fi
}

write_last_images() {
  local tag="$1"
  local runtime_image="$2"
  local bench_image="$3"
  cat >"$DEPLOY_DIR/.last-image.env" <<EOF
NOKV_IMAGE_TAG=$tag
NOKV_IMAGE=$runtime_image
NOKV_BENCH_IMAGE=$bench_image
EOF
}

current_public_ip_cidr() {
  local ip
  ip="$(curl -fsS https://api.ipify.org 2>/dev/null || true)"
  if [[ -z "$ip" ]]; then
    return 1
  fi
  printf '%s/32\n' "$ip"
}

project_number() {
  gcloud projects describe "$GCP_PROJECT" --format='value(projectNumber)'
}

default_compute_service_account() {
  printf '%s-compute@developer.gserviceaccount.com\n' "$(project_number)"
}
