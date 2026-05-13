#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
load_last_images
require_cmd gcloud

if [[ -z "${NOKV_BENCH_IMAGE:-}" ]]; then
  die "NOKV_BENCH_IMAGE is required; run deploy/gcp/build-push-images.sh first"
fi

loadgen="${GCP_CLUSTER_NAME}-loadgen-1"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
remote_dir="/mnt/nokv/results/${run_id}"
local_dir="$SCRIPT_DIR/results/${run_id}"
signing_key_file="$SCRIPT_DIR/generated/eunomia-signing-key.txt"

if [[ -f "$signing_key_file" ]]; then
  signing_key="$(tr -d '\n' <"$signing_key_file")"
else
  signing_key="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-}"
fi

if [[ -z "$signing_key" ]]; then
  die "No signing key found; run deploy/gcp/create-cluster.sh first or set NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY"
fi

if [[ "$GCP_BENCHMARK_START_GRACE_SECONDS" != "0" ]]; then
  echo "Waiting ${GCP_BENCHMARK_START_GRACE_SECONDS}s for VM metadata, IAP, and startup scripts to begin settling..."
  sleep "$GCP_BENCHMARK_START_GRACE_SECONDS"
fi

retry_transport() {
  local action="$1"
  local attempt=1
  local max_attempts="$GCP_TRANSPORT_RETRIES"
  local delay_seconds="$GCP_TRANSPORT_RETRY_DELAY_SECONDS"
  local rc=0

  while [[ "$attempt" -le "$max_attempts" ]]; do
    set +e
    "$action"
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      return 0
    fi
    if [[ "$rc" -ne 255 || "$attempt" -eq "$max_attempts" ]]; then
      return "$rc"
    fi
    echo "Transport attempt ${attempt}/${max_attempts} failed; retrying in ${delay_seconds}s..." >&2
    sleep "$delay_seconds"
    attempt=$((attempt + 1))
  done
}

cmd=$(cat <<EOF
set -euo pipefail
wait_for_tcp() {
  local addr="\$1"
  local timeout_seconds="\$2"
  local delay_seconds="\$3"
  local host="\${addr%:*}"
  local port="\${addr##*:}"
  local deadline=\$((SECONDS + timeout_seconds))

  echo "waiting up to \${timeout_seconds}s for \${addr}"
  while [[ "\$SECONDS" -lt "\$deadline" ]]; do
    if (echo >"/dev/tcp/\${host}/\${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep "\$delay_seconds"
  done

  echo "timed out waiting for \${addr}" >&2
  return 1
}

wait_for_docker() {
  local deadline=\$((SECONDS + $GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS))
  echo "waiting up to ${GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS}s for Docker on loadgen"
  while [[ "\$SECONDS" -lt "\$deadline" ]]; do
    if command -v docker >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for Docker on loadgen" >&2
  return 1
}

wait_for_docker
token="\$(curl -fsS -H 'Metadata-Flavor: Google' 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' | jq -r .access_token)"
printf '%s' "\$token" | sudo docker login -u oauth2accesstoken --password-stdin "https://$ARTIFACT_HOST"
sudo docker pull "$NOKV_BENCH_IMAGE"
for addr in \\
  10.42.0.11:2380 10.42.0.12:2380 10.42.0.13:2380 \\
  10.42.0.21:2379 10.42.0.22:2379 10.42.0.23:2379 \\
  10.42.0.31:20160 10.42.0.32:20160 10.42.0.33:20160 \\
  10.42.0.41:8090; do
  wait_for_tcp "\$addr" "$GCP_SERVICE_READY_TIMEOUT_SECONDS" "$GCP_SERVICE_READY_RETRY_DELAY_SECONDS"
done
sudo mkdir -p "$remote_dir"
sudo docker run --rm --network host \\
  -v "$remote_dir:/results" \\
  -e NOKV_FSMETA_PROFILE="$NOKV_FSMETA_PROFILE" \\
  -e NOKV_FSMETA_WORKLOADS="$NOKV_FSMETA_WORKLOADS" \\
  -e NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS="${NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS:-}" \\
  -e NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS="${NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS:-}" \\
  -e NOKV_FSMETA_STABILIZE_SECONDS="${NOKV_FSMETA_STABILIZE_SECONDS:-}" \\
  -e NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="$signing_key" \\
  -e NOKV_FSMETA_ADDR="10.42.0.41:8090" \\
  -e NOKV_FSMETA_COORDINATOR_ADDR="10.42.0.21:2379,10.42.0.22:2379,10.42.0.23:2379" \\
  "$NOKV_BENCH_IMAGE"
EOF
)

ssh_loadgen_once() {
  if is_truthy "$GCP_USE_IAP"; then
    gcloud compute ssh "$loadgen" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --tunnel-through-iap \
      --command="$cmd"
  else
    gcloud compute ssh "$loadgen" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --command="$cmd"
  fi
}

scp_results_once() {
  if is_truthy "$GCP_USE_IAP"; then
    gcloud compute scp --recurse "${loadgen}:${remote_dir}" "$local_dir" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --tunnel-through-iap
  else
    gcloud compute scp --recurse "${loadgen}:${remote_dir}" "$local_dir" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT"
  fi
}

retry_transport ssh_loadgen_once

mkdir -p "$local_dir"
retry_transport scp_results_once

echo "Copied benchmark results to: $local_dir"
