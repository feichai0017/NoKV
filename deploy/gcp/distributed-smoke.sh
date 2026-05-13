#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

load_config "${1:-}"
load_last_images
require_cmd gcloud

if [[ -z "${NOKV_IMAGE:-}" || -z "${NOKV_BENCH_IMAGE:-}" ]]; then
  die "NOKV_IMAGE and NOKV_BENCH_IMAGE are required; run deploy/gcp/build-push-images.sh first"
fi

signing_key_file="$SCRIPT_DIR/generated/eunomia-signing-key.txt"
if [[ -f "$signing_key_file" ]]; then
  signing_key="$(tr -d '\n' <"$signing_key_file")"
else
  signing_key="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-}"
fi

if [[ -z "$signing_key" ]]; then
  die "No signing key found; run deploy/gcp/create-cluster.sh first or set NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY"
fi

loadgen="${GCP_CLUSTER_NAME}-loadgen-1"
remote_helper="/tmp/nokv-distributed-smoke-loadgen.sh"
local_helper="$SCRIPT_DIR/bin/distributed-smoke-loadgen.sh"
run_id="distributed-$(date -u +%Y%m%dT%H%M%SZ)"
remote_dir="/mnt/nokv/results/${run_id}"
local_dir="$SCRIPT_DIR/results/${run_id}"
results_copied=0

all_coord_addr="10.42.0.21:2379,10.42.0.22:2379,10.42.0.23:2379"

shell_quote() {
  printf "'"
  printf '%s' "${1:-}" | sed "s/'/'\\\\''/g"
  printf "'"
}

ssh_instance() {
  local instance="$1"
  local remote_cmd="$2"
  if is_truthy "$GCP_USE_IAP"; then
    gcloud compute ssh "$instance" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --tunnel-through-iap \
      --command="$remote_cmd"
  else
    gcloud compute ssh "$instance" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --command="$remote_cmd"
  fi
}

scp_to_instance() {
  local source="$1"
  local target="$2"
  if is_truthy "$GCP_USE_IAP"; then
    gcloud compute scp "$source" "$target" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --tunnel-through-iap
  else
    gcloud compute scp "$source" "$target" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT"
  fi
}

scp_from_instance() {
  local source="$1"
  local target="$2"
  if is_truthy "$GCP_USE_IAP"; then
    gcloud compute scp --recurse "$source" "$target" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --tunnel-through-iap
  else
    gcloud compute scp --recurse "$source" "$target" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT"
  fi
}

retry_transport() {
  local label="$1"
  shift

  local attempt=1
  local rc=0
  while [[ "$attempt" -le "$GCP_TRANSPORT_RETRIES" ]]; do
    set +e
    "$@"
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      return 0
    fi
    if [[ "$rc" -ne 255 || "$attempt" -eq "$GCP_TRANSPORT_RETRIES" ]]; then
      return "$rc"
    fi
    echo "${label}: transport attempt ${attempt}/${GCP_TRANSPORT_RETRIES} failed; retrying in ${GCP_TRANSPORT_RETRY_DELAY_SECONDS}s..." >&2
    sleep "$GCP_TRANSPORT_RETRY_DELAY_SECONDS"
    attempt=$((attempt + 1))
  done
}

copy_results_once() {
  if [[ "$results_copied" -eq 1 ]]; then
    return 0
  fi
  mkdir -p "$local_dir"
  retry_transport "copy distributed smoke results" scp_from_instance "${loadgen}:${remote_dir}" "$local_dir"
  results_copied=1
}

copy_partial_results_on_failure() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    echo "Distributed smoke failed; attempting to copy partial results before destroy..."
    copy_results_once || true
  fi
  return "$status"
}
trap copy_partial_results_on_failure EXIT

env_assign() {
  local name="$1"
  local value="$2"
  printf '%s=%s ' "$name" "$(shell_quote "$value")"
}

remote_env_prefix() {
  local action="$1"
  local phase="$2"
  local workloads="$3"
  local coord_addr="$4"
  local skip_meta_root="$5"
  local skip_coord="$6"
  local skip_store="$7"

  printf 'env '
  env_assign NOKV_IMAGE "$NOKV_IMAGE"
  env_assign NOKV_BENCH_IMAGE "$NOKV_BENCH_IMAGE"
  env_assign ARTIFACT_HOST "$ARTIFACT_HOST"
  env_assign NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY "$signing_key"
  env_assign NOKV_DISTRIBUTED_SMOKE_RUN_ID "$run_id"
  env_assign NOKV_DISTRIBUTED_SMOKE_PHASE "$phase"
  env_assign NOKV_DISTRIBUTED_SMOKE_SKIP_META_ROOT "$skip_meta_root"
  env_assign NOKV_DISTRIBUTED_SMOKE_SKIP_COORD "$skip_coord"
  env_assign NOKV_DISTRIBUTED_SMOKE_SKIP_STORE "$skip_store"
  env_assign NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS "${NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS:-180}"
  env_assign GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS "$GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS"
  env_assign GCP_SERVICE_READY_TIMEOUT_SECONDS "$GCP_SERVICE_READY_TIMEOUT_SECONDS"
  env_assign GCP_SERVICE_READY_RETRY_DELAY_SECONDS "$GCP_SERVICE_READY_RETRY_DELAY_SECONDS"
  env_assign NOKV_FSMETA_PROFILE "smoke"
  env_assign NOKV_FSMETA_WORKLOADS "$workloads"
  env_assign NOKV_FSMETA_COORDINATOR_ADDR "$coord_addr"
  env_assign NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS "${NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS:-300}"
  env_assign NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS "${NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS:-1}"
  env_assign NOKV_FSMETA_STABILIZE_SECONDS "${NOKV_DISTRIBUTED_SMOKE_BENCH_STABILIZE_SECONDS:-10}"
  env_assign NOKV_FSMETA_TIMEOUT "${NOKV_DISTRIBUTED_SMOKE_BENCH_TIMEOUT:-5m}"
  env_assign NOKV_DISTRIBUTED_SMOKE_ACTION "$action"
}

run_loadgen_action() {
  local action="$1"
  local phase="$2"
  local workloads="${3:-mixed}"
  local coord_addr="${4:-$all_coord_addr}"
  local skip_meta_root="${5:-}"
  local skip_coord="${6:-}"
  local skip_store="${7:-}"

  local prefix
  local cmd
  prefix="$(remote_env_prefix "$action" "$phase" "$workloads" "$coord_addr" "$skip_meta_root" "$skip_coord" "$skip_store")"
  cmd="${prefix}/bin/bash $(shell_quote "$remote_helper") $(shell_quote "$action")"
  retry_transport "loadgen ${action}:${phase}" ssh_instance "$loadgen" "$cmd"
}

capture_loadgen_action() {
  local action="$1"
  local phase="$2"
  local workloads="${3:-mixed}"
  local coord_addr="${4:-$all_coord_addr}"
  local skip_meta_root="${5:-}"
  local skip_coord="${6:-}"
  local skip_store="${7:-}"

  local prefix
  local cmd
  local tmp
  local rc
  prefix="$(remote_env_prefix "$action" "$phase" "$workloads" "$coord_addr" "$skip_meta_root" "$skip_coord" "$skip_store")"
  cmd="${prefix}/bin/bash $(shell_quote "$remote_helper") $(shell_quote "$action")"
  tmp="$(mktemp "${TMPDIR:-/tmp}/nokv-distributed-smoke-capture.XXXXXX")"
  set +e
  retry_transport "loadgen ${action}:${phase}" ssh_instance "$loadgen" "$cmd" >"$tmp"
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    rm -f "$tmp"
    return "$rc"
  fi
  cat "$tmp"
  rm -f "$tmp"
}

run_bench_phase() {
  local phase="$1"
  local workloads="$2"
  local coord_addr="${3:-$all_coord_addr}"
  local skip_meta_root="${4:-}"
  local skip_coord="${5:-}"
  local skip_store="${6:-}"
  run_loadgen_action "run-bench" "$phase" "$workloads" "$coord_addr" "$skip_meta_root" "$skip_coord" "$skip_store"
}

coord_addr_excluding() {
  case "$1" in
    coord-1|1) printf '10.42.0.22:2379,10.42.0.23:2379\n' ;;
    coord-2|2) printf '10.42.0.21:2379,10.42.0.23:2379\n' ;;
    coord-3|3) printf '10.42.0.21:2379,10.42.0.22:2379\n' ;;
    *) printf '%s\n' "$all_coord_addr" ;;
  esac
}

node_ordinal() {
  printf '%s\n' "${1##*-}"
}

stop_container() {
  local instance="$1"
  local container="$2"
  local timeout_seconds="$3"
  local cmd
  cmd="sudo docker stop -t $(shell_quote "$timeout_seconds") $(shell_quote "$container")"
  retry_transport "stop ${container} on ${instance}" ssh_instance "$instance" "$cmd"
}

start_container() {
  local instance="$1"
  local container="$2"
  local cmd
  cmd="sudo docker start $(shell_quote "$container")"
  retry_transport "start ${container} on ${instance}" ssh_instance "$instance" "$cmd"
}

restart_container() {
  local instance="$1"
  local container="$2"
  local timeout_seconds="$3"
  local cmd
  cmd="sudo docker restart -t $(shell_quote "$timeout_seconds") $(shell_quote "$container")"
  retry_transport "restart ${container} on ${instance}" ssh_instance "$instance" "$cmd"
}

settle() {
  local label="$1"
  local seconds="$2"
  if [[ "$seconds" != "0" ]]; then
    echo "${label}: waiting ${seconds}s"
    sleep "$seconds"
  fi
}

echo "Distributed smoke run id: $run_id"

if [[ "$GCP_BENCHMARK_START_GRACE_SECONDS" != "0" ]]; then
  settle "Initial cold-start grace" "$GCP_BENCHMARK_START_GRACE_SECONDS"
fi

retry_transport "copy distributed smoke helper" scp_to_instance "$local_helper" "${loadgen}:${remote_helper}"
retry_transport "chmod distributed smoke helper" ssh_instance "$loadgen" "chmod +x $(shell_quote "$remote_helper")"

run_loadgen_action "prepare" "prepare"
run_loadgen_action "wait-all" "initial-wait"
run_loadgen_action "assert-meta-root" "initial-meta-root"
run_loadgen_action "assert-coordinator-grant" "initial-coordinator-grant"
run_loadgen_action "assert-store-execution" "initial-store-execution"

run_bench_phase "baseline" "mixed"

meta_root_leader="$(capture_loadgen_action "meta-root-leader" "detect-meta-root-leader" | awk '/^meta-root-[0-9]+$/ {line=$0} END {if (line == "") exit 1; print line}')"
meta_root_id="$(node_ordinal "$meta_root_leader")"
meta_root_instance="${GCP_CLUSTER_NAME}-meta-root-${meta_root_id}"
echo "Injecting meta-root leader fault: ${meta_root_leader}"
stop_container "$meta_root_instance" "nokv-meta-root" 45
settle "Meta-root leader election" "${NOKV_DISTRIBUTED_SMOKE_META_ROOT_SETTLE_SECONDS:-20}"
run_loadgen_action "assert-meta-root" "meta-root-leader-down" "mixed" "$all_coord_addr" "$meta_root_leader"
run_bench_phase "meta-root-leader-down" "mixed" "$all_coord_addr" "$meta_root_leader"
start_container "$meta_root_instance" "nokv-meta-root"
settle "Meta-root restart" "${NOKV_DISTRIBUTED_SMOKE_RESTART_SETTLE_SECONDS:-20}"
run_loadgen_action "wait-all" "meta-root-restored"
run_loadgen_action "assert-meta-root" "meta-root-restored"

coord_holder="$(capture_loadgen_action "coordinator-holder" "detect-coordinator-holder" | awk '/^coord-[0-9]+$/ {line=$0} END {if (line == "") exit 1; print line}')"
coord_id="$(node_ordinal "$coord_holder")"
coord_instance="${GCP_CLUSTER_NAME}-coordinator-${coord_id}"
live_coord_addr="$(coord_addr_excluding "$coord_holder")"
echo "Injecting coordinator holder fault: ${coord_holder}"
stop_container "$coord_instance" "nokv-coordinator" 45
settle "Coordinator grant handoff" "${NOKV_DISTRIBUTED_SMOKE_COORDINATOR_SETTLE_SECONDS:-40}"
run_loadgen_action "assert-coordinator-grant" "coordinator-holder-down" "mixed" "$live_coord_addr" "" "$coord_holder"
run_bench_phase "coordinator-holder-down" "mixed,negative-lookup" "$live_coord_addr" "" "$coord_holder"
start_container "$coord_instance" "nokv-coordinator"
settle "Coordinator restart" "${NOKV_DISTRIBUTED_SMOKE_RESTART_SETTLE_SECONDS:-20}"
run_loadgen_action "wait-all" "coordinator-restored"
run_loadgen_action "assert-coordinator-grant" "coordinator-restored"

store_id="${NOKV_DISTRIBUTED_SMOKE_STORE_FAULT_ID:-3}"
store_node="store-${store_id}"
store_instance="${GCP_CLUSTER_NAME}-store-${store_id}"
echo "Injecting store fault: ${store_node}"
stop_container "$store_instance" "nokv-store" 30
settle "Store raft failover" "${NOKV_DISTRIBUTED_SMOKE_STORE_SETTLE_SECONDS:-45}"
run_loadgen_action "assert-store-execution" "store-down" "mixed" "$all_coord_addr" "" "" "$store_node"
run_bench_phase "store-${store_id}-down" "mixed,hotspot-fanin,negative-lookup" "$all_coord_addr" "" "" "$store_node"
start_container "$store_instance" "nokv-store"
settle "Store restart" "${NOKV_DISTRIBUTED_SMOKE_RESTART_SETTLE_SECONDS:-20}"
run_loadgen_action "wait-all" "store-restored"
run_loadgen_action "assert-store-execution" "store-restored"

echo "Restarting gateway"
restart_container "${GCP_CLUSTER_NAME}-gateway-1" "nokv-fsmeta" 30
settle "Gateway restart" "${NOKV_DISTRIBUTED_SMOKE_GATEWAY_SETTLE_SECONDS:-10}"
run_loadgen_action "wait-all" "gateway-restored"
run_bench_phase "gateway-restart" "negative-lookup"

run_loadgen_action "assert-meta-root" "final-meta-root"
run_loadgen_action "assert-coordinator-grant" "final-coordinator-grant"
run_loadgen_action "assert-store-execution" "final-store-execution"
run_bench_phase "final" "mixed"

copy_results_once

echo "Distributed smoke passed."
echo "Copied distributed smoke results to: $local_dir"
