#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config_arg="${1:-}"
runtime_config_arg="$config_arg"
smoke_config_file=""

# Distributed smoke validates control-plane and storage-plane failure behavior.
# Keep the VM shape cheap and quota-friendly; the output is correctness signal,
# not performance evidence.
if [[ -z "$config_arg" ]]; then
  smoke_machine_type="${GCP_SMOKE_MACHINE_TYPE:-e2-standard-2}"
  smoke_config_file="$(mktemp "${TMPDIR:-/tmp}/nokv-distributed-smoke-config.XXXXXX")"
  runtime_config_arg="$smoke_config_file"

  {
    if [[ -f "$SCRIPT_DIR/config.env" ]]; then
      printf '. "%s"\n' "$SCRIPT_DIR/config.env"
    fi
    cat <<EOF
GCP_META_ROOT_MACHINE_TYPE=${GCP_META_ROOT_MACHINE_TYPE:-$smoke_machine_type}
GCP_COORDINATOR_MACHINE_TYPE=${GCP_COORDINATOR_MACHINE_TYPE:-$smoke_machine_type}
GCP_STORE_MACHINE_TYPE=${GCP_STORE_MACHINE_TYPE:-$smoke_machine_type}
GCP_GATEWAY_MACHINE_TYPE=${GCP_GATEWAY_MACHINE_TYPE:-$smoke_machine_type}
GCP_LOADGEN_MACHINE_TYPE=${GCP_LOADGEN_MACHINE_TYPE:-$smoke_machine_type}
GCP_USE_COMPACT_PLACEMENT=${GCP_USE_COMPACT_PLACEMENT:-false}
GCP_BENCHMARK_START_GRACE_SECONDS=${GCP_BENCHMARK_START_GRACE_SECONDS:-90}
GCP_SERVICE_READY_TIMEOUT_SECONDS=${GCP_SERVICE_READY_TIMEOUT_SECONDS:-900}
GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS=${GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS:-600}
NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS=${NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS:-180}
NOKV_DISTRIBUTED_SMOKE_BENCH_STABILIZE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_BENCH_STABILIZE_SECONDS:-10}
NOKV_DISTRIBUTED_SMOKE_META_ROOT_SETTLE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_META_ROOT_SETTLE_SECONDS:-20}
NOKV_DISTRIBUTED_SMOKE_COORDINATOR_SETTLE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_COORDINATOR_SETTLE_SECONDS:-40}
NOKV_DISTRIBUTED_SMOKE_STORE_SETTLE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_STORE_SETTLE_SECONDS:-45}
NOKV_DISTRIBUTED_SMOKE_GATEWAY_SETTLE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_GATEWAY_SETTLE_SECONDS:-10}
NOKV_DISTRIBUTED_SMOKE_RESTART_SETTLE_SECONDS=${NOKV_DISTRIBUTED_SMOKE_RESTART_SETTLE_SECONDS:-20}
EOF
  } >"$smoke_config_file"
fi

cleanup() {
  local status=$?
  echo "Destroying distributed smoke VMs to stop compute billing..."
  if [[ -n "$runtime_config_arg" ]]; then
    "$SCRIPT_DIR/destroy-cluster.sh" "$runtime_config_arg" --delete-infra || true
  else
    "$SCRIPT_DIR/destroy-cluster.sh" --delete-infra || true
  fi
  if [[ -n "$smoke_config_file" ]]; then
    rm -f "$smoke_config_file"
  fi
  return "$status"
}
trap cleanup EXIT

"$SCRIPT_DIR/create-cluster.sh" "$runtime_config_arg" || exit $?
"$SCRIPT_DIR/distributed-smoke.sh" "$runtime_config_arg"
