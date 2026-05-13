#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config_arg="${1:-}"
runtime_config_arg="$config_arg"
smoke_config_file=""

# Smoke is a deployment-chain check, not a lab-grade performance run. Keep it
# outside the narrow C4 quota so failures are about NoKV wiring, not benchmark
# machine-family capacity.
if [[ -z "$config_arg" ]]; then
  smoke_machine_type="${GCP_SMOKE_MACHINE_TYPE:-e2-standard-2}"
  smoke_config_file="$(mktemp "${TMPDIR:-/tmp}/nokv-smoke-config.XXXXXX")"
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
NOKV_FSMETA_STABILIZE_SECONDS=${NOKV_FSMETA_STABILIZE_SECONDS:-15}
EOF
  } >"$smoke_config_file"
fi

cleanup() {
  local status=$?
  echo "Destroying benchmark VMs to stop compute billing..."
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

NOKV_FSMETA_PROFILE=smoke \
NOKV_FSMETA_WORKLOADS="${NOKV_FSMETA_WORKLOADS:-mixed,hotspot-fanin,negative-lookup}" \
"$SCRIPT_DIR/run-fsmeta-benchmark.sh" "$runtime_config_arg"
