#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CACHE_DIR="${GOCACHE:-${ROOT_DIR}/.gocache}"
export GOCACHE="${CACHE_DIR}"
export CHAOS_TRACE_METRICS=${CHAOS_TRACE_METRICS:-1}

LOG_DIR="${ROOT_DIR}/artifacts/transport"
mkdir -p "${LOG_DIR}"

SCENARIOS=(
  "TestGRPCTransportHandlesPartition"
  "TestGRPCTransportMetricsWatchdog"
  "TestGRPCTransportMetricsBlockedPeers"
)

run_scenario() {
  local name="$1"
  echo "==> Running ${name}"
  local log_file="${LOG_DIR}/${name}.log"
  if go test -run "^${name}$" -count=1 -v ./raftstore/transport | tee "${log_file}"; then
    echo "-- ${name} chaos metrics --"
    if ! grep -E "TRANSPORT_METRIC" "${log_file}"; then
      echo "   (no TRANSPORT_METRIC lines emitted)"
    fi
  else
    echo "Scenario ${name} failed. Review ${log_file} for details." >&2
    return 1
  fi
}

for scenario in "${SCENARIOS[@]}"; do
  run_scenario "${scenario}"
done

echo "gRPC transport chaos scenarios completed."
