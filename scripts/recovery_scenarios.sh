#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CACHE_DIR="${GOCACHE:-${ROOT_DIR}/.gocache}"
export GOCACHE="${CACHE_DIR}"
export RECOVERY_TRACE_METRICS=1

LOG_DIR="${ROOT_DIR}/artifacts/recovery"
mkdir -p "${LOG_DIR}"

SCENARIOS=(
  "TestRecoveryRemovesStaleValueLogSegment"
  "TestRecoveryCleansMissingSSTFromManifest"
  "TestRecoveryManifestRewriteCrash"
  "TestRecoverySlowFollowerSnapshotBacklog"
  "TestRecoverySnapshotExportRoundTrip"
  "TestRecoveryWALReplayRestoresData"
)

run_scenario() {
  local name="$1"
  echo "==> Running ${name}"
  local log_file="${LOG_DIR}/${name}.log"
  if go test -run "^${name}$" -count=1 -v ./... | tee "${log_file}"; then
    echo "-- ${name} metrics --"
    if ! grep -F "RECOVERY_METRIC" "${log_file}"; then
      echo "   (no RECOVERY_METRIC lines emitted)"
    fi
  else
    echo "Test ${name} failed. See ${log_file} for details." >&2
    return 1
  fi
}

for scenario in "${SCENARIOS[@]}"; do
  run_scenario "${scenario}"
done

echo "All recovery scenarios completed successfully."
