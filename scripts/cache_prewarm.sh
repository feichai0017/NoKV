#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/logs}"
LOG_FILE="${LOG_FILE:-${LOG_DIR}/cache_prewarm.log}"

mkdir -p "${LOG_DIR}" "${ROOT_DIR}/benchmark_data"

echo "[cache-prewarm] running read-path benchmark with cache variants" | tee "${LOG_FILE}"
(
        cd "${ROOT_DIR}" >/dev/null
        GOCACHE="${GOCACHE:-${ROOT_DIR}/.gocache}" \
        GOMODCACHE="${GOMODCACHE:-${ROOT_DIR}/.gomodcache}" \
        go test ./benchmark -run TestCacheReadScenarios -count=1 -v
) 2>&1 | tee -a "${LOG_FILE}"

echo "[cache-prewarm] results stored in ${LOG_FILE}" | tee -a "${LOG_FILE}"