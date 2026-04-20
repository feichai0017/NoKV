#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <spec-path> <output-path>" >&2
    exit 1
fi

SPEC_PATH="$1"
OUTPUT_PATH="$2"
TMP_OUTPUT="$(mktemp)"
trap 'rm -f "${TMP_OUTPUT}"' EXIT

cd "${ROOT_DIR}"
if ./scripts/tla/tlc.sh -seed 1 "${SPEC_PATH}" >"${TMP_OUTPUT}" 2>&1; then
    STATUS="success"
else
    STATUS="failure"
fi

mkdir -p "$(dirname "${OUTPUT_PATH}")"
{
    echo "# Recorded TLC output"
    echo "spec=${SPEC_PATH}"
    echo "status=${STATUS}"
    echo
    grep -E '^(Error: Invariant|Error: The behavior up to this point is:|State [0-9]+:|/\\ |Model checking completed\. No error has been found\.|[0-9]+ states generated, [0-9]+ distinct states found, [0-9]+ states left on queue\.|The depth of the complete state graph search is [0-9]+\.)' "${TMP_OUTPUT}" || true
} >"${OUTPUT_PATH}"

if [[ "${STATUS}" == "failure" ]]; then
    exit 1
fi
