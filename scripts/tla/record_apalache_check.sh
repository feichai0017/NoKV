#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ $# -ne 5 ]]; then
    echo "usage: $0 <spec-path> <cfg-path> <inv-list> <length> <output-path>" >&2
    exit 1
fi

SPEC_PATH="$1"
CFG_PATH="$2"
INV_LIST="$3"
CHECK_LENGTH="$4"
OUTPUT_PATH="$5"
TMP_OUTPUT="$(mktemp)"
trap 'rm -f "${TMP_OUTPUT}"' EXIT

cd "${ROOT_DIR}"
./scripts/tla/apalache.sh --features=no-rows check --config="${CFG_PATH}" --no-deadlock --length="${CHECK_LENGTH}" --inv="${INV_LIST}" "${SPEC_PATH}" >"${TMP_OUTPUT}" 2>&1

mkdir -p "$(dirname "${OUTPUT_PATH}")"
{
    echo "# Recorded Apalache bounded-check output"
    echo "spec=${SPEC_PATH}"
    echo "config=${CFG_PATH}"
    echo "invariants=${INV_LIST}"
    echo "length=${CHECK_LENGTH}"
    echo
    grep -E '^(PASS #0:|PASS #1:|PASS #13:|The outcome is: |Checker reports no error up to computation length )' "${TMP_OUTPUT}" || true
} >"${OUTPUT_PATH}"
