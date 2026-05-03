#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <spec-path>" >&2
    exit 1
fi

SPEC_PATH="$1"
TMP_OUTPUT="$(mktemp)"
trap 'rm -f "${TMP_OUTPUT}"' EXIT

cd "${ROOT_DIR}"
if ./scripts/tla/tlc.sh "${SPEC_PATH}" >"${TMP_OUTPUT}" 2>&1; then
    cat "${TMP_OUTPUT}"
    echo "expected TLC to find a counterexample for ${SPEC_PATH}, but it succeeded" >&2
    exit 1
fi

if grep -Eiq 'counterexample|Invariant .* is violated|The behavior up to this point is' "${TMP_OUTPUT}"; then
    cat "${TMP_OUTPUT}"
    exit 0
fi

cat "${TMP_OUTPUT}"
echo "TLC failed without an invariant counterexample for ${SPEC_PATH}" >&2
exit 1
