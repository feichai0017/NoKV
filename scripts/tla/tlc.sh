#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/third_party/tla"
TLA2TOOLS_JAR="${TOOLS_DIR}/tla2tools.jar"

if [[ ! -f "${TLA2TOOLS_JAR}" ]]; then
    echo "tla2tools.jar is missing. Run './scripts/tla/setup.sh' or 'make install-tla-tools' first." >&2
    exit 1
fi

cd "${ROOT_DIR}"
SPEC_PATH="${1:-}"
SPEC_NAME="spec"
if [[ -n "${SPEC_PATH}" ]]; then
    SPEC_NAME="$(basename "${SPEC_PATH}" .tla)"
fi

STATE_ROOT="${ROOT_DIR}/.tlc-states"
mkdir -p "${STATE_ROOT}"
META_DIR="$(mktemp -d "${STATE_ROOT}/${SPEC_NAME}-XXXXXX")"

exec java -cp "${TLA2TOOLS_JAR}" tlc2.TLC -metadir "${META_DIR}" "$@"
