#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/third_party/tla"
APALACHE_BIN="${TOOLS_DIR}/apalache/bin/apalache-mc"

if [[ ! -x "${APALACHE_BIN}" ]]; then
    echo "apalache-mc is missing. Run './scripts/tla/setup.sh' or 'make install-tla-tools' first." >&2
    exit 1
fi

cd "${ROOT_DIR}"
exec "${APALACHE_BIN}" "$@"
