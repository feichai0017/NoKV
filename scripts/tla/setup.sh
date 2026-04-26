#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/third_party/tla"

TLA_VERSION="${TLA_VERSION:-1.7.4}"

TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"

mkdir -p "${TOOLS_DIR}"

download() {
    local url="$1"
    local dst="$2"
    local tmp="${dst}.tmp"
    curl -L --fail --retry 3 --retry-delay 1 -o "${tmp}" "${url}"
    mv "${tmp}" "${dst}"
}

ensure_tla2tools() {
    local versioned_jar="${TOOLS_DIR}/tla2tools-${TLA_VERSION}.jar"
    local jar_link="${TOOLS_DIR}/tla2tools.jar"
    if [[ ! -f "${versioned_jar}" ]]; then
        echo "Downloading tla2tools.jar v${TLA_VERSION}..."
        download "${TLA_URL}" "${versioned_jar}"
    fi
    ln -sfn "$(basename "${versioned_jar}")" "${jar_link}"
}

ensure_tla2tools

echo "Installed TLA tooling under ${TOOLS_DIR}"
echo "  TLC: ${TOOLS_DIR}/tla2tools.jar"
