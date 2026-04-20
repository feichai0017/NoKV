#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/third_party/tla"

TLA_VERSION="${TLA_VERSION:-1.7.4}"
APALACHE_VERSION="${APALACHE_VERSION:-0.56.1}"

TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
APALACHE_URL="https://github.com/apalache-mc/apalache/releases/download/v${APALACHE_VERSION}/apalache-${APALACHE_VERSION}.tgz"
APALACHE_SHA_URL="https://github.com/apalache-mc/apalache/releases/download/v${APALACHE_VERSION}/sha256sum.txt"

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

ensure_apalache() {
    local versioned_tgz="${TOOLS_DIR}/apalache-${APALACHE_VERSION}.tgz"
    local versioned_dir="${TOOLS_DIR}/apalache-${APALACHE_VERSION}"
    local current_link="${TOOLS_DIR}/apalache"
    if [[ ! -f "${versioned_tgz}" ]]; then
        echo "Downloading Apalache v${APALACHE_VERSION}..."
        download "${APALACHE_URL}" "${versioned_tgz}"
    fi

    local expected actual
    expected="$(curl -L --fail --retry 3 --retry-delay 1 "${APALACHE_SHA_URL}" | awk '/apalache-'"${APALACHE_VERSION}"'\.tgz$/ {print $1}')"
    actual="$(shasum -a 256 "${versioned_tgz}" | awk '{print $1}')"
    if [[ -z "${expected}" || "${actual}" != "${expected}" ]]; then
        echo "Apalache checksum verification failed" >&2
        echo "expected: ${expected:-<missing>}" >&2
        echo "actual:   ${actual}" >&2
        exit 1
    fi

    if [[ ! -d "${versioned_dir}" ]]; then
        echo "Extracting Apalache v${APALACHE_VERSION}..."
        tar -xzf "${versioned_tgz}" -C "${TOOLS_DIR}"
    fi
    ln -sfn "$(basename "${versioned_dir}")" "${current_link}"
}

ensure_tla2tools
ensure_apalache

echo "Installed TLA tooling under ${TOOLS_DIR}"
echo "  TLC:      ${TOOLS_DIR}/tla2tools.jar"
echo "  Apalache: ${TOOLS_DIR}/apalache/bin/apalache-mc"
