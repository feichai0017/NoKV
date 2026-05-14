#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/third_party/tla"

TLA_VERSION="${TLA_VERSION:-1.7.4}"
TLA_SHA1="${TLA_SHA1:-bee4a54f3ee3d4afc347c3240ec2d9e93b075104}"

TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"

mkdir -p "${TOOLS_DIR}"

download() {
    local url="$1"
    local dst="$2"
    local tmp="${dst}.tmp"
    curl -L --fail --retry 3 --retry-delay 1 -o "${tmp}" "${url}"
    verify_sha1 "${tmp}" "${TLA_SHA1}"
    mv "${tmp}" "${dst}"
}

sha1_file() {
    local path="$1"
    if command -v sha1sum >/dev/null 2>&1; then
        sha1sum "${path}" | awk '{print $1}'
    else
        shasum -a 1 "${path}" | awk '{print $1}'
    fi
}

verify_sha1() {
    local path="$1"
    local want="$2"
    local got
    got="$(sha1_file "${path}")"
    if [[ "${got}" != "${want}" ]]; then
        echo "checksum mismatch for ${path}: got ${got}, want ${want}" >&2
        rm -f "${path}"
        exit 1
    fi
}

ensure_tla2tools() {
    local versioned_jar="${TOOLS_DIR}/tla2tools-${TLA_VERSION}.jar"
    local jar_link="${TOOLS_DIR}/tla2tools.jar"
    if [[ ! -f "${versioned_jar}" ]]; then
        echo "Downloading tla2tools.jar v${TLA_VERSION}..."
        download "${TLA_URL}" "${versioned_jar}"
    fi
    verify_sha1 "${versioned_jar}" "${TLA_SHA1}"
    ln -sfn "$(basename "${versioned_jar}")" "${jar_link}"
}

ensure_tla2tools

echo "Installed TLA tooling under ${TOOLS_DIR}"
echo "  TLC: ${TOOLS_DIR}/tla2tools.jar"
