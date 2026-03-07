#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$(go env GOPATH)/bin:${PATH}"

REQUIRED_BUF_VERSION="${REQUIRED_BUF_VERSION:-1.66.0}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required tool '$1' not found in PATH" >&2
    exit 1
  fi
}

assert_version() {
  local name="$1"
  local got="$2"
  local expected="$3"
  if [[ "${got}" != "${expected}" ]]; then
    echo "error: ${name} version mismatch: expected '${expected}', got '${got}'" >&2
    exit 1
  fi
}

require_cmd buf

assert_version "buf" "$(buf --version | awk '{print $NF}')" "${REQUIRED_BUF_VERSION}"

(cd "${ROOT_DIR}" && buf generate)
