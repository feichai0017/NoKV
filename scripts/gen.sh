#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="${ROOT_DIR}/pb"
export PATH="$(go env GOPATH)/bin:${PATH}"

PROTOBUF_MODULE_VERSION="$(go list -m -f '{{.Version}}' google.golang.org/protobuf)"
REQUIRED_PROTOC_VERSION="${REQUIRED_PROTOC_VERSION:-33.4}"
REQUIRED_PROTOC_GEN_GO_VERSION="${REQUIRED_PROTOC_GEN_GO_VERSION:-${PROTOBUF_MODULE_VERSION}}"
REQUIRED_PROTOC_GEN_GO_GRPC_VERSION="${REQUIRED_PROTOC_GEN_GO_GRPC_VERSION:-1.6.1}"

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

require_cmd protoc
require_cmd protoc-gen-go
require_cmd protoc-gen-go-grpc

assert_version "protoc" "$(protoc --version | awk '{print $2}')" "${REQUIRED_PROTOC_VERSION}"
assert_version "protoc-gen-go" "$(protoc-gen-go --version | awk '{print $2}')" "${REQUIRED_PROTOC_GEN_GO_VERSION}"
assert_version "protoc-gen-go-grpc" "$(protoc-gen-go-grpc --version | awk '{print $2}')" "${REQUIRED_PROTOC_GEN_GO_GRPC_VERSION}"

protoc \
  --proto_path="${PROTO_DIR}" \
  --go_out=paths=source_relative:"${PROTO_DIR}" \
  metapb.proto \
  storagepb.proto \
  kvrpcpb.proto \
  pdpb.proto \
  raftcmdpb.proto

protoc \
  --proto_path="${PROTO_DIR}" \
  --go-grpc_out=paths=source_relative,require_unimplemented_servers=false:"${PROTO_DIR}" \
  kvrpcpb.proto \
  pdpb.proto
