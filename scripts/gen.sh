#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="${ROOT_DIR}/pb"

protoc   --proto_path="${PROTO_DIR}"   --go_out=paths=source_relative:"${PROTO_DIR}"   pb.proto
