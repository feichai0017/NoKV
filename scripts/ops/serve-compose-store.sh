#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"

impl=${NOKV_RAFTSTORE_IMPL:-${NOKV_STORE_IMPL:-go}}

case "$impl" in
  go|"")
    exec "$SCRIPT_DIR/serve-store.sh" "$@"
    ;;
  rust)
    ;;
  *)
    nokv_die "serve-compose-store.sh: NOKV_RAFTSTORE_IMPL must be go or rust, got $impl"
    ;;
esac

args=()
has_region_selector=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all-regions)
      has_region_selector=1
      args+=("$1")
      shift
      ;;
    --region-id)
      if [[ $# -lt 2 ]]; then
        nokv_die "serve-compose-store.sh: --region-id requires a value"
      fi
      has_region_selector=1
      args+=("$1" "$2")
      shift 2
      ;;
    --extra|--)
      if [[ "$has_region_selector" -eq 0 ]]; then
        args+=(--all-regions)
        has_region_selector=1
      fi
      args+=("$@")
      exec "$SCRIPT_DIR/serve-rust-store.sh" "${args[@]}"
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

if [[ "$has_region_selector" -eq 0 ]]; then
  args+=(--all-regions)
fi

exec "$SCRIPT_DIR/serve-rust-store.sh" "${args[@]}"
