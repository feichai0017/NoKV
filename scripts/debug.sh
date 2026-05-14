#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/debug.sh <go-test-regexp>" >&2
  exit 2
fi

exec dlv test -- -test.run="$1"
