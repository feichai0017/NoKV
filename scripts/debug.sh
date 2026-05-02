#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/debug.sh <go-test-regexp>" >&2
  exit 2
fi

exec dlv test -- -test.run="$1"
