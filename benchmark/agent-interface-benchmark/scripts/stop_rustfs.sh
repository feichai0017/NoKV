#!/usr/bin/env bash
set -euo pipefail

container="${YANEX_RUSTFS_CONTAINER:-yanex-demo-rustfs}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to stop RustFS" >&2
  exit 1
fi

if docker container inspect "${container}" >/dev/null 2>&1; then
  docker stop "${container}" >/dev/null
  echo "stopped ${container}"
else
  echo "container ${container} does not exist"
fi
