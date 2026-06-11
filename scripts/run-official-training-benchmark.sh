#!/usr/bin/env bash
#
# Wrapper for official training-storage harnesses.
#
# This script does not emulate MLPerf Storage or DLIO. It runs the official
# command supplied after `--`, captures stdout/stderr/status, and records the
# NoKV benchmark environment next to those artifacts.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS=""
MOUNT=""
RESULT_DIR=""

usage() {
    cat <<EOF
Usage: scripts/run-official-training-benchmark.sh --harness NAME --mount PATH --result-dir DIR -- COMMAND [args...]

  --harness NAME      label such as mlperf-storage or dlio
  --mount PATH        mounted filesystem path passed via NOKV_OFFICIAL_MOUNT
  --result-dir DIR    output directory for stdout/stderr/status/env/manifest
  --                  separates wrapper options from the official command

Example:
  scripts/run-official-training-benchmark.sh \\
    --harness dlio --mount /mnt/nokv --result-dir bench/results/dlio-nokv -- \\
    /path/to/official/dlio/command --data-dir /mnt/nokv
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    --harness) HARNESS="$2"; shift ;;
    --mount) MOUNT="$2"; shift ;;
    --result-dir) RESULT_DIR="$2"; shift ;;
    --help | -h) usage; exit 0 ;;
    --) shift; break ;;
    *) echo "error: unknown argument before --: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

[[ -n "$HARNESS" ]] || { echo "error: --harness is required" >&2; exit 2; }
[[ -n "$MOUNT" ]] || { echo "error: --mount is required" >&2; exit 2; }
[[ -d "$MOUNT" ]] || { echo "error: mount path does not exist: $MOUNT" >&2; exit 2; }
[[ -n "$RESULT_DIR" ]] || { echo "error: --result-dir is required" >&2; exit 2; }
[[ $# -gt 0 ]] || { echo "error: official command is required after --" >&2; exit 2; }

mkdir -p "$RESULT_DIR"
ENV_ID="${NOKV_BENCH_ENV_ID:-$(date -u +%Y%m%dT%H%M%SZ)-official-${HARNESS}}"
export NOKV_BENCH_ENV_ID="$ENV_ID"
export NOKV_OFFICIAL_MOUNT="$MOUNT"

"${NOKV_BENCH_PYTHON_BIN:-python3}" "$ROOT_DIR/scripts/fs-bench-env.py" \
    --out "$RESULT_DIR/env.json" --env-id "$ENV_ID" --mode "official-${HARNESS}" \
    --profile "${NOKV_BENCH_PROFILE:-official}" --tiers "${NOKV_BENCH_NOKV_TIERS:-external}" \
    --concurrency "${NOKV_BENCH_CONCURRENCY_SWEEP:-external}" \
    --product-workloads "official-${HARNESS}" --primitive-workloads "none" \
    --repeats "${NOKV_BENCH_REPEATS:-1}"

set +e
"$@" >"$RESULT_DIR/stdout.log" 2>"$RESULT_DIR/stderr.log"
STATUS=$?
set -e
printf "%s\n" "$STATUS" >"$RESULT_DIR/status.txt"

"${NOKV_BENCH_PYTHON_BIN:-python3}" - "$RESULT_DIR/manifest.json" "$HARNESS" "$MOUNT" "$ENV_ID" "$STATUS" "$@" <<'PY'
import json
import sys
from pathlib import Path

manifest, harness, mount, env_id, status, *cmd = sys.argv[1:]
doc = {
    "harness": harness,
    "mount": mount,
    "env_id": env_id,
    "status": int(status),
    "command": cmd,
    "artifacts": {
        "stdout": "stdout.log",
        "stderr": "stderr.log",
        "status": "status.txt",
        "env": "env.json",
    },
}
Path(manifest).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
PY

exit "$STATUS"
