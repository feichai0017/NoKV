#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CONTROLPLANE_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
BENCH_DIR=$(cd -- "$CONTROLPLANE_DIR/.." && pwd)
REPO_ROOT=$(cd -- "$BENCH_DIR/.." && pwd)

BENCHTIME=${CONTROL_PLANE_BENCHTIME:-500ms}
INPROC_COUNT=${CONTROL_PLANE_INPROC_COUNT:-5}
PROCESS_COUNT=${CONTROL_PLANE_PROCESS_COUNT:-5}
RECOVERY_COUNT=${CONTROL_PLANE_RECOVERY_COUNT:-5}
SUFFIX=${CONTROL_PLANE_RESULT_SUFFIX:-}

stamp=$(date +"%Y%m%d_%H%M%S")
result_dir="$BENCH_DIR/benchmark_results/control_plane/${stamp}${SUFFIX}"
mkdir -p "$result_dir"

echo "control-plane benchmark results -> $result_dir"
echo "benchtime=$BENCHTIME inproc_count=$INPROC_COUNT process_count=$PROCESS_COUNT recovery_count=$RECOVERY_COUNT"

inproc_raw="$result_dir/inprocess_raw.txt"
process_raw="$result_dir/process_raw.txt"
recovery_raw="$result_dir/recovery_raw.txt"
summary_md="$result_dir/summary.md"

(
	cd "$BENCH_DIR"
	go test ./controlplane \
		-run '^$' \
		-bench 'BenchmarkControlPlaneAllocID(Local|Remote|RemoteTCP)Window(Default|One)$' \
		-benchmem \
		-count "$INPROC_COUNT" \
		-benchtime "$BENCHTIME"
) | tee "$inproc_raw"

(
	cd "$BENCH_DIR"
	go test ./controlplane \
		-run '^$' \
		-bench 'BenchmarkControlPlaneProcess(NoKVRemoteTCP|EtcdCAS)Window(Default|One)$' \
		-benchmem \
		-count "$PROCESS_COUNT" \
		-benchtime "$BENCHTIME"
) | tee "$process_raw"

(
	cd "$REPO_ROOT"
	go test ./coordinator/integration \
		-run TestSeparatedModeCoordinatorRecoveryLatency \
		-count "$RECOVERY_COUNT" \
		-v
) | tee "$recovery_raw"

{
	echo "# Control-Plane Evaluation"
	echo
	echo "- Generated: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
	echo "- In-process count: $INPROC_COUNT"
	echo "- Process count: $PROCESS_COUNT"
	echo "- Recovery count: $RECOVERY_COUNT"
	echo "- Benchtime: $BENCHTIME"
	echo
	echo "## In-Process Allocator Table"
	echo
	(
		cd "$BENCH_DIR"
		go run ./controlplane/cmd/controlplane_table -input "$inproc_raw" -suite inprocess
	)
	echo
	echo "## Process-Separated Allocator Table"
	echo
	(
		cd "$BENCH_DIR"
		go run ./controlplane/cmd/controlplane_table -input "$process_raw" -suite process
	)
	echo
	echo "## Recovery Logs"
	echo
	echo '```text'
	grep 'separated coordinator recovery latency:' "$recovery_raw" || true
	echo '```'
	echo
	echo "## Raw Files"
	echo
	echo "- $inproc_raw"
	echo "- $process_raw"
	echo "- $recovery_raw"
} > "$summary_md"

echo "summary -> $summary_md"
