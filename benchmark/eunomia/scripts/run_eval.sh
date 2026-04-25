#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
EUNOMIA_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
BENCH_DIR=$(cd -- "$EUNOMIA_DIR/.." && pwd)
REPO_ROOT=$(cd -- "$BENCH_DIR/.." && pwd)

BENCHTIME=${CONTROL_PLANE_BENCHTIME:-500ms}
PERF_COUNT=${CONTROL_PLANE_PERF_COUNT:-5}
RECOVERY_COUNT=${CONTROL_PLANE_RECOVERY_COUNT:-5}
SUFFIX=${CONTROL_PLANE_RESULT_SUFFIX:-}

stamp=$(date +"%Y%m%d_%H%M%S")
result_dir="$EUNOMIA_DIR/results/${stamp}${SUFFIX}"
mkdir -p "$result_dir"

echo "eunomia benchmark results -> $result_dir"
echo "benchtime=$BENCHTIME perf_count=$PERF_COUNT recovery_count=$RECOVERY_COUNT"

recovery_raw="$result_dir/recovery_raw.txt"
witness_raw="$result_dir/witness_raw.txt"
ablation_raw="$result_dir/ablation_raw.txt"
etcd_raw="$result_dir/etcd_raw.txt"
crdb_raw="$result_dir/crdb_66562_raw.txt"
summary_md="$result_dir/summary.md"

(
	cd "$REPO_ROOT"
	go test ./coordinator/integration \
		-run TestSeparatedModeCoordinatorRecoveryLatency \
		-count "$RECOVERY_COUNT" \
		-v
) | tee "$recovery_raw"

(
	cd "$BENCH_DIR"
	go test ./eunomia \
		-run '^$' \
		-bench 'BenchmarkControlPlane(AllocID|TSO|Metadata)WitnessTax$' \
		-benchmem \
		-count "$PERF_COUNT" \
		-benchtime "$BENCHTIME"
) | tee "$witness_raw"

(
	cd "$BENCH_DIR"
	go test ./eunomia \
		-run TestControlPlaneDetachedAblationRunner \
		-count 1 \
		-v
) | tee "$ablation_raw"

(
	cd "$BENCH_DIR"
	go test ./eunomia/crdb \
		-run 'TestControlPlaneCRDB66562(IssueSchedule|RootedGate)' \
		-count 1 \
		-v
) | tee "$crdb_raw"

(
	cd "$BENCH_DIR"
	go test ./eunomia/etcd \
		-run 'TestControlPlaneEtcd(ReadIndex(Pilot|RealDelayedInFlightReply)|LeaseKeepAliveBufferedSuccessAfterRevoke(WithWitnessGate)?)' \
		-count 1 \
		-v
) | tee "$etcd_raw"

(
	cd "$BENCH_DIR"
	go test ./eunomia \
		-run 'TestControlPlaneNoKVLateReplyControl' \
		-count 1 \
		-v
) | tee -a "$etcd_raw"

{
	echo "# Control-Plane Evaluation"
	echo
	echo "- Generated: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
	echo "- Perf count: $PERF_COUNT"
	echo "- Recovery count: $RECOVERY_COUNT"
	echo "- Benchtime: $BENCHTIME"
	echo
	echo "## Recovery Logs"
	echo
	echo '```text'
	grep 'separated coordinator recovery latency:' "$recovery_raw" || true
	echo '```'
	echo
	echo "## Witness Tax Logs"
	echo
	echo '```text'
	grep 'BenchmarkControlPlane.*WitnessTax' "$witness_raw" || true
	echo '```'
	echo
	echo "## Detached Ablation Logs"
	echo
	echo '```text'
	grep 'detached_ablation' "$ablation_raw" || true
	echo '```'
	echo
	echo "## CRDB Issue Logs"
	echo
	echo '```text'
	grep 'crdb_66562' "$crdb_raw" || true
	echo '```'
	echo
	echo "## Etcd Issue Logs"
	echo
	echo '```text'
	grep -E 'etcd_pilot|etcd_delayed_capture|etcd_lease_buffered|etcd_lease_buffered_with_gate|etcd_wal|control_late_reply' "$etcd_raw" || true
	echo '```'
	echo
	echo "## Raw Files"
	echo
	echo "- $recovery_raw"
	echo "- $witness_raw"
	echo "- $ablation_raw"
	echo "- $crdb_raw"
	echo "- $etcd_raw"
} > "$summary_md"

echo "summary -> $summary_md"
