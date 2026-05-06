#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mode="${NOKV_FSMETA_BENCH_MODE:-compose}"
profile="${NOKV_FSMETA_PROFILE:-medium}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
coord_addr="${NOKV_FSMETA_COORDINATOR_ADDR:-127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392}"
fsmeta_addr="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
mount="${NOKV_FSMETA_MOUNT:-fsmeta-bench}"
wait_attempts="${NOKV_FSMETA_WAIT_ATTEMPTS:-180}"
wait_interval="${NOKV_FSMETA_WAIT_INTERVAL:-1}"
output_dir="${NOKV_FSMETA_OUTPUT_DIR:-$ROOT/benchmark/data/fsmeta/results}"

case "$profile" in
	medium)
		default_clients=8
		default_dirs=8
		default_files_per_dir=128
		default_files=1024
		default_reads=128
		default_groups=4
		default_entries_per_group=16
		default_artifacts_per_entry=6
		default_session_ttl=2s
		default_timeout=10m
		default_stabilize_seconds=15
		;;
	long)
		default_clients=12
		default_dirs=16
		default_files_per_dir=256
		default_files=4096
		default_reads=256
		default_groups=8
		default_entries_per_group=32
		default_artifacts_per_entry=8
		default_session_ttl=2s
		default_timeout=90m
		default_stabilize_seconds=30
		;;
	*)
		echo "unknown NOKV_FSMETA_PROFILE=$profile; use medium or long" >&2
		exit 2
		;;
esac

clients="${NOKV_FSMETA_CLIENTS:-$default_clients}"
dirs="${NOKV_FSMETA_DIRS:-$default_dirs}"
files_per_dir="${NOKV_FSMETA_FILES_PER_DIR:-$default_files_per_dir}"
files="${NOKV_FSMETA_FILES:-$default_files}"
reads="${NOKV_FSMETA_READS_PER_CLIENT:-$default_reads}"
groups="${NOKV_FSMETA_GROUPS:-$default_groups}"
entries_per_group="${NOKV_FSMETA_ENTRIES_PER_GROUP:-$default_entries_per_group}"
artifacts_per_entry="${NOKV_FSMETA_ARTIFACTS_PER_ENTRY:-$default_artifacts_per_entry}"
session_ttl="${NOKV_FSMETA_SESSION_TTL:-$default_session_ttl}"
timeout="${NOKV_FSMETA_TIMEOUT:-$default_timeout}"
stabilize_seconds="${NOKV_FSMETA_STABILIZE_SECONDS:-$default_stabilize_seconds}"

case "$output_dir" in
	/*) ;;
	*) output_dir="$ROOT/$output_dir" ;;
esac
mkdir -p "$output_dir"

wait_port() {
	local addr="$1"
	local host="${addr%:*}"
	local port="${addr##*:}"
	for _ in $(seq 1 "$wait_attempts"); do
		if nc -z "$host" "$port" >/dev/null 2>&1; then
			return 0
		fi
		sleep "$wait_interval"
	done
	echo "timed out waiting for $addr" >&2
	return 1
}

run_bench() {
	local addr="$1"
	local workloads="$2"
	local output="$3"
	echo "running fsmeta workloads: $workloads -> $output"
	(
		cd benchmark
		NOKV_FSMETA_BENCH=1 go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -v -timeout "$timeout" -args \
			-fsmeta_addr "$addr" \
			-fsmeta_coordinator_addr "$coord_addr" \
			-fsmeta_mount "$mount" \
			-fsmeta_workloads "$workloads" \
			-fsmeta_clients "$clients" \
			-fsmeta_dirs "$dirs" \
			-fsmeta_files_per_dir "$files_per_dir" \
			-fsmeta_files "$files" \
			-fsmeta_reads_per_client "$reads" \
			-fsmeta_groups "$groups" \
			-fsmeta_entries_per_group "$entries_per_group" \
			-fsmeta_artifacts_per_entry "$artifacts_per_entry" \
			-fsmeta_session_ttl "$session_ttl" \
			-fsmeta_timeout "$timeout" \
			-fsmeta_output "$output"
	)
}

run_compose_benchmarks() {
	local workloads="${NOKV_FSMETA_WORKLOADS:-mixed,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_compose_${profile}_${run_id}.csv}"
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" == "1" ]]; then
		if [[ "${NOKV_FSMETA_COMPOSE_BUILD:-1}" == "1" ]]; then
			echo "starting Docker Compose NoKV cluster with local build"
			docker compose up -d --build
		else
			echo "starting Docker Compose NoKV cluster"
			docker compose up -d
		fi
		wait_port "${fsmeta_addr%%,*}"
		wait_port "${coord_addr%%,*}"
		if [[ "$stabilize_seconds" != "0" ]]; then
			# A listening gRPC port is not enough for a fair storage benchmark:
			# freshly started Compose clusters can still be electing Raft leaders
			# and publishing coordinator duty grants.
			echo "waiting ${stabilize_seconds}s for raft leaders and coordinator grants to settle"
			sleep "$stabilize_seconds"
		fi
	fi
	run_bench "$fsmeta_addr" "$workloads" "$output"
	echo "wrote fsmeta benchmark summary: $output"
	if [[ "${NOKV_FSMETA_COMPOSE_DOWN:-0}" == "1" ]]; then
		docker compose down -v
	fi
}

run_derived_cache_benchmarks() {
	local plain_addr="${NOKV_FSMETA_PLAIN_ADDR:-127.0.0.1:8090}"
	local cached_addr="${NOKV_FSMETA_CACHED_ADDR:-127.0.0.1:8091}"
	local tmp_dir="${NOKV_FSMETA_CACHE_TMPDIR:-$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-cache.XXXXXX")}"
	local plain_pid=""
	local cached_pid=""
	cleanup_cache_gateways() {
		if [[ -n "$plain_pid" ]]; then
			kill "$plain_pid" 2>/dev/null || true
		fi
		if [[ -n "$cached_pid" ]]; then
			kill "$cached_pid" 2>/dev/null || true
		fi
		wait "$plain_pid" "$cached_pid" 2>/dev/null || true
		if [[ -z "${NOKV_FSMETA_CACHE_TMPDIR:-}" ]]; then
			rm -rf "$tmp_dir"
		fi
	}
	trap cleanup_cache_gateways EXIT

	mkdir -p "$tmp_dir/plain" "$tmp_dir/cached" "$tmp_dir/negative" "$tmp_dir/dirpage"
	echo "starting plain fsmeta gateway on $plain_addr"
	go run ./cmd/nokv-fsmeta \
		--addr "$plain_addr" \
		--coordinator-addr "$coord_addr" \
		>"$tmp_dir/plain/fsmeta.log" 2>&1 &
	plain_pid="$!"
	wait_port "$plain_addr"

	echo "starting cached fsmeta gateway on $cached_addr"
	go run ./cmd/nokv-fsmeta \
		--addr "$cached_addr" \
		--coordinator-addr "$coord_addr" \
		--negative-cache-dir "$tmp_dir/negative" \
		--dirpage-cache-dir "$tmp_dir/dirpage" \
		>"$tmp_dir/cached/fsmeta.log" 2>&1 &
	cached_pid="$!"
	wait_port "$cached_addr"

	run_bench "$plain_addr" "hotspot-fanin,negative-lookup" "$output_dir/fsmeta_derived_cache_off_${run_id}.csv"
	run_bench "$cached_addr" "hotspot-fanin,negative-lookup" "$output_dir/fsmeta_derived_cache_on_${run_id}.csv"
	echo "done"
}

case "$mode" in
	compose|mixed)
		run_compose_benchmarks
		;;
	derived-cache|cache)
		run_derived_cache_benchmarks
		;;
	*)
		echo "unknown NOKV_FSMETA_BENCH_MODE=$mode; use compose or derived-cache" >&2
		exit 2
		;;
esac
