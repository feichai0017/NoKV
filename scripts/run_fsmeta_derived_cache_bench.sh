#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

coord_addr="${NOKV_FSMETA_COORDINATOR_ADDR:-127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392}"
plain_addr="${NOKV_FSMETA_PLAIN_ADDR:-127.0.0.1:8090}"
cached_addr="${NOKV_FSMETA_CACHED_ADDR:-127.0.0.1:8091}"
clients="${NOKV_FSMETA_CLIENTS:-8}"
files="${NOKV_FSMETA_FILES:-2048}"
reads="${NOKV_FSMETA_READS_PER_CLIENT:-128}"
timeout="${NOKV_FSMETA_TIMEOUT:-5m}"
wait_attempts="${NOKV_FSMETA_WAIT_ATTEMPTS:-160}"
wait_interval="${NOKV_FSMETA_WAIT_INTERVAL:-0.25}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${NOKV_FSMETA_OUTPUT_DIR:-$ROOT/data/fsmeta/results}"
tmp_dir="${NOKV_FSMETA_CACHE_TMPDIR:-$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-cache.XXXXXX")}"

case "$out_dir" in
	/*) ;;
	*) out_dir="$ROOT/$out_dir" ;;
esac

plain_pid=""
cached_pid=""

cleanup() {
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
trap cleanup EXIT

mkdir -p "$out_dir" "$tmp_dir/plain" "$tmp_dir/cached" "$tmp_dir/negative" "$tmp_dir/dirpage"

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

run_bench() {
	local label="$1"
	local addr="$2"
	local output="$out_dir/fsmeta_derived_cache_${label}_${run_id}.csv"
	echo "running $label benchmark -> $output"
	(
		cd benchmark
		NOKV_FSMETA_BENCH=1 go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -v -args \
			-fsmeta_drivers native-fsmeta \
			-fsmeta_addr "$addr" \
			-fsmeta_coordinator_addr "$coord_addr" \
			-fsmeta_workloads hotspot-fanin,negative-lookup \
			-fsmeta_readdirplus=true \
			-fsmeta_clients "$clients" \
			-fsmeta_files "$files" \
			-fsmeta_reads_per_client "$reads" \
			-fsmeta_timeout "$timeout" \
			-fsmeta_output "$output"
	)
}

run_bench "off" "$plain_addr"
run_bench "on" "$cached_addr"

echo "done"
