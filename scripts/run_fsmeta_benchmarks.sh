#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Host-side benchmark code validates Eunomia grant evidence returned by the
# Compose coordinators. Keep the host test process and local Compose containers
# on the same dev/test key material; production deployments override this env.
export NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-rM0DUr4noWKwu7NlAoX2A6FXpdUyLESmwvqNYOkeNIc=}"

mode="${NOKV_FSMETA_BENCH_MODE:-compose}"
profile="${NOKV_FSMETA_PROFILE:-median}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
coord_addr="${NOKV_FSMETA_COORDINATOR_ADDR:-127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392}"
fsmeta_addr="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
fsmeta_metrics_addr="${NOKV_FSMETA_METRICS_ADDR:-127.0.0.1:9400}"
mount="${NOKV_FSMETA_MOUNT:-fsmeta-bench}"
wait_attempts="${NOKV_FSMETA_WAIT_ATTEMPTS:-180}"
wait_interval="${NOKV_FSMETA_WAIT_INTERVAL:-1}"
output_dir="${NOKV_FSMETA_OUTPUT_DIR:-$ROOT/benchmark/data/fsmeta/results}"
capture_profiles="${NOKV_FSMETA_CAPTURE_PROFILES:-0}"
profile_seconds="${NOKV_FSMETA_PROFILE_SECONDS:-30}"
profile_dir="${NOKV_FSMETA_PROFILE_DIR:-$ROOT/benchmark/data/fsmeta/profiles/fsmeta_${profile}_${run_id}}"
profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=127.0.0.1:9400,store1=127.0.0.1:9200,store2=127.0.0.1:9201,store3=127.0.0.1:9202,coord1=127.0.0.1:9100,coord2=127.0.0.1:9101,coord3=127.0.0.1:9102,root1=127.0.0.1:9380,root2=127.0.0.1:9381,root3=127.0.0.1:9382}"
cache_tmp_dir=""
plain_pid=""
cached_pid=""
profile_pids=()

case "$profile" in
	median)
		default_clients=12
		default_dirs=16
		default_files_per_dir=256
		default_files=4096
		default_reads=512
		default_groups=8
		default_entries_per_group=64
		default_artifacts_per_entry=8
		default_workspaces=4
		default_session_ttl=5m
		default_stale_session_ttl=2s
		default_timeout=25m
		default_stabilize_seconds=20
		;;
	long)
		default_clients=16
		default_dirs=32
		default_files_per_dir=512
		default_files=16384
		default_reads=1024
		default_groups=16
		default_entries_per_group=128
		default_artifacts_per_entry=10
		default_workspaces=8
		default_session_ttl=5m
		default_stale_session_ttl=2s
		default_timeout=120m
		default_stabilize_seconds=45
		;;
	*)
		echo "unknown NOKV_FSMETA_PROFILE=$profile; use median or long" >&2
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
workspaces="${NOKV_FSMETA_WORKSPACES:-$default_workspaces}"
session_ttl="${NOKV_FSMETA_SESSION_TTL:-$default_session_ttl}"
stale_session_ttl="${NOKV_FSMETA_STALE_SESSION_TTL:-$default_stale_session_ttl}"
lookup_cache_entries="${NOKV_FSMETA_LOOKUP_CACHE_ENTRIES:-4096}"
lookup_cache_ttl="${NOKV_FSMETA_LOOKUP_CACHE_TTL:-1s}"
timeout="${NOKV_FSMETA_TIMEOUT:-$default_timeout}"
stabilize_seconds="${NOKV_FSMETA_STABILIZE_SECONDS:-$default_stabilize_seconds}"

case "$output_dir" in
	/*) ;;
	*) output_dir="$ROOT/$output_dir" ;;
esac
mkdir -p "$output_dir"

case "$profile_dir" in
	/*) ;;
	*) profile_dir="$ROOT/$profile_dir" ;;
esac

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

peras_idle_snapshot() {
	local metrics_url="http://${fsmeta_metrics_addr%%,*}/debug/vars"
	python3 - "$metrics_url" <<'PY'
import json
import sys
import urllib.request

url = sys.argv[1]
with urllib.request.urlopen(url, timeout=2) as response:
	data = json.load(response)

peras = data.get("nokv_fsmeta_peras")
if not isinstance(peras, dict):
	executor = data.get("nokv_fsmeta_executor", {})
	peras = executor.get("peras_committer", {})

def number(name):
	value = peras.get(name, 0)
	if isinstance(value, bool):
		return int(value)
	if isinstance(value, (int, float)):
		return int(value)
	return 0

state = {
	"pending": number("pending"),
	"install_queue_depth": number("segment_install_queue_depth"),
	"seal_queue_depth": number("segment_seal_queue_depth"),
	"flush_total": number("flush_total"),
	"segment_total": number("segment_total"),
	"seal_total": number("seal_total"),
}
print(json.dumps(state, sort_keys=True, separators=(",", ":")))
if state["pending"] == 0 and state["install_queue_depth"] == 0 and state["seal_queue_depth"] == 0:
	sys.exit(0)
sys.exit(1)
PY
}

wait_fsmeta_peras_idle() {
	local timeout_seconds="${NOKV_FSMETA_PERAS_IDLE_TIMEOUT_SECONDS:-180}"
	local interval_seconds="${NOKV_FSMETA_PERAS_IDLE_INTERVAL_SECONDS:-1}"
	local stable_polls="${NOKV_FSMETA_PERAS_IDLE_STABLE_POLLS:-2}"
	local deadline=$((SECONDS + timeout_seconds))
	local last=""
	local stable=0
	local snapshot=""

	while (( SECONDS < deadline )); do
		if snapshot="$(peras_idle_snapshot 2>/dev/null)"; then
			if [[ "$snapshot" == "$last" ]]; then
				stable=$((stable + 1))
			else
				stable=1
				last="$snapshot"
			fi
			if (( stable >= stable_polls )); then
				return 0
			fi
		else
			stable=0
			last=""
		fi
		sleep "$interval_seconds"
	done
	echo "timed out waiting for fsmeta Peras idle state; last=$last" >&2
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
			-fsmeta_workspaces "$workspaces" \
			-fsmeta_session_ttl "$session_ttl" \
			-fsmeta_stale_session_ttl "$stale_session_ttl" \
			-fsmeta_lookup_cache_entries "$lookup_cache_entries" \
			-fsmeta_lookup_cache_ttl "$lookup_cache_ttl" \
			-fsmeta_timeout "$timeout" \
			-fsmeta_output "$output"
	)
}

profiles_enabled() {
	case "$capture_profiles" in
		1|true|TRUE|yes|YES) return 0 ;;
		*) return 1 ;;
	esac
}

write_profile_manifest() {
	local workloads="$1"
	cat >"$profile_dir/manifest.txt" <<EOF
run_id=$run_id
benchmark_profile=$profile
workloads=$workloads
clients=$clients
dirs=$dirs
files_per_dir=$files_per_dir
files=$files
reads_per_client=$reads
groups=$groups
entries_per_group=$entries_per_group
artifacts_per_entry=$artifacts_per_entry
workspaces=$workspaces
session_ttl=$session_ttl
stale_session_ttl=$stale_session_ttl
lookup_cache_entries=$lookup_cache_entries
lookup_cache_ttl=$lookup_cache_ttl
profile_seconds=$profile_seconds
targets=$profile_targets
EOF
}

start_profile_capture() {
	local workloads="$1"
	if ! profiles_enabled; then
		return
	fi
	mkdir -p "$profile_dir"
	write_profile_manifest "$workloads"
	echo "capturing fsmeta profile bundle in $profile_dir"
	IFS=',' read -r -a targets <<<"$profile_targets"
	for target in "${targets[@]}"; do
		local name="${target%%=*}"
		local addr="${target#*=}"
		if [[ -z "$name" || -z "$addr" || "$name" == "$addr" ]]; then
			echo "skip malformed profile target: $target" >&2
			continue
		fi
		(
			curl -fsS --max-time "$((profile_seconds + 15))" \
				"http://$addr/debug/pprof/profile?seconds=$profile_seconds" \
				-o "$profile_dir/${name}.cpu.pprof" \
				>"$profile_dir/${name}.cpu.log" 2>&1 || \
				echo "cpu profile capture failed for $name at $addr" >>"$profile_dir/${name}.cpu.log"
		) &
		profile_pids+=("$!")
	done
}

fetch_profile_file() {
	local url="$1"
	local output="$2"
	curl -fsS --max-time 15 "$url" -o "$output" >/dev/null 2>&1 || \
		echo "profile fetch failed: $url" >"$output.error"
}

collect_profile_snapshots() {
	if ! profiles_enabled; then
		return
	fi
	IFS=',' read -r -a targets <<<"$profile_targets"
	for target in "${targets[@]}"; do
		local name="${target%%=*}"
		local addr="${target#*=}"
		if [[ -z "$name" || -z "$addr" || "$name" == "$addr" ]]; then
			continue
		fi
		fetch_profile_file "http://$addr/debug/vars" "$profile_dir/${name}.vars.json"
		fetch_profile_file "http://$addr/debug/pprof/goroutine?debug=2" "$profile_dir/${name}.goroutine.txt"
		fetch_profile_file "http://$addr/debug/pprof/heap" "$profile_dir/${name}.heap.pprof"
		fetch_profile_file "http://$addr/debug/pprof/allocs" "$profile_dir/${name}.allocs.pprof"
	done
}

finish_profile_capture() {
	if ! profiles_enabled; then
		return
	fi
	for pid in "${profile_pids[@]}"; do
		wait "$pid" || true
	done
	collect_profile_snapshots
	tar -C "$(dirname "$profile_dir")" -czf "$profile_dir.tar.gz" "$(basename "$profile_dir")"
	echo "wrote fsmeta profile bundle: $profile_dir.tar.gz"
}

print_bench_summary() {
	local output="$1"
	if [[ ! -f "$output" ]]; then
		return
	fi
	echo "fsmeta benchmark CSV summary:"
	sed -n '1,120p' "$output"
}

run_compose_benchmarks() {
	local workloads="${NOKV_FSMETA_WORKLOADS:-multi-workspace-autoscale,mixed,durable-snapshot,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_compose_${profile}_${run_id}_isolated.csv}"
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" == "1" ]]; then
		# The benchmark mount must exist before the fsmeta gateway starts.
		# Otherwise the run depends on asynchronous root watch catch-up during
		# the benchmark bootstrap window, which makes long/profiled CI runs flaky.
		export NOKV_MOUNT_IDS="${NOKV_MOUNT_IDS:-default,$mount}"
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
	wait_fsmeta_peras_idle
	start_profile_capture "$workloads"
	local combined_tmp="$output.tmp"
	local wrote_header=0
	local bench_status=0
	rm -f "$combined_tmp" "$output"
	IFS=',' read -r -a workload_list <<<"$workloads"
	for workload in "${workload_list[@]}"; do
		workload="${workload//[[:space:]]/}"
		if [[ -z "$workload" ]]; then
			continue
		fi
		local safe_workload="${workload//[^A-Za-z0-9_-]/_}"
		local workload_output="$output_dir/fsmeta_compose_${profile}_${run_id}_${safe_workload}.csv"
		echo "running isolated fsmeta workload: $workload"
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if [[ "$bench_status" -ne 0 ]]; then
			break
		fi
		if [[ "$wrote_header" -eq 0 ]]; then
			cat "$workload_output" >"$combined_tmp"
			wrote_header=1
		else
			tail -n +2 "$workload_output" >>"$combined_tmp"
		fi
		print_bench_summary "$workload_output"
		wait_fsmeta_peras_idle
	done
	finish_profile_capture
	if [[ "$bench_status" -ne 0 ]]; then
		rm -f "$combined_tmp"
		exit "$bench_status"
	fi
	if [[ "$wrote_header" -eq 0 ]]; then
		echo "no fsmeta workloads selected" >&2
		rm -f "$combined_tmp"
		exit 2
	fi
	mv "$combined_tmp" "$output"
	print_bench_summary "$output"
	echo "wrote isolated fsmeta benchmark summary: $output"
	if [[ "${NOKV_FSMETA_COMPOSE_DOWN:-0}" == "1" ]]; then
		docker compose down -v
	fi
}

cleanup_cache_gateways() {
	if [[ -n "$plain_pid" ]]; then
		kill "$plain_pid" 2>/dev/null || true
		wait "$plain_pid" 2>/dev/null || true
	fi
	if [[ -n "$cached_pid" ]]; then
		kill "$cached_pid" 2>/dev/null || true
		wait "$cached_pid" 2>/dev/null || true
	fi
	if [[ -z "${NOKV_FSMETA_CACHE_TMPDIR:-}" && -n "$cache_tmp_dir" ]]; then
		rm -rf "$cache_tmp_dir"
	fi
}

run_derived_cache_benchmarks() {
	local plain_addr="${NOKV_FSMETA_PLAIN_ADDR:-127.0.0.1:8090}"
	local cached_addr="${NOKV_FSMETA_CACHED_ADDR:-127.0.0.1:8091}"
	cache_tmp_dir="${NOKV_FSMETA_CACHE_TMPDIR:-$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-cache.XXXXXX")}"
	plain_pid=""
	cached_pid=""
	trap cleanup_cache_gateways EXIT

	mkdir -p "$cache_tmp_dir/plain" "$cache_tmp_dir/cached" "$cache_tmp_dir/negative" "$cache_tmp_dir/dirpage"
	echo "starting plain fsmeta gateway on $plain_addr"
	go run ./cmd/nokv-fsmeta \
		--addr "$plain_addr" \
		--coordinator-addr "$coord_addr" \
		>"$cache_tmp_dir/plain/fsmeta.log" 2>&1 &
	plain_pid="$!"
	wait_port "$plain_addr"

	echo "starting cached fsmeta gateway on $cached_addr"
	go run ./cmd/nokv-fsmeta \
		--addr "$cached_addr" \
		--coordinator-addr "$coord_addr" \
		--negative-cache-dir "$cache_tmp_dir/negative" \
		--dirpage-cache-dir "$cache_tmp_dir/dirpage" \
		>"$cache_tmp_dir/cached/fsmeta.log" 2>&1 &
	cached_pid="$!"
	wait_port "$cached_addr"

	local plain_output="$output_dir/fsmeta_derived_cache_${profile}_off_${run_id}.csv"
	local cached_output="$output_dir/fsmeta_derived_cache_${profile}_on_${run_id}.csv"
	run_bench "$plain_addr" "hotspot-fanin,negative-lookup" "$plain_output"
	print_bench_summary "$plain_output"
	run_bench "$cached_addr" "hotspot-fanin,negative-lookup" "$cached_output"
	print_bench_summary "$cached_output"
	echo "done"
}

case "$mode" in
	compose)
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
