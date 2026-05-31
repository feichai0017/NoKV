#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The benchmark harness can run against a local host process or the local
# Docker fsmeta demo. Distributed coordinator addresses are left empty in the
# local modes.
export NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-rM0DUr4noWKwu7NlAoX2A6FXpdUyLESmwvqNYOkeNIc=}"

mode="${NOKV_FSMETA_BENCH_MODE:-local}"
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
profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=127.0.0.1:9400}"
local_pid=""
local_tools_dir=""
local_tmp_dir=""
rust_tools_dir=""
rust_work_dir=""
rust_raftstore_bin=""
rust_pids=()
profile_pids=()
profile_root_dir=""
compose_built=0
local_mount_key_id="${NOKV_FSMETA_LOCAL_MOUNT_KEY_ID:-2}"
local_log_dir="${NOKV_FSMETA_LOCAL_LOG_DIR:-$ROOT/benchmark/data/fsmeta/ci}"
rust_log_dir="${NOKV_FSMETA_RUST_LOG_DIR:-$ROOT/benchmark/data/fsmeta/rust}"
rust_coord_addr="${NOKV_FSMETA_RUST_COORDINATOR_ADDR:-127.0.0.1:2379}"
rust_raftstore_addr="${NOKV_FSMETA_RUST_RAFTSTORE_ADDR:-127.0.0.1:23880}"
rust_raftstore_metrics_addr="${NOKV_FSMETA_RUST_RAFTSTORE_METRICS_ADDR:-127.0.0.1:9480}"
rust_root_service_addrs="${NOKV_FSMETA_RUST_ROOT_SERVICE_ADDRS:-127.0.0.1:2380,127.0.0.1:2381,127.0.0.1:2382}"
rust_root_transport_addrs="${NOKV_FSMETA_RUST_ROOT_TRANSPORT_ADDRS:-127.0.0.1:2480,127.0.0.1:2481,127.0.0.1:2482}"
rust_route_stabilize_seconds="${NOKV_FSMETA_RUST_ROUTE_STABILIZE_SECONDS:-2}"
rust_cargo_profile="${NOKV_FSMETA_RUST_CARGO_PROFILE:-release}"

case "$profile" in
	median)
		default_timeout=25m
		default_stabilize_seconds=20
		;;
	long)
		default_timeout=120m
		default_stabilize_seconds=45
		;;
	official)
		default_timeout=120m
		default_stabilize_seconds=45
		;;
	*)
		echo "unknown NOKV_FSMETA_PROFILE=$profile; use median, long, or official" >&2
		exit 2
		;;
esac

clients="${NOKV_FSMETA_CLIENTS:-0}"
dirs="${NOKV_FSMETA_DIRS:-0}"
files_per_dir="${NOKV_FSMETA_FILES_PER_DIR:-0}"
users="${NOKV_FSMETA_USERS:-0}"
messages_per_user="${NOKV_FSMETA_MESSAGES_PER_USER:-0}"
workspaces="${NOKV_FSMETA_WORKSPACES:-0}"
checkpoints_per_workspace="${NOKV_FSMETA_CHECKPOINTS_PER_WORKSPACE:-0}"
files_per_checkpoint="${NOKV_FSMETA_FILES_PER_CHECKPOINT:-0}"
session_ttl="${NOKV_FSMETA_SESSION_TTL:-0}"
lookup_cache_entries="${NOKV_FSMETA_LOOKUP_CACHE_ENTRIES:-4096}"
lookup_cache_ttl="${NOKV_FSMETA_LOOKUP_CACHE_TTL:-1s}"
timeout="${NOKV_FSMETA_TIMEOUT:-$default_timeout}"
stabilize_seconds="${NOKV_FSMETA_STABILIZE_SECONDS:-$default_stabilize_seconds}"
reset_between_workloads="${NOKV_FSMETA_RESET_BETWEEN_WORKLOADS:-1}"

case "$output_dir" in
	/*) ;;
	*) output_dir="$ROOT/$output_dir" ;;
esac
mkdir -p "$output_dir"

case "$local_log_dir" in
	/*) ;;
	*) local_log_dir="$ROOT/$local_log_dir" ;;
esac
mkdir -p "$local_log_dir"

case "$rust_log_dir" in
	/*) ;;
	*) rust_log_dir="$ROOT/$rust_log_dir" ;;
esac
mkdir -p "$rust_log_dir"

case "$profile_dir" in
	/*) ;;
	*) profile_dir="$ROOT/$profile_dir" ;;
esac
profile_root_dir="$profile_dir"

enabled() {
	case "$1" in
		1|true|TRUE|yes|YES) return 0 ;;
		*) return 1 ;;
	esac
}

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
			-fsmeta_scale_profile "$profile" \
			-fsmeta_clients "$clients" \
			-fsmeta_dirs "$dirs" \
			-fsmeta_files_per_dir "$files_per_dir" \
			-fsmeta_users "$users" \
			-fsmeta_messages_per_user "$messages_per_user" \
			-fsmeta_workspaces "$workspaces" \
			-fsmeta_checkpoints_per_workspace "$checkpoints_per_workspace" \
			-fsmeta_files_per_checkpoint "$files_per_checkpoint" \
			-fsmeta_session_ttl "$session_ttl" \
			-fsmeta_lookup_cache_entries "$lookup_cache_entries" \
			-fsmeta_lookup_cache_ttl "$lookup_cache_ttl" \
			-fsmeta_timeout "$timeout" \
			-fsmeta_output "$output"
	)
}

profiles_enabled() {
	enabled "$capture_profiles"
}

write_profile_manifest() {
	local workloads="$1"
	cat >"$profile_dir/manifest.txt" <<EOF
run_id=$run_id
bench_mode=$mode
benchmark_profile=$profile
profile_file=benchmark/fsmeta/profiles/official/workloads.yaml
workloads=$workloads
clients_override=$clients
dirs_override=$dirs
files_per_dir_override=$files_per_dir
users_override=$users
messages_per_user_override=$messages_per_user
workspaces_override=$workspaces
checkpoints_per_workspace_override=$checkpoints_per_workspace
files_per_checkpoint_override=$files_per_checkpoint
session_ttl_override=$session_ttl
reset_between_workloads=$reset_between_workloads
lookup_cache_entries=$lookup_cache_entries
lookup_cache_ttl=$lookup_cache_ttl
profile_seconds=$profile_seconds
targets=$profile_targets
EOF
}

write_benchmark_manifest() {
	local output="$1"
	local workloads="$2"
	cat >"${output}.manifest.txt" <<EOF
run_id=$run_id
bench_mode=$mode
scale_profile=$profile
profile_file=benchmark/fsmeta/profiles/official/workloads.yaml
workloads=$workloads
clients_override=$clients
dirs_override=$dirs
files_per_dir_override=$files_per_dir
users_override=$users
messages_per_user_override=$messages_per_user
workspaces_override=$workspaces
checkpoints_per_workspace_override=$checkpoints_per_workspace
files_per_checkpoint_override=$files_per_checkpoint
session_ttl_override=$session_ttl
reset_between_workloads=$reset_between_workloads
lookup_cache_entries=$lookup_cache_entries
lookup_cache_ttl=$lookup_cache_ttl
timeout=$timeout
note=exact resolved per-workload scales are emitted by the Go benchmark manifests next to each workload CSV
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

start_compose_cluster() {
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" != "1" ]]; then
		return
	fi
	export NOKV_FSMETA_MOUNT="$mount"
	export NOKV_FSMETA_MOUNT_KEY_ID="$local_mount_key_id"
	if [[ "${NOKV_FSMETA_COMPOSE_BUILD:-1}" == "1" && "$compose_built" == "0" ]]; then
		echo "starting Docker Compose local fsmeta demo with local build"
		docker compose up -d --build
		compose_built=1
	else
		echo "starting Docker Compose local fsmeta demo"
		docker compose up -d
	fi
	wait_port "${fsmeta_addr%%,*}"
	if [[ "$stabilize_seconds" != "0" ]]; then
		echo "waiting ${stabilize_seconds}s for local fsmeta warmup"
		sleep "$stabilize_seconds"
	fi
}

reset_compose_cluster() {
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" != "1" ]]; then
		return
	fi
	docker compose down -v
}

run_compose_benchmarks() {
	local coord_addr=""
	local workloads="${NOKV_FSMETA_WORKLOADS:-mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_compose_${profile}_${run_id}_isolated.csv}"
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if ! enabled "$reset_between_workloads"; then
		start_compose_cluster
		profile_dir="$profile_root_dir"
		profile_pids=()
		start_profile_capture "$workloads"
	fi
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
		if enabled "$reset_between_workloads"; then
			reset_compose_cluster
			start_compose_cluster
			profile_dir="$profile_root_dir/$safe_workload"
			profile_pids=()
			start_profile_capture "$workload"
		fi
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if enabled "$reset_between_workloads"; then
			finish_profile_capture
		fi
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
		if ! enabled "$reset_between_workloads"; then
			:
		fi
	done
	if ! enabled "$reset_between_workloads"; then
		finish_profile_capture
	fi
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
	write_benchmark_manifest "$output" "$workloads"
	print_bench_summary "$output"
	echo "wrote isolated fsmeta benchmark summary: $output"
	if [[ "${NOKV_FSMETA_COMPOSE_DOWN:-0}" == "1" ]]; then
		docker compose down -v
	fi
}

ensure_local_gateway_binary() {
	if [[ -n "$local_tools_dir" ]]; then
		return
	fi
	local_tools_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-local-tools.XXXXXX")"
	go build -o "$local_tools_dir/nokv-fsmeta" ./cmd/nokv-fsmeta
}

stop_local_gateway() {
	if [[ -n "$local_pid" ]]; then
		kill "$local_pid" 2>/dev/null || true
		wait "$local_pid" 2>/dev/null || true
		local_pid=""
	fi
	if [[ -z "${NOKV_FSMETA_LOCAL_WORKDIR:-}" && -n "$local_tmp_dir" ]]; then
		rm -rf "$local_tmp_dir"
		local_tmp_dir=""
	fi
}

cleanup_local_gateway() {
	stop_local_gateway
	if [[ -n "$local_tools_dir" ]]; then
		rm -rf "$local_tools_dir"
		local_tools_dir=""
	fi
}

ensure_rust_distributed_tools() {
	if [[ -n "$rust_tools_dir" ]]; then
		return
	fi
	rust_tools_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-rust-tools.XXXXXX")"
	go build -o "$rust_tools_dir/nokv" ./cmd/nokv
	go build -o "$rust_tools_dir/nokv-fsmeta" ./cmd/nokv-fsmeta
	local profile_dir
	case "$rust_cargo_profile" in
		debug | dev)
			cargo build --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
			profile_dir="debug"
			;;
		release)
			cargo build --release --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
			profile_dir="release"
			;;
		*)
			cargo build --profile "$rust_cargo_profile" --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
			profile_dir="$rust_cargo_profile"
			;;
	esac
	local target_dir="${CARGO_TARGET_DIR:-$ROOT/raftstore/target}"
	rust_raftstore_bin="$target_dir/$profile_dir/nokv-raftstore-server"
	if [[ ! -x "$rust_raftstore_bin" ]]; then
		echo "built Rust raftstore binary not found: $rust_raftstore_bin" >&2
		exit 1
	fi
}

start_rust_process() {
	local name="$1"
	local log_file="$2"
	shift 2
	echo "starting $name (log: $log_file)"
	"$@" >"$log_file" 2>&1 &
	rust_pids+=("$!")
}

register_rust_benchmark_mount() {
	local suffix="$1"
	local log_file="$rust_log_dir/mount-register-${profile}-${run_id}-${suffix}.log"
	rm -f "$log_file"
	for _ in $(seq 1 "$wait_attempts"); do
		if "$rust_tools_dir/nokv" fsmeta-mount-register \
			--coordinator-addr "$rust_coord_addr" \
			--mount "$mount" \
			--root-inode 1 \
			--timeout 10s \
			>>"$log_file" 2>&1; then
			tail -1 "$log_file"
			return 0
		fi
		sleep "$wait_interval"
	done
	echo "timed out registering Rust fsmeta benchmark mount $mount" >&2
	cat "$log_file" >&2 || true
	return 1
}

stop_rust_distributed_stack() {
	for pid in "${rust_pids[@]}"; do
		kill "$pid" 2>/dev/null || true
	done
	for pid in "${rust_pids[@]}"; do
		wait "$pid" 2>/dev/null || true
	done
	rust_pids=()
	if [[ -z "${NOKV_FSMETA_RUST_WORKDIR:-}" && -n "$rust_work_dir" ]]; then
		rm -rf "$rust_work_dir"
		rust_work_dir=""
	fi
}

cleanup_rust_distributed_stack() {
	stop_rust_distributed_stack
	if [[ -n "$rust_tools_dir" ]]; then
		rm -rf "$rust_tools_dir"
		rust_tools_dir=""
	fi
}

start_rust_distributed_stack() {
	local suffix="$1"
	ensure_rust_distributed_tools
	stop_rust_distributed_stack

	if [[ -n "${NOKV_FSMETA_RUST_WORKDIR:-}" ]]; then
		rust_work_dir="$NOKV_FSMETA_RUST_WORKDIR"
		if enabled "$reset_between_workloads"; then
			rust_work_dir="$rust_work_dir/$suffix"
			rm -rf "$rust_work_dir"
		fi
	else
		rust_work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-rust.XXXXXX")"
	fi
	mkdir -p "$rust_work_dir"

	local root_service=()
	local root_transport=()
	IFS=',' read -r -a root_service <<<"$rust_root_service_addrs"
	IFS=',' read -r -a root_transport <<<"$rust_root_transport_addrs"
	if [[ "${#root_service[@]}" -ne 3 ]]; then
		echo "NOKV_FSMETA_RUST_ROOT_SERVICE_ADDRS requires exactly 3 comma-separated addresses" >&2
		exit 2
	fi
	if [[ "${#root_transport[@]}" -ne 3 ]]; then
		echo "NOKV_FSMETA_RUST_ROOT_TRANSPORT_ADDRS requires exactly 3 comma-separated addresses" >&2
		exit 2
	fi
	local root_peer_args=()
	local root_service_peer_args=()
	for i in 0 1 2; do
		local id="$((i + 1))"
		root_peer_args+=("-peer" "$id=${root_transport[$i]}")
		root_service_peer_args+=("-root-peer" "$id=${root_service[$i]}")
	done

	for i in 0 1 2; do
		local id="$((i + 1))"
		start_rust_process "meta-root-$id" "$rust_log_dir/meta-root-${profile}-${run_id}-${suffix}-${id}.log" \
			"$rust_tools_dir/nokv" meta-root \
			--addr "${root_service[$i]}" \
			--node-id "$id" \
			--workdir "$rust_work_dir/meta-root-$id" \
			--transport-addr "${root_transport[$i]}" \
			"${root_peer_args[@]}"
	done
	for addr in "${root_service[@]}"; do
		wait_port "$addr"
	done

	start_rust_process "coordinator" "$rust_log_dir/coordinator-${profile}-${run_id}-${suffix}.log" \
		"$rust_tools_dir/nokv" coordinator \
		--addr "$rust_coord_addr" \
		--coordinator-id "fsmeta-bench" \
		"${root_service_peer_args[@]}"
	wait_port "$rust_coord_addr"

	start_rust_process "raftstore" "$rust_log_dir/raftstore-${profile}-${run_id}-${suffix}.log" \
		env \
		NOKV_RAFTSTORE_ADDR="$rust_raftstore_addr" \
		NOKV_RAFTSTORE_ADVERTISE_ADDR="$rust_raftstore_addr" \
		NOKV_RAFTSTORE_COORDINATOR_ADDR="$rust_coord_addr" \
		NOKV_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=250 \
		NOKV_RAFTSTORE_HOLT_DIR="$rust_work_dir/raftstore-holt" \
		NOKV_RAFTSTORE_LOG_DIR="$rust_work_dir/raftstore-log" \
		"$rust_raftstore_bin" --metrics-addr "$rust_raftstore_metrics_addr"
	wait_port "$rust_raftstore_addr"
	wait_port "$rust_raftstore_metrics_addr"

	if [[ "$rust_route_stabilize_seconds" != "0" ]]; then
		echo "waiting ${rust_route_stabilize_seconds}s for Rust raftstore route publication"
		sleep "$rust_route_stabilize_seconds"
	fi

	register_rust_benchmark_mount "$suffix"

	start_rust_process "fsmeta-raftstore" "$rust_log_dir/fsmeta-raftstore-${profile}-${run_id}-${suffix}.log" \
		"$rust_tools_dir/nokv-fsmeta" \
		--runtime raftstore \
		--addr "$fsmeta_addr" \
		--metrics-addr "$fsmeta_metrics_addr" \
		--coordinator-addr "$rust_coord_addr" \
		--bootstrap-mount "$mount"
	if ! wait_port "${fsmeta_addr%%,*}"; then
		cat "$rust_log_dir/fsmeta-raftstore-${profile}-${run_id}-${suffix}.log" >&2 || true
		return 1
	fi
	wait_port "${fsmeta_metrics_addr%%,*}"
}

start_local_gateway() {
	local suffix="$1"
	ensure_local_gateway_binary
	stop_local_gateway

	local work_dir
	if [[ -n "${NOKV_FSMETA_LOCAL_WORKDIR:-}" ]]; then
		work_dir="$NOKV_FSMETA_LOCAL_WORKDIR"
		if enabled "$reset_between_workloads"; then
			work_dir="$work_dir/$suffix"
			rm -rf "$work_dir"
		fi
	else
		local_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-local.XXXXXX")"
		work_dir="$local_tmp_dir/db"
	fi
	mkdir -p "$work_dir"

	local log_file="$local_log_dir/fsmeta-local-${profile}-${run_id}-${suffix}.log"
	echo "starting local fsmeta gateway on $fsmeta_addr work_dir=$work_dir"
	"$local_tools_dir/nokv-fsmeta" \
		--addr "$fsmeta_addr" \
		--metrics-addr "$fsmeta_metrics_addr" \
		--local-work-dir "$work_dir" \
		--local-mount-id "$mount" \
		--local-mount-key-id "$local_mount_key_id" \
		>"$log_file" 2>&1 &
	local_pid="$!"
	if ! wait_port "${fsmeta_addr%%,*}"; then
		cat "$log_file" >&2 || true
		return 1
	fi
	wait_port "${fsmeta_metrics_addr%%,*}"
}

run_local_benchmarks() {
	local workloads="${NOKV_FSMETA_WORKLOADS:-mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_local_${profile}_${run_id}_isolated.csv}"
	local coord_addr=""
	local profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=$fsmeta_metrics_addr}"
	trap cleanup_local_gateway EXIT
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if ! enabled "$reset_between_workloads"; then
		start_local_gateway "shared"
		profile_dir="$profile_root_dir"
		profile_pids=()
		start_profile_capture "$workloads"
	fi
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
		local workload_output="$output_dir/fsmeta_local_${profile}_${run_id}_${safe_workload}.csv"
		echo "running isolated local fsmeta workload: $workload"
		if enabled "$reset_between_workloads"; then
			start_local_gateway "$safe_workload"
			profile_dir="$profile_root_dir/$safe_workload"
			profile_pids=()
			start_profile_capture "$workload"
		fi
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if enabled "$reset_between_workloads"; then
			finish_profile_capture
			stop_local_gateway
		fi
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
	done
	if ! enabled "$reset_between_workloads"; then
		finish_profile_capture
	fi
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
	write_benchmark_manifest "$output" "$workloads"
	print_bench_summary "$output"
	echo "wrote isolated local fsmeta benchmark summary: $output"
}

run_rust_distributed_benchmarks() {
	coord_addr="$rust_coord_addr"
	profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=$fsmeta_metrics_addr,raftstore=$rust_raftstore_metrics_addr}"
	local workloads="${NOKV_FSMETA_WORKLOADS:-mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_rust_${profile}_${run_id}_isolated.csv}"
	trap cleanup_rust_distributed_stack EXIT
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if ! enabled "$reset_between_workloads"; then
		start_rust_distributed_stack "shared"
		profile_dir="$profile_root_dir"
		profile_pids=()
		start_profile_capture "$workloads"
	fi
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
		local workload_output="$output_dir/fsmeta_rust_${profile}_${run_id}_${safe_workload}.csv"
		echo "running isolated Rust distributed fsmeta workload: $workload"
		if enabled "$reset_between_workloads"; then
			start_rust_distributed_stack "$safe_workload"
			profile_dir="$profile_root_dir/$safe_workload"
			profile_pids=()
			start_profile_capture "$workload"
		fi
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if enabled "$reset_between_workloads"; then
			finish_profile_capture
			stop_rust_distributed_stack
		fi
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
	done
	if ! enabled "$reset_between_workloads"; then
		finish_profile_capture
	fi
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
	write_benchmark_manifest "$output" "$workloads"
	print_bench_summary "$output"
	echo "wrote isolated Rust distributed fsmeta benchmark summary: $output"
}

case "$mode" in
	compose)
		run_compose_benchmarks
		;;
	local)
		run_local_benchmarks
		;;
	rust|rust-distributed)
		run_rust_distributed_benchmarks
		;;
	*)
		echo "unknown NOKV_FSMETA_BENCH_MODE=$mode; use compose, local, or rust" >&2
		exit 2
		;;
esac
