#!/usr/bin/env bash
set -euo pipefail

wait_port() {
  local addr="$1"
  local host="${addr%:*}"
  local port="${addr##*:}"
  local timeout_seconds="${2:-180}"
  local delay_seconds="${3:-1}"
  local deadline=$((SECONDS + timeout_seconds))
  echo "waiting up to ${timeout_seconds}s for ${addr}"
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay_seconds"
  done
  echo "timed out waiting for ${addr}" >&2
  return 1
}

profile="${NOKV_FSMETA_PROFILE:-median}"
case "$profile" in
  smoke)
    default_clients=2
    default_dirs=2
    default_files_per_dir=16
    default_files=64
    default_reads=16
    default_groups=2
    default_entries_per_group=8
    default_artifacts_per_entry=2
    default_workspaces=1
    default_session_ttl=1m
    default_stale_session_ttl=1s
    default_timeout=5m
    default_stabilize_seconds=15
    default_port_wait_timeout_seconds=600
    default_port_wait_delay_seconds=1
    ;;
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
    default_stabilize_seconds=60
    default_port_wait_timeout_seconds=900
    default_port_wait_delay_seconds=1
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
    default_stabilize_seconds=120
    default_port_wait_timeout_seconds=900
    default_port_wait_delay_seconds=1
    ;;
  *)
    echo "unknown NOKV_FSMETA_PROFILE=$profile; use smoke, median, or long" >&2
    exit 2
    ;;
esac

: "${NOKV_FSMETA_ADDR:?NOKV_FSMETA_ADDR is required}"
: "${NOKV_FSMETA_COORDINATOR_ADDR:?NOKV_FSMETA_COORDINATOR_ADDR is required}"

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
output="${NOKV_FSMETA_OUTPUT:-/results/fsmeta_${profile}_${run_id}.csv}"
mkdir -p "$(dirname "$output")"

export NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-rM0DUr4noWKwu7NlAoX2A6FXpdUyLESmwvqNYOkeNIc=}"

workloads="${NOKV_FSMETA_WORKLOADS:-multi-workspace-autoscale,mixed,durable-snapshot,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup}"
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
port_wait_timeout_seconds="${NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS:-$default_port_wait_timeout_seconds}"
port_wait_delay_seconds="${NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS:-$default_port_wait_delay_seconds}"

wait_port "${NOKV_FSMETA_ADDR%%,*}" "$port_wait_timeout_seconds" "$port_wait_delay_seconds"
wait_port "${NOKV_FSMETA_COORDINATOR_ADDR%%,*}" "$port_wait_timeout_seconds" "$port_wait_delay_seconds"
if [[ "$stabilize_seconds" != "0" ]]; then
  echo "waiting ${stabilize_seconds}s for raft leaders and coordinator grants to settle"
  sleep "$stabilize_seconds"
fi

bench_args=(
  -fsmeta_addr "$NOKV_FSMETA_ADDR" \
  -fsmeta_coordinator_addr "$NOKV_FSMETA_COORDINATOR_ADDR" \
  -fsmeta_mount "${NOKV_FSMETA_MOUNT:-fsmeta-bench}" \
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

bench_binary="${NOKV_FSMETA_BENCH_BINARY:-/usr/local/bin/nokv-fsmeta-bench.test}"
if [[ -x "$bench_binary" ]]; then
  NOKV_FSMETA_BENCH=1 "$bench_binary" \
    -test.run TestBenchmarkFSMeta \
    -test.count 1 \
    -test.v \
    -test.timeout "$timeout" \
    "${bench_args[@]}"
else
  cd /workspace/benchmark
  NOKV_FSMETA_BENCH=1 go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -v -timeout "$timeout" -args "${bench_args[@]}"
fi

echo "wrote fsmeta benchmark summary: $output"
