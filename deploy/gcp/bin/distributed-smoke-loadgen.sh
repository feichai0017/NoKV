#!/usr/bin/env bash
set -euo pipefail

META_ROOT_IDS="1 2 3"
COORDINATOR_IDS="1 2 3"
STORE_IDS="1 2 3"

ALL_COORDINATORS="10.42.0.21:2379,10.42.0.22:2379,10.42.0.23:2379"
RESULT_ROOT="/mnt/nokv/results/${NOKV_DISTRIBUTED_SMOKE_RUN_ID:-distributed-smoke}"

META_ROOT_LEADER_ID=""
COORDINATOR_HOLDER_ID=""

log() {
  printf '[distributed-smoke] %s\n' "$*" >&2
}

die() {
  printf '[distributed-smoke] error: %s\n' "$*" >&2
  exit 1
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    die "$name is required"
  fi
}

meta_root_ip() {
  printf '10.42.0.1%s\n' "$1"
}

coordinator_ip() {
  printf '10.42.0.2%s\n' "$1"
}

store_ip() {
  printf '10.42.0.3%s\n' "$1"
}

skip_matches() {
  local raw="$1"
  local kind="$2"
  local id="$3"
  case ",${raw}," in
    *",${id},"*|*",${kind}-${id},"*) return 0 ;;
    *) return 1 ;;
  esac
}

wait_for_tcp() {
  local addr="$1"
  local timeout_seconds="$2"
  local delay_seconds="$3"
  local host="${addr%:*}"
  local port="${addr##*:}"
  local deadline=$((SECONDS + timeout_seconds))

  log "waiting up to ${timeout_seconds}s for ${addr}"
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay_seconds"
  done

  log "timed out waiting for ${addr}"
  return 1
}

wait_for_docker() {
  local timeout_seconds="${GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS:-600}"
  local deadline=$((SECONDS + timeout_seconds))

  log "waiting up to ${timeout_seconds}s for Docker"
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    if command -v docker >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  log "timed out waiting for Docker"
  return 1
}

eventually() {
  local label="$1"
  local timeout_seconds="$2"
  local delay_seconds="$3"
  shift 3

  local deadline=$((SECONDS + timeout_seconds))
  local attempt=1
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    if "$@"; then
      log "${label}: ok"
      return 0
    fi
    log "${label}: not ready, retrying in ${delay_seconds}s (attempt ${attempt})"
    sleep "$delay_seconds"
    attempt=$((attempt + 1))
  done

  "$@"
}

prepare() {
  require_env NOKV_IMAGE
  require_env NOKV_BENCH_IMAGE
  require_env ARTIFACT_HOST

  wait_for_docker

  local token
  token="$(curl -fsS -H 'Metadata-Flavor: Google' \
    'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' | jq -r .access_token)"
  printf '%s' "$token" | sudo docker login -u oauth2accesstoken --password-stdin "https://${ARTIFACT_HOST}"
  sudo docker pull "$NOKV_IMAGE"
  sudo docker pull "$NOKV_BENCH_IMAGE"
  sudo mkdir -p "$RESULT_ROOT"
  sudo chmod 0777 "$RESULT_ROOT"
}

wait_all() {
  local timeout_seconds="${GCP_SERVICE_READY_TIMEOUT_SECONDS:-900}"
  local delay_seconds="${GCP_SERVICE_READY_RETRY_DELAY_SECONDS:-2}"
  local id
  local ip

  for id in $META_ROOT_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_META_ROOT:-}" meta-root "$id"; then
      continue
    fi
    ip="$(meta_root_ip "$id")"
    wait_for_tcp "${ip}:2380" "$timeout_seconds" "$delay_seconds"
    wait_for_tcp "${ip}:9380" "$timeout_seconds" "$delay_seconds"
  done

  for id in $COORDINATOR_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_COORD:-}" coord "$id"; then
      continue
    fi
    ip="$(coordinator_ip "$id")"
    wait_for_tcp "${ip}:2379" "$timeout_seconds" "$delay_seconds"
    wait_for_tcp "${ip}:9100" "$timeout_seconds" "$delay_seconds"
  done

  for id in $STORE_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_STORE:-}" store "$id"; then
      continue
    fi
    ip="$(store_ip "$id")"
    wait_for_tcp "${ip}:20160" "$timeout_seconds" "$delay_seconds"
    wait_for_tcp "${ip}:9200" "$timeout_seconds" "$delay_seconds"
  done

  wait_for_tcp "10.42.0.41:8090" "$timeout_seconds" "$delay_seconds"
  wait_for_tcp "10.42.0.41:9400" "$timeout_seconds" "$delay_seconds"
}

assert_meta_root_once() {
  local leader_count=0
  local leader_node=""
  local id
  local ip
  local json
  local is_leader
  local leader_id

  for id in $META_ROOT_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_META_ROOT:-}" meta-root "$id"; then
      continue
    fi
    ip="$(meta_root_ip "$id")"
    if ! json="$(curl -fsS "http://${ip}:9380/debug/vars")"; then
      log "meta-root-${id}: metrics unavailable"
      return 1
    fi
    is_leader="$(jq -r '.nokv_meta_root.is_leader // false' <<<"$json")"
    leader_id="$(jq -r '.nokv_meta_root.leader_id // 0' <<<"$json")"
    log "meta-root-${id}: is_leader=${is_leader} leader_id=${leader_id}"
    if [[ "$is_leader" == "true" ]]; then
      leader_count=$((leader_count + 1))
      leader_node="$id"
    fi
  done

  if [[ "$leader_count" -ne 1 ]]; then
    log "expected exactly one live meta-root leader, got ${leader_count}"
    return 1
  fi
  META_ROOT_LEADER_ID="$leader_node"
}

assert_meta_root() {
  eventually "meta-root leader check" \
    "${NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS:-180}" \
    "${GCP_SERVICE_READY_RETRY_DELAY_SECONDS:-2}" \
    assert_meta_root_once
}

meta_root_leader() {
  assert_meta_root
  printf 'meta-root-%s\n' "$META_ROOT_LEADER_ID"
}

assert_coordinator_grant_once() {
  local self_count=0
  local self_holder=""
  local holder_seen=""
  local id
  local ip
  local json
  local holder
  local held_by_self
  local active
  local violations

  for id in $COORDINATOR_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_COORD:-}" coord "$id"; then
      continue
    fi
    ip="$(coordinator_ip "$id")"
    if ! json="$(curl -fsS "http://${ip}:9100/debug/vars")"; then
      log "coord-${id}: metrics unavailable"
      return 1
    fi
    holder="$(jq -r '.nokv_coordinator.state.grant.holder_id // ""' <<<"$json")"
    held_by_self="$(jq -r '.nokv_coordinator.state.grant.held_by_self // false' <<<"$json")"
    active="$(jq -r '.nokv_coordinator.state.grant.active // false' <<<"$json")"
    violations="$(jq -r '([.nokv_coordinator_eunomia.guarantee_violations_total[]?] | add) // 0' <<<"$json")"
    log "coord-${id}: active=${active} held_by_self=${held_by_self} holder=${holder} guarantee_violations=${violations}"

    if [[ "$violations" != "0" ]]; then
      log "coord-${id}: guarantee violations must stay zero"
      return 1
    fi
    if [[ -n "$holder" && "$holder" != "null" ]]; then
      if [[ -z "$holder_seen" ]]; then
        holder_seen="$holder"
      elif [[ "$holder_seen" != "$holder" ]]; then
        log "coordinators disagree on holder: ${holder_seen} vs ${holder}"
        return 1
      fi
    fi
    if [[ "$held_by_self" == "true" ]]; then
      self_count=$((self_count + 1))
      self_holder="coord-${id}"
      if [[ "$active" != "true" ]]; then
        log "coord-${id}: self-held grant is not active"
        return 1
      fi
    fi
  done

  if [[ "$self_count" -ne 1 ]]; then
    log "expected exactly one live coordinator holder, got ${self_count}"
    return 1
  fi
  if [[ -n "$holder_seen" && "$holder_seen" != "$self_holder" ]]; then
    log "holder metadata ${holder_seen} does not match self holder ${self_holder}"
    return 1
  fi
  COORDINATOR_HOLDER_ID="$self_holder"
}

assert_coordinator_grant() {
  eventually "coordinator grant check" \
    "${NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS:-180}" \
    "${GCP_SERVICE_READY_RETRY_DELAY_SECONDS:-2}" \
    assert_coordinator_grant_once
}

coordinator_holder() {
  assert_coordinator_grant
  printf '%s\n' "$COORDINATOR_HOLDER_ID"
}

assert_store_execution_once() {
  require_env NOKV_IMAGE
  require_env NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY

  local id
  local ip
  local addr
  local json
  local state
  local region_count
  local raft_group_count
  local missing_count

  for id in $STORE_IDS; do
    if skip_matches "${NOKV_DISTRIBUTED_SMOKE_SKIP_STORE:-}" store "$id"; then
      continue
    fi
    ip="$(store_ip "$id")"
    addr="${ip}:20160"
    if ! json="$(sudo docker run --rm --network host \
      -e NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-}" \
      "$NOKV_IMAGE" execution -addr "$addr" -json 2>&1)"; then
      log "store-${id}: execution status failed: ${json}"
      return 1
    fi
    state="$(jq -r '.restart.state // ""' <<<"$json")"
    region_count="$(jq -r '.restart.region_count // 0' <<<"$json")"
    raft_group_count="$(jq -r '.restart.raft_group_count // 0' <<<"$json")"
    missing_count="$(jq -r '(.restart.missing_raft_pointer // []) | length' <<<"$json")"
    log "store-${id}: restart_state=${state} regions=${region_count} raft_groups=${raft_group_count} missing_raft_pointer=${missing_count}"

    if [[ "$state" != "ready" ]]; then
      return 1
    fi
    if [[ "$region_count" -le 0 || "$raft_group_count" -le 0 || "$missing_count" -ne 0 ]]; then
      return 1
    fi
  done
}

assert_store_execution() {
  eventually "store execution check" \
    "${NOKV_DISTRIBUTED_SMOKE_ASSERT_TIMEOUT_SECONDS:-180}" \
    "${GCP_SERVICE_READY_RETRY_DELAY_SECONDS:-2}" \
    assert_store_execution_once
}

run_bench() {
  require_env NOKV_BENCH_IMAGE
  require_env NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY

  local phase="${NOKV_DISTRIBUTED_SMOKE_PHASE:-phase}"
  local phase_dir="${RESULT_ROOT}/${phase}"
  local workloads="${NOKV_FSMETA_WORKLOADS:-mixed}"
  local coordinator_addr="${NOKV_FSMETA_COORDINATOR_ADDR:-$ALL_COORDINATORS}"
  local output="/results/fsmeta_${phase}_$(date -u +%Y%m%dT%H%M%SZ).csv"

  sudo mkdir -p "$phase_dir"
  sudo chmod 0777 "$phase_dir"
  log "running fsmeta smoke phase=${phase} workloads=${workloads} coordinator_addr=${coordinator_addr}"

  sudo docker run --rm --network host \
    -v "${phase_dir}:/results" \
    -e NOKV_FSMETA_PROFILE="${NOKV_FSMETA_PROFILE:-smoke}" \
    -e NOKV_FSMETA_WORKLOADS="$workloads" \
    -e NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS="${NOKV_FSMETA_PORT_WAIT_TIMEOUT_SECONDS:-300}" \
    -e NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS="${NOKV_FSMETA_PORT_WAIT_DELAY_SECONDS:-1}" \
    -e NOKV_FSMETA_STABILIZE_SECONDS="${NOKV_FSMETA_STABILIZE_SECONDS:-10}" \
    -e NOKV_FSMETA_TIMEOUT="${NOKV_FSMETA_TIMEOUT:-5m}" \
    -e NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="$NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY" \
    -e NOKV_FSMETA_OUTPUT="$output" \
    -e NOKV_FSMETA_ADDR="10.42.0.41:8090" \
    -e NOKV_FSMETA_COORDINATOR_ADDR="$coordinator_addr" \
    "$NOKV_BENCH_IMAGE"
}

case "${1:-}" in
  prepare)
    prepare
    ;;
  wait-all)
    wait_all
    ;;
  assert-meta-root)
    assert_meta_root
    ;;
  meta-root-leader)
    meta_root_leader
    ;;
  assert-coordinator-grant)
    assert_coordinator_grant
    ;;
  coordinator-holder)
    coordinator_holder
    ;;
  assert-store-execution)
    assert_store_execution
    ;;
  run-bench)
    run_bench
    ;;
  *)
    die "usage: $0 {prepare|wait-all|assert-meta-root|meta-root-leader|assert-coordinator-grant|coordinator-holder|assert-store-execution|run-bench}"
    ;;
esac
