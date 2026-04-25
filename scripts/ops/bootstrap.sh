#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"
source "$SCRIPT_DIR/../lib/workdir.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/ops/bootstrap.sh --config <path> --path-template <template>

Options:
  --config PATH          Raft configuration file (default: ./raft_config.example.json)
  --path-template TMPL   Template for store workdirs, e.g. /data/store-{id}
  --state STATE          Optional region state (running|tombstone)
  --skip-existing        Exit successfully for stores that already have CURRENT

Notes:
  - bootstrap.sh only seeds fresh store workdirs from config.regions.
  - It must not be used to restart a store that already has runtime raft/local metadata.
  - Runtime topology changes are recovered from local metadata, not from config.regions.
  - --skip-existing is intended for Docker Compose restart workflows; the default
    remains fail-fast to prevent accidental reseeding of runtime directories.
USAGE
}

CONFIG=""
PATH_TEMPLATE=""
REGION_STATE="running"
SKIP_EXISTING=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG=$2
      shift 2
      ;;
    --path-template)
      PATH_TEMPLATE=$2
      shift 2
      ;;
    --state)
      REGION_STATE=$2
      shift 2
      ;;
    --skip-existing)
      SKIP_EXISTING=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "bootstrap.sh: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

ROOT_DIR=$NOKV_ROOT_DIR
if [[ -z "$CONFIG" ]]; then
  CONFIG="$ROOT_DIR/raft_config.example.json"
fi

if [[ ! -f "$CONFIG" ]]; then
  nokv_die "bootstrap.sh: configuration not found: $CONFIG"
fi

if [[ -z "$PATH_TEMPLATE" ]]; then
  nokv_die "bootstrap.sh: --path-template is required"
fi

if [[ "$PATH_TEMPLATE" != *"{id}"* ]]; then
  nokv_die "bootstrap.sh: --path-template must contain '{id}' placeholder"
fi

if ! command -v nokv-config >/dev/null 2>&1; then
  nokv_die "bootstrap.sh: nokv-config binary not found in PATH"
fi

STORE_LINES=()
while IFS= read -r _line; do STORE_LINES+=("$_line"); done < <(nokv_config_store_lines "$CONFIG")
if [[ "${#STORE_LINES[@]}" -eq 0 ]]; then
  nokv_die "bootstrap.sh: no stores defined in $CONFIG"
fi

REGION_LINES=()
while IFS= read -r _line; do REGION_LINES+=("$_line"); done < <(nokv_config_region_lines "$CONFIG")
if [[ "${#REGION_LINES[@]}" -eq 0 ]]; then
  nokv_die "bootstrap.sh: no regions defined in $CONFIG"
fi

for store_line in "${STORE_LINES[@]}"; do
  read -r store_id _ <<<"$store_line"
  if [[ -z "$store_id" ]]; then
    continue
  fi
  store_path=${PATH_TEMPLATE//\{id\}/$store_id}
  mkdir -p "$store_path"
  if [[ -f "$store_path/CURRENT" ]]; then
    if [[ "$SKIP_EXISTING" -eq 1 ]]; then
      echo "bootstrap.sh: store $store_id already bootstrapped; skipping"
      continue
    fi
    nokv_die "bootstrap.sh: store $store_id already bootstrapped; refusing to seed into an existing runtime workdir"
  fi
  nokv_assert_fresh_workdir "$store_path" "bootstrap.sh: store $store_id has stale files; refusing to seed into dirty directory"
  for region_line in "${REGION_LINES[@]}"; do
    read -r region_id start_key end_key epoch_ver epoch_conf peer_str _ <<<"$region_line"
    args=(--workdir "$store_path" --region-id "$region_id" --epoch-version "$epoch_ver" --epoch-conf-version "$epoch_conf" --state "$REGION_STATE" --bootstrap-store-id "$store_id")
    if [[ "$start_key" != "-" ]]; then
      args+=(--start-key "$start_key")
    fi
    if [[ "$end_key" != "-" ]]; then
      args+=(--end-key "$end_key")
    fi
    IFS=',' read -ra peers <<<"$peer_str"
    for peer in "${peers[@]}"; do
      if [[ -n "$peer" ]]; then
        args+=(--peer "$peer")
      fi
    done
    nokv-config catalog "${args[@]}"
  done
  echo "bootstrap.sh: bootstrapped store ${store_id} at ${store_path}"
done
