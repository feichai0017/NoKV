#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: bootstrap_from_config.sh --config <path> --path-template <template>

Options:
  --config PATH          Raft configuration file (default: ./raft_config.example.json)
  --path-template TMPL   Template for store workdirs, e.g. /data/store-{id}
  --state STATE          Optional region state (running|tombstone)
USAGE
}

CONFIG=""
PATH_TEMPLATE=""
REGION_STATE="running"

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
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ -z "$CONFIG" ]]; then
  CONFIG="$ROOT_DIR/raft_config.example.json"
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "bootstrap_from_config: configuration not found: $CONFIG" >&2
  exit 1
fi

if [[ -z "$PATH_TEMPLATE" ]]; then
  echo "bootstrap_from_config: --path-template is required" >&2
  exit 1
fi

if [[ "$PATH_TEMPLATE" != *"{id}"* ]]; then
  echo "bootstrap_from_config: --path-template must contain '{id}' placeholder" >&2
  exit 1
fi

if ! command -v nokv-config >/dev/null 2>&1; then
  echo "bootstrap_from_config: nokv-config binary not found in PATH" >&2
  exit 1
fi

mapfile -t STORE_LINES < <(nokv-config stores --config "$CONFIG" --format simple)
if [[ "${#STORE_LINES[@]}" -eq 0 ]]; then
  echo "bootstrap_from_config: no stores defined in $CONFIG" >&2
  exit 1
fi

mapfile -t REGION_LINES < <(nokv-config regions --config "$CONFIG" --format simple)
if [[ "${#REGION_LINES[@]}" -eq 0 ]]; then
  echo "bootstrap_from_config: no regions defined in $CONFIG" >&2
  exit 1
fi

for store_line in "${STORE_LINES[@]}"; do
  read -r store_id _ <<<"$store_line"
  if [[ -z "$store_id" ]]; then
    continue
  fi
  store_path=${PATH_TEMPLATE//\{id\}/$store_id}
  mkdir -p "$store_path"
  if [[ -f "$store_path/CURRENT" ]]; then
    echo "bootstrap_from_config: store $store_id already bootstrapped; skipping"
    continue
  fi
  for region_line in "${REGION_LINES[@]}"; do
    read -r region_id start_key end_key epoch_ver epoch_conf peer_str _ <<<"$region_line"
    args=(--workdir "$store_path" --region-id "$region_id" --epoch-version "$epoch_ver" --epoch-conf-version "$epoch_conf" --state "$REGION_STATE")
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
    nokv-config manifest "${args[@]}"
  done
  echo "bootstrapped store ${store_id} at ${store_path}"
done
