#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/gcp/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

config_arg="${1:-}"
load_config "$config_arg"
load_last_images
require_cmd gcloud openssl

if [[ -z "${NOKV_IMAGE:-}" || -z "${NOKV_BENCH_IMAGE:-}" ]]; then
  die "NOKV_IMAGE and NOKV_BENCH_IMAGE are required; run deploy/gcp/build-push-images.sh first"
fi

"$SCRIPT_DIR/create-infra.sh" "$config_arg"

generated_dir="$SCRIPT_DIR/generated"
mkdir -p "$generated_dir"
created_instances=""

cleanup_created_instances() {
  local status=$?
  if [[ "$status" -eq 0 || -z "$created_instances" ]]; then
    return "$status"
  fi
  echo "Cluster creation failed; deleting instances created in this run to stop VM billing:" >&2
  while IFS= read -r instance; do
    [[ -n "$instance" ]] || continue
    echo "  $instance" >&2
    gcloud compute instances delete "$instance" \
      --zone="$GCP_ZONE" \
      --project="$GCP_PROJECT" \
      --quiet || true
  done <<<"$created_instances"
  return "$status"
}
trap cleanup_created_instances EXIT

signing_key_file="$generated_dir/eunomia-signing-key.txt"
if [[ ! -f "$signing_key_file" ]]; then
  openssl rand -base64 32 >"$signing_key_file"
fi
signing_key="$(tr -d '\n' <"$signing_key_file")"

raft_config="$generated_dir/raft_config.gcp.json"
cat >"$raft_config" <<'JSON'
{
  "max_retries": 5,
  "meta_root": {
    "peers": [
      {
        "node_id": 1,
        "addr": "10.42.0.11:2380",
        "transport_addr": "10.42.0.11:2480",
        "work_dir": "/mnt/nokv/meta-root"
      },
      {
        "node_id": 2,
        "addr": "10.42.0.12:2380",
        "transport_addr": "10.42.0.12:2480",
        "work_dir": "/mnt/nokv/meta-root"
      },
      {
        "node_id": 3,
        "addr": "10.42.0.13:2380",
        "transport_addr": "10.42.0.13:2480",
        "work_dir": "/mnt/nokv/meta-root"
      }
    ]
  },
  "coordinator": {
    "addr": "10.42.0.21:2379,10.42.0.22:2379,10.42.0.23:2379"
  },
  "store_work_dir_template": "/mnt/nokv/store-{id}",
  "stores": [
    {
      "store_id": 1,
      "listen_addr": "10.42.0.31:20160",
      "addr": "10.42.0.31:20160"
    },
    {
      "store_id": 2,
      "listen_addr": "10.42.0.32:20160",
      "addr": "10.42.0.32:20160"
    },
    {
      "store_id": 3,
      "listen_addr": "10.42.0.33:20160",
      "addr": "10.42.0.33:20160"
    }
  ],
  "fsmeta_region_bootstrap": {
    "mounts": [
      {"mount_id": "default", "mount_key_id": 1},
      {"mount_id": "fsmeta-bench", "mount_key_id": 2}
    ],
    "bucket_count": 16,
    "region_id_base": 1000,
    "peer_id_base": 100000,
    "leader_store_ids": [1, 2, 3]
  }
}
JSON

startup_script() {
  local role="$1"
  local ordinal="$2"
  local output="$3"
  cat >"$output" <<EOF
#!/usr/bin/env bash
set -euxo pipefail

ROLE="$role"
ORDINAL="$ordinal"
NOKV_IMAGE="$NOKV_IMAGE"
NOKV_BENCH_IMAGE="$NOKV_BENCH_IMAGE"
ARTIFACT_HOST="$ARTIFACT_HOST"
SIGNING_KEY="$signing_key"
COORDINATOR_ADDR="10.42.0.21:2379,10.42.0.22:2379,10.42.0.23:2379"
FSMETA_ADDR="10.42.0.41:8090"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl docker.io jq mdadm
systemctl enable --now docker

mkdir -p /etc/nokv /mnt/nokv
cat >/etc/nokv/raft_config.json <<'JSON'
$(cat "$raft_config")
JSON

mount_nokv() {
  if mountpoint -q /mnt/nokv; then
    return
  fi
  mapfile -t devices < <(find /dev/disk/by-id -type l \\( -name 'google-local-nvme-ssd-*' -o -name 'google-local-ssd-*' \\) | sort || true)
  if [[ "\${#devices[@]}" -eq 0 ]]; then
    mkdir -p /mnt/nokv
    chmod 0777 /mnt/nokv
    return
  fi
  local target
  if [[ "\${#devices[@]}" -eq 1 ]]; then
    target="\${devices[0]}"
    if ! blkid "\$target" >/dev/null 2>&1; then
      mkfs.ext4 -F "\$target"
    fi
  else
    target="/dev/md/nokv"
    if [[ ! -e "\$target" ]]; then
      mdadm --create "\$target" --level=0 --raid-devices="\${#devices[@]}" "\${devices[@]}" --force
      mkfs.ext4 -F "\$target"
    fi
  fi
  mount -o noatime,nodiratime "\$target" /mnt/nokv
  chmod 0777 /mnt/nokv
}
mount_nokv

token="\$(curl -fsS -H 'Metadata-Flavor: Google' 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' | jq -r .access_token)"
printf '%s' "\$token" | docker login -u oauth2accesstoken --password-stdin "https://\$ARTIFACT_HOST"
docker pull "\$NOKV_IMAGE"

case "\$ROLE" in
  meta-root)
    docker rm -f "nokv-meta-root" >/dev/null 2>&1 || true
    docker run -d --name nokv-meta-root --restart unless-stopped --network host \\
      -e "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY=\$SIGNING_KEY" \\
      -v /mnt/nokv:/mnt/nokv \\
      -v /etc/nokv:/etc/nokv:ro \\
      "\$NOKV_IMAGE" \\
      meta-root --config=/etc/nokv/raft_config.json --scope=host --node-id="\$ORDINAL" --metrics-addr=0.0.0.0:9380
    ;;
  coordinator)
    docker rm -f "nokv-coordinator" >/dev/null 2>&1 || true
    docker run -d --name nokv-coordinator --restart unless-stopped --network host \\
      -e "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY=\$SIGNING_KEY" \\
      -v /etc/nokv:/etc/nokv:ro \\
      "\$NOKV_IMAGE" \\
      coordinator --config=/etc/nokv/raft_config.json --scope=host --addr=0.0.0.0:2379 --metrics-addr=0.0.0.0:9100 \\
        --coordinator-id="coord-\$ORDINAL" --grant-candidates=coord-1,coord-2,coord-3 \\
        --grant-duties=alloc_id,tso,region_lookup --grant-ttl=30s --grant-renew-before=10s
    ;;
  store)
    docker rm -f "nokv-store" >/dev/null 2>&1 || true
    docker run --rm --network host \\
      -e "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY=\$SIGNING_KEY" \\
      -v /mnt/nokv:/mnt/nokv \\
      -v /etc/nokv:/etc/nokv:ro \\
      --entrypoint /usr/local/bin/bootstrap.sh \\
      "\$NOKV_IMAGE" \\
      --config /etc/nokv/raft_config.json --path-template /mnt/nokv/store-{id} --skip-existing
    docker run -d --name nokv-store --restart unless-stopped --network host \\
      -e "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY=\$SIGNING_KEY" \\
      -v /mnt/nokv:/mnt/nokv \\
      -v /etc/nokv:/etc/nokv:ro \\
      "\$NOKV_IMAGE" \\
      serve --config=/etc/nokv/raft_config.json --scope=host --store-id="\$ORDINAL" \\
        --coordinator-addr="\$COORDINATOR_ADDR" --metrics-addr=0.0.0.0:9200 \\
        --storage-max-batch-count=1024
    ;;
  gateway)
    docker rm -f "nokv-fsmeta" >/dev/null 2>&1 || true
    mkdir -p /mnt/nokv/negative-cache /mnt/nokv/dirpage-cache
    chmod 0777 /mnt/nokv/negative-cache /mnt/nokv/dirpage-cache
    docker run -d --name nokv-fsmeta --restart unless-stopped --network host \\
      -e "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY=\$SIGNING_KEY" \\
      -v /mnt/nokv:/mnt/nokv \\
      --entrypoint /usr/local/bin/nokv-fsmeta \\
      "\$NOKV_IMAGE" \\
      --addr=0.0.0.0:8090 --coordinator-addr="\$COORDINATOR_ADDR" \\
        --metrics-addr=0.0.0.0:9400 --negative-cache-dir=/mnt/nokv/negative-cache --dirpage-cache-dir=/mnt/nokv/dirpage-cache
    ;;
  loadgen)
    docker pull "\$NOKV_BENCH_IMAGE"
    mkdir -p /mnt/nokv/results
    chmod 0777 /mnt/nokv/results
    ;;
  *)
    echo "unknown role: \$ROLE" >&2
    exit 2
    ;;
esac
EOF
}

create_instance() {
  local name="$1"
  local role="$2"
  local ordinal="$3"
  local ip="$4"
  local machine_type="$5"
  local startup="$generated_dir/startup-${name}.sh"
  startup_script "$role" "$ordinal" "$startup"

  if gcloud compute instances describe "$name" --zone="$GCP_ZONE" --project="$GCP_PROJECT" >/dev/null 2>&1; then
    echo "Instance already exists: $name"
    return
  fi

  network_interface="network=${GCP_NETWORK},subnet=${GCP_SUBNET},private-network-ip=${ip}"
  if ! is_truthy "$GCP_ASSIGN_EXTERNAL_IPS"; then
    network_interface="${network_interface},no-address"
  fi

  if is_truthy "$GCP_USE_COMPACT_PLACEMENT"; then
    gcloud compute instances create "$name" \
      --project="$GCP_PROJECT" \
      --zone="$GCP_ZONE" \
      --machine-type="$machine_type" \
      --image-family="$GCP_IMAGE_FAMILY" \
      --image-project="$GCP_IMAGE_PROJECT" \
      --boot-disk-size="${GCP_BOOT_DISK_SIZE_GB}GB" \
      --network-interface="$network_interface" \
      --tags="$GCP_CLUSTER_NAME" \
      --scopes=cloud-platform \
      --maintenance-policy=TERMINATE \
      --no-restart-on-failure \
      --resource-policies="$GCP_RESOURCE_POLICY" \
      --metadata-from-file=startup-script="$startup" \
      --quiet
  else
    gcloud compute instances create "$name" \
      --project="$GCP_PROJECT" \
      --zone="$GCP_ZONE" \
      --machine-type="$machine_type" \
      --image-family="$GCP_IMAGE_FAMILY" \
      --image-project="$GCP_IMAGE_PROJECT" \
      --boot-disk-size="${GCP_BOOT_DISK_SIZE_GB}GB" \
      --network-interface="$network_interface" \
      --tags="$GCP_CLUSTER_NAME" \
      --scopes=cloud-platform \
      --metadata-from-file=startup-script="$startup" \
      --quiet
  fi
  created_instances="${created_instances}${name}"$'\n'
}

create_instance "${GCP_CLUSTER_NAME}-meta-root-1" meta-root 1 10.42.0.11 "$GCP_META_ROOT_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-meta-root-2" meta-root 2 10.42.0.12 "$GCP_META_ROOT_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-meta-root-3" meta-root 3 10.42.0.13 "$GCP_META_ROOT_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-coordinator-1" coordinator 1 10.42.0.21 "$GCP_COORDINATOR_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-coordinator-2" coordinator 2 10.42.0.22 "$GCP_COORDINATOR_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-coordinator-3" coordinator 3 10.42.0.23 "$GCP_COORDINATOR_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-store-1" store 1 10.42.0.31 "$GCP_STORE_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-store-2" store 2 10.42.0.32 "$GCP_STORE_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-store-3" store 3 10.42.0.33 "$GCP_STORE_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-gateway-1" gateway 1 10.42.0.41 "$GCP_GATEWAY_MACHINE_TYPE"
create_instance "${GCP_CLUSTER_NAME}-loadgen-1" loadgen 1 10.42.0.51 "$GCP_LOADGEN_MACHINE_TYPE"

echo "Cluster create requests submitted. Check startup progress with:"
echo "  gcloud compute instances list --filter='name~${GCP_CLUSTER_NAME}' --project=${GCP_PROJECT}"
