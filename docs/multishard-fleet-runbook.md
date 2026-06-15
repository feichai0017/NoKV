<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Multi-Shard Fleet Deployment Runbook

How to run a NoKV-FS metadata fleet across multiple machines: a sharded,
subtree-partitioned metadata plane on top of a shared etcd control plane and a
shared S3/RustFS object store. The local single-box gate
`scripts/run-multishard-fleet-smoke.sh` exercises this exact shape in one
process tree; this runbook is the cross-machine version of the same flow.

## Components

| Component | Role | Cardinality |
| --- | --- | --- |
| etcd | control plane: shard ownership, leases, epochs, checkpoint/log pointers | 3 or 5 nodes (quorum) in prod; 1 for dev |
| S3 / RustFS | durable object data + metadata checkpoint images + shared-log segments | shared, reachable by every node |
| `nokv serve` (metadata node) | owns one or more shards; serves the framed RPC | one process per owned shard (v1: one shard per process) |
| `nokv` client / FUSE / SDK | routes each path to its shard's owner via etcd | per training/compute node |

Invariant: the control plane stores **no** inode/dentry/chunk state — only
ownership and recovery pointers. Authoritative metadata lives in each shard's
local Holt engine; it is recoverable from the object store (checkpoint + log).

## Prerequisites

- Build the binary with the etcd backend: `cargo build --release -p nokv --features etcd`.
- An etcd cluster reachable from every metadata node and every client
  (`http://etcd-0:2379,http://etcd-1:2379,http://etcd-2:2379`).
- An S3-compatible endpoint (RustFS/MinIO/S3) with a bucket, reachable by all
  metadata nodes (object data + checkpoints + shared log live here).
- Decide the shard layout: a default/root shard (`mount-<N>:/`, index 0) plus one
  subtree shard per dataset/run prefix, each with a unique `shard_index`
  (1, 2, 3, …). The index is encoded in the high bits of every inode the shard
  mints, so it must be stable and unique per shard.

## 1. Shared control + object args (every process uses these)

```sh
ETCD=http://etcd-0:2379,http://etcd-1:2379,http://etcd-2:2379
PREFIX=/nokv/control/prod            # etcd key namespace for this cluster
S3=( --object-backend rustfs --s3-bucket nokv --s3-endpoint http://s3-0:9000
     --s3-access-key-id "$AK" --s3-secret-access-key "$SK" )
CTRL=( --mount 1 --control-backend etcd
       --control-etcd-endpoints "$ETCD" --control-etcd-prefix "$PREFIX"
       --control-etcd-lease-ttl-seconds 10
       --metadata-shared-log-prefix metadata/prod/shared-log
       --metadata-checkpoint-archive-prefix metadata/prod/checkpoints )
```

**Lease TTL guidance.** `--control-etcd-lease-ttl-seconds T` drives everything:
the server renews at `min(default, T/3)` and self-fences at `T`, both derived
automatically. Pick `T` for your failover-RTO target (smaller = faster failover,
more etcd traffic). Do **not** set a renewal interval larger than `T` — the CLI
caps it at `T/3` so the lease can't lapse, but a hand-set
`--shard-owner-renewal-interval-ms` is still honored if smaller.

## 2. Start each metadata node (one shard per process)

Node hosting the **default** shard (owns `/` and anything unsharded):

```sh
nokv --meta /var/lib/nokv/meta-default --server-bind 0.0.0.0:7740 \
     "${S3[@]}" "${CTRL[@]}" \
     --shard-id "mount-1:/" --shard-index 0 --node-id "metanode-0:7740" \
     serve
```

Node hosting a **dataset** subtree shard:

```sh
nokv --meta /var/lib/nokv/meta-dataset --server-bind 0.0.0.0:7741 \
     "${S3[@]}" "${CTRL[@]}" \
     --shard-id "mount-1:/dataset" --shard-index 1 --node-id "metanode-1:7741" \
     serve
```

- `--node-id` MUST be the address clients can reach (it is published as the
  shard's `endpoint` in etcd and is how the fleet client dials this shard). Use a
  routable host:port, not `127.0.0.1`.
- `--shard-index` declares (registers) this shard's identity in etcd on open; the
  path prefix is derived from `--shard-id`. Each shard gets its own per-shard
  Holt state dir, checkpoint prefix, and shared-log prefix automatically.
- Add more subtree shards by repeating with new `--shard-id`/`--shard-index` on
  more nodes.

## 3. Point clients at the control plane (not a single server)

```sh
nokv "${S3[@]}" --mount 1 --control-backend etcd \
     --control-etcd-endpoints "$ETCD" --control-etcd-prefix "$PREFIX" \
     ls /dataset            # routes to the /dataset shard's owner via etcd
```

The client builds a shard map from `list_shards`, routes each request by
longest-prefix (path) or inode high-bits, caches it, and on `NotOwner` /
stale-owner refreshes from etcd and retries — so it follows handoffs
transparently. (The same wiring backs FUSE mounts and the Python SDK when given
the control endpoints.)

## 4. Failover / rebalance

A shard's authoritative Holt state is local, but its checkpoint image + shared
log live in the object store, so **handoff == failover restore** (no node-to-node
data movement):

1. The owner dies (or is drained). Its etcd lease expires after `T` seconds; the
   stable session key auto-deletes.
2. Bring up a replacement on any node, pointed at the same shard, with the failed
   epoch:
   ```sh
   nokv --meta /var/lib/nokv/meta-dataset-b --server-bind 0.0.0.0:7742 \
        "${S3[@]}" "${CTRL[@]}" \
        --shard-id "mount-1:/dataset" --shard-index 1 --node-id "metanode-2:7742" \
        --failover-from-epoch <prev_epoch> serve
   ```
   It acquires at `prev_epoch + 1` (etcd `create_revision==0` on the session key
   guarantees the old owner is gone — no split-brain), restores the latest
   checkpoint image, replays the shared-log segments above it, then marks
   serving. The old owner, if it comes back, is fenced by the epoch and by its
   own expired lease deadline.
3. Clients re-resolve to the new endpoint automatically.

Rebalancing a shard to a different node is the same procedure (drain → failover
acquire on the target).

## 5. Cross-machine smoke (reuse the local gate against external services)

`scripts/run-multishard-fleet-smoke.sh` is single-box by default but is fully
env-parameterized; to point it at an external etcd + RustFS (so it does NOT start
its own) and at real binds, set:

```sh
NOKV_FLEET_ETCD_ENDPOINTS="$ETCD" \           # set ⇒ script uses external etcd, skips local start
NOKV_FLEET_RUSTFS_ENDPOINT="http://s3-0:9000" \
NOKV_FLEET_RUSTFS_ACCESS_KEY="$AK" NOKV_FLEET_RUSTFS_SECRET_KEY="$SK" \
NOKV_FLEET_SERVER_A_BIND="0.0.0.0:7740" NOKV_FLEET_SERVER_B_BIND="0.0.0.0:7741" \
NOKV_FLEET_SERVER_B2_BIND="0.0.0.0:7742" \
NOKV_FLEET_METRICS_JSON=/tmp/fleet-metrics.json \
  scripts/run-multishard-fleet-smoke.sh
```

For a true multi-machine run, execute the per-node `nokv serve` commands from §2
on separate hosts (against the shared etcd + S3) and drive the §3 client from a
fourth host, rather than the monolithic script — the script colocates all roles
on one box and is meant as the single-box gate.

## 6. Validation checklist

- `nokv … cat /dataset/x` and `… cat /other/y` route to different nodes; the
  returned inodes carry different shard indices in their high bits.
- Kill a shard's owner; a failover owner acquires `epoch+1` and a client read of
  that shard succeeds after re-resolve (no data loss across the checkpoint
  boundary — the shared log is replayed).
- `curl http://<node>/fsck` reports `dangling_count:0` on every shard.
- A partitioned/paused owner stops committing by its lease deadline (`T`) even if
  it cannot reach etcd.

## Known limits (v1)

- One shard per server process; one shard owner at a time (single-writer per
  shard + failover, not multi-writer replication).
- Cross-shard `rename`/`hardlink`/`clone` return `EXDEV` (no distributed 2PC).
- Subtree shards are created at registration time; live migration of an
  already-populated subtree across shards is not supported.
- Cross-shard queries (find/aggregate/grep at a scope above a graft point) see
  only the routed shard.
