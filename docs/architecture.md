<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Architecture

NoKV is a Rust-first filesystem for AI training and agent workspaces. The
repository is intentionally product-shaped: metadata semantics, object body
storage, clients, FUSE, docs, and examples live at the repository root instead
of behind a nested workspace.

The implemented tree is the Rust client/server filesystem slice: FUSE and the
SDK talk to `nokv-server`, `nokv-server` commits semantic metadata commands
through an OpenRaft metadata group, and Holt stores the applied state machine.
The product target, including production metadata HA, CSI, Python/fsspec, and
node-local cache, is recorded in [Product Design](./product-design.md).

## Layers

```text
Application surface
  nokv-client    Rust SDK
  nokv           CLI
  nokv-fuse      low-level FUSE frontend
  nokv-python    planned Python/fsspec bindings
  nokv-csi       planned Kubernetes CSI integration

Metadata layer
  nokv-types     mount, inode, dentry, body descriptor, watch event types
  nokv-protocol  metadata RPC wire DTOs
  nokv-meta      schema, MetadataCommand, Holt store, service core
  nokv-server    long-running metad process, RPC, health, and control plane

Body storage layer
  nokv-object    S3-compatible object storage, including RustFS
```

## Write Path

```mermaid
flowchart LR
    App["AI training / agent client"] --> API["NoKV metad"]
    App["FUSE / SDK / CLI"] --> API["nokv-meta service"]
    API --> Command["MetadataCommand"]
    Command --> Holt["Holt metadata store"]
    API --> Object["S3-compatible object store"]
```

For artifact publication, object bytes are uploaded first. The metadata commit
then publishes the dentry, inode projection, and body descriptor atomically.
Failed metadata publish leaves staged objects for later garbage collection.

`nokv-server` runs the same local `nokv-meta` service in a long-lived
process. It owns health, readiness, stats, manual GC endpoints, and the first
metadata RPC. The SDK hot path uses a length-prefixed framed RPC on the same
port; HTTP stays limited to health, stats, and manual GC control. The RPC
supports both inode/name operations and path-oriented SDK operations, so
server-side path resolution can avoid multi-round-trip nested creates. It also
supports ordered non-atomic batches: each subrequest has its own result/error,
but the batch removes per-operation network round trips for SDK workloads. The
Rust SDK has a metadata client for namespace operations and an object-backed
file client that uploads object blocks directly, asks `metad` to atomically publish
the body manifest, fetches body read plans, and reads object ranges directly
from the configured object store. The server stats endpoint reports
metadata-store write attribution counters so benchmark runs can distinguish
current writes, history writes, watch writes, and dedupe writes. The FUSE
frontend uses the same metadata client/server boundary as the SDK.

## FUSE Path

The current FUSE frontend is inode-first. It maps kernel `lookup`, `getattr`,
`readdir`, `open`, and `read` calls to `metad` inode APIs and object-store range
reads. It does not resolve paths through the Rust SDK and does not own metadata
semantics. Live mounts register observed directory scopes with the metadata
watch log and translate typed watch events into FUSE `inval_entry` and
`inval_inode` notifications. Snapshot mounts are read-only and do not start the
invalidation worker.

## Metadata Layout

The canonical model is inode/dentry, described in
[Metadata Schema](./metadata-schema.md):

```text
inode_current:
  mount_id | inode_id -> inode attributes

dentry_current:
  mount_id | parent_inode | name -> dentry + inode projection

chunk_manifest_current:
  mount_id | inode_id | generation | u64::MAX -> body summary
  mount_id | inode_id | generation | chunk_index -> block manifest

history:
  family | user_key_len | user_key | inverted_commit_version -> old value
```

Path indexes are derived accelerators for artifact and checkpoint fast paths;
they are not namespace truth.

## Object Storage

NoKV stores file bodies outside the metadata service. File bytes are split
into immutable object blocks and published through metadata manifests. The first
production body backend is S3-compatible storage. RustFS, MinIO, Ceph RGW, and
AWS S3 all use the same object-store boundary. See
[Object Layout](./object-layout.md).

## Metadata Disaster Recovery

File bodies are durable in the object store, but the namespace that gives them
meaning — inodes, dentries, versions, and CoW relationships — lives in the local
Holt engine. Losing that node would lose the namespace even though every object
survives. To close that single point of total loss, the metadata engine is
periodically archived to the same object store.

A background worker exports a Holt checkpoint image and publishes it under a
configurable object-key prefix (`--metadata-checkpoint-archive-prefix`, on by
default; disable with `--no-metadata-checkpoint-archive`). Publication mirrors
the body write path — **object-first, pointer-second**:

```text
1. checkpoint image  -> {prefix}/ckpt/{seq}.image     (object-first)
2. CURRENT manifest  -> {prefix}/CURRENT              (atomic pointer swap)
3. prune checkpoints older than the retained window   (after the swap)
```

The single `CURRENT` object names the live checkpoint and the retained-checkpoint
window, so retention works without an object `list`. A crash between steps 1 and
2 leaves an orphan checkpoint object (reclaimed on a later backup), never a
manifest that points at a missing checkpoint.

Recovery runs on a replacement node with an empty metadata directory:

```text
nokv restore        # GET CURRENT -> GET checkpoint -> install into a fresh store
nokv serve          # resume serving the recovered namespace
```

`restore` installs the checkpoint into a fresh Holt store (which must be empty — a
checkpoint install cannot merge into a populated store) and rehydrates the
allocator, so the recovered node both serves the prior namespace and accepts new
writes. `nokv backup` triggers an out-of-band archive on a running server, and
`/stats` reports the worker's `metadata_backup` state. The recovery-point
objective is the worker interval; the bodies were always safe in the object store.

## Consistency Checking

`nokv fsck` verifies the live namespace against the object store: it walks every
live file at its current body generation and confirms each referenced block
still exists (`head`). This is the read-side complement to the object-first write
ordering — the ordering guarantees metadata never references a missing object,
and fsck detects any drift after the fact (an out-of-band deletion, an
eventual-consistency anomaly in external storage, or a latent bug), reporting
each dangling reference as `(inode, generation, object_key)`. Superseded and
snapshot-pinned generations are not mistaken for drift (the scan uses each
inode's current body generation), and a clone's borrowed block keys resolve
against the source objects that still exist. Reclaiming the opposite drift —
orphan objects written but never referenced — is a planned extension that needs
an object-store `list`.

## Distributed Direction

The planned distributed layer is not a generic KV database. It should replicate
metadata commands over mount or shard scoped Raft groups, with Holt as the
state machine storage engine and object bodies remaining in external storage.

Holt is the metadata engine inside each shard. NoKV `metad` owns filesystem
semantics such as inode/dentry updates, watch/snapshot policy, publish rules,
and object GC decisions.

`nokv serve` now defaults to a single-voter OpenRaft metadata group. The
local OpenRaft log is stored under the configured metadata directory, and each
application log entry contains a batch of semantic `MetadataCommand`s. It is not
a raw KV mutation, Percolator transaction, or old raftstore command. The
OpenRaft state machine applies committed batches through the storage-neutral
metadata store trait, so filesystem semantics stay in `nokv-meta` while log
ordering and recovery stay in `nokv-cluster`.

There is no separate compatibility log path. `nokv serve` always uses
the OpenRaft metadata group path, including single-node deployments. The
`--metadata-raft-log-sync data|none` option controls the OpenRaft file-log sync
policy: use `data` when the local log is the durability boundary, and `none`
only for local performance experiments where losing the process-local log is
acceptable.

The OpenRaft v1 target remains one metadata group per mount. Cross-mount atomic
operations are not part of the contract. Multi-node HA uses voters through
storage-neutral transport DTOs for vote, append entries, and snapshot
installation. Learner read scaling remains planned production hardening work;
the object-backed metadata archive is implemented (see Metadata Disaster Recovery
above).
