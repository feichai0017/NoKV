# FSMetadata

`fsmeta` is NoKV's native metadata service for distributed filesystem-style
workloads. It is not a POSIX filesystem frontend. It is a typed metadata API
layer built on top of NoKV's existing distributed substrate:

```text
fsmeta/client
    -> fsmeta/server
        -> fsmeta/exec
            -> raftstore/client
                -> coordinator (TSO, region routing, store discovery)
                -> raftstore stores
```

The point of `fsmeta` is to expose metadata operations in their natural shape
instead of forcing users to rebuild filesystem semantics from raw key/value
operations.

## API Surface

The current v1 service is defined in `pb/fsmeta/fsmeta.proto` and implemented
by `fsmeta/server`.

| RPC | Meaning |
|---|---|
| `Create` | Create one dentry and inode atomically. |
| `Lookup` | Read one dentry by `(mount, parent inode, name)`. |
| `ReadDir` | Scan dentries under one parent inode. |
| `ReadDirPlus` | Scan dentries and fetch inode attributes in one typed operation. |
| `WatchSubtree` | Live prefix-scoped metadata change stream with explicit ack/back-pressure. |
| `SnapshotSubtree` | Publish a stable MVCC read epoch for later snapshot-version reads. |
| `Rename` | Atomically move one dentry from one parent/name to another. |
| `Unlink` | Delete one dentry. |

`ReadDirPlus` is the main Stage 1 shape advantage: the native path performs one
dentry scan plus one batched inode read at one snapshot timestamp. A generic KV
schema usually implements the same operation as one scan plus N point reads.

## Current Scope

Stage 1 intentionally keeps the model small:

| Area | Status |
|---|---|
| Dentry and inode binary codecs | Implemented |
| Plan-driven operation contracts | Implemented |
| Create / Lookup / ReadDir / ReadDirPlus / Rename / Unlink | Implemented |
| Cross-region 2PC consumption | Implemented through `raftstore/client` |
| Server-side `AssertionNotExist` | Implemented in Percolator prewrite |
| Native gRPC service and typed Go client | Implemented |
| Docker Compose service | Implemented |
| Live `WatchSubtree` | Implemented in Stage 2.2 |
| `SnapshotSubtree` MVCC epoch | Implemented in Stage 2.3 |
| Historical watch catch-up, hardlink ref-count, xattrs, quota fence | Future work |

The current service is a metadata substrate, not a complete filesystem stack.
FUSE, POSIX compatibility, historical watch catch-up, quota, recursive subtree
materialization, and snapshot GC retention enforcement belong to later stages.

## SnapshotSubtree

`SnapshotSubtree` returns a `read_version` from coordinator TSO and publishes the
epoch into rooted truth as `SnapshotEpochPublished`. Subsequent `ReadDir` /
`ReadDirPlus` calls can pass that version through `ReadDirRequest.snapshot_version`
to read a stable MVCC view.

V0 is intentionally a read-epoch primitive:

- It does not copy the subtree.
- It does not recursively traverse children.
- It does not enforce MVCC GC retention yet, because data-plane GC is not enabled.
- It records the snapshot epoch in `meta/root` so future GC / audit / namespace
  authority work has a durable contract to depend on.

## Running With Docker Compose

Build and start the full local stack:

```bash
docker compose up -d --build
```

This starts:

| Component | Endpoint |
|---|---|
| `nokv-fsmeta` | `127.0.0.1:8090` |
| fsmeta metrics | `http://127.0.0.1:9400/debug/vars` |
| coordinators | `127.0.0.1:2390`, `2391`, `2392` |
| Redis gateway | `127.0.0.1:6380` |

The compose bootstrap job is idempotent for existing Docker volumes. It uses
`scripts/ops/bootstrap.sh --skip-existing`, so repeated `docker compose up` runs
do not fail when stores already contain a `CURRENT` manifest.

Check service health:

```bash
docker compose ps --all
curl -sf http://127.0.0.1:9400/debug/vars >/dev/null && echo fsmeta-ok
```

## Running The Service Directly

After a NoKV coordinator and raftstore cluster are running, start fsmeta with:

```bash
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --metrics-addr 127.0.0.1:9400
```

`nokv-fsmeta` only needs coordinator endpoints. Store addresses are discovered
through the coordinator store registry, following the same bootstrap pattern as
TiKV clients using PD.

When the raftstore nodes run inside Docker Compose, use the compose-managed
`fsmeta` service. Those stores advertise Docker-network names such as
`nokv-store-1:20160`, which are not host-reachable unless the stores are started
with host-scoped advertised addresses.

## Native-vs-Generic Benchmark

The benchmark lives under `benchmark/fsmeta`.

Run it inside the Docker network so both paths can resolve coordinator-advertised
store addresses:

```bash
RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
docker run --rm --network nokv_default \
  -v "$PWD":/workspace \
  -w /workspace/benchmark \
  -e NOKV_FSMETA_BENCH=1 \
  golang:1.26.2-bookworm \
  go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -timeout 30m -v -args \
    -fsmeta_drivers native-fsmeta,generic-kv \
    -fsmeta_mount "fsmeta-formal-${RUN_TS}" \
    -fsmeta_addr nokv-fsmeta:8090 \
    -fsmeta_coordinator_addr nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379 \
    -fsmeta_workloads checkpoint-storm,hotspot-fanin \
    -fsmeta_clients 8 \
    -fsmeta_dirs 16 \
    -fsmeta_files_per_dir 32 \
    -fsmeta_files 512 \
    -fsmeta_reads_per_client 16 \
    -fsmeta_page_limit 512 \
    -fsmeta_readdirplus=true \
    -fsmeta_timeout 25m \
    -fsmeta_output "../data/fsmeta/results/fsmeta_native_vs_generic_${RUN_TS}.csv"
```

The test binary runs from the package directory `benchmark/fsmeta`. The example
above writes a local ignored artifact to `<repo>/benchmark/data/...` via
`../data/...`. Curated, committed results live under
`benchmark/fsmeta/results/`; point `-fsmeta_output` at `results/...` when
promoting a run to a tracked artifact.

For the Stage 1 result interpretation, see
`docs/notes/2026-04-25-fsmeta-stage1-benchmark-results.md`.

Run the Stage 2.2 WatchSubtree evidence workload:

```bash
RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
docker run --rm --network nokv_default \
  -v "$PWD":/workspace \
  -w /workspace/benchmark \
  -e NOKV_FSMETA_BENCH=1 \
  golang:1.26.2-bookworm \
  go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -timeout 20m -v -args \
    -fsmeta_drivers native-fsmeta \
    -fsmeta_mount "fsmeta-watch-${RUN_TS}" \
    -fsmeta_addr nokv-fsmeta:8090 \
    -fsmeta_coordinator_addr nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379 \
    -fsmeta_workloads watch-subtree \
    -fsmeta_clients 8 \
    -fsmeta_files 512 \
    -fsmeta_watch_window 1024 \
    -fsmeta_timeout 15m \
    -fsmeta_output "../data/fsmeta/results/fsmeta_watchsubtree_${RUN_TS}.csv"
```

## Why Native Metadata API Matters

The generic-KV baseline intentionally uses the same NoKV raftstore and
Percolator substrate. The difference is API shape:

| Path | Create | ReadDirPlus |
|---|---|---|
| `native-fsmeta` | Server-side multi-key `AssertionNotExist` | Scan + batched inode fetch at one snapshot |
| `generic-kv` | Client-side read-then-write checks | Scan + one point Get per dentry |

This isolates the question NoKV is trying to answer in Stage 1: what does the
system gain when filesystem metadata operations are first-class operations
instead of application-level conventions over a raw KV API?
