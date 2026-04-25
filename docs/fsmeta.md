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
| Hardlink ref-count, xattrs, subtree watch, quota fence | Not in Stage 1 |

The current service is a metadata substrate, not a complete filesystem stack.
FUSE, POSIX compatibility, subtree-level watch, quota, and snapshot semantics
belong to later stages.

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
  golang:1.26 \
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
    -fsmeta_output "../data/fsmeta/results/fsmeta_formal_native_vs_generic_${RUN_TS}.csv"
```

The generated CSV under `benchmark/data/` is a local run artifact and is ignored
by Git. Curated committed results live under `benchmark/fsmeta/results/`.

For the Stage 1 result interpretation, see
`docs/notes/2026-04-25-fsmeta-stage1-benchmark-results.md`.

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
