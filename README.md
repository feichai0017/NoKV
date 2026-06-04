<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.svg" width="200" alt="NoKV" />

  <p>
    <strong>A metadata control plane for AI, ML, and agent workloads.</strong>
  </p>

  <h3>Recognized In The AI-Native Storage Ecosystem</h3>

  <table>
    <tr>
      <td align="center" width="360">
        <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
          <img src="./docs/public/img/recognition/cncf.svg" width="128" alt="Cloud Native Computing Foundation" />
        </a>
        <br />
        <strong>Linux Foundation CNCF Landscape</strong>
        <br />
        <sub>Listed in AI Native Infra / Storage and Cloud Native Storage.</sub>
      </td>
      <td align="center" width="360">
        <a href="https://dbdb.io/db/nokv">
          <img src="./docs/public/img/recognition/dbdb.svg" width="128" alt="DBDB.io Database of Databases" />
        </a>
        <br />
        <strong>DBDB.io Database of Databases</strong>
        <br />
        <sub>Profiled by the CMU Database Group catalog as a Go-native log-structured key/value DBMS.</sub>
      </td>
    </tr>
  </table>

  <p>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV"><img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" /></a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV"><img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" /></a>
    <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
    <a href="https://dbdb.io/db/nokv"><img alt="DBDB.io" src="https://img.shields.io/badge/DBDB.io-profiled-244A64" /></a>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" /></a>
    <a href="https://codecov.io/gh/feichai0017/NoKV"><img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" /></a>
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>
</div>

<br/>

## Goal

NoKV provides a shared metadata control plane for AI, ML, and agent workloads.

The target workload is not generic key/value storage. It is the metadata around
artifacts, workspaces, runs, checkpoints, traces, datasets, model outputs, and
derived files. These records need durable namespace truth, point-in-time reads,
watchable updates, and atomic publish semantics even when the actual bytes live
outside NoKV.

NoKV's stable product core is `fsmeta`: a filesystem-shaped workspace metadata
service backed by a local embedded KV engine today, with a distributed raftstore
runtime for scale-out deployments.

NoKV does not own object bodies, model weights, or POSIX file data. Those remain
in object stores, local filesystems, model stores, or application-defined body
stores. NoKV owns the metadata truth, namespace commit point, compact body
references, and lifecycle coordination.

## Why

AI infrastructure has a metadata control-plane gap.

A single run or agent session can produce prompts, tool outputs, model
responses, checkpoints, logs, traces, evaluation artifacts, and derived
datasets. Today those records are often split across:

- object store prefixes for artifact bytes;
- SQL tables for run metadata;
- local files for checkpoints;
- observability systems for traces;
- framework-specific tracking stores;
- custom glue code for workspace state.

That split creates predictable pain:

- no common metadata publish point for external artifact bodies;
- partial writes and stale staged entries after crashes or retries;
- expensive client-side directory reconstruction from generic KV scans;
- duplicated indexing, lifecycle, and GC code across every AI stack;
- weak workspace change feeds for collaborative or multi-agent systems;
- framework lock-in around artifact layout and run metadata;
- growing maintenance cost from stitching SQL, object-store listing, and
  coordination logic into each new product.

NoKV makes that metadata layer explicit.

## Why NoKV?

NoKV is purpose-built for metadata semantics, not generic storage wrapped in
conventions.

### Native metadata operations

`fsmeta` exposes namespace operations directly: create, lookup, fused directory
listing, atomic rename, overwrite publish, subtree move, snapshot, watch, writer
sessions, quota, and remove. Applications do not have to rediscover those
protocols with ad hoc `Get` / `Put` / `Scan` sequences.

### Tunable local engine

NoKV ships with its own embedded KV engine, WAL, LSM, MVCC, transaction path,
watch plumbing, and metadata executor. The default development path is local
and self-contained:

```text
application / SDK
  -> fsmeta
  -> fsmeta/runtime/local
  -> local MVCC KV
```

That keeps early integration work light: no SQL schema migration layer, no
separate coordination service, no object-store prefix polling loop, and less
application glue around every metadata operation.

### Distributed path when needed

The same metadata contract can run on the distributed runtime:

```text
application / SDK
  -> fsmeta client/server
  -> fsmeta/runtime/raftstore
  -> coordinator + meta/root + TSO + raftstore + transactions
```

This path is for larger agent platforms, distributed workspace services, and
DFS-scale namespace metadata. The local runtime remains the default product path
for development and small deployments.

### Bring your own data plane

NoKV stores metadata and compact references. Artifact bytes stay outside NoKV.

Examples:

- local file body stores for tests and development;
- S3/R2/GCS-style body stores for production;
- model registry or dataset storage owned by another system;
- application-defined checkpoint payload stores.

The boundary is deliberate: NoKV owns namespace truth, body references, metadata
versions, watches, and lifecycle coordination.

## `fsmeta` - Workspace Namespace Metadata Service

`fsmeta` is the stable NoKV product contract.

It provides a durable, versioned, watchable namespace shaped like filesystem
metadata. It is not a FUSE frontend, not a POSIX filesystem, and not an object
store. It is the metadata kernel consumed by AI workload stores, artifact SDKs,
workspace services, distributed filesystem frontends, and object namespace
layers.

Native API surface is available through embedded Go via
`fsmeta/runtime/local.Open` and through the distributed gRPC gateway
`nokv-fsmeta`.

| Primitive | Semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode. |
| `Lookup` | Reads a dentry by `(mount, parent_inode, name)`. |
| `LookupPlus` | Reads a dentry and inode attributes together. |
| `ReadDir` | Scans one directory page by dentry prefix. |
| `ReadDirPlus` | Scans dentries and batch-reads inode attrs under one snapshot version. |
| `UpdateInode` | Updates size, mode, updated timestamp, and bounded opaque attrs. |
| `Rename` | Moves one namespace entry when the destination does not exist. |
| `RenameReplace` | Atomically publishes or overwrites a file entry, useful for artifact replacement. |
| `RenameSubtree` | Moves a subtree root with rooted authority handoff. |
| `Remove` | Removes one non-directory namespace entry and returns removed inode metadata. |
| `RemoveDirectory` | Removes one empty directory after child-count verification. |
| `Link` / `Unlink` | Non-directory hard-link semantics with link-count updates. |
| `SnapshotSubtree` | Publishes an MVCC read-version token for point-in-time namespace reads. |
| `WatchSubtree` | Prefix-scoped change feed with ready cursor, ack, replay, and overflow handling. |
| Writer sessions | `OpenWriteSession`, `HeartbeatWriteSession`, `CloseWriteSession`, `ExpireWriteSessions`. |
| Quota usage | Persistent quota counters plus rooted quota fences. |

`InodeRecord.OpaqueAttrs` is application-owned metadata capped at 16 KiB. It is
for compact body references, checksums, media types, or small descriptors. It is
not for storing artifact bodies or large trace payloads.

### Artifact publish model

Large bodies are written outside `fsmeta`. The metadata commit happens in
`fsmeta`.

Typical flow:

1. Write the body to a `BodyStore`.
2. Encode the resulting body reference into inode opaque attrs.
3. Create a hidden staged namespace entry.
4. Publish it to the final path with `RenameReplace`.

Readers observe either the old body reference or the new one, never a
half-published artifact path.

### Authority lifecycle

Some metadata facts are rooted truth, while high-frequency inode and dentry
updates remain data-plane writes.

| Domain | Rooted truth | Runtime view |
|---|---|---|
| Mount | `MountRegistered` / `MountRetired` | Mount admission cache. |
| Subtree authority | `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | RenameSubtree era frontier and pending handoff repair. |
| Snapshot epoch | `SnapshotEpochPublished` / `SnapshotEpochRetired` | Snapshot-version reads and MVCC-GC retention. |
| Quota fence | `QuotaFenceUpdated` | Quota fence cache plus persisted usage counters. |
| WatchSubtree | Not rooted truth | Raftstore apply observer plus fsmeta watch router. |

Documentation: [`docs/guide/fsmeta.md`](docs/guide/fsmeta.md)

## SDK Status

| SDK | State |
|---|---|
| [`sdk/artifact/`](./sdk/artifact) | Go artifact namespace SDK over `fsmeta`, with a local file body store. |
| [`sdk/artifact/python`](./sdk/artifact/python) | Python adapter for artifact-repository-style integrations and local tests. |
| [`sdk/runmetadata/python`](./sdk/runmetadata/python) | In-memory prototype for run lifecycle, artifact refs, lineage, and recent artifact listing. |
| [`sdk/runmetadata/typescript`](./sdk/runmetadata/typescript) | In-memory TypeScript prototype with the same run metadata direction. |
| [`sdk/workspace`](./sdk/workspace) | Planned surface; directory exists but no implementation yet. |

Current known gaps:

- production Python NoKV/fsmeta client;
- object-store-backed `BodyStore`;
- body garbage collection and reference ownership policy;
- staged-entry recovery policy;
- durable run metadata backend;
- workspace SDK;
- full tracking/search/index plane for runs, metrics, traces, datasets, and
  evaluations.

## Headline Evidence

### Underlying KV layer

Apple M3 Pro - `records=1M` - `ops=1M` - `value_size=1000` - `conc=16`

| Workload | Description | **NoKV** | Badger | Pebble |
|---|---|---:|---:|---:|
| **YCSB-A** | 50/50 read/update | **175,905** | 108,232 | 169,792 |
| **YCSB-B** | 95/5 read/update | **525,631** | 188,893 | 137,483 |
| **YCSB-C** | 100% read | **409,136** | 242,463 | 90,474 |
| **YCSB-D** | 95% read, 5% insert latest | **632,031** | 284,205 | 198,139 |
| **YCSB-E** | 95% scan, 5% insert | **45,620** | 15,027 | 40,793 |
| **YCSB-F** | read-modify-write | **157,732** | 84,601 | 122,192 |

Units: ops/sec. Full latency details live in
[`benchmark/README.md`](./benchmark/README.md). Single-node localhost, not
multi-host production.

## Why NoKV vs X?

| If you need... | You should probably use... | Where NoKV fits |
|---|---|---|
| A complete distributed filesystem | CephFS, JuiceFS | NoKV is not a filesystem. It can provide the metadata substrate a filesystem-shaped frontend consumes. |
| A production object store | MinIO, Ceph RGW, S3-compatible storage | NoKV is not an object store. It provides namespace metadata and body references above an object backend. |
| A custom AI workload metadata service | NoKV | NoKV gives you namespace, watch, snapshot, atomic publish, and local-first metadata execution without rebuilding the control plane. |
| A production distributed KV | TiKV, FoundationDB, CockroachDB | NoKV does not compete with generic KV systems. It is metadata-native and can run on its own engine today. |
| Production distributed SQL | CockroachDB, TiDB, Postgres | Use SQL for relational query workloads. Use NoKV when namespace semantics and metadata commit points are the core problem. |
| Just an embedded LSM | Pebble, Badger | NoKV's engine is not a drop-in LSM library; it exists to serve the metadata runtime. |
| A Raft library | etcd/raft, dragonboat | NoKV's raftstore is built on top of `etcd/raft` `RawNode`; owned code is the metadata/runtime integration. |
