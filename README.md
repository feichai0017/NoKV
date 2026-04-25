<div align="center">
  <img src="./img/logo.svg" width="200" alt="NoKV" />

  <h1>NoKV</h1>

  <p>
    <strong>An open-source namespace metadata substrate for distributed filesystems and object storage — with formally verified authority handoff.</strong>
  </p>

  <p>
    <em>Native fsmeta primitives · Eunomia handoff protocol (TLA+ verified) · Own LSM · Own Raft · Own MVCC · Own control plane</em>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" /></a>
    <a href="https://codecov.io/gh/feichai0017/NoKV"><img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" /></a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV"><img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" /></a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV"><img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" /></a>
    <a href="https://github.com/avelino/awesome-go#databases-implemented-in-go"><img alt="Mentioned in Awesome" src="https://awesome.re/mentioned-badge.svg" /></a>
    <a href="https://landscape.cncf.io/?item=app-definition-and-development--database--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
  </p>

  <p>
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>

</div>

<br/>

## What is NoKV?

**NoKV is the open-source counterpart of the "stateless schema layer + transactional KV" pattern** that powers Meta Tectonic (over ZippyDB), Google Colossus (over Bigtable), and DeepSeek 3FS (over FoundationDB). It exposes namespace metadata as a first-class service via **`fsmeta`** (gRPC + embedded Go), backed by an in-house transactional KV substrate, and ships with **`Eunomia`**, a TLA+-verified protocol for authority handoff continuation legality.

**Three audiences, one substrate:**

- 🗂️ **Distributed filesystems** — DFS frontends (FUSE / NFS / SMB drivers) consume `fsmeta` for inode / dentry / mount / subtree authority
- 🪣 **Object storage namespace layers** — S3-compatible gateways consume the same `fsmeta` for bucket / prefix / version metadata
- 🧪 **AI dataset metadata** — checkpoint storms, dataset versioning, prefix-scoped change feeds for training pipelines

**Why the substrate matters:**

1. **Native metadata primitives** — server-side `ReadDirPlus`, `AssertionNotExist`, `WatchSubtree`, `SnapshotSubtree`, atomic cross-region `RenameSubtree`, `Link` / `Unlink` with inode GC. These exist in hyperscaler stacks (Tectonic / Colossus / 3FS) but were missing from the open-source CNCF landscape.
2. **Formally verified authority handoff (Eunomia)** — 4 minimal invariants (`Primacy / Inheritance / Silence / Finality`) + 10 TLA+ specs with contrast family. Closes a bug class (etcd `#21638`, CockroachDB `#66562`) that no industrial system has fixed end-to-end.
3. **Single source of truth (`meta/root`)** — Mount lifecycle / subtree authority era / snapshot epoch / quota fence all live in one rooted, replicated, auditable event log; no parallel truths across config files, runtime caches, and coordinator state.
4. **Vertical integration** — own LSM (with ART memtable) + own Raft + own Percolator MVCC + own coordinator. No external dependency gates how a metadata primitive interacts with the storage layer.

> NoKV is a **substrate**, not a finished filesystem server. Build a FUSE driver / S3 gateway / dataset metadata service on top; we don't ship those out of the box. We do ship the primitives, the truth kernel, the verified handoff protocol, and a Redis-compatible gateway over the underlying KV.

<br/>

## 📊 Headline Evidence

### `fsmeta` native API vs generic KV (same NoKV cluster, Docker Compose)

| Operation | native-fsmeta | generic-KV | Speedup |
|---|---:|---:|---:|
| `ReadDirPlus` (avg) | **12.0 ms** | 510.3 ms | **42.5×** |
| `ReadDirPlus` (p50) | 11.3 ms | 508.7 ms | **44.8×** |
| `Create` (checkpoint storm, avg) | 338.6 ms | 434.7 ms | 1.28× |

> CSV: [`benchmark/fsmeta/results/fsmeta_formal_native_vs_generic_20260425T051640Z.csv`](./benchmark/fsmeta/results/)

### `WatchSubtree` end-to-end change-feed latency (3-node Docker Compose, 512 events)

| Metric | p50 | p95 | p99 |
|---|---:|---:|---:|
| `watch_notify` | **178 ms** | **472 ms** | 1235 ms |

> CSV: [`benchmark/fsmeta/results/fsmeta_watchsubtree_20260425T083316Z.csv`](./benchmark/fsmeta/results/) — sub-second end-to-end change feed for prefix-scoped metadata watches.

### Underlying KV layer (YCSB single-node, NoKV vs Badger / Pebble)

Apple M3 Pro · `records=1M` · `ops=1M` · `value_size=1000` · `conc=16`

| Workload | Description | **NoKV** | Badger | Pebble |
|---|---|---:|---:|---:|
| **YCSB-A** | 50/50 read/update | **175,905** | 108,232 | 169,792 |
| **YCSB-B** | 95/5 read/update | **525,631** | 188,893 | 137,483 |
| **YCSB-C** | 100% read | **409,136** | 242,463 | 90,474 |
| **YCSB-D** | 95% read, 5% insert (latest) | **632,031** | 284,205 | 198,139 |
| **YCSB-E** | 95% scan, 5% insert | **45,620** | 15,027 | 40,793 |
| **YCSB-F** | read-modify-write | **157,732** | 84,601 | 122,192 |

> Units: ops/sec. Full latency in [`benchmark/README.md`](./benchmark/README.md). Single-node localhost, not multi-host production.

<br/>

## 🧭 Why NoKV vs X?

| If you need… | You should probably use… | Why NoKV exists |
|---|---|---|
| A **complete distributed filesystem** (FUSE-mountable, full POSIX) | **CephFS, JuiceFS** | NoKV is the metadata substrate, not the full FS server |
| A **production object store** | **MinIO, Ceph RGW** | Same — NoKV provides namespace metadata, not S3 HTTP / object body I/O |
| **Hyperscaler-style "schema layer + transactional KV"** for your own DFS / OSS / dataset metadata | — | **This is what NoKV is for**: the open-source counterpart to Tectonic over ZippyDB, Colossus over Bigtable, 3FS over FDB |
| Production distributed SQL | **CockroachDB**, TiDB | Different scope |
| Production distributed KV | **TiKV, FoundationDB** | NoKV's KV layer is an in-house substrate for `fsmeta`, not a TiKV/FDB replacement |
| Just an embedded LSM | **Pebble**, **Badger** | NoKV's engine is not a drop-in library |
| A Raft library | **etcd/raft**, dragonboat | NoKV has its own Raft integrated with the storage substrate |

NoKV's value comes from **owning the entire vertical** so namespace-metadata-natural primitives can be implemented as first-class server-side ops (not client-side compositions over `Get`/`Put`/`Scan`).

<br/>

## 🏗️ Architecture

<p align="center">
  <img src="./img/architecture.svg" alt="NoKV Architecture" width="100%" />
</p>

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1  Userspace API (namespace semantics)                │
│   fsmeta/  — Create / Lookup / ReadDir / ReadDirPlus /      │
│              RenameSubtree / Unlink / Link / SnapshotSubtree│
│              WatchSubtree (catch-up replay + ack window)    │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│ Layer 2  Distributed runtime                                 │
│                                                              │
│  Control plane:              Execution plane:                │
│  • meta/root  — typed         • raftstore  — per-region Raft │
│    rooted truth (Eunomia      • percolator — 2PC + MVCC      │
│    Tenure / Legacy /            + AssertionNotExist          │
│    Handover, mount, subtree   • apply observer → fsmeta      │
│    authority, snapshot          watch router                 │
│    epoch, quota fence)        • SST snapshot install         │
│  • coordinator — TSO,                                        │
│    routing, store discovery,                                 │
│    rooted event publish,                                     │
│    streaming root-event sub                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│ Layer 3  Single-node storage engine                          │
│   engine/  — LSM + ART memtable + WAL + value log + manifest │
│   + per-CF/prefix value separation policy                    │
└──────────────────────────────────────────────────────────────┘
```

Four invariants that distinguish this stack:

1. **Single source of truth.** All authority lifecycle (mount, subtree, snapshot, quota) lives in `meta/root` as typed rooted events. No parallel truths across config / cache / coordinator.
2. **Layer separation enforced in code.** Layer 1 doesn't import raftstore client; Layer 2 doesn't know about fsmeta concepts; Layer 3 doesn't know there's a namespace.
3. **Multi-gateway-safe by construction.** Quota usage and subtree authority handoff use Percolator-transactional updates against `meta/root`, not in-process counters. N gateways stay coherent without coordination.
4. **Streaming lifecycle, not polling.** `coordinator.WatchRootEvents` pushes mount retire / quota fence updates / pending handoff to gateways within milliseconds, not poll intervals.

Deep-dive: [`docs/architecture.md`](docs/architecture.md) · [`docs/runtime.md`](docs/runtime.md) · [`docs/control_and_execution_protocols.md`](docs/control_and_execution_protocols.md)

<br/>

## 🗂️ `fsmeta` — Namespace Metadata Service

Native API surface (gRPC at `nokv-fsmeta:8090`, also embedded Go via `fsmeta/exec.OpenWithRaftstore`):

| Primitive | Semantics |
|---|---|
| `Create(parent, name, attr, asserts_not_exist)` | Atomic dentry + inode creation; server-side `AssertionNotExist`, no client TOCTOU |
| `Link(from, to_parent, to_name)` | Hard link — same-mount only, non-directory; new dentry + `LinkCount++` in one 2PC |
| `Lookup` / `ReadDir` / `ReadDirPlus` | Single dentry / paginated scan / **fused scan + batch inode fetch** (42.5× over generic KV) |
| `Unlink` | Atomic delete; `LinkCount--`; **last-link triggers inode GC in same transaction** |
| `RenameSubtree` | Cross-region atomic rename (Percolator 2PC) + rooted era handoff (Start → Mutate → Complete; monitor reconciles pending) |
| `SnapshotSubtree` | MVCC read-version token + `SnapshotEpochPublished` rooted event; auto-retire on publish failure |
| `RetireSnapshotSubtree` | Client-driven epoch retire |
| `WatchSubtree(prefix)` bidi stream | Prefix-scoped live change feed: `Ready{cursor}` signal, per-region replay ring, cursor-based catch-up, `Ack(cursor)` for flow control, mount-retire auto-close |
| `GetQuotaUsage` | Read transactional quota usage (multi-gateway safe) |

Authority lifecycle (rooted in `meta/root`, managed via `nokv mount` / `nokv quota` CLI):

| Domain | Rooted truth | Runtime view |
|---|---|---|
| **Mount** | `MountRegistered` / `MountRetired` (auto-declares era=0 SubtreeAuthority) | Mount admission cache |
| **Subtree authority** | `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | RenameSubtree era frontier |
| **Snapshot epoch** | `SnapshotEpochPublished` / `SnapshotEpochRetired` | Read-version cache |
| **Quota fence** | `QuotaFenceUpdated` (mount + subtree level, bytes + inodes) | Usage in raftstore (transactional, not in-memory) |

Documentation: [`docs/fsmeta.md`](docs/fsmeta.md) · [positioning note](docs/notes/2026-04-24-fsmeta-positioning.md) · [namespace authority events umbrella](docs/notes/2026-04-25-namespace-authority-events-umbrella.md)

<br/>

## 🔬 `Eunomia` — TLA+-Verified Authority Handoff

A **service-level correctness class** for what continuation is legal after authority handoff. Closes a bug class (etcd `#21638` filed upstream 2026-04-19, CockroachDB `#66562` 5-year open issue) that no industrial system has fixed end-to-end.

Four minimal invariants:

```
Eunomia := Primacy ∧ Inheritance ∧ Silence ∧ Finality
```

| Invariant | What it forbids |
|---|---|
| **Primacy** | Two active authorities for the same lock / region / subtree at the same time |
| **Inheritance** | Successor must cover predecessor's served frontier (the missing piece in CRDB #66562) |
| **Silence** | A sealed era cannot deliver replies after seal (the missing piece in etcd #21638) |
| **Finality** | A handoff must close explicitly; no "half-open" detached state |

Verified by 10 TLA+ specs under [`spec/`](./spec/) — positive model + contrast family proving each invariant is necessary:

| Spec | Role | TLC outcome |
|---|---|---|
| [`Eunomia.tla`](./spec/Eunomia.tla) | Positive — repeated rooted handoff cycle | ✅ 3924 distinct states, depth 20, all 4 invariants hold (incl. inductive Primacy) |
| [`EunomiaMultiDim.tla`](./spec/EunomiaMultiDim.tla) | Multi-dimensional frontier coverage | ✅ holds |
| [`SubtreeAuthority.tla`](./spec/SubtreeAuthority.tla) | Eunomia projected onto fsmeta `RenameSubtree` | ✅ all 4 invariants hold |
| [`MountLifecycle.tla`](./spec/MountLifecycle.tla) | Mount as monotonic Eunomia instance | ✅ holds |
| [`LeaseOnly.tla`](./spec/LeaseOnly.tla) | Contrast — no rooted seal | ❌ violates `NoOldReplyAfterSuccessor` |
| [`TokenOnly.tla`](./spec/TokenOnly.tla) | Contrast — bounded-freshness token only | ❌ violates same — freshness ≠ authority lineage |
| [`ChubbyFencedLease.tla`](./spec/ChubbyFencedLease.tla) | Contrast — per-reply sequencer fencing only | ❌ stale-reject holds, **successor coverage fails** |
| [`LeaseStartOnly.tla`](./spec/LeaseStartOnly.tla) | Contrast — no lease-start coverage | ❌ violates `NoWriteBehindServedRead` |
| [`SubtreeWithoutSeal.tla`](./spec/SubtreeWithoutSeal.tla) | Subtree contrast — no seal | ❌ Primacy violated |
| [`SubtreeWithoutFrontierCoverage.tla`](./spec/SubtreeWithoutFrontierCoverage.tla) | Subtree contrast — no coverage | ❌ Inheritance violated |

Reference implementation: [`meta/root/state/eunomia.go`](./meta/root/state) + [`coordinator/protocol/eunomia/`](./coordinator/protocol/eunomia) + 3-layer admission (server gate / client witness / offline `eunomia-audit`).

`make tlc-eunomia` / `make apalache-check-eunomia` / `make tlc-leaseonly-counterexample` — artifacts checked in under [`spec/artifacts/`](./spec/artifacts/).

Research drafts are kept local-only and are intentionally excluded from Git.

<br/>

## 🚦 Quick Start

### Run a full cluster

```bash
# Local processes — meta-root + coordinator + 3-store cluster + fsmeta gateway
./scripts/dev/cluster.sh --config ./raft_config.example.json

# Or: Docker Compose (cluster + fsmeta + Redis gateway, with mount-init bootstrap)
docker compose up --build
```

![NoKV demo](./img/nokv-demo.gif)

### Use `fsmeta` from Go (embedded — same Executor as the gRPC server)

```go
package main

import (
    "context"

    "github.com/feichai0017/NoKV/fsmeta"
    fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
)

func main() {
    ctx := context.Background()
    rt, err := fsmetaexec.OpenWithRaftstore(ctx, fsmetaexec.Options{
        CoordinatorAddr: "127.0.0.1:2379",
    })
    if err != nil {
        panic(err)
    }
    defer rt.Close()

    // mount must be registered first (see `nokv mount register`)
    err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
        Mount: "default", Parent: 1, Name: "hello.txt", Inode: 100,
    }, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1, Size: 13})
    if err != nil {
        panic(err)
    }

    page, _ := rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
        Mount: "default", Parent: 1, Limit: 100,
    })
    for _, e := range page {
        println(e.Dentry.Name, e.Inode.Size)
    }
}
```

### Use `fsmeta` from any language (gRPC)

```bash
# Bootstrap a mount (required before first write)
nokv mount register --coordinator-addr 127.0.0.1:2379 \
  --mount default --root-inode 1 --schema-version 1

# Set a quota fence (mount-level)
nokv quota set --coordinator-addr 127.0.0.1:2379 \
  --mount default --limit-bytes 1073741824 --limit-inodes 1000000

# Run the standalone fsmeta gRPC gateway with metrics
nokv-fsmeta --addr 127.0.0.1:8090 --coordinator-addr 127.0.0.1:2379 \
  --metrics-addr 127.0.0.1:9101
```

Then use any gRPC client against `fsmeta.proto` (Go typed client at `fsmeta/client/`).

### Use the Redis gateway (secondary product line, KV layer only)

```bash
nokv-redis --addr 127.0.0.1:6380 --coordinator-addr 127.0.0.1:2379
redis-cli -p 6380 set hello world
redis-cli -p 6380 get hello
```

> `nokv-redis` is a thin RESP gateway over the underlying transactional KV. It does **not** consume `fsmeta` — it's a separate product surface for users who only need Redis-style KV access to NoKV's storage substrate.

### Inspect runtime state

```bash
# Live, via expvar (executor / watch / quota / mount metrics)
curl http://127.0.0.1:9101/debug/vars | jq '.nokv_fsmeta_executor, .nokv_fsmeta_watch, .nokv_fsmeta_quota, .nokv_fsmeta_mount'

# Offline forensics from a stopped node's workdir
nokv stats --workdir ./artifacts/cluster/store-1
nokv manifest --workdir ./artifacts/cluster/store-1
nokv regions --workdir ./artifacts/cluster/store-1 --json

# Eunomia audit (read-only against meta-root)
nokv eunomia-audit --workdir ./artifacts/cluster/coord-1
```

Full guide: [`docs/getting_started.md`](docs/getting_started.md) · CLI reference: [`docs/cli.md`](docs/cli.md)

<br/>

## ✨ Notable Design Points

| | Feature | Reference |
|---|---|---|
| 🌡️ | **Ingest Buffer for anti-stall LSM** — "catch first, sort later" absorbs L0 pressure without blocking writes | [`engine/lsm/`](./engine/lsm) · [design note](docs/notes/2026-02-01-compaction-and-ingest.md) |
| 🪣 | **Value Log with KV separation + hash buckets + parallel GC** — WiscKey + HashKV merged into a single pragmatic design | [`engine/vlog/`](./engine/vlog) · [design note](docs/notes/2026-02-05-vlog-design-and-gc.md) |
| 🧠 | **Adaptive memtable index (SkipList ↔ ART)** over arena memory — ART pinned for fsmeta deployments (prefix-heavy dentries compress to single inner-node prefixes) | [`engine/lsm/memtable.go`](./engine/lsm/memtable.go) · [design note](docs/notes/2026-02-09-memory-kernel-arena-and-adaptive-index.md) |
| 🚦 | **MPSC write pipeline with adaptive coalescing** — thousands of concurrent producers, one long-lived consumer, backlog-aware batching | [`internal/runtime/write_pipeline.go`](./internal/runtime/write_pipeline.go) · [design note](docs/notes/2026-02-09-write-pipeline-mpsc-and-adaptive-batching.md) |
| 🔍 | **Per-CF / per-prefix value separation policy** — fsmeta keys (`fsm\x00`) forced inline, never redirected to vlog | [`engine/kv/value_separation.go`](./engine/kv/value_separation.go) |
| 🎯 | **Thermos as a side-channel observer** — hot-key detection without putting it on the main read path | [`thermos/`](./thermos) · [design note](docs/notes/2026-01-16-thermos-design.md) |
| 🧰 | **VFS abstraction with 18-op fault injection** — cross-platform atomic rename semantics, FaultFS for testing any syscall failure | [`engine/vfs/`](./engine/vfs) · [design note](docs/notes/2026-02-15-vfs-abstraction-and-deterministic-reliability.md) |
| 📦 | **SST-based Raft snapshot install** — snapshots ship materialized SST files, target node ingests directly | [`raftstore/snapshot/`](./raftstore/snapshot) · [design note](docs/notes/2026-03-31-sst-snapshot-install.md) |
| 🏛️ | **Delos-lite rooted truth kernel** — typed event log is the single source of truth; coordinator and raftstore are consumers | [`meta/root/`](./meta/root) · [design note](docs/notes/2026-04-03-delos-lite-metadata-root-roadmap.md) |
| 🪞 | **Apply observer at store level** — post-commit semantic events with `(region_id, term, index, commit_version)` cursor; survives raft snapshot replay | [`raftstore/store/observer.go`](./raftstore/store/observer.go) |
| 🔁 | **Cross-region atomic Rename / RenameSubtree** — Percolator 2PC + commit-ts-expired auto-retry (3×) + Eunomia era handoff with monitor reconcile | [`fsmeta/exec/runner.go`](./fsmeta/exec/runner.go) · [Eunomia spec](./spec/SubtreeAuthority.tla) |

All design notes under [`docs/notes/`](./docs/notes/) are dated decision records — read them to understand *why* something is the way it is, not just what it does.

<br/>

## 🧩 Modules

| Module | Responsibility | Docs |
|---|---|---|
| **[`fsmeta/`](./fsmeta)** | **Namespace metadata schema, executor, gRPC service, embedded API** | **[fsmeta](docs/fsmeta.md)** |
| [`fsmeta/exec/watch/`](./fsmeta/exec/watch) | WatchSubtree router + RemoteSource + catch-up replay | [fsmeta](docs/fsmeta.md) |
| [`spec/`](./spec) | TLA+ Eunomia + contrast family + SubtreeAuthority + MountLifecycle | [spec/README.md](./spec/README.md) |
| [`meta/root/`](./meta/root) | Typed rooted truth kernel (Delos-lite); Eunomia state | [Rooted Truth](docs/rooted_truth.md) |
| [`coordinator/`](./coordinator) | Routing, TSO, store discovery, root-event publish, streaming subscribe; Eunomia gate | [Coordinator](docs/coordinator.md) |
| [`raftstore/`](./raftstore) | Multi-Raft, transport, membership, SST snapshot install, apply observer | [RaftStore](docs/raftstore.md) |
| [`percolator/`](./percolator) | Distributed MVCC 2PC + AssertionNotExist | [Percolator](docs/percolator.md) |
| [`engine/lsm/`](./engine/lsm) | MemTable, flush, leveled compaction, SST | [LSM](docs/memtable.md) · [flush](docs/flush.md) · [compaction](docs/compaction.md) |
| [`engine/wal/`](./engine/wal) | WAL segments, CRC, rotation, replay | [WAL](docs/wal.md) |
| [`engine/vlog/`](./engine/vlog) | KV-separated value log + hash buckets + parallel GC | [ValueLog](docs/vlog.md) |
| [`engine/manifest/`](./engine/manifest) | VersionEdit log, atomic CURRENT | [Manifest](docs/manifest.md) |
| [`engine/vfs/`](./engine/vfs) | VFS abstraction, FaultFS, cross-platform atomic rename | [VFS](docs/vfs.md) |
| [`thermos/`](./thermos) | Hot-key observer | [Thermos](docs/thermos.md) |
| [`cmd/nokv/`](./cmd/nokv) | CLI: stats, manifest, regions, vlog, migrate, mount, quota, eunomia-audit | [CLI](docs/cli.md) |
| [`cmd/nokv-fsmeta/`](./cmd/nokv-fsmeta) | Standalone fsmeta gRPC gateway | [fsmeta](docs/fsmeta.md) |
| [`cmd/nokv-redis/`](./cmd/nokv-redis) | Redis-compatible gateway (secondary product line) | [Redis](docs/nokv-redis.md) |

<br/>

## 📡 Observability

Four independent expvar metric namespaces (per-domain admission visibility):

| Endpoint | Metric namespace | Fields |
|---|---|---|
| `nokv-fsmeta --metrics-addr :PORT/debug/vars` | `nokv_fsmeta_executor` | `txn_retries_total`, `txn_retry_exhausted_total` |
| | `nokv_fsmeta_watch` | `subscribers`, `events_total`, `delivered_total`, `dropped_total`, `overflow_total` |
| | `nokv_fsmeta_quota` | `checks_total`, `rejects_total`, `cache_hits_total`, `cache_misses_total`, `fence_updates_total`, `usage_mutations_total` |
| | `nokv_fsmeta_mount` | `cache_hits`, `cache_misses`, `admission_rejects_total` |

Plus structured logs from coordinator and each store. More: [`docs/stats.md`](docs/stats.md) · [`docs/cli.md`](docs/cli.md) · [`docs/testing.md`](docs/testing.md).

<br/>

## 🧭 Topology & Configuration

All deployment shapes share one configuration file: [`raft_config.example.json`](./raft_config.example.json).

```jsonc
{
  "coordinator": { "addr": "127.0.0.1:2379", ... },
  "stores": [
    { "store_id": 1, "listen_addr": "127.0.0.1:20170", ... },
    { "store_id": 2, "listen_addr": "127.0.0.1:20171", ... },
    { "store_id": 3, "listen_addr": "127.0.0.1:20172", ... }
  ],
  "regions": [
    { "id": 1, "range": [-inf, "m"), "leader": 1, ... },
    { "id": 2, "range": ["m", +inf), "leader": 2, ... }
  ]
}
```

Local scripts, Docker Compose, and all CLI tools consume the same file. Programmatic access: `import "github.com/feichai0017/NoKV/config"` and call `config.LoadFile` / `Validate`.

<br/>

## 🤝 Community

- [Contributing Guide](./CONTRIBUTING.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Security Policy](./SECURITY.md)

<br/>

## 📖 Further Reading

- [`docs/fsmeta.md`](docs/fsmeta.md) — namespace metadata service complete reference
- [`docs/architecture.md`](docs/architecture.md) — three-layer architecture deep dive
- [`docs/runtime.md`](docs/runtime.md) — function-level call chains
- [`docs/control_and_execution_protocols.md`](docs/control_and_execution_protocols.md) — control-plane / execution-plane contract
- [`docs/notes/`](docs/notes/) — dated design decision records
- Local-only research drafts are intentionally excluded from Git.
- [`docs/SUMMARY.md`](docs/SUMMARY.md) — full table of contents (mdbook index)

<br/>

## 📄 License

[Apache-2.0](./LICENSE)

---

<div align="center">
<sub><strong>Open-source namespace metadata substrate for DFS, OSS, and AI dataset metadata — with TLA+-verified authority handoff.</strong></sub><br/>
<sub>Built from scratch — no external storage engine, no external Raft library, no external coordinator.</sub>
</div>
