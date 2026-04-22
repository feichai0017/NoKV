<div align="center">
  <img src="./img/logo.svg" width="200" alt="NoKV" />

  <h1>NoKV</h1>

  <p>
    <strong>A full-stack, formally-specified, distributed storage platform — built as one coherent system.</strong>
  </p>

  <p>
    <em>Own LSM engine · Own Raft · Own control plane · Own MVCC · Own Redis frontend</em>
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

NoKV is a distributed key-value storage platform written from the ground up in Go. **Nothing is borrowed from another database** — the LSM engine, the Raft implementation, the control plane, the MVCC layer, and the Redis-compatible frontend are all native to this repository and share a single storage substrate.

The interesting part isn't that NoKV has "WAL, LSM, MVCC, Raft". The interesting part is that **these pieces are built as one system**:

- **One storage core, three deployment shapes.** The same `DB` substrate runs embedded in a Go program, seeded as the first node of a cluster, or replicated across a multi-Raft mesh. You don't migrate data — you migrate shape.
- **Execution plane and control plane are separate on purpose.** `raftstore/` executes writes. `coordinator/` answers "who holds authority, and for how long". `meta/root/` is the rooted truth they both consume.
- **Correctness is formally specified.** Six TLA+ specifications under [`spec/`](./spec/) model control-plane authority handoff, with machine-checked contrast models showing why weaker designs break.
- **Fault injection is a first-class primitive.** 18 operation-level VFS hooks, 8 Raft protocol-phase failpoints, and a shared `FaultFS` interface let any write path be stress-tested under crash-consistent failure schedules.

> NoKV is structured so new storage, replication, and control-plane ideas can land without rewriting the system each time. Treat it as a **serious engineering base** for exploring distributed storage internals, not as a production database.

<br/>

## 📊 Benchmarks

Performance-first YCSB on a single node, **NoKV vs industrial-grade embedded KVs** (Badger, Pebble). Apple M3 Pro, `records=1M`, `ops=1M`, `value_size=1000`, `conc=16`.

| Workload | Description | **NoKV** | Badger | Pebble |
|---|---|---:|---:|---:|
| **YCSB-A** | 50/50 read/update | **175,905** | 108,232 | 169,792 |
| **YCSB-B** | 95/5 read/update | **525,631** | 188,893 | 137,483 |
| **YCSB-C** | 100% read | **409,136** | 242,463 | 90,474 |
| **YCSB-D** | 95% read, 5% insert (latest) | **632,031** | 284,205 | 198,139 |
| **YCSB-E** | 95% scan, 5% insert | **45,620** | 15,027 | 40,793 |
| **YCSB-F** | read-modify-write | **157,732** | 84,601 | 122,192 |

> Units: **operations per second** (higher is better). Full latency distribution and methodology in [`benchmark/README.md`](./benchmark/README.md).

Representative latency snapshot (NoKV):

| Workload | Avg | P95 | P99 |
|---|---:|---:|---:|
| YCSB-A | 5.68 µs | 204 µs | 308 µs |
| YCSB-B | 1.90 µs | 24 µs | 750 µs |
| YCSB-C | 2.44 µs | 15 µs | 26 µs |
| YCSB-D | 1.58 µs | 22 µs | 638 µs |
| YCSB-F | 6.34 µs | 233 µs | 371 µs |

**Caveat**: single-node, localhost, read-heavy profile favors NoKV's flush/compaction design. These numbers are honest but not representative of multi-tenant production stress.

<br/>

## 🧭 Why NoKV vs X?

| If you need… | You should probably use… | Why NoKV exists |
|---|---|---|
| Production distributed SQL | **CockroachDB**, TiDB | Different scope — NoKV is a research-grade KV layer |
| Production distributed KV | **TiKV** | NoKV is not yet battle-tested at scale |
| Just an embedded LSM library | **Pebble**, **Badger** | NoKV ships its own engine, but is not trying to be a drop-in library |
| A Raft library | **etcd/raft**, dragonboat | NoKV has its own Raft integrated with the storage substrate |
| **A self-contained platform to study how LSM, Raft, MVCC, and control-plane design interact** | — | **This is what NoKV is for.** |

The project's value comes from **owning the entire vertical**. If you want to change how compaction interacts with Raft log GC, or how control-plane lease renewal affects write latency, no external dependency gets in the way.

<br/>

## 🏗️ Architecture

<p align="center">
  <img src="./img/architecture.svg" alt="NoKV Architecture" width="100%" />
</p>

Four boundaries that set NoKV apart from typical single-purpose KV prototypes:

- **One storage core, two deployment shapes.** Embedded and distributed modes sit on the same `DB` substrate. No data dump/reimport when you grow from local to clustered.
- **Migration is a protocol, not a hack.** `plan → init → seeded → expand` has explicit lifecycle state, failpoint coverage, and restart semantics at every stage.
- **Execution plane / control plane split.** `RaftAdmin` runs membership changes leader-side; `Coordinator` owns routing, TSO, heartbeats, and cluster view. They never share mutable state.
- **Recovery metadata is partitioned.** Manifest (storage engine), local recovery catalog (Raft), raft durable log, and logical region snapshots each have distinct ownership — no single corrupt file blocks all recovery paths.

Deep-dive: [`docs/architecture.md`](docs/architecture.md) · [`docs/runtime.md`](docs/runtime.md) · [`docs/control_and_execution_protocols.md`](docs/control_and_execution_protocols.md)

<br/>

## ✨ Notable Design Points

Selected features that are genuinely non-obvious — and have design notes explaining *why* they're the way they are:

| | Feature | Reference |
|---|---|---|
| 🌡️ | **Ingest Buffer for anti-stall LSM** — "catch first, sort later" absorbs L0 pressure without blocking writes | [`engine/lsm/`](./engine/lsm) · [design note](docs/notes/2026-02-01-compaction-and-ingest.md) |
| 🪣 | **Value Log with KV separation + hash buckets + parallel GC** — WiscKey + HashKV merged into a single pragmatic design | [`engine/vlog/`](./engine/vlog) · [design note](docs/notes/2026-02-05-vlog-design-and-gc.md) |
| 🧠 | **Adaptive memtable index (SkipList ↔ ART)** over arena-managed memory — no Go GC pressure on hot writes | [`engine/lsm/memtable.go`](./engine/lsm/memtable.go) · [design note](docs/notes/2026-02-09-memory-kernel-arena-and-adaptive-index.md) |
| 🚦 | **MPSC write pipeline with adaptive coalescing** — thousands of concurrent producers, one long-lived consumer, backlog-aware batching | [`internal/runtime/write_pipeline.go`](./internal/runtime/write_pipeline.go) · [design note](docs/notes/2026-02-09-write-pipeline-mpsc-and-adaptive-batching.md) |
| 🔍 | **GRF-inspired range filter** for cheap bounded-scan pruning at block granularity | [`engine/lsm/range_filter.go`](./engine/lsm/range_filter.go) · [design note](docs/notes/2026-04-05-range-filter-from-grf.md) |
| 🎯 | **Thermos as a side-channel observer** — hot-key detection without putting it on the main read path | [`thermos/`](./thermos) · [design note](docs/notes/2026-01-16-thermos-design.md) |
| 🧰 | **VFS abstraction with 18-op fault injection** — cross-platform atomic rename semantics, FaultFS for testing any syscall failure | [`engine/vfs/`](./engine/vfs) · [design note](docs/notes/2026-02-15-vfs-abstraction-and-deterministic-reliability.md) |
| 📦 | **SST-based Raft snapshot install** — snapshots ship materialized SST files, target node ingests directly | [`raftstore/snapshot/`](./raftstore/snapshot) · [design note](docs/notes/2026-03-31-sst-snapshot-install.md) |
| 🏛️ | **Delos-lite rooted truth kernel** — minimal typed event log is the single source of truth; Coordinator and raftstore are consumers | [`meta/root/`](./meta/root) · [design note](docs/notes/2026-04-03-delos-lite-metadata-root-roadmap.md) |

All design notes under [`docs/notes/`](./docs/notes/) are dated decision records — read them to understand *why* something is the way it is, not just what it does.

<br/>

## 🔬 Formally Specified

Control-plane correctness is modeled in TLA+ under [`spec/`](./spec/), with a **contrast family** that machine-checks why weaker designs fail:

| Spec | Role | TLC outcome |
|---|---|---|
| [`CCC.tla`](./spec/CCC.tla) | Positive model — repeated authority handoff with closure lifecycle | ✅ 3924 distinct states, depth 20, invariants hold |
| [`CCCMultiDim.tla`](./spec/CCCMultiDim.tla) | Multi-dimensional frontier coverage | ✅ 326 distinct states, invariants hold |
| [`LeaseOnly.tla`](./spec/LeaseOnly.tla) | Contrast — no reply-side guard, no rooted closure | ❌ counterexample: old-generation reply delivered after successor |
| [`TokenOnly.tla`](./spec/TokenOnly.tla) | Contrast — bounded-freshness token only | ❌ counterexample: freshness ≠ authority lineage |
| [`ChubbyFencedLease.tla`](./spec/ChubbyFencedLease.tla) | Contrast — per-reply sequencer fencing | ❌ counterexample: stale-reject holds, but successor coverage fails |
| [`LeaseStartOnly.tla`](./spec/LeaseStartOnly.tla) | Contrast — no lease-start coverage | ❌ counterexample: write accepted behind predecessor's served read |

Run `make tlc-ccc` / `make tlc-leaseonly-counterexample` / etc. to reproduce. Artifact outputs are checked in under [`spec/artifacts/`](./spec/artifacts/).

<br/>

## 🚦 Quick Start

Spin up a full three-node Raft cluster with Coordinator and Redis gateway in one command:

![NoKV demo](./img/nokv-demo.gif)

```bash
# Local processes
./scripts/dev/cluster.sh --config ./raft_config.example.json

# In another terminal: Redis gateway on top of the running cluster
go run ./cmd/nokv-redis \
  --addr 127.0.0.1:6380 \
  --raft-config ./raft_config.example.json \
  --metrics-addr 127.0.0.1:9100

# Or: Docker Compose (cluster + gateway + Coordinator in one stack)
docker compose up --build
```

Point any Redis client at `127.0.0.1:6380`. Inspect runtime state:

```bash
# Online — via expvar
go run ./cmd/nokv stats --expvar http://127.0.0.1:9100

# Offline forensics — from a stopped node's workdir
go run ./cmd/nokv stats --workdir ./artifacts/cluster/store-1
go run ./cmd/nokv manifest --workdir ./artifacts/cluster/store-1
go run ./cmd/nokv regions --workdir ./artifacts/cluster/store-1 --json
```

### Embedded mode

```go
package main

import (
    "fmt"
    "log"

    NoKV "github.com/feichai0017/NoKV"
)

func main() {
    opt := NoKV.NewDefaultOptions()
    opt.WorkDir = "./workdir-demo"

    db, err := NoKV.Open(opt)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _ = db.Set([]byte("hello"), []byte("world"))

    entry, _ := db.Get([]byte("hello"))
    fmt.Printf("value=%s\n", entry.Value)
}
```

> API notes:
> - `DB.Get` returns **detached** entries — caller owns the bytes, do not `DecrRef`.
> - `DB.GetInternalEntry` returns **borrowed** entries — caller must `DecrRef` exactly once.
> - `DB.Set`/`DB.SetBatch`/`DB.SetWithTTL` reject `nil` values; use `DB.Del` or `DB.DeleteRange(start, end)` for deletes.
> - `DB.NewIterator` exposes user-facing entries; `DB.NewInternalIterator` scans raw internal keys.

Full guide: [`docs/getting_started.md`](docs/getting_started.md)

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

Local scripts, Docker Compose, and all CLI tools consume the same file. Need more stores or regions? Edit the JSON and re-run — no code changes.

Programmatic access: `import "github.com/feichai0017/NoKV/config"` and call `config.LoadFile` / `Validate`.

<br/>

## 🧩 Modules

| Module | Responsibility | Docs |
|---|---|---|
| [`engine/lsm/`](./engine/lsm) | MemTable, flush pipeline, leveled compaction, SST | [LSM](docs/memtable.md) · [flush](docs/flush.md) · [compaction](docs/compaction.md) |
| [`engine/wal/`](./engine/wal) | WAL segments, CRC, rotation, replay, watchdog | [WAL](docs/wal.md) |
| [`engine/vlog/`](./engine/vlog) | KV-separated value log, hash buckets, parallel GC | [ValueLog](docs/vlog.md) |
| [`engine/manifest/`](./engine/manifest) | VersionEdit log, atomic `CURRENT` handling | [Manifest](docs/manifest.md) |
| [`engine/vfs/`](./engine/vfs) | VFS abstraction, FaultFS, cross-platform atomic rename | [VFS](docs/vfs.md) |
| [`percolator/`](./percolator) | Distributed MVCC 2PC (prewrite/commit/rollback/resolve) | [Percolator](docs/percolator.md) |
| [`raftstore/`](./raftstore) | Multi-Raft region management, transport, membership, snapshot install | [RaftStore](docs/raftstore.md) |
| [`coordinator/`](./coordinator) | Control plane: routing, TSO, heartbeats, lease management | [Coordinator](docs/coordinator.md) |
| [`meta/root/`](./meta/root) | Typed rooted truth kernel (Delos-lite), replicated/local backends | [Rooted Truth](docs/rooted_truth.md) |
| [`thermos/`](./thermos) | Hot-key detection and throttling | [Thermos](docs/thermos.md) |
| [`spec/`](./spec) | TLA+ specifications and contrast models | [spec/README.md](./spec/README.md) |
| [`cmd/nokv/`](./cmd/nokv) | CLI: stats, manifest, regions, vlog, migrate, coordinator | [CLI](docs/cli.md) |
| [`cmd/nokv-redis/`](./cmd/nokv-redis) | Redis-compatible gateway | [Redis](docs/nokv-redis.md) |

<br/>

## 📡 Observability

- **expvar metrics** via `Stats.StartStats` — flush backlog, WAL segments, value-log GC stats, region/cache/hot metrics
- **CLI inspection** from either live endpoint (`--expvar`) or stopped workdir (`--workdir`)
- **Structured logs** streamed from Coordinator and each store, also written under `artifacts/cluster/<name>/server.log`

More in [`docs/stats.md`](docs/stats.md) · [`docs/cli.md`](docs/cli.md) · [`docs/testing.md`](docs/testing.md).

<br/>

## 🤝 Community

- [Contributing Guide](./CONTRIBUTING.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Security Policy](./SECURITY.md)

<br/>

## 🔌 Redis Gateway

`cmd/nokv-redis` exposes a RESP-compatible endpoint. In embedded mode (`--workdir`) commands execute through `DB` APIs; in distributed mode (`--raft-config`) calls are routed through `raftstore/client` and committed via Percolator 2PC.

- TTL is persisted in the value entry (`expires_at`) through the same 2PC write path
- `--metrics-addr` exposes Redis-gateway metrics under `NoKV.Stats.redis` via expvar
- `--coordinator-addr` overrides the coordinator endpoint when you don't want the default

Full command matrix: [`docs/nokv-redis.md`](docs/nokv-redis.md).

<br/>

## 📖 Further Reading

- [`docs/architecture.md`](docs/architecture.md) — shortest path from "what is NoKV" to "which package owns what"
- [`docs/runtime.md`](docs/runtime.md) — function-level call chains for embedded and distributed read/write paths
- [`docs/control_and_execution_protocols.md`](docs/control_and_execution_protocols.md) — control-plane / execution-plane contract
- [`docs/notes/`](docs/notes/) — dated design decision records
- [`docs/SUMMARY.md`](docs/SUMMARY.md) — full table of contents (mdbook index)

<br/>

## 📄 License

[Apache-2.0](./LICENSE)

---

<div align="center">
<sub>Built from scratch — no external storage engine, no external Raft library, no external coordinator.</sub>
</div>
