# 🚀 NoKV – High-Performance Distributed KV Engine

<div align="center">
  <img src="./img/logo.svg" width="220" alt="NoKV Logo" />

  <p>
    <!-- Build / Quality -->
    <a href="https://github.com/feichai0017/NoKV/actions">
      <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" />
    </a>
    <a href="https://codecov.io/gh/feichai0017/NoKV">
      <img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" />
    </a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV">
      <img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" />
    </a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV">
      <img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" />
    </a>
    <a href="https://github.com/avelino/awesome-go#databases-implemented-in-go">
      <img alt="Mentioned in Awesome" src="https://awesome.re/mentioned-badge.svg" />
    </a>
    <a href="https://dbdb.io/db/nokv">
      <img alt="DBDB.io" src="https://img.shields.io/badge/dbdb.io-listed-2f80ed" />
    </a>
  </p>

  <p>
    <!-- Meta -->
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV">
      <img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" />
    </a>
  </p>

  <p><strong>LSM Tree • ValueLog • MVCC • Multi-Raft Regions • Redis-Compatible</strong></p>
</div>


NoKV is a Go-native storage engine that mixes RocksDB-style manifest discipline with Badger-inspired value separation. You can embed it locally, drive it via multi-Raft regions, or front it with a Redis protocol gateway—all from a single topology file.

## 📊 CI Benchmark Snapshot

Latest public benchmark snapshot currently checked into the repository, taken
from the latest successful `main` CI YCSB run available at the time of update
([run #314](https://github.com/feichai0017/NoKV/actions/runs/23393350444)).
This snapshot used the then-current benchmark profile:
`A-F`, `records=1,000,000`, `ops=1,000,000`, `value_size=1000`,
`value_threshold=1024`, `conc=16`.

Methodology and harness details live in [`benchmark/README.md`](./benchmark/README.md).

| Engine | Workload | Mode | Ops/s | Avg Latency | P95 | P99 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| NoKV | YCSB-A | 50/50 read/update | 163,798 | 6.105µs | 292.906µs | 515.926µs |
| NoKV | YCSB-B | 95/5 read/update | 556,189 | 1.797µs | 13.966µs | 28.292µs |
| NoKV | YCSB-C | 100% read | 434,281 | 2.302µs | 14.217µs | 22.262µs |
| NoKV | YCSB-D | 95% read, 5% insert (latest) | 637,162 | 1.569µs | 12.235µs | 27.1µs |
| NoKV | YCSB-E | 95% scan, 5% insert | 53,174 | 18.806µs | 105.387µs | 166.3µs |
| NoKV | YCSB-F | read-modify-write | 151,063 | 6.619µs | 328.164µs | 688.678µs |
| Badger | YCSB-A | 50/50 read/update | 88,957 | 11.241µs | 396.923µs | 638.214µs |
| Badger | YCSB-B | 95/5 read/update | 178,449 | 5.603µs | 323.102µs | 694.409µs |
| Badger | YCSB-C | 100% read | 203,481 | 4.914µs | 52.698µs | 2.22693ms |
| Badger | YCSB-D | 95% read, 5% insert (latest) | 210,475 | 4.751µs | 270.033µs | 604.131µs |
| Badger | YCSB-E | 95% scan, 5% insert | 15,249 | 65.578µs | 3.929808ms | 7.265708ms |
| Badger | YCSB-F | read-modify-write | 71,885 | 13.911µs | 494.737µs | 839.982µs |
| Pebble | YCSB-A | 50/50 read/update | 182,451 | 5.48µs | 442.877µs | 1.568458ms |
| Pebble | YCSB-B | 95/5 read/update | 151,214 | 6.613µs | 600.575µs | 1.27692ms |
| Pebble | YCSB-C | 100% read | 101,625 | 9.84µs | 796.221µs | 1.617434ms |
| Pebble | YCSB-D | 95% read, 5% insert (latest) | 214,067 | 4.671µs | 454.498µs | 1.169641ms |
| Pebble | YCSB-E | 95% scan, 5% insert | 44,210 | 22.619µs | 1.238978ms | 2.135797ms |
| Pebble | YCSB-F | read-modify-write | 132,530 | 7.545µs | 701.732µs | 1.589012ms |

## 🚦 Quick Start

Start an end-to-end playground with either the local script or Docker Compose. Both spin up a three-node Raft cluster with a PD-lite service and expose the Redis-compatible gateway.

![NoKV demo](./img/nokv-demo.gif)

```bash
# Option A: local processes
./scripts/run_local_cluster.sh --config ./raft_config.example.json
# In another shell: launch the Redis gateway on top of the running cluster
go run ./cmd/nokv-redis \
  --addr 127.0.0.1:6380 \
  --raft-config ./raft_config.example.json \
  --metrics-addr 127.0.0.1:9100

# Option B: Docker Compose (cluster + gateway + PD)
docker compose up --build
# Tear down
docker compose down -v
```

Once the cluster is running you can point any Redis client at `127.0.0.1:6380` (or the address exposed by Compose).

For quick CLI checks:

```bash
# Online stats from a running node
go run ./cmd/nokv stats --expvar http://127.0.0.1:9100

# Offline forensics from a stopped node workdir
go run ./cmd/nokv stats --workdir ./artifacts/cluster/store-1
```

Minimal embedded snippet:

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
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	key := []byte("hello")
	if err := db.Set(key, []byte("world")); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	entry, err := db.Get(key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("value=%s\n", entry.Value)
}
```

> Note:
> - `DB.Get` returns detached entries (do not call `DecrRef`).
> - `DB.GetInternalEntry` returns borrowed entries and callers must call `DecrRef` exactly once.
> - `DB.SetWithTTL` accepts `time.Duration` (relative TTL). `DB.Set`/`DB.SetBatch`/`DB.SetWithTTL` reject `nil` values; use `DB.Del` or `DB.DeleteRange(start,end)` for deletes.
> - `DB.NewIterator` exposes user-facing entries, while `DB.NewInternalIterator` scans raw internal keys (`cf+user_key+ts`).

> ℹ️ `run_local_cluster.sh` rebuilds `nokv` and `nokv-config`, seeds local peer catalogs via `nokv-config manifest`, starts PD-lite (`nokv pd`), streams PD/store logs to the current terminal, and also writes them under `artifacts/cluster/store-<id>/server.log` and `artifacts/cluster/pd.log`. Use `Ctrl+C` to exit cleanly; if the process crashes, wipe the workdir (`rm -rf ./artifacts/cluster`) before restarting to avoid WAL replay errors.

---

## 🧭 Topology & Configuration

Everything hangs off a single file: [`raft_config.example.json`](./raft_config.example.json).

```jsonc
"pd": { "addr": "127.0.0.1:2379", "docker_addr": "nokv-pd:2379" },
"stores": [
  { "store_id": 1, "listen_addr": "127.0.0.1:20170", ... },
  { "store_id": 2, "listen_addr": "127.0.0.1:20171", ... },
  { "store_id": 3, "listen_addr": "127.0.0.1:20172", ... }
],
"regions": [
  { "id": 1, "range": [-inf,"m"), peers: 101/201/301, leader: store 1 },
  { "id": 2, "range": ["m",+inf), peers: 102/202/302, leader: store 2 }
]
```

- **Local scripts** (`run_local_cluster.sh`, `serve_from_config.sh`, `bootstrap_from_config.sh`) ingest the same JSON, so local runs match production layouts.
- **Docker Compose** mounts the file into each container; manifests, transports, and Redis gateway all stay in sync.
- Need more stores or regions? Update the JSON and re-run the script/Compose—no code changes required.
- Programmatic access: import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` for a single source of truth across tools.

### 🧬 Tech Stack Snapshot

| Layer | Tech/Package | Why it matters |
| --- | --- | --- |
| Storage Core | `lsm/`, `wal/`, `vlog/` | Hybrid log-structured design with manifest-backed durability and value separation. |
| Concurrency | `percolator/`, `raftstore/client` | Distributed 2PC, lock management, and MVCC version semantics in raft mode. |
| Replication | `raftstore/*` + `pd/*` | Multi-Raft data plane plus PD-backed control plane (routing, TSO, heartbeats). |
| Tooling | `cmd/nokv`, `cmd/nokv-config`, `cmd/nokv-redis` | CLI, config helper, Redis-compatible gateway share the same topology file. |
| Observability | `stats`, `hotring`, expvar | Built-in metrics, hot-key analytics, and crash recovery traces. |

---

## 🧱 Architecture Overview

```mermaid
graph TD
    Client[Client API] -->|Set/Get| DBCore
    DBCore -->|Append| WAL
    DBCore -->|Insert| MemTable
    DBCore -->|ValuePtr| ValueLog
    MemTable -->|Flush Task| FlushMgr
    FlushMgr -->|Build SST| SSTBuilder
    SSTBuilder -->|LogEdit| Manifest
    Manifest -->|Version| LSMLevels
    LSMLevels -->|Compaction| Compactor
    FlushMgr -->|Discard Stats| ValueLog
    ValueLog -->|GC updates| Manifest
    DBCore -->|Stats/HotKeys| Observability
```

Key ideas:
- **Durability path** – WAL first, memtable second. ValueLog writes occur before WAL append so crash replay can fully rebuild state.
- **Metadata** – manifest stores SST topology, WAL checkpoints, and vlog head/deletion metadata.
- **Background workers** – flush manager handles `Prepare → Build → Install → Release`, compaction reduces level overlap, and value log GC rewrites segments based on discard stats.
- **Distributed transactions** – Percolator 2PC runs in raft mode; embedded mode exposes non-transactional DB APIs.

Dive deeper in [docs/architecture.md](docs/architecture.md).

---

## 🧩 Module Breakdown

| Module | Responsibilities | Source | Docs |
| --- | --- | --- | --- |
| WAL | Append-only segments with CRC, rotation, replay (`wal.Manager`). | [`wal/`](./wal) | [WAL internals](docs/wal.md) |
| LSM | MemTable, flush pipeline, leveled compactions, iterator merging. | [`lsm/`](./lsm) | [Memtable](docs/memtable.md)<br>[Flush pipeline](docs/flush.md)<br>[Cache](docs/cache.md)<br>[Range filter](docs/range_filter.md) |
| Manifest | VersionEdit log + CURRENT handling, WAL/vlog checkpoints, Region metadata. | [`manifest/`](./manifest) | [Manifest semantics](docs/manifest.md) |
| ValueLog | Large value storage, GC, discard stats integration. | [`vlog.go`](./vlog.go), [`vlog/`](./vlog) | [Value log design](docs/vlog.md) |
| Percolator | Distributed MVCC 2PC primitives (prewrite/commit/rollback/resolve/status). | [`percolator/`](./percolator) | [Percolator transactions](docs/percolator.md) |
| RaftStore | Multi-Raft Region management, hooks, metrics, transport. | [`raftstore/`](./raftstore) | [RaftStore overview](docs/raftstore.md) |
| HotRing | Hot key tracking, throttling helpers. | [`hotring/`](./hotring) | [HotRing overview](docs/hotring.md) |
| Observability | Periodic stats, hot key tracking, CLI integration. | [`stats.go`](./stats.go), [`cmd/nokv`](./cmd/nokv) | [Stats & observability](docs/stats.md)<br>[CLI reference](docs/cli.md) |
| Filesystem | Pebble-inspired `vfs` abstraction + mmap-backed file helpers shared by SST/vlog, WAL, and manifest. | [`vfs/`](./vfs), [`file/`](./file) | [VFS](docs/vfs.md)<br>[File abstractions](docs/file.md) |

Each module has a dedicated document under `docs/` describing APIs, diagrams, and recovery notes.

---


## 📡 Observability & CLI

- `Stats.StartStats` publishes metrics via `expvar` (flush backlog, WAL segments, value log GC stats, raft/region/cache/hot metrics).
- `cmd/nokv` gives you:
  - `nokv stats --workdir <dir> [--json] [--no-region-metrics]`
  - `nokv manifest --workdir <dir>`
  - `nokv regions --workdir <dir> [--json]`
  - `nokv vlog --workdir <dir>`
- `hotring` continuously surfaces hot keys in stats + CLI so you can pre-warm caches or debug skewed workloads.

More in [docs/cli.md](docs/cli.md) and [docs/testing.md](docs/testing.md#4-observability-in-tests).

---

## 🔌 Redis Gateway

- `cmd/nokv-redis` exposes a RESP-compatible endpoint. In embedded mode (`--workdir`) commands execute through regular DB APIs; in distributed mode (`--raft-config`) calls are routed through `raftstore/client` and committed with TwoPhaseCommit.
- In raft mode, TTL is persisted directly in each value entry (`expires_at`) through the same 2PC write path as the value payload.
- `--metrics-addr` exposes Redis gateway metrics under `NoKV.Stats.redis` via expvar. In raft mode, `--pd-addr` can override `config.pd` when you need a non-default PD endpoint.
- A ready-to-use cluster configuration is available at `raft_config.example.json`, matching both `scripts/run_local_cluster.sh` and the Docker Compose setup.

> For the complete command matrix, configuration and deployment guides, see [docs/nokv-redis.md](docs/nokv-redis.md).

---

## 📄 License

Apache-2.0. See [LICENSE](LICENSE).
