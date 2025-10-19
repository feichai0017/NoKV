# NoKV Architecture & Design Deep Dive

> NoKV follows the same broad lineage as **RocksDB** (LSM tree + manifest/WAL orchestration) and **BadgerDB** (value separation via vlog), but the implementation is 100% Go and tailored for embeddability, MVCC transactions, and strong observability.

---

## 1. Package Map

| Package / File | Key Responsibilities | Notable Types & Functions | Related Tests |
| --- | --- | --- | --- |
| [`db.go`](../db.go) | Entry point that wires WAL, LSM, ValueLog, HotRing, Stats, Oracle. | `DB.doWrites`, `DB.initVLog`, `DB.prefetchLoop`, `DB.Close`. | `db_test.go`, `db_recovery_test.go`, `txn_test.go` |
| [`wal/`](../wal) | Append-only log segments with CRC, rotation, replay. | `wal.Manager`, `Append`, `Replay`, `VerifyDir`. | `wal/manager_test.go` |
| [`lsm/`](../lsm) | MemTable, flush pipeline, levelled files, compaction scheduler (see [compaction.md](compaction.md)). | `lsm.LSM`, `lsm/flush.Manager`, `lsm.compactionManager`. | `lsm/lsm_test.go`, `lsm/compact_test.go`, `lsm/compaction_cache_test.go` |
| [`manifest/`](../manifest) | Versioned metadata (files, WAL checkpoints, vlog head) stored in `MANIFEST-*`. | `manifest.Manager`, `manifest.Edit`, `manifest.Version`. | `manifest/manager_test.go` |
| [`vlog/`](../vlog) + [`vlog.go`](../vlog.go) | Value-log segments, head tracking, GC, discard stats. | `vlog.Manager`, `valueLog.write`, `valueLog.doRunGC`. | `vlog/vlog_test.go`, `vlog/gc_test.go` |
| [`txn.go`](../txn.go) | MVCC timestamps, conflict detection, managed transactions, iterator snapshots. | `oracle`, `Txn`, `Txn.ReadTs`, `Txn.Commit`. | `txn_test.go`, `txn_iterator_test.go` |
| [`stats.go`](../stats.go) | Periodic snapshot of backlog, throughput, hot keys via `expvar`. | `Stats`, `Stats.collect`, `StatsSnapshot`. | `stats_test.go`, CLI golden tests |
| [`cmd/nokv`](../cmd/nokv) | CLI inspection (`stats`, `manifest`, `vlog`). | `rootCmd`, `statsCmd.run`, `manifestCmd.run`. | `cmd/nokv/main_test.go` |

The packages are deliberately thin and communicate through clear data structures (e.g. `utils.Entry`, `utils.ValuePtr`), enabling targeted testing and easier reasoning during recovery.

---

## 2. Storage Layout

```text
WorkDir/
├── CURRENT                # points to MANIFEST-xxxxx
├── MANIFEST-000123        # VersionEdit log
├── wal/000001.wal         # WAL segments (default 64 MiB)
├── vlog/000001.vlog       # ValueLog segments for large/value-separated payloads
├── 000001.sst             # SSTables produced by flush/compaction
├── tmp/                   # staging files for flush, compaction, manifest rewrite
└── archive/               # optional retention area for diagnostics
```

- **CURRENT** is atomically swapped (`CURRENT.tmp → CURRENT`) whenever the manifest is rewritten, mirroring RocksDB's safety guarantees.
- WAL segments and vlog segments rotate independently, yet both checkpoints are persisted through `manifest.EditLogPointer` and `manifest.EditValueLogHead` so recovery knows where to resume.

---

## 3. End-to-End Write Flow

```mermaid
sequenceDiagram
    participant Client
    participant DB as DB.doWrites
    participant WAL as wal.Manager
    participant VLog as valueLog
    participant Mem as MemTable
    participant Flush as flush.Manager
    participant Manifest
    participant Levels as LSM Levels
    participant GC as ValueLog GC
    Client->>DB: Set(key,value)
    DB->>DB: aggregate into writeBatch
    DB->>VLog: optional value separation (processValueLogBatches)
    DB->>WAL: Append(batch)
    WAL-->>DB: EntryInfo (segment,offset)
    DB->>Mem: applyBatch (mutate active skiplist)
    Mem-->>Flush: freeze when full
    Flush->>Manifest: StageInstall → LogEdit(AddFile, LogPointer)
    Manifest-->>Flush: success
    Flush->>Levels: Install SST
    Flush->>WAL: Release segments
    Flush->>GC: send discard stats
```

1. **Batching** – `DB.doWrites` drains `writeCh` and aggregates requests based on `Options.WriteBatch*` thresholds.
2. **Value separation** – `processValueLogBatches` writes large payloads to vlog segments (Badger-style) and embeds `ValuePtr` metadata into WAL/LSM entries.
3. **Durability** – `wal.Manager.Append` writes `[length|payload|crc]` tuples; rotation occurs when `segmentSize` would be exceeded.
4. **MemTable apply** – `applyBatches` mutates the active skiplist, and immutable tables are handed to `lsm.flush.Manager`.
5. **Flush pipeline** – the manager enforces `Prepare → Build → Install → Release` stages with metrics for queueing and build latency.
6. **Manifest + WAL checkpoint** – `manifest.Manager.LogEdit` persists the new SST plus WAL pointer. Only after success do we rename temp SST files and release WAL segments.
7. **Discard stats → ValueLog** – flush completion pushes discard statistics back to `valueLog` so GC can evict obsolete vlog segments.

Compared with RocksDB:
- NoKV reuses the canonical WAL→MemTable→SST path but coordinates ValueLog pointers like Badger to keep hot keys in LSM while parking large values on sequential vlog segments.

```mermaid
sequenceDiagram
    participant Client
    participant DB as DB.doWrites
    participant WAL as wal.Manager
    participant VLog as valueLog
    participant Mem as MemTable
    participant Flush as flush.Manager
    participant Manifest
    participant Levels as LSM Levels
    participant GC as ValueLog GC
    Client->>DB: Set(key,value)
    DB->>DB: aggregate into writeBatch
    DB->>VLog: optional value separation (processValueLogBatches)
    DB->>WAL: Append(batch)
    WAL-->>DB: EntryInfo (segment,offset)
    DB->>Mem: applyBatch (mutate active skiplist)
    Mem-->>Flush: freeze when full
    Flush->>Manifest: StageInstall → LogEdit(AddFile, LogPointer)
    Manifest-->>Flush: success
    Flush->>Levels: Install SST
    Flush->>WAL: Release segments
    Flush->>GC: send discard stats
```

1. **Batching** – `DB.doWrites` drains `writeCh` and aggregates requests based on `Options.WriteBatch*` thresholds.
2. **Value separation** – `processValueLogBatches` writes large payloads to vlog segments (Badger-style) and embeds `ValuePtr` metadata into WAL/LSM entries.
3. **Durability** – `wal.Manager.Append` writes `[length|payload|crc]` tuples; rotation occurs when `segmentSize` would be exceeded.
4. **MemTable apply** – `applyBatches` mutates the active skiplist, and immutable tables are handed to `lsm.flush.Manager`.
5. **Flush pipeline** – the manager enforces `Prepare → Build → Install → Release` stages with metrics for queueing and build latency.
6. **Manifest + WAL checkpoint** – `manifest.Manager.LogEdit` persists the new SST plus WAL pointer. Only after success do we rename temp SST files and release WAL segments.
7. **Discard stats → ValueLog** – flush completion pushes discard statistics back to `valueLog` so GC can evict obsolete vlog segments.

## 4. Read Path & Iterators

```mermaid
flowchart LR
    subgraph Frontend
        Client([Client API])
        Txn[Txn Snapshot]
    end
    subgraph Storage
        HotRing[hotring.HotRing]
        IterPool[iteratorPool]
        LSM[lsm.Iterator]
        VLogRead[ValueLog Reader]
    end
    Client --> Txn --> LSM
    LSM -->|ValuePtr| VLogRead
    LSM --> Client
    Client --> HotRing
    Txn --> IterPool
```

- `DB.NewTransaction` obtains a read timestamp (`oracle.newReadTs`) and builds an iterator merging memtables + table iterators; pending writes live in the transaction structure.
- When entries carry a `ValuePtr`, `valueLog.get` streams the value directly from the vlog segment, allowing SSTs to stay compact.
- `hotring.HotRing` tracks read frequency and emits Top-N hot keys in `StatsSnapshot` and `nokv stats`, making it easier to prefetch or throttle at the application layer.
- Iterator pooling keeps allocation costs low, inspired by Badger's reusable iterators.

---

## 5. Background Loops

| Component | Source | Purpose | Notable Metrics |
| --- | --- | --- | --- |
| Flush Manager | `lsm/flush.Manager` | Coordinates immutable memtables; exposes queue length and stage latency. | `Flush.Queue`, `Flush.BuildNs`, `Flush.Completed` |
| Compaction Manager | `lsm.compactionManager` | Prioritises L0 size, ingest buffer backlog, and level fanout (details in [compaction.md](compaction.md)). | `NoKV.Compaction.RunsTotal`, `LastDurationMs` |
| ValueLog GC | `valueLog.doRunGC` + `vlog.Manager` | Rewrites live entries into new vlog segments based on discard stats. | `NoKV.ValueLog.GcRuns`, `SegmentsRemoved`, `HeadUpdates` |
| Prefetch Loop | `DB.prefetchLoop` | Uses HotRing signals to schedule asynchronous reads into `prefetched` cache. | `Stats.HotKeys` with timestamps |
| Stats Sampler | `Stats.StartStats` | Polls flush, compaction, WAL, transaction metrics and publishes via `expvar`. | `NoKV.Txns.*`, `NoKV.Flush.*`, etc. |

Badger similarly exposes value log GC and LSM compactions, but NoKV emphasises structured metrics for CLI/expvar users without external tooling.

---

## 6. Crash Recovery Pipeline

1. **Directory checks** – `DB.runRecoveryChecks` verifies manifest, WAL, and vlog directory invariants.
2. **Manifest replay** – `manifest.Open` reads `CURRENT`, applies each `manifest.Edit` (`AddFile`, `DeleteFile`, `LogPointer`, `ValueLogHead/Delete/Update`). Missing SSTs detected here are pruned (`manifest.CleanObsolete` in tests mirrors RocksDB's file descriptor check).
3. **WAL replay** – `wal.Manager.Replay` streams segment entries; truncated tails stop replay gracefully, similar to RocksDB's tolerance for torn writes.
4. **ValueLog head rebuild** – `valueLog.recover` enumerates segments, honors manifest head metadata, and updates the active file ID/offset. Orphaned segments flagged via `manifest.EditDeleteValueLog` are removed.
5. **Pending flush tasks** – `lsm/flush.Manager` metrics reveal leftover immutables; replayed WAL entries repopulate the memtable.
6. **Statistics bootstrap** – `Stats` scheduler is restarted so CLI snapshots stay accurate immediately after restart.

`db_recovery_test.go` intentionally injects faults at each stage (missing SSTs, stale vlog files, partial manifest rewrites) to guarantee the process is idempotent.

---

## 7. Observability & Tooling

- **Expvar endpoint** – `Stats.collect` publishes counters and gauges such as `NoKV.ValueLog.GcRuns`, `NoKV.Txns.Active`, `NoKV.Flush.Queue`.
- **CLI (`cmd/nokv`)** – `nokv stats/manifest/vlog --json` consumes the same snapshots for offline inspection, mirroring `ldb` from RocksDB but returning structured JSON for automation.
- **Recovery traces** – enabling `RECOVERY_TRACE_METRICS` prints structured key/value logs during replay and GC, aiding CI triage.
- **Hot key surfacing** – `StatsSnapshot.Hot.Keys` lists keys with hit counts; great for verifying cache prefetch from the CLI.

---

## 8. Design Comparison

| Feature | RocksDB | BadgerDB | NoKV |
| --- | --- | --- | --- |
| Language / Runtime | C++ with custom env | Go | Go (no CGO dependencies) |
| Log strategy | WriteBatch into WAL + MemTable | ValueLog holds values, LSM holds keys | Hybrid: WAL + MemTable + ValueLog (large values) |
| Manifest | VersionEdit + CURRENT | Value log relies on vlog directory metadata | `manifest.Manager` mirrors RocksDB semantics and also tracks vlog head & deletions |
| Transactions | Optional WriteBatch, no MVCC by default | Managed transactions, optimistic concurrency | MVCC snapshots via `oracle`, conflict tracking, iterator merge |
| Hot key analytics | External perf counters | Basic metrics | Built-in `hotring` with CLI export |
| Tooling | `ldb`, `sst_dump` | `badger` CLI | `nokv` CLI (stats/manifest/vlog) + expvar |
| Observability | PerfContext, event listeners | Metrics (Prometheus) | Structured expvar + recovery traces |
| GC | Compaction-driven | Value log GC + discard stats | LSM compaction + vlog GC using discard stats from flush manager |

NoKV positions itself between the two: it adopts RocksDB's manifest/WAL discipline and Badger's value separation, while contributing additional observability and MVCC semantics without CGO dependencies.

---

## 9. Subsystem Deep Dives

For detailed walkthroughs of individual modules, refer to:

* [Memtable design](memtable.md) – skiplist arena sizing, WAL coupling, and recovery.
* [Compaction & cache strategy](compaction.md) – ingest buffers, priority scoring, and cache telemetry.
* [Transactions & MVCC](txn.md) – oracle timestamps, conflict detection, and commit flow.
* [Value log design](vlog.md) – updated manager semantics, discard stats, and GC.
* [Cache & bloom filters](cache.md) – hot/cold block caches and observability counters.
* [HotRing overview](hotring.md) – hot key tracking and throttling.
* [Stats & observability](stats.md) – expvar pipeline and CLI integration.
* [File abstractions](file.md) – mmap helpers underpinning WAL/SST/vlog layers.

---

## 9. Example Scenarios

### 9.1 Batched write with crash mid-flush
1. Client issues 1,000 `Set` calls; `DB.doWrites` batches them into 64-entry chunks.
2. WAL append succeeds and the memtable crosses the flush threshold.
3. `flush.Manager` enters `StageBuild`, writes `000012.sst.tmp`, but the process crashes before install.
4. On restart, `manifest.Manager` sees no `AddFile` edit for `000012` and `FlushManager` re-enqueues the immutable memtable.
5. WAL replay reproduces the 1,000 entries; flush restarts, manifest receives the edit, WAL segments rotate, discard stats notify ValueLog GC.

### 9.2 Transactional read-modify-write
1. `Txn := db.NewTransaction(true)` reserves a write timestamp via `oracle.nextTxnTs` and snapshots current read ts.
2. `Txn.Get` merges in-flight writes and LSM iterators. If a value pointer is encountered, it streams from vlog.
3. `Txn.Set` buffers the mutation and marks the key for conflict detection.
4. `Txn.Commit` reuses the DB write pipeline; conflicts detected through `oracle` watermark raise `ErrConflict` before WAL append, keeping WAL free of aborted transactions (contrary to RocksDB's WriteBatch semantics).

---

## 10. RaftStore Architecture

NoKV's replication layer lives in `raftstore/` and mirrors TinyKV's modular design:

| Sub-package | Responsibility | Key types |
| --- | --- | --- |
| `store/` | Orchestrates multi-Raft Regions, routing, lifecycle hooks, and metrics. Maintains an in-memory region catalog plus `RegionMetrics`, persisting updates through the manifest. | `Store`, `RegionHooks`, `RegionMetrics` |
| `peer/` | Wraps etcd/raft `RawNode`, handling Ready processing, log persistence, snapshot application, and Region metadata updates. | `Peer`, `ApplyFunc`, `SetRegionMeta` |
| `engine/` | Provides shared-WAL `PeerStorage` (reusing the DB WAL/manifest pipeline) alongside disk and memory alternatives, plus snapshot import/export helpers. | `WALStorage`, `DiskStorage` |
| `transport/` | gRPC transport implementation with retry, TLS, and failure injection capabilities used by integration tests. | `GRPCTransport`, `Transport` |

### 10.1 Region lifecycle & metrics

* `Store` builds a `RegionMetrics` recorder by default and wires it via `RegionHooks`, tracking transitions such as `Running → Removing → Tombstone`. User-defined hooks can be chained without losing the built-in recorder.
* Region metadata is always deep-copied via `manifest.CloneRegionMeta*`, preventing aliasing between packages.
* `Store.RegionSnapshot()` / `RegionMetas()` expose the current catalog; helpers like `SplitRegion` make upcoming split/merge flows easier to implement.
* Metrics reported through `StatsSnapshot`/expvar/`nokv stats` include `region_total/new/running/removing/tombstone/other`, while `nokv regions --workdir <dir> [--json]` renders the manifest-backed catalog (ID, state, key range, peers).

### 10.2 Store registry & CLI integration

`store.RegisterStore` records each running store instance (automatically invoked from `raftstore/api.NewStoreWithConfig`). The CLI can inspect `store.Stores()` to reuse the first available store—for example to fetch `RegionMetrics` before printing `nokv stats`.

---

## 10. Extensibility Outlook

- **Raft / replication** – WAL manager already exposes segment metadata; integrating raft logs would primarily require shipping WAL entries and manifest edits across nodes, akin to RocksDB's raft-enabled forks.
- **Column families** – manifest edits and flush manager structures are ready to carry additional column family identifiers.
- **Backup / snapshot** – exposing `manifest.Version` as a serialisable checkpoint mirrors RocksDB's `Checkpoint` API and can coexist with vlog archival.

---

## 11. Distributed KV TODO Roadmap

### Phase 0 — Column Families & Raw KV Surface
- **Done**: Extend `utils.Entry`, write pipeline, and WAL paths with a `columnFamily` tag while keeping default CF compatibility.
- **Done**: Define CF identifiers (`default`, `lock`, `write`) and helper routines analogous to TinyKV's `KeyWithCF`.
- **Done**: Update `DB.Set/Get/Del/NewIterator` to accept CF-aware options and add mixed-CF coverage in unit tests.
- Document CF key layout in `docs/cli.md` / `docs/testing.md`; add CLI flag for CF stats if useful.

### Phase 1 — Raft Core (TinyKV Project2A)
- **Done**: Introduced `raft/` package wrapping etcd/raft's `RawNode`, `MemoryStorage`, and single-node election/proposal tests.
- Implement ticking (`tickElection`, `tickHeartbeat`) and configurable timeouts; integrate with existing `utils.WaterMark` for proposal tracking.
- Add unit tests mirroring TinyKV labs (single-node commit, leader election, log replication corner cases).

### Phase 2 — Raft KV Integration (TinyKV Project2B)
- **Done**: `raftstore` peers reuse the DB WAL + manifest through `engine.WALStorage`, sharing the persistence path for Ready entries, HardState, and snapshots. Restart recovery and three-node replication are covered in tests.
- **Done**: Snapshot resend queues feed `LogRaftTruncate` (2025‑10‑18) so manifest reflects the latest truncation point; `lsm.canRemoveWalSegment` honors this metadata to avoid premature GC.
- **Done**: `raftstore` is split into `peer/` (FSM + lifecycle), `engine/` (WAL/Disk storage), and `transport/` (in-memory/gRPC) subpackages, mirroring TinyKV's layering.
- **Done**: Added `GRPCTransport` (`raftstore/transport/grpc_transport.go`) with comprehensive tests for cross-node replication, partition recovery, and backpressure, replacing the old `net/rpc` implementation.
- **Done**: Integrated raft apply with `utils.WaterMark` backpressure, introduced `Peer.WaitApplied`, and expanded slow follower/failover tests (`peer_test.go`, `levels_slow_follower_test.go`).
- **Done**: Added typed WAL records plus manifest validation (`EditRaftPointer`), alongside automated fault-injection suites to keep truncation metadata consistent (`RecordMetrics`, backlog instrumentation, recovery scripts).
- **Done**: Hardened gRPC transport with retry/timeouts + TLS hooks and added watchdog instrumentation + chaos script coverage (see `scripts/transport_chaos.sh`).

### Phase 3 — Log GC & Snapshots (TinyKV Project2C)
- **Done**: `manifest.LogRaftTruncate` persists index/term + segment/offset; WAL GC consults `SegmentIndex`/`TruncatedOffset`. Tests (`raftstore/engine/wal_storage_test.go`, `lsm/levels_slow_follower_test.go`) verify truncation/cleanup coupling and `MaybeCompact` keeps manifest in sync.
- **Done**: `wal_storage.ApplySnapshot` / `compactTo` write back truncation metadata so restart recovers correctly; integration tests ensure slow followers are not GC'd prematurely.
- **Done**: Added `engine.ExportSnapshot/ImportSnapshot` and `TestRecoverySnapshotExportRoundTrip`; `scripts/recovery_scenarios.sh` now emits `RECOVERY_METRIC` for snapshot flows.
- **Done**: WAL typed-record counters surface via `StatsSnapshot`/CLI (`WALRecordCounts`, `WALRemovableRaftSegments`), recording lag during GC.
- **Done**: `TestRecoverySlowFollowerSnapshotBacklog` covers slow follower + snapshot + WAL GC; recovery scripts gained matching scenarios/metrics.
- **Done**: WAL watchdog now consumes backlog metrics to trigger automated segment GC and emits typed-record alerts surfaced via stats/CLI.

### Phase 4 — Multi-Raft & Region Management (TinyKV Project3A/3B)
- **Done**: Manifest persists `RegionMeta`; `store.regionManager`/`RegionMetrics` expose the catalog and live counts; `nokv regions` and `StatsSnapshot` report region state distribution.
- **Done**: Config change handling (add/remove peers) and leader transfer keep logical membership in sync with manifest updates.
- **Done**: Split & merge flow through raft admin commands (`Peer.ProposeAdmin` + store handlers). Child peers are bootstrapped, manifest is updated with rollback on failure, and merge removes source peers/regions. Unit tests exercise split → merge lifecycles.
- **Done**: Split/merge lifecycle tests (including raft-driven `ProposeSplit`/`ProposeMerge`) validate peer bootstrap, metadata updates, and peer teardown.

### Phase 5 — Cluster Scheduler (TinyKV Project3C)
- **Done**: Introduced a lightweight scheduler coordinator consuming region/store heartbeats; stores emit periodic heartbeats (configurable interval/StoreID) and the `nokv scheduler` CLI exposes aggregated snapshots.
- **Done**: Added planner scaffolding with a sample leader-balance heuristic executed via the store heartbeat loop.
- **Next**: Harden operation queues/cooldowns, enrich store metrics (capacity/usage), add failure detection, and supply scenario-driven tests for load/peer rebalance.

### Phase 6 — Distributed MVCC & 2PC (TinyKV Project4)
- Extend MVCC layer to operate across regions: lock table persistence (`lock` CF), write records (`write` CF), and data (`default` CF).
- Implement handlers for `KvGet`, `KvPrewrite`, `KvCommit`, `KvScan`, `KvCheckTxnStatus`, `KvBatchRollback`, `KvResolveLock` through raft proposals.
- Wire primary/secondary lock resolution, TTL management, and latch manager; add integration tests mirroring TinyKV txn suites.

### Phase 7 — Observability, Tooling & Docs
- Expose raft/region/txn metrics via `Stats` and CLI (`nokv raft`, `nokv regions`); integrate with recovery traces.
- Document deployment guidance (`docs/distributed.md`), update testing matrix, and describe operational runbooks (snapshots, log GC, scheduler tuning).
- Provide reproducible scenarios (scripts or benchmarks) to validate leader transfer, failover, and transaction conflict handling end-to-end.

### Cross-Cutting Work
- Build a deterministic multi-node test harness (in-process transport, chaos injections).
- Ensure `go test ./...` covers new packages and add CI workflows executing TinyKV-style acceptance criteria.
- Maintain backward compatibility for single-node embedding by gating distributed components behind configuration flags.

For deeper implementation details, continue with module-specific documents:
- [WAL subsystem](wal.md)
- [Flush pipeline](flush.md)
- [ValueLog manager](vlog.md)
- [Manifest semantics](manifest.md)
- [Crash recovery plan](recovery.md)
- [Testing matrix](testing.md)
- [CLI reference](cli.md)
- [RaftStore overview](raftstore.md)
