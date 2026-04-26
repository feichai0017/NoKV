# 2026-04-26 LSM Data-Plane WAL + Memtable Sharding Design

> Status: design doc + scaffolding plan. The actual refactor is too large to
> land safely in a single session; this note locks the architectural decisions
> so the implementation can land incrementally without re-litigating tradeoffs.
>
> Companion docs:
> - `2026-04-26-lsm-engine-throughput-roadmap.md` (the broader roadmap)
> - WAL series: `feat(wal): durability/retention/group-commit/catalog/sharded` commits

---

## 1. Why this exists

`feature/lsm-write-path-optimizations` has driven YCSB-A from 175K to ~400K
ops/sec. Profiles show the remaining write-throughput ceiling is `wal.Manager.mu`:
- 19% CPU in `pthread_cond_wait`
- 16.57% in `runtime.lock2` (almost all from this single mutex)
- 18.90% in `syscall.rawsyscalln` (fsync)
- Memtable apply is only 2% — Sharded Memtable alone does not move the needle.

Multi commit worker on **shared WAL** is verified negative (workers contend on
the same mu). The ceiling is architectural: one Manager = one bufio = one
fsync worker = one fd. To break it, the LSM data plane needs multiple Manager
instances.

But sharding only the WAL while keeping a single memtable is messy: recovery
has to merge multiple WAL streams into one memtable, and flush has to rotate
N WAL shards in lock-step. The cleanest design shards both layers together.

## 2. Design summary

```
                    DB.commitDispatcher (1 goroutine, owns MPSC consumer)
                                  │
                                  │ batch
                                  ▼
                       routing: hash(workerID) % N
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
       commitProcessor[0]   commitProcessor[1]   commitProcessor[N-1]
              │                   │                   │
              │ shardID=0         │ shardID=1         │ shardID=N-1
              ▼                   ▼                   ▼
        ┌──────────┐         ┌──────────┐         ┌──────────┐
        │ lsmShard │         │ lsmShard │         │ lsmShard │
        │  ──────  │         │  ──────  │         │  ──────  │
        │  wal     │         │  wal     │         │  wal     │
        │  active  │         │  active  │         │  active  │
        │  immut[] │         │  immut[] │         │  immut[] │
        └──────────┘         └──────────┘         └──────────┘
              │                   │                   │
              └───────────────────┴───────────────────┘
                                  │ flushed SSTs
                                  ▼
                      Shared L0..LN (already supports
                      multiple SSTs via L0 sublevels)
```

### 2.1 Number of shards

Default proposal: **N = 4** for LSM data plane, with raft WAL fan-out
also lowered from 8 to 4 (raft groups are not the bottleneck — control
plane traffic and replicated entries are dwarfed by user writes, so 8
raft shards was overprovisioned). Total Manager instances:
- 4 raft shards (down from 8)
- 4 LSM data shards (new — replace the single `db.wal`)
- = **8 Manager instances**

Each Manager: ~4 MiB bufio + 1 fd + 2 goroutines (fsync worker + watchdog).
8 × ≈ 32 MiB + 8 fd + 16 goroutines. Well within process limits.

If the system is fd-constrained (small ulimit) both raft and LSM counts
can be tuned down to 2.

### 2.2 Routing

**Strategy**: commit-worker affinity. Each commit processor goroutine is
pinned to one shard. Round-robin assignment from the dispatcher.

Why not hash-by-key? A commit batch may contain entries spanning many keys.
Splitting a batch across shards breaks SetBatch atomicity guarantees that
percolator and fsmeta both rely on. Pinning a whole batch to one shard keeps
atomicity at the cost of slightly uneven shard load when batch sizes vary.

### 2.3 SetBatch atomicity invariant

- A single `commit request` lives in exactly one batch.
- A single `batch` is processed end-to-end by exactly one commit processor.
- A single `commit processor` is pinned to exactly one shard.
- → A single `commit request` writes to exactly one WAL shard and one
  memtable shard. **Atomicity preserved.**

### 2.4 Read path

`Get(key)`: query each shard's memtable + immutables in parallel (or sequential
since each Get is fast), then continue to L0..LN. Resolve by MVCC version.

`Iterator`: N memtable iterators + N immutable iterator-stacks + L0..LN. The
existing merge iterator infrastructure already supports many sources; we add
N-way memtable sources at the top of the heap.

Cost: O(N) per Get on the memtable layer. With N=4 this is ~+3-4× memtable
work per Get, but memtable Get was sub-microsecond before, so absolute cost
stays well under bloom-filter or block-read latency.

### 2.5 Recovery

Each shard's WAL replays into its own memtable. Shards are independent;
recovery is parallelizable. Cross-shard ordering is not required because:
- Internal keys carry MVCC timestamps.
- Latest-version-wins is enforced at read time, not at insert order.

### 2.6 Flush

Each shard's active memtable rotates independently when full
(`MemTableSize / N` per shard, or `MemTableSize` per shard with N×total
memory). Flush worker pool sized to handle N concurrent flushes.

Output: each shard produces SSTs into the **shared L0**. With our existing
L0 sublevels logic the new SSTs from different shards happily occupy
different sublevels (their keyranges typically overlap because shards see
disjoint write streams but shared key space).

### 2.7 Range tombstones

Tracked per-memtable today. With sharding:
- A `DELETE_RANGE [a, b)` issued through one batch lives in one shard's
  memtable.
- Read path checks all shards' range tombstones (already O(N) work).
- Range tombstone propagation to lower levels is unchanged (single
  rtCollector at level manager).

### 2.8 Range filter / SST-level optimizations

L0 sublevels and the prefix bloom + Snappy compression and the catalog
sidecar all live below the memtable layer. They are unchanged.

## 3. Implementation phases

### Phase 1: Scaffolding (no behavior change)

1. New `lsmShard` struct bundling `(wal *wal.Manager, memTable *memTable,
   immutables []*memTable)`.
2. `LSM.shards []*lsmShard` with default `len(shards) == 1`.
3. Helper accessors that walk all shards (for read paths).
4. Refactor `LSM.wal`, `LSM.memTable`, `LSM.immutables` to delegate to
   `LSM.shards[0]`.
5. All tests pass with N=1.

### Phase 2: Per-shard WAL Manager

1. Add `LSMShardCount` option (default 1).
2. `Open` creates N WAL Managers under
   `<workdir>/wal-XX/` directories.
3. Each shard owns its own memtable + WAL.
4. `SetBatchGroup` takes a `shardID` argument.
5. Recovery: parallel WAL replay across shards.
6. Tests with N=2/4.

### Phase 3: Commit pipeline binding

1. `CommitWorkers` and `LSMShardCount` are coupled (or commit workers
   round-robin to shards if mismatched).
2. Each commit processor is pinned to one shard.
3. Bench validates throughput scales with N until disk fsync becomes
   bottleneck.

### Phase 4: Read path adaptation

1. `Get` walks all shards' memtables and immutables.
2. `NewIterator` produces N-way merge.
3. Range tombstone checks scan all shards.

### Phase 5: Flush coordination

1. Per-shard flush triggers.
2. Flush worker pool sized for N.
3. SST output → shared L0 (sublevels handle this).

### Phase 6: Tuning

1. Default `LSMShardCount` based on benchmark sweep.
2. Document tradeoffs (read latency cost vs write throughput).
3. Update `README.md` benchmarks.

## 4. Backwards compatibility

- `LSMShardCount=1` produces the same on-disk layout as today (single
  WAL directory, single memtable).
- Upgrading existing data: existing WAL becomes shard 0; new shards start
  empty. No format change.
- Downgrading from N>1 to N=1: requires a clean shutdown so all shards
  flush their memtables (no WAL data left).

## 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| SetBatch atomicity broken across shards | Routing pins entire batch to one shard. |
| Read latency increase from N-way merge | Memtable ART Gets are sub-µs; N=4 keeps cost negligible. |
| Flush coordination races | Each shard's flush is independent; shared L0 admits concurrent SST adds (already supported). |
| Recovery time grows with N | Recovery is per-shard parallelizable. |
| File descriptor exhaustion | Default N=4 keeps total fd count bounded; tunable. |
| Snapshot consistency across shards | Memtable Gets walk all shards atomically with per-shard read locks; iterators capture all shards at construction time. |

## 6. Why not "shared WAL + multi commit worker"

Bench-verified negative: workers contend on `wal.Manager.mu`, throughput
drops 10% at N=4 vs N=1. The lock is the architectural ceiling; no amount
of fan-out above it helps.

## 7. Why not "sharded WAL only, single memtable"

Recovery has to merge N WAL streams into one memtable, ordered by version.
Flush has to rotate all N WALs in lock-step when the memtable rotates. Both
are messier than just sharding the memtable too.

## 8. Estimated effort

- Phase 1 (scaffolding): 2-3 days
- Phase 2 (per-shard WAL): 4-5 days
- Phase 3 (commit binding): 1-2 days
- Phase 4 (read path): 3-4 days
- Phase 5 (flush coord): 2-3 days
- Phase 6 (tuning): 1-2 days

**Total: 13-19 days = ~2-3 weeks of focused engineering.**

This effort is justified because the alternative is the throughput ceiling
we just hit. There is no smaller change that meaningfully lifts it.

## 9. Decision log

- **Decision 2026-04-26**: shard at LSM level, not at DB or coordinator
  level. Reason: the LSM is where the lock contention is, not above it.
- **Decision 2026-04-26**: shard wal AND memtable, not just wal. Reason:
  recovery and flush coordination is simpler with full sharding.
- **Decision 2026-04-26**: route by commit-worker affinity, not by key
  hash. Reason: preserves SetBatch atomicity without splitting batches.
- **Decision 2026-04-26**: default N=4 for LSM, 4 for raft (lowered from
  8 — raft groups carry control traffic, not user writes, so the 8-shard
  default was overprovisioned) = 8 total Managers. The legacy single
  `db.wal` is dissolved into the 4 LSM shards; there is no separate
  control-plane Manager. Tunable via `LSMShardCount`.
