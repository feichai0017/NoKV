# 2026-04-27 Sharded WAL + Memtable: punch the LSM data plane all the way through

> Status: shipped. Covers commits `765416a6` (PR #158, sharding trunk), `58ad25ef` (PR #159, WAL fsync/rotation fence), and `858e0b40` (PR #160, cross-shard hint cache). This note isn't a roadmap — it's the **decision record** for that set of PRs: why we did it, what shape it took, what invariants hold, and the trade-offs we knowingly accepted.

---

## 1. Problem statement

After `feature/lsm-write-path-optimizations` pushed YCSB-A to ~400K ops/s, the profile looked like:

| Path | CPU share |
|---|---|
| `runtime.lock2` (vast majority from `wal.Manager.mu`) | 16.57% |
| `pthread_cond_wait` (cond wait on the same lock) | 19% |
| `syscall.rawsyscalln` (fsync) | 18.90% |
| memtable apply | 2% |

This is a **single-point structural bottleneck**:

- One `wal.Manager` ⇒ one fd ⇒ one fsync worker ⇒ one `bufio.Writer`.
- We tried "single WAL + multiple commit workers" — **measured throughput regression of 10%**: workers contended on `mu`, and the lock ate the parallelism.
- A single-shard WAL with shared memtable forces recovery to merge N WAL streams into one memtable's version order, and flush has to lock-step rotate N WALs. Complex and error-prone.

Conclusion: **sharding WAL + memtable together is the cleanest shape**.

## 2. Overall design

```
   many caller goroutines
         │ lock-free push
         ▼
   ╔═════════════════════════════════╗
   ║ utils.MPSCQueue (CommitQueue)   ║  Vyukov MPSC ring
   ║ multi-producer CAS / single pop ║
   ╚═════════════════════════════════╝
         │ pop
         ▼
   commitDispatcher (1 goroutine)
         │  shardID = fnv1a32(firstUserKey) & (N-1)
         ▼
   ┌────┬────┬────┬────┐
   │ ch │ ch │ ch │ ch │  N buffered chans, cap=32
   └─┬──┴─┬──┴─┬──┴─┬──┘
     ▼    ▼    ▼    ▼
   ┌────────────────────┐
   │ commitProcessor[i] │  N goroutines, pinned to shard
   │ 1) drain channel   │
   │ 2) merge → 1× vlog │
   │       + 1× SetBatch│
   │       + 1× Sync    │
   │ 3) per-batch failedAt restoration                   │
   │ 4) → syncQueue (per-shard bucket)                   │
   └─┬────┬────┬────┬───┘
     ▼    ▼    ▼    ▼
   ┌─────────────────────────────────────────────┐
   │  vlog (N hash-partitioned buckets by key)   │
   └─────────────────────────────────────────────┘
   ┌─────────────────────────────────────────────┐
   │  lsm.shards[shardID]                        │
   │   ├─ wal.Manager (own fd / bufio / fsync)  │
   │   │   └─ activeSyncRefs fence (rotation)   │
   │   ├─ memTable (ART)                        │
   │   ├─ immutables[] (pending flush)          │
   │   ├─ sync.RWMutex (per-shard)              │
   │   └─ highestFlushedSeg.atomic.Uint32       │
   │      ── retention high watermark (per-shard) │
   └─────────────────────────────────────────────┘
                │ flush
                ▼
   shared levelManager (L0 sublevels + L1..L6)
```

### 2.1 Data structures (`engine/lsm/shard.go`)

```go
type lsmShard struct {
    id   int
    lock sync.RWMutex
    memTable           *memTable
    immutables         []*memTable
    wal                *wal.Manager
    highestFlushedSeg  atomic.Uint32 // per-shard retention watermark
}
```

The old `lock` / `memTable` / `immutables` / `wal` fields on `LSM` are deleted, all replaced by `shards []*lsmShard`. `memTable` gets a back-pointer `shard *lsmShard` so it knows which WAL to write to and which shard it belongs to.

### 2.2 Manager budget

```
4 raft shards (down from 8)
+ 4 LSM data shards (new)
= 8 wal.Manager instances
```

The old global `db.wal` is dissolved; there is no separate control-plane Manager. The `wal *wal.Manager` field on `db` becomes `lsmWALs []*wal.Manager`.

Each Manager ≈ 4 MiB bufio + 1 fd + 2 goroutines (fsync worker + watchdog), totaling ≈ 32 MiB + 8 fds + 16 goroutines. Far below process limits.

### 2.3 Routing: per-key affinity

We initially used round-robin to dispatch whole batches to shards. After landing it, a hidden issue surfaced: percolator's lock-on/lock-off protocol writes the same key twice under the same `startTS` (prewrite writes CFLock, commit deletes CFLock). If those two writes land on different shard memtables, **the version is identical with no tiebreaker** — `Get` walks every shard taking max-version, and a tie depends on traversal order.

Fix: dispatcher routes by the user-key hash of the batch's first entry:

```go
shardID := fnv1a32(firstUserKey) & (N-1)
```

- The same user-key always lands on the same shard ⇒ percolator / fsmeta same-startTS writes preserve last-write-wins.
- The whole batch still goes to one shard ⇒ SetBatch atomicity preserved.
- If a batch contains many keys, all of them follow the first key's shard.

### 2.4 SetBatch atomicity invariant

Formally:

- A `*CommitRequest` lives in exactly one `CommitBatch`.
- A `CommitBatch` is end-to-end handed to exactly one `commitProcessor`.
- A `commitProcessor` is one-to-one bound to a shard.
- ⇒ a `CommitRequest` appends one WAL shard and applies one memtable shard. **WAL append + memtable apply are atomic at the LSM layer.**

Burst coalesce doesn't break this invariant: the merged `lsm.SetBatchGroup` still treats each original batch as an independent group; per-group atomicity is enforced inside `applyWriteBatches`.

### 2.5 Commit burst coalesce

Each commit processor, after pulling one batch, **first drains every ready batch in the channel** (non-blocking select default), merging into one `vlog.write` + one `lsm.SetBatchGroup` + one (optional) `Sync`. This folds N WAL-write syscalls into one — directly attacking the 47% `bufio.Flush` hotspot in the profile.

`failedAt` is restored back to per-batch from the merged apply: a boundaries array tracks each batch's offset in the merged request slice.

When `burst==1`, we use `runSingleCommit` fast-path and skip merge bookkeeping.

Per-shard channel cap raised from initial 2 to 32, leaving room for dispatcher bursts.

> **Tried and rejected**: replacing the chan with a `utils.SPSCQueue` (own lock-free ring + parked flag), bench-measured 30-40% slower than buffered chan cap=32 — the Go runtime's buffered-channel path already amortizes most scheduling cost; user-space atomic traffic exceeds the saved scheduler ops. SPSCQueue removed (commit `4675a597`).

### 2.6 Cross-shard MVCC reads

`LSM.Get` is no longer a single-memtable lookup:

```go
var best *kv.Entry
for _, s := range lsm.shards {
    s.lock.RLock()
    if entry := s.memTable.Get(key); entry != nil {
        if best == nil || entry.Version > best.Version {
            best = entry
        }
    }
    // immutables likewise
    s.lock.RUnlock()
}
// no hit → levels.Get (shared L0..LN)
```

Iterator / range tombstone / MaxVersion all walk every shard. At N=4, ART memtable lookup is sub-µs; the O(N) growth is < bloom/block read cost.

### 2.7 Cross-shard hint cache (PR #160 spinoff)

The cross-shard walk used ~17% CPU in the profile. Hint cache uses a 64K bucket xxhash table to cache `(userKey → most-recent-write shardID)`. When `Get` hits the hint, it walks only one shard:

```go
if shardID, ok := lsm.lookupShardHint(key); ok && !lsm.hasRangeTombstones() {
    tables, release := lsm.getMemTablesForShard(shardID)
    if best := bestMemtableEntry(key, tables); best != nil {
        return best, nil
    }
    // miss → fallback to full walk
}
```

Correctness is held by the fallback (a stale hint doesn't break consistency, worst-case is one extra walk). **Disabled when range tombstones exist** to avoid missing an RT-covered key in another shard.

### 2.8 Recovery + retention: dump the global logPointer

The old design kept a global `logPointer` in manifest as the retention watermark; recovery skipped segments ≤ logPointer. Under multi-shard, this watermark is **inaccurate** — shard A flushes and writes logPointer=100, but shard B's segments 80-99 may not have flushed yet, and they get falsely deleted, losing data.

New approach:

- Each shard maintains `highestFlushedSeg.atomic.Uint32` in memory and advances it after every flush.
- Each shard registers its own retention callback: `RetentionMark{ FirstSegment: highestFlushedSeg+1 }`.
- The recovery path **no longer reads manifest's logPointer to skip segments** — it replays every segment present on disk.
- WAL segments are `inline-delete`d on flush completion; they don't depend on retention as backstop.
- If a flush completes but inline-delete didn't run (mid-crash), recovery re-applies those segments' entries — MVCC makes repeated apply **idempotent** (same key + version lands on the same ART node), and any duplicate SST has equivalent contents that subsequent compaction merges naturally.

### 2.9 WAL fsync/rotation lifecycle fence (PR #159)

Spinoff issue: `runFsyncBatch`, in order to keep phase-2 fsync from blocking new callers, releases `m.mu` before calling `active.Sync()`. Concurrently, `switchSegmentLocked` (rotate / Close) holds the lock and calls `m.active.Sync()` + `m.active.Close()`. The two paths race on the same fd, potentially closing an fd that's in mid-syscall — **no data loss** (rotation's own Sync has hit disk) but the fsync worker may receive a spurious EBADF and propagate that fake error to batched fsync waiters.

Fix: segment-level refcount + cond.

```go
type Manager struct {
    activeFileCond *sync.Cond
    activeSyncRefs int  // > 0 means an fsync is using m.active
}

// runFsyncBatch
flushBufioLocked()
active := m.pinActiveForSyncLocked(flushErr)  // refs++ under lock
m.mu.Unlock()
syncErr = active.Sync()
m.mu.Lock()
m.unpinActiveSyncLocked()  // refs-- + Broadcast on 0

// switchSegmentLocked
m.active.Sync()
m.rebuildActiveCatalogLocked()
m.waitActiveSyncRefsLocked()  // ← cond.Wait until refs == 0
m.active.Close()
```

`Close` waits too (same hazard class). Coverage test: `TestManagerRotateWaitsForInflightBatchedFsync`.

### 2.10 Flush + range tombstones

Each shard rotates on its own; a shared `flushQueue` + N flush workers. SSTs land in shared L0+sublevels (existing logic, unchanged).

DELETE_RANGE goes through a single batch ⇒ routes to one shard's memtable per §2.3. The read path already walks every shard, no change needed.

## 3. Results

YCSB-A 50/50 R-W (1KB value, 500K records / 500K ops):

| Phase | ops/s | p99 |
|---|---|---|
| Pre-shard baseline | 175K | — |
| Phase 1 (pipelined write etc. landed) | ~400K | — |
| **Sharded data plane (N=4, conc=128)** | **725K** (benchmark-tuned) | 491µs |
| Same, production default | ~605K | — |

Industrial-baseline comparison (`make bench`, 500K rec / 500K ops / conc=64 / value=1KB):

| Workload | NoKV | Badger | Pebble | NoKV vs best peer |
|---|---|---|---|---|
| A 50/50 RW | 617K | 340K | 274K | +81% |
| B 95/5 RW | 685K | 546K | 585K | +17% |
| C 100% read | 1037K | 593K | 564K | +75% |
| D 95% R + 5% latest | 694K | 765K | 686K | -9% (Badger wins) |
| E 95% scan | 140K | 44K | 219K | -36% (Pebble wins) |
| F RMW | 402K | 228K | 306K | +31% |

5 of 6 workloads first, 1 of 6 second.

Profile validation: `runtime.lock2` dropped from 16.57% to <1%; the remaining 47% is in `syscall.write` (the io_uring route's target).

## 4. Decision log

- Sharding at the LSM layer instead of DB / coordinator — that's where the lock contention lives.
- Shard WAL and memtable together, not just WAL — simpler recovery / flush coordination.
- Per-key affinity routing instead of round-robin — fixes percolator same-startTS writes.
- N=4 LSM + N=4 raft = 8 Managers. The old global `db.wal` is dissolved into 4 LSM shards; there is no separate control-plane Manager.
- Drop the global manifest logPointer; use per-shard `highestFlushedSeg` + inline-delete.
- Buffered chan cap=32 + burst coalesce; **rejected** the `utils.SPSCQueue` replacement (30-40% slower in bench).
- WAL fsync/rotation race fixed with segment-level refcount + cond, not by reverting to fsync-under-lock.
- Shard hint cache uses 64K bucket xxhash on baseKey; fast path disabled when range tombstones exist.

## 5. Known trade-offs + future work

| Item | Nature |
|---|---|
| If a batch contains keys spanning shards, all go to the first key's shard | Per-key affinity side effect; subsequent single-key writes still hit the same shard |
| Shared levels / manifest / blockCache / flushQueue | No pain at N=4; manifest write contention may surface at N≥8 |
| Cross-shard memtable Get is N× ART lookups | Hint cache reduces hot keys to single-shard; cold keys still pay N× |
| Large dataset (≥ 1GB) makes BlockCache too small → cache miss dominates YCSB-C | Roadmap: pinned filter cache + negative cache (PR #161 in flight) + Ribbon |

Each item is its own PR, its own design note.
