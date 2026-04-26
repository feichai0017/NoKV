# 2026-04-27 Sharded WAL + Memtable：把 LSM 数据面写穿到底

> 状态：已落地。覆盖 commits `765416a6` (PR #158, sharding 主干)、`58ad25ef`
> (PR #159, WAL fsync/rotation fence)、`858e0b40` (PR #160, cross-shard
> hint cache)。本文不是路线图，而是这一组 PR 的 **decision record**——
> 为什么要这么做、形状是什么、不变量怎么保住、剩下哪些已知 trade-off。

---

## 1. 问题陈述

`feature/lsm-write-path-optimizations` 把 YCSB-A 推到 ~400K ops/s 之后，
profile 长这样：

| 路径 | CPU 占比 |
|---|---|
| `runtime.lock2`（绝大多数来自 `wal.Manager.mu`） | 16.57% |
| `pthread_cond_wait`（同一把锁的 cond 等待）| 19% |
| `syscall.rawsyscalln`（fsync）| 18.90% |
| memtable apply | 2% |

特征是**单点结构性瓶颈**：

- 一个 `wal.Manager` ⇒ 一个 fd ⇒ 一个 fsync worker ⇒ 一个 bufio.Writer
- 试过保持单 WAL 加多 commit worker，**实测吞吐反退 10%**——worker 互
  相在 `mu` 上排队，并行度被锁吃掉。
- 单只 shard WAL、memtable 还共享，会让 recovery 必须把 N 个 WAL 流
  合并成一个 memtable 的版本顺序，flush 还得 lock-step rotate N 个
  WAL，复杂且容易错。

结论：**WAL + memtable 一起分 shard 是最干净的形状**。

## 2. 整体设计

```
   多 caller goroutine
         │ lock-free push
         ▼
   ╔═════════════════════════════════╗
   ║ utils.MPSCQueue (CommitQueue)   ║  Vyukov MPSC ring
   ║ 多生产者 CAS / 单消费者 pop     ║
   ╚═════════════════════════════════╝
         │ pop
         ▼
   commitDispatcher (1 goroutine)
         │  shardID = fnv1a32(firstUserKey) & (N-1)
         ▼
   ┌────┬────┬────┬────┐
   │ ch │ ch │ ch │ ch │  N 个 buffered chan, cap=32
   └─┬──┴─┬──┴─┬──┴─┬──┘
     ▼    ▼    ▼    ▼
   ┌────────────────────┐
   │ commitProcessor[i] │  N 个 goroutine, pin 到 shard
   │ 1) drain channel   │
   │ 2) merge → 1× vlog │
   │       + 1× SetBatch│
   │       + 1× Sync    │
   │ 3) per-batch failedAt 还原                   │
   │ 4) → syncQueue (per-shard 分桶)              │
   └─┬────┬────┬────┬───┘
     ▼    ▼    ▼    ▼
   ┌─────────────────────────────────────────────┐
   │  vlog (按 key hash 分 N 个 bucket)          │
   └─────────────────────────────────────────────┘
   ┌─────────────────────────────────────────────┐
   │  lsm.shards[shardID]                        │
   │   ├─ wal.Manager (独立 fd / bufio / fsync) │
   │   │   └─ activeSyncRefs fence (rotation)   │
   │   ├─ memTable (ART)                        │
   │   ├─ immutables[] (待 flush)               │
   │   ├─ sync.RWMutex (per-shard)              │
   │   └─ highestFlushedSeg.atomic.Uint32       │
   │      ── retention 高水位 (per-shard)        │
   └─────────────────────────────────────────────┘
                │ flush
                ▼
   共享 levelManager (L0 sublevels + L1..L6)
```

### 2.1 数据结构（`engine/lsm/shard.go`）

```go
type lsmShard struct {
    id   int
    lock sync.RWMutex
    memTable           *memTable
    immutables         []*memTable
    wal                *wal.Manager
    highestFlushedSeg  atomic.Uint32 // per-shard retention 高水位
}
```

`LSM` 上原来的 `lock` / `memTable` / `immutables` / `wal` 字段全部
删除，统一收进 `shards []*lsmShard`。`memTable` 加一个反向指针
`shard *lsmShard`，自己知道往哪个 WAL 写、归属哪个 shard。

### 2.2 总 Manager 预算

```
4 raft shards (从 8 降到 4)
+ 4 LSM data shards (新)
= 8 wal.Manager 实例
```

老的全局 `db.wal` 解散，没有独立 control-plane Manager。`db` 上的
`wal *wal.Manager` 字段换成 `lsmWALs []*wal.Manager`。

每个 Manager ≈ 4 MiB bufio + 1 fd + 2 goroutine（fsync worker +
watchdog），合计 ≈ 32 MiB + 8 fd + 16 goroutine。远低于进程上限。

### 2.3 路由：per-key affinity

最初用过 round-robin 派发整个 batch 到 shard，落地之后发现一个隐藏 issue：
percolator 的 lock-on/lock-off 协议在同一个 `startTS` 写同一个 key 两次
（先 prewrite 写 CFLock，再 commit 删 CFLock）。这两次写如果落到不同
shard 的 memtable，**版本号相同没有 tiebreaker**——`Get` 走遍所有 shard
取 max-version，遇到平票就看遍历顺序。

修法：dispatcher 改成按 batch 第一条 entry 的 user-key 哈希路由：

```go
shardID := fnv1a32(firstUserKey) & (N-1)
```

- 同一 user-key 永远落同一 shard ⇒ percolator / fsmeta 的
  same-startTS 写法保持 last-write-wins。
- 整个 batch 仍只去一个 shard ⇒ SetBatch 原子性保留。
- 一个 batch 里多个 key 的话，全部跟着第一条 key 的 shard 走。

### 2.4 SetBatch 原子性不变量

形式化：

- 一个 `*CommitRequest` 只活在一个 `CommitBatch` 里。
- 一个 `CommitBatch` 端到端只交给一个 `commitProcessor`。
- 一个 `commitProcessor` 与一个 shard 一一绑定。
- ⇒ 一个 `CommitRequest` 只 append 一个 WAL shard、apply 一个
  memtable shard，**WAL 写 + memtable apply 在 LSM 层原子**。

burst coalesce 不破坏这个不变量：merge 后的 `lsm.SetBatchGroup` 仍把
每个原 batch 当成独立 group，per-group atomic 由 `applyWriteBatches`
内部保证。

### 2.5 commit burst coalesce

每个 commit processor 拿到一个 batch 之后，**先 drain 通道里所有
ready batch**（非阻塞 select default），merge 成一次
`vlog.write` + 一次 `lsm.SetBatchGroup` + 一次 (optional) `Sync`。
这把 N 个 WAL 写 syscall 折叠成 1 个，对应 profile 上 47% 的
`bufio.Flush` 热点。

`failedAt` 从 merged apply 还原回每 batch：用 boundaries 数组追踪
每 batch 在 merged request slice 里的偏移。

burst==1 时走 `runSingleCommit` fast path，跳过 merge bookkeeping。

per-shard channel cap 从初始 2 抬到 32，给 dispatcher 留 burst 空间。

> **试过的反案**：把 chan 换成 `utils.SPSCQueue`（自实现 lock-free
> 环 + parked 标志），bench 实测比 buffered chan cap=32 慢 30-40%——
> Go runtime 对 channel 的 buffered 路径已经 amortize 大部分调度开销，
> user-space atomic 流量超过省下的 scheduler op。SPSCQueue 已删除（commit
> `4675a597`）。

### 2.6 跨 shard MVCC 读

`LSM.Get` 不再是单 memtable lookup：

```go
var best *kv.Entry
for _, s := range lsm.shards {
    s.lock.RLock()
    if entry := s.memTable.Get(key); entry != nil {
        if best == nil || entry.Version > best.Version {
            best = entry
        }
    }
    // immutables 同理
    s.lock.RUnlock()
}
// 没命中 → levels.Get（共享 L0..LN）
```

Iterator / range tombstone / MaxVersion 都改成走遍所有 shard。N=4 下
ART memtable sub-µs，O(N) 增量 < bloom/block 读。

### 2.7 Cross-shard hint cache（PR #160 副产品）

跨 shard walk 在 profile 里占 ~17% CPU。Hint cache 用 64K bucket
xxhash 表把 (userKey → 最近写入 shardID) 缓起来，Get 命中 hint 时只
walk 一个 shard：

```go
if shardID, ok := lsm.lookupShardHint(key); ok && !lsm.hasRangeTombstones() {
    tables, release := lsm.getMemTablesForShard(shardID)
    if best := bestMemtableEntry(key, tables); best != nil {
        return best, nil
    }
    // miss → fallback 全 walk
}
```

正确性靠 fallback 兜底（hint stale 不破坏一致性，最坏多走一次）。
**有 range tombstone 时禁用**——避免漏判某 shard 的 RT 覆盖。

### 2.8 Recovery + Retention：弃用全局 logPointer

老设计 manifest 里有一个全局 `logPointer` 当 retention 高水位，recovery
跳过 ≤ logPointer 的段。多 shard 下这个高水位**不准**——shard A 刚
flush 完写 logPointer=100，shard B 的 80-99 段可能还没 flush，被误删
就丢数据。

新方案：

- 每个 shard 在内存里维护 `highestFlushedSeg.atomic.Uint32`，flush 完
  同步推进。
- 每个 shard 注册自己的 retention callback：
  `RetentionMark{ FirstSegment: highestFlushedSeg+1 }`。
- recovery 路径**不再读 manifest 的 logPointer 跳过段**——直接 replay
  磁盘上还在的所有段。
- WAL 段在 flush 完成时 `inline-delete`，不依赖 retention 兜底。
- 万一 flush 完但 inline-delete 没跑（crash 中），recovery 重新 apply
  这些段的 entry——MVCC 让重复 apply **幂等**（同 key 同 version 落
  同一个 ART 节点），SST 可能多一份但内容等价，下一轮 compaction
  自然合并。

### 2.9 WAL fsync/rotation lifecycle fence（PR #159）

副产品 issue：`runFsyncBatch` 为了让 phase-2 fsync 不堵新 caller，
释放了 `m.mu` 之后再调 `active.Sync()`。同期 `switchSegmentLocked`
（rotate / Close）会在持锁下 `m.active.Sync()` + `m.active.Close()`。
两路在同一个 fd 上 race，可能 close 一个正在 syscall 里的 fd ——
**数据不丢**（rotation 自己 Sync 已落盘），但 fsync worker 可能拿到
伪 EBADF，把伪错误向上抛给 batched fsync waiter。

修法：segment 级 refcount + cond。

```go
type Manager struct {
    activeFileCond *sync.Cond
    activeSyncRefs int  // > 0 表示有 fsync 正在用 m.active
}

// runFsyncBatch
flushBufioLocked()
active := m.pinActiveForSyncLocked(flushErr)  // refs++ 在 lock 下
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

`Close` 也要 wait（同一类危险）。覆盖测试：
`TestManagerRotateWaitsForInflightBatchedFsync`。

### 2.10 Flush + Range tombstones

每个 shard 自己 rotate；共享一个 `flushQueue` + N 个 flush worker。
SST 进共享 L0+sublevels（已有逻辑无须改）。

DELETE_RANGE 走单 batch ⇒ 按 §2.3 路由落单 shard memtable。读路径
已经 walk all shards，无须改。

## 3. 实测结果

YCSB-A 50/50 R-W (1KB value, 500K records / 500K ops):

| 阶段 | ops/s | p99 |
|---|---|---|
| Pre-shard baseline | 175K | — |
| Phase 1（pipelined write 等已落地）| ~400K | — |
| **Sharded data plane (N=4, conc=128)** | **725K** (benchmark-tuned) | 491µs |
| 同上 production-default | ~605K | — |

工业基线对比（`make bench`，500K rec / 500K ops / conc=64 / value=1KB）：

| Workload | NoKV | Badger | Pebble | NoKV vs best peer |
|---|---|---|---|---|
| A 50/50 RW | 617K | 340K | 274K | +81% |
| B 95/5 RW | 685K | 546K | 585K | +17% |
| C 100% read | 1037K | 593K | 564K | +75% |
| D 95% R + 5% latest | 694K | 765K | 686K | -9% (Badger 赢) |
| E 95% scan | 140K | 44K | 219K | -36% (Pebble 赢) |
| F RMW | 402K | 228K | 306K | +31% |

5/6 workload 第一，1/6 第二。

profile 验证：`runtime.lock2` 从 16.57% 降到 < 1%；剩余 47% 在
`syscall.write`（io_uring 路线的目标）。

## 4. 决策日志

- shard 在 LSM 层而不是 DB / coordinator 层 — 锁争用就在 LSM 层。
- WAL 和 memtable 一起分，不只是 WAL — recovery / flush 协调更简单。
- per-key affinity 路由，不是 round-robin — 修 percolator same-startTS
  路径。
- N=4 LSM + N=4 raft = 8 个 Manager。原全局 `db.wal` 解散进 4 个
  LSM shard，没有独立 control-plane Manager。
- 弃用全局 manifest logPointer，per-shard `highestFlushedSeg` +
  inline-delete。
- buffered chan cap=32 + burst coalesce，**否决** `utils.SPSCQueue`
  替换（实测慢 30-40%）。
- WAL fsync/rotation race 用 segment-level refcount + cond 解决，
  不退回持锁 fsync。
- shard hint cache 64K bucket xxhash on baseKey，
  range-tombstone 时禁用 fast path。

## 5. 已知 trade-off + 后续

| 项 | 性质 |
|---|---|
| 同 batch 含跨 shard key 时全部走 first key 的 shard | per-key affinity 副作用，反正后续单 key 写还是同 shard |
| 共享 levels / manifest / blockCache / flushQueue | N=4 没痛点；N≥8 时 manifest 写争用会浮现 |
| 跨 shard memtable Get 是 N 倍 ART lookup | hint cache 已经把热点 key 压回单 shard；冷 key 仍 N× |
| 大 dataset (>= 1GB) 时 BlockCache 装不下 → cache miss 主导 YCSB-C | 路线：Filter pinned cache + Negative cache（PR #161 在路上）+ Ribbon |

每项独立 PR、独立 design note。
