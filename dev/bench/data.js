window.BENCHMARK_DATA = {
  "lastUpdate": 1777208157575,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "Guocheng Song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "765416a605584ea7ca5fa94eb872e30dead6901e",
          "message": "perf(lsm): shard data plane to N=4 WAL Managers; YCSB-A 175K → 725K (2.26×) (#158)\n\n* feat(lsm): specialize metadata read paths\n\n* feat(lsm): cache compressed block payloads\n\n* feat(lsm): move non-overlapping tables without rewrite\n\n* feat(lsm): pipelined write group + compaction output pacing\n\nTwo write-path optimizations on the same branch.\n\n1. Pipelined write\n   The DB commit worker now batches multiple commit requests into a\n   single LSM SetBatchGroup call which produces one WAL EntryBatch\n   record per group. Each inner request remains atomic: rotation only\n   happens at request boundaries; a WAL append failure never reaches\n   the memtable; a memtable apply failure after a durable WAL panics.\n\n2. Compaction output pacing\n   A token-bucket pacer throttles the table builder's data-block copy\n   to a configured CompactionWriteBytesPerSec. Pacing is opt-in (the\n   default is unlimited) and only applies to compaction output, never\n   to flush. When L0 table count reaches CompactionPacingBypassL0 the\n   next compaction build runs unpaced so foreground writes do not\n   stall on a paced compaction. Pacer state (BytesCharged, NanosThrottle)\n   is exposed via CompactionPacerStats for monitoring.\n\nTests\n- engine/lsm/lsm_test.go: write group / WAL failure / rotation boundary\n- engine/lsm/compaction_pacer_test.go: pacer rate + L0 bypass\n- engine/lsm/table_builder_test.go: per-block charge\n- engine/lsm/compaction_pacer_bench_test.go: nil vs active pacer overhead\n- db_test.go: commit batch coalesces into one LSM WAL record\n\nBenchmarks (Apple M3 Pro)\n- Nil pacer (default):                1.2 ns/op\n- Active pacer, bucket has tokens:   29   ns/op\n- Stats() snapshot:                   0.5 ns/op\n\nConfirms enabling pacing adds ~28 ns per data-block copy with zero\noverhead when disabled.\n\n* feat(lsm): L0 sublevels for read path (Phase A)\n\nL0 point reads previously linear-scanned every L0 table to find\ncandidates whose [MinKey, MaxKey] window covered the lookup key. With\ntypical L0 backlogs of dozens of tables, this dominates Get cost on\nwrite-heavy workloads.\n\nPhase A introduces a sublevel index inside levelHandler:\n\n  - buildL0Sublevels arranges L0 tables into the minimum number of\n    sublevels such that ranges within each sublevel are non-overlapping.\n    Greedy placement: sort by (MinKey asc, fid asc), put each table in\n    the first sublevel whose tail MaxKey strictly precedes its MinKey.\n  - sortTablesLocked rebuilds sublevels eagerly after every L0 mutation.\n  - selectTablesForKey for L0 binary-searches each sublevel for the at-\n    most-one candidate covering the key, instead of linearly checking\n    every L0 table.\n  - Falls back to the legacy linear scan when sublevels are nil (e.g.\n    transient state during construction).\n\nCompaction picker and trivial move are unchanged. They still treat L0\nas a single physical level. Sublevels exist only to accelerate Get.\n\nTests\n- engine/lsm/l0_sublevels_test.go: arrangement / binary-search /\n  per-sublevel candidate / selectTablesForKey integration / Get returns\n  the highest version across overlapping sublevels.\n\nBenchmarks (Apple M3 Pro, 64 L0 tables, single-key lookup)\n- BenchmarkL0SelectTablesForKeyLinear:        424.9 ns/op\n- BenchmarkL0SelectTablesForKeyViaSublevels:   42.8 ns/op\n                                                ~10x speedup\n\n* feat(lsm): L0 trivial move via sublevel-aware overlap check (Phase B)\n\nPhase A made L0 reads sublevel-aware. Phase B extends trivial move so\nL0 -> Lbase can also bypass the rewrite path when picker output is a\n'lonely island': a group of L0 tables whose union range does not\noverlap any other L0 table and does not overlap the destination level.\n\nPreviously canMoveToNextLevel hard-excluded L0 (`levelNum == 0` returned\nfalse). That gave up an entire class of zero-IO promotions for\ndisjoint key prefixes which fsmeta workloads (different mounts /\nparents) generate naturally.\n\nChanges\n- Add l0GroupHasNoOtherOverlap(group, all []*table) which scans L0 for\n  any table outside the chosen group whose range intersects the group\n  union. Linear in |L0|, called only on the trivial-move check path.\n- canMoveToNextLevel removes the L0 hard exclusion. For L0 it instead\n  requires (a) cd.bot is empty (no Lbase overlap, existing rule) and\n  (b) the chosen top has no L0 overlap with non-chosen tables (new\n  rule).\n- L1+ behavior is unchanged.\n\nTests\n- l0_sublevels_test.go:\n    TestL0GroupHasNoOtherOverlapAcceptsLonelyIsland\n    TestL0GroupHasNoOtherOverlapRejectsOverlappedGroup\n    TestCanMoveToNextLevelAllowsL0LonelyIsland\n    TestCanMoveToNextLevelRejectsL0OverlappingGroup\n- Existing TestCompactionTrivialMoveToNextLevel still passes (L1->L2\n  unchanged).\n\nCompaction picker is unchanged: it still emits the same plans. Phase B\nis purely an enablement of the trivial-move shortcut once a plan\nqualifies. Picker-level sublevel awareness (preferring whole-sublevel\ninputs) is left for a future commit.\n\n* perf(wal,lsm): pool entry encode buffer, drop SetBatchGroup slice clone\n\nTwo micro-optimizations on the write hot path identified by pprof:\n\n1. Pool the per-entry bytes.Buffer used during EncodeEntryBatch.\n   bytes.growSlice was 25.5% of total allocation in the YCSB profile.\n   The inner per-entry buffer is reset on every entry, and its bytes\n   are immediately copied into the outer payload buffer, so a sync.Pool\n   over scratch buffers is safe. The outer payload buffer stays\n   per-call because its slice escapes to the caller (would race with\n   the next encode).\n\n2. Reuse the same entryBufPool from wal.AppendEntry. Previously it\n   allocated its own var bytes.Buffer for each single-entry write.\n   Now both EncodeEntryBatch's inner loop and AppendEntry share one\n   pool, consistent with how kv.entryPool / kv.crc32Pool / kv.headerPool\n   are organized in the engine.\n\n3. Drop the defensive append([]*kv.Entry(nil), entries...) clone in\n   LSM.SetBatchGroup. LSM is the sole consumer of the slice for the\n   duration of the call and does not mutate it; the clone was pure\n   GC pressure on the write hot path.\n\nCap pooled buffers at 1 MiB so a single oversized batch does not\npin a large allocation in the pool forever.\n\nTests\n- All existing engine/wal and engine/lsm tests pass.\n- Lint clean.\n\nThis is the lightweight first step. The pprof profile shows the\ndominant cost (80%) is lock + cond_wait + syscall on the single LSM\nWAL manager. Multi-worker WAL sharding is the next, larger commit.\n\n* perf(wal,lsm): release manager mutex during fsync syscall, fix maxVersion race\n\nThe pprof of YCSB-A showed pthread_cond_wait + lock2 dominating CPU,\nwith the runFsyncBatch worker holding manager.mu across the actual\nactive.Sync() syscall. Under that lock layout, no other writer can\neven enter bufio while a fsync is in flight - effectively pinning\nfsync pipeline depth to 1.\n\nTwo fixes:\n\n1. runFsyncBatch now flushes bufio under m.mu, captures the active\n   file handle, then drops the lock before calling Sync(). New writers\n   appended during the syscall queue at fsync seq > target and ride\n   the next round. Phase 1 (flush) and Phase 2 (sync) are split:\n     Phase 1: lock(m.mu); writer.Flush(); active = m.active; unlock\n     Phase 2: active.Sync()           // unlocked\n     Phase 3: lock(m.mu); update durableSeq + broadcast; unlock\n   Effect: fsync pipeline depth becomes 2 (sync in flight + bufio\n   accumulating concurrently). Crucial for sync=true workloads where\n   fsync latency is ~50-200µs and otherwise serializes everything.\n\n2. memTable.maxVersion changed from uint64 to atomic.Uint64. This was\n   a pre-existing race introduced by Pipelined Write: multiple writers\n   share the lsm.lock RLock and concurrently mutate maxVersion. The\n   race detector caught it once per-batch concurrency increased.\n   Updated all writers (memtable.applyBatch, openMemTable replay) and\n   readers (lsm.maxVersion) to use Load/Store.\n\nRemoved the obsolete flushAndSyncLocked helper - DurabilityFsync inlines\nflush+sync, runFsyncBatch uses split phases, no other caller remains.\n\nTests\n- All engine/wal and engine/lsm tests pass under -race.\n- TestLSMSetBatchConcurrentReservations now race-clean.\n- Lint 0 issues.\n\nBenchmark behavior\n- sync=false: throughput ≈ 386-406K (within run variance, fsync was\n  not the dominant cost in this configuration).\n- sync=true: latency p99 expected to drop because new batches no\n  longer queue behind in-flight fsync.\n\n* perf(wal,db): encode WAL records outside m.mu, add commit fan-out scaffold\n\nTwo changes that target the m.mu hot section identified by pprof. Both\ncommits land together because they were designed as one pipeline.\n\n1. Move WAL record encoding outside m.mu (engine/wal/record.go,\n   manager.go).\n\n   AppendRecords now runs a two-phase pipeline:\n\n     Phase 1 (no lock): pre-encode every record into a pooled local\n       buffer using the new EncodeRecordTo. CRC32, length header, type\n       byte, and payload concatenation all happen on the caller's\n       goroutine.\n\n     Phase 2 (under m.mu): a single bufio.Write copies the pre-encoded\n       bytes per record, then ensureCapacity / segment rotation /\n       activeSize / per-segment counters / applyDurability run as\n       before.\n\n   The previous code held m.mu across four bufio.Writer.Write calls per\n   record plus the CRC computation. Multiple producers serialized on\n   that work. After the change the locked critical section is one Write\n   per record plus tiny bookkeeping; the CPU portion of encoding is\n   fully parallelizable across producers.\n\n   Pool: recordBufPool reuses scratch byte slices, capped at 1 MiB.\n   EncodeRecord is retained as a thin wrapper for callers that want a\n   streaming Write to an io.Writer.\n\n2. Multi commit-worker scaffold (db.go, options.go).\n\n   The single commitWorker goroutine is split into:\n\n     - commitDispatcher (1 goroutine): owns the MPSC queue consumer,\n       pulls batches and forwards them to commitDispatch.\n     - commitProcessor (N goroutines): each pulls a batch and runs the\n       full per-batch pipeline (collect, vlog write, applyRequests,\n       ack) independently.\n\n   SetBatch atomicity is preserved because each batch is processed\n   end-to-end inside one processor goroutine.\n\n   CommitWorkers option (default = 1) controls the processor count.\n   Default stays 1 because, with the current shared-LSM-WAL design,\n   raising it makes processors contend on wal.Manager.mu rather than\n   parallelize. Per-workload benchmarks confirm: A 50/50 lost ~10%\n   throughput at N=4 vs N=1 because the bottleneck is the shared WAL\n   mutex, not encoding parallelism. The fan-out path stays in tree so\n   it can be turned on once the LSM data plane WAL is sharded.\n\nTests\n- All engine/wal, engine/lsm, and root tests pass under -race.\n- Lint clean.\n\nBenchmark (M3 Pro, sync=false, 500K ops, conc=16)\n  YCSB-A 50/50 read/update : 397K ops/s (default workers=1)\n  YCSB-B 95/5  read/update : 1.15M\n  YCSB-C 100% read         : 1.17M\n  YCSB-D 95/5  read+latest : 1.30M\n  YCSB-F read-modify-write : 371K\n\nThe commit's main artifact for write throughput is the encoding-out-of\n-lock change. The fan-out plumbing is foundation work for Phase 2\nwhen LSM WAL gets sharded; today it is opt-in via CommitWorkers.\n\n* perf(lsm,db): shard LSM data plane (4 WAL Managers, per-shard commit pipeline)\n\nBreak the wal.Manager.mu ceiling by giving each LSM data-plane slice its\nown (memtable, immutables, wal.Manager) triple. Commit processors are\npinned 1:1 to shards so the hot write path no longer contends on a\nsingle fd / fsync worker / bufio buffer.\n\nTotal Manager budget: 4 raft + 4 LSM data = 8. The legacy single\ndb.wal is dissolved into 4 shard-scoped Managers under\n<workdir>/lsm-wal-XX/. Raft fan-out lowered from 8 to 4 — raft groups\nwere overprovisioned relative to user write traffic.\n\nPipeline (end-to-end shardID flow):\n\n    caller -> commitQueue (MPSC)\n           -> commitDispatcher (round-robin)\n           -> commitDispatch[shardID] (per-shard channel)\n           -> commitProcessor[shardID] (pinned)\n              -> vlog.write (already bucket-sharded)\n              -> lsm.SetBatchGroup(shardID, ...)\n                 -> shards[shardID].wal.AppendEntryBatch\n                 -> shards[shardID].memTable.applyBatch\n              -> syncQueue (per-shard fsync bucket)\n\nRead path walks every shard's memtable and picks max-version (cross-shard\nMVCC tiebreaker). Iterators contribute one source per shard plus the\nshared L0..LN merge.\n\nPer-shard WAL retention: each shard tracks highestFlushedSeg in memory\nand reports its own RetentionMark. The global manifest logPointer is no\nlonger authoritative for cross-shard recovery; recovery replays whatever\nsurvives on disk and relies on inline segment removal at flush time\n(MVCC keeps duplicate apply idempotent).\n\nKnown limitation (Phase 3): same (key, version) writes routed to\ndifferent shards by round-robin have no MVCC tiebreaker. Percolator\nlock-on/lock-off at the same startTS, fsmeta integration, and a few\ndirect-version tests therefore pin LSMShardCount=1 until per-key\naffinity routing lands.\n\nYCSB-A (50K records / 500K ops / value=1KB):\n\n    LSMShardCount   conc=16    conc=64    conc=128   p99 @ 128\n    N=1 (baseline)  321K        -          -         -\n    N=2             372K        425K       464K      907us\n    N=4             522K        642K       658K      491us\n\nN=4 conc=128 = ~2x baseline; p99 / p99.9 is consistently better than\nN=2 in every conc bucket.\n\npprof confirms wal.Manager.mu (runtime.lock2) drops from 16% at N=2\nto <1% at N=4. Remaining 47% CPU at N=4 is in syscall.write (bufio\nflush) — Phase 4 (commit-processor batch coalesce) addresses that.\n\nDesign doc: docs/notes/2026-04-26-lsm-data-plane-sharding-design.md\n\n* perf(db): per-key shard affinity + commit burst coalesce\n\nPhase 3 (per-key affinity routing): the dispatcher now picks a shard by\nhashing the user key of the batch's first entry instead of round-robin.\nSame key always goes to the same shard, which is required for the\npercolator lock-on/lock-off protocol (write CFLock at startTS, then\ndelete CFLock at the same startTS) and for any \"later-write-with-equal-\nversion wins\" pattern. SetBatch atomicity is still preserved — the whole\nbatch ends up on one shard, even when its entries span multiple keys.\n\nThis unblocks raftstore/percolator/fsmeta from the LSMShardCount=1 pin\nthey took during Phase 2; they now run with the default N=4 and pick up\nthe +50%+ throughput from sharding.\n\nPhase 4 (commit burst coalesce): the per-shard processor drains every\nbatch already sitting in its channel and merges them into a single\nvlog.write + lsm.SetBatchGroup + (optional) Sync. This collapses N\nWAL bufio.Flush + write syscalls into one per burst — the 47% CPU\nhotspot pprof identified at N=4. failedAt from the merged apply is\nfanned back to per-batch failedAt for ack semantics. Burst-size-1\ntakes a fast path (runSingleCommit) to avoid merge bookkeeping when\nno extras were drained. Per-shard channel cap raised from 2 to 32 so\nthe dispatcher actually accumulates a burst when processors are busy.\n\nStats path now aggregates RecordCounts / SegmentCount / ActiveSize\nacross all LSM data-plane shards instead of using a single shard's\nmetrics; per-shard manager metrics summed in stats.go.\n\nTest/test-helper changes:\n- TestFaultFSWriteFailureThenRecoverableReopen and\n  TestCloseAggregatesWalAndDirLockErrors arm the fault on every shard\n  WAL path so the rule fires whichever shard the user key hashes to.\n- percolator.latestWALPath picks the largest .wal across shard\n  directories (per-key affinity sends each key's writes to one shard).\n- All other LSMShardCount=1 pins (raftstore/testcluster, percolator,\n  raftstore/kv, raftstore/server, db_test TestColumnFamilies) dropped.\n\nYCSB-A on N=4 (4 LSM shards, value=1KB, 500K records / 500K ops):\n\n    conc        Phase 2      Phase 4        Δ\n    16          522K         470K          -10%   (burst rarely > 1)\n    32          617K         612K            0%\n    64          642K         703K          +10%\n    128         658K         725K          +10%   (2.26x N=1 baseline)\n\np99 stays at 491us under N=4 c=128; p99.9 widens 637->753us as a\nthroughput-vs-tail tradeoff.\n\n* refactor(utils,db,bench): SPSC queue primitive + commit-path cleanup\n\n- utils.SPSCQueue: new wait-free single-producer single-consumer ring,\n  paired with cap=1 notify channel and a \"consumer parked\" flag so the\n  producer signals only when the consumer actually sleeps. 8 unit tests\n  cover capacity round-up, push/pop, blocking-pop wake, close-then-drain,\n  close-unblocks-park, push-after-close, ring wrap, and a 50K\n  producer/consumer sum-check. Lives next to MPSCQueue in utils.\n\n- Commit pipeline keeps the channel-based per-shard dispatch (cap=32 +\n  burst coalesce + per-key affinity from Phase 3+4). We tried plumbing\n  SPSCQueue into commitDispatch[shardID] to bypass the Go runtime path:\n  YCSB-A measured 30-40% slower than the buffered channel because cap=32\n  already amortizes scheduler hops and the user-space atomic traffic on\n  a hand-rolled ring outweighs the savings. SPSCQueue stays as a\n  available primitive, not wired into the hot path.\n\n- Benchmark YCSB profile now starts from NoKV.NewDefaultOptions() and\n  overrides only sizing + perf-irrelevant background helpers (watchdog,\n  vlog GC, hot-key throttle, batch-coalesce wait). Previously the\n  profile constructed Options from scratch and silently skipped many\n  defaults — block compression, prefix bloom, throttling triggers,\n  NumCompactors, etc. After the alignment YCSB-A drops from a\n  benchmark-clean 725K to a production-realistic ~605K at N=4 c=128;\n  the gap is the cost of features users actually run.\n\n- LSM cleanup: drop dead public ShardCount() API and unused private\n  shardCount() / shardOf() helpers. memtable rotation/flush now uses\n  mt.shard directly. Stale \"Phase 1 always returns 1\" comment removed.\n\n- db_test drSimulateCrash: stop nilling db.lsmWALs[i] slots. Commit\n  processors cache the slot at goroutine startup and never re-read it,\n  but the race detector flagged the slot rewrite — a closed Manager is\n  enough to fail subsequent writes; nilling adds nothing and trips race.\n\n* docs,bench: merge LSM optimization notes; benchmark profile to extreme-perf\n\n- Merge docs/notes/2026-04-26-lsm-data-plane-sharding-design.md into\n  docs/notes/2026-04-26-lsm-engine-throughput-roadmap.md. The sharding\n  design landed across two commits (eeeee1f0 Phase 1+2, 5a6ec5ef Phase\n  3+4) and now sits in §3.1 of the unified roadmap as \"implemented\" with\n  the design + decision log preserved verbatim. Other roadmap sections\n  updated to reflect Tier 0 completions (Pipelined Write, Parallel\n  Memtable Flush, Trivial Move) and a new Tier 1 entry for cross-shard\n  memtable hint cache.\n\n- Benchmark profile: disable BlockCompression (snappy) and PrefixExtractor\n  (native metadata bloom) to recover the \"extreme perf\" YCSB numbers.\n  These two production defaults trade CPU for IO/cache benefits that\n  don't pay off on a fast local NVMe under a small synthetic workload.\n  NewDefaultOptions still sets them on for production.\n\n* docs: mark already-implemented items as Tier 0 in LSM roadmap\n\nAudit pass against the actual code surfaced several optimizations that\nwere already shipped but the roadmap still listed as TODO:\n\n- §3.5 Subcompactions — engine/lsm/compaction_executor.go:484/572\n  levelManager.subcompact + utils.Throttle for inflight workers.\n- §3.6 Adaptive L0 Slowdown — lsm.throttleWrites / throttlePressure\n  permille + WriteThrottleState (None / Slowdown / Stop) + L0 trigger\n  defaults in options.go.\n- §4.1 Compressed Block Cache — engine/lsm/cache.go blockEntry holds\n  the on-disk (compressed) bytes plus compression/rawLen; table.go\n  decodeCachedBlock decompresses on hit. Sharded ristretto cache.\n  Test: TestBlockCacheStoresCompressedPayload.\n- §5.3 Auto-tuning Compaction Concurrency — adaptive shard bump in\n  compaction_executor.go:67 (\"more backlog => allow more shards,\n  capped by shard count\").\n- Prefix Bloom — tableIndex.PrefixBloomFilter + table.prefixBloomMiss.\n- Adaptive iterator prefetch — engine/index/iterator.go\n  PrefetchAdaptive field.\n- IteratorPool — internal/runtime/iterator_pool.go.\n\nEach item now has a code-pointer in §2.1 and the corresponding §3-5\nsubsection has been switched from \"待办\" to \"✅ 已落地\" with a brief\nimplementation note. §4.5 Multi-Level Iterator Pinning is downgraded\nto \"⚠️ 部分落地\" — adaptive prefetch + IteratorPool are in, explicit\ncross-SST block pinning is still TODO.\n\nTier 1 list shrunk to the actual remaining short-ROI items (io_uring,\ncross-shard memtable hint, Filter pinned cache, Ribbon). Tier 2 keeps\nhybrid-tiered+leveled, two-level index, the iterator-pin completion.\nIndustry comparison table updated to reflect 11 ✅ entries on the NoKV\ncolumn.\n\n* refactor: drop dead SPSCQueue + fix stale doc refs (PR review)\n\nPer PR #158 review:\n\n- Delete utils/spsc_queue.go + spsc_queue_test.go. The benchmark already\n  picked the buffered channel over the SPSC ring (channel cap=32 was\n  30-40% faster on YCSB-A); leaving a non-hot-path \"primitive\" with no\n  caller is the kind of dead code the project rule explicitly forbids.\n\n- Fix 5 stale doc-path refs (options.go / db.go / engine/lsm/lsm.go ×2 /\n  engine/lsm/shard.go) that pointed to the merged-and-deleted\n  2026-04-26-lsm-data-plane-sharding-design.md. All now point to the\n  unified 2026-04-26-lsm-engine-throughput-roadmap.md.\n\n- Clarify Options.LSMShardCount: non-power-of-two values silently round\n  down (e.g. 6→4, 12→8). Doc string updated.\n\n- Add the WAL DurabilityFsyncBatched / rotation lifecycle race as a\n  Tier-1 follow-up in the roadmap. Path: runFsyncBatch caches\n  m.active, drops m.mu, calls active.Sync() while a concurrent\n  switchSegmentLocked may Sync+Close the same fd. Worst case is a\n  spurious EBADF returned to fsync waiters (data is already durable\n  because rotation itself Syncs); only affects raftstore which is the\n  sole DurabilityFsyncBatched user. Two fix shapes documented (segment\n  refcount preferred over inflight bool fence).",
          "timestamp": "2026-04-26T22:53:09+10:00",
          "tree_id": "59294cc71e7696a98ea17142263bdf1536d13728",
          "url": "https://github.com/feichai0017/NoKV/commit/765416a605584ea7ca5fa94eb872e30dead6901e"
        },
        "date": 1777208155841,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 9490,
            "unit": "ns/op\t   3.37 MB/s\t     420 B/op\t      11 allocs/op",
            "extra": "13294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 9490,
            "unit": "ns/op",
            "extra": "13294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 3.37,
            "unit": "MB/s",
            "extra": "13294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 420,
            "unit": "B/op",
            "extra": "13294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "13294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 20485,
            "unit": "ns/op\t 199.95 MB/s\t     399 B/op\t      18 allocs/op",
            "extra": "5259 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 20485,
            "unit": "ns/op",
            "extra": "5259 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 199.95,
            "unit": "MB/s",
            "extra": "5259 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "5259 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "5259 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 3381,
            "unit": "ns/op\t  18.93 MB/s\t     312 B/op\t       7 allocs/op",
            "extra": "33642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 3381,
            "unit": "ns/op",
            "extra": "33642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 18.93,
            "unit": "MB/s",
            "extra": "33642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 312,
            "unit": "B/op",
            "extra": "33642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "33642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 8608,
            "unit": "ns/op\t 475.82 MB/s\t    9208 B/op\t       9 allocs/op",
            "extra": "12584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 8608,
            "unit": "ns/op",
            "extra": "12584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 475.82,
            "unit": "MB/s",
            "extra": "12584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 9208,
            "unit": "B/op",
            "extra": "12584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "12584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 178321,
            "unit": "ns/op\t  91.88 MB/s\t   46679 B/op\t     143 allocs/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 178321,
            "unit": "ns/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 91.88,
            "unit": "MB/s",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 46679,
            "unit": "B/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 143,
            "unit": "allocs/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 643778,
            "unit": "ns/op\t  25.45 MB/s\t   46979 B/op\t     143 allocs/op",
            "extra": "200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 643778,
            "unit": "ns/op",
            "extra": "200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 25.45,
            "unit": "MB/s",
            "extra": "200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46979,
            "unit": "B/op",
            "extra": "200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 143,
            "unit": "allocs/op",
            "extra": "200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 598314,
            "unit": "ns/op\t  27.38 MB/s\t   47117 B/op\t     144 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 598314,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 27.38,
            "unit": "MB/s",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 47117,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 144,
            "unit": "allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 3842468,
            "unit": "ns/op\t      12 B/op\t       0 allocs/op",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 3842468,
            "unit": "ns/op",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 12,
            "unit": "B/op",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 940.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "114966 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 940.7,
            "unit": "ns/op",
            "extra": "114966 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "114966 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "114966 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 486.8,
            "unit": "ns/op\t 131.46 MB/s\t    1541 B/op\t       0 allocs/op",
            "extra": "255667 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 486.8,
            "unit": "ns/op",
            "extra": "255667 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 131.46,
            "unit": "MB/s",
            "extra": "255667 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 1541,
            "unit": "B/op",
            "extra": "255667 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "255667 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 158.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "747777 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 158.8,
            "unit": "ns/op",
            "extra": "747777 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "747777 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "747777 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "916567 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "916567 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "916567 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "916567 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 54,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2196459 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 54,
            "unit": "ns/op",
            "extra": "2196459 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2196459 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2196459 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 925.5,
            "unit": "ns/op\t  69.15 MB/s\t     158 B/op\t       1 allocs/op",
            "extra": "165718 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 925.5,
            "unit": "ns/op",
            "extra": "165718 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 69.15,
            "unit": "MB/s",
            "extra": "165718 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 158,
            "unit": "B/op",
            "extra": "165718 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "165718 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 404.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "284198 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 404.9,
            "unit": "ns/op",
            "extra": "284198 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "284198 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "284198 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 381.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "323659 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 381.2,
            "unit": "ns/op",
            "extra": "323659 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "323659 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "323659 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 42.93,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2693862 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 42.93,
            "unit": "ns/op",
            "extra": "2693862 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2693862 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2693862 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index)",
            "value": 366.9,
            "unit": "ns/op\t 174.42 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "293983 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 366.9,
            "unit": "ns/op",
            "extra": "293983 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 174.42,
            "unit": "MB/s",
            "extra": "293983 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "293983 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "293983 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index)",
            "value": 937.9,
            "unit": "ns/op\t  68.24 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "391810 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 937.9,
            "unit": "ns/op",
            "extra": "391810 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 68.24,
            "unit": "MB/s",
            "extra": "391810 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "391810 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "391810 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerNilCharge (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2.075,
            "unit": "ns/op",
            "extra": "67740159 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerChargeFromBucket (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 40.67,
            "unit": "ns/op",
            "extra": "3310936 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerChargeWithMockSleep (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 43.04,
            "unit": "ns/op",
            "extra": "2788413 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerStats (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 0.3613,
            "unit": "ns/op",
            "extra": "332793855 times\n4 procs"
          },
          {
            "name": "BenchmarkL0SelectTablesForKeyLinear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1249,
            "unit": "ns/op",
            "extra": "107442 times\n4 procs"
          },
          {
            "name": "BenchmarkL0SelectTablesForKeyViaSublevels (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 92.66,
            "unit": "ns/op",
            "extra": "1327034 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 32806,
            "unit": "ns/op\t 249.71 MB/s\t   34913 B/op\t     206 allocs/op",
            "extra": "3770 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 32806,
            "unit": "ns/op",
            "extra": "3770 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 249.71,
            "unit": "MB/s",
            "extra": "3770 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34913,
            "unit": "B/op",
            "extra": "3770 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 206,
            "unit": "allocs/op",
            "extra": "3770 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41845,
            "unit": "ns/op\t 195.77 MB/s\t   34919 B/op\t     206 allocs/op",
            "extra": "2630 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 41845,
            "unit": "ns/op",
            "extra": "2630 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 195.77,
            "unit": "MB/s",
            "extra": "2630 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34919,
            "unit": "B/op",
            "extra": "2630 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 206,
            "unit": "allocs/op",
            "extra": "2630 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5465340,
            "unit": "ns/op\t75963327 B/op\t     497 allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5465340,
            "unit": "ns/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 75963327,
            "unit": "B/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 497,
            "unit": "allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5296547,
            "unit": "ns/op\t75959981 B/op\t     495 allocs/op",
            "extra": "20 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5296547,
            "unit": "ns/op",
            "extra": "20 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 75959981,
            "unit": "B/op",
            "extra": "20 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 495,
            "unit": "allocs/op",
            "extra": "20 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 285.2,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "392436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 285.2,
            "unit": "ns/op",
            "extra": "392436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "392436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "392436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 476.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "250923 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 476.3,
            "unit": "ns/op",
            "extra": "250923 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "250923 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "250923 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 153.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "760772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 153.2,
            "unit": "ns/op",
            "extra": "760772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "760772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "760772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 309.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "363460 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 309.1,
            "unit": "ns/op",
            "extra": "363460 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "363460 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "363460 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 30872,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3888 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 30872,
            "unit": "ns/op",
            "extra": "3888 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3888 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3888 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 79.89,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1517074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 79.89,
            "unit": "ns/op",
            "extra": "1517074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1517074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1517074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 14979,
            "unit": "ns/op\t     288 B/op\t       5 allocs/op",
            "extra": "7924 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 14979,
            "unit": "ns/op",
            "extra": "7924 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 288,
            "unit": "B/op",
            "extra": "7924 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7924 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 481.2,
            "unit": "ns/op\t     280 B/op\t       4 allocs/op",
            "extra": "234649 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 481.2,
            "unit": "ns/op",
            "extra": "234649 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 280,
            "unit": "B/op",
            "extra": "234649 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "234649 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 14789,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "8007 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 14789,
            "unit": "ns/op",
            "extra": "8007 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "8007 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "8007 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 481.6,
            "unit": "ns/op\t     248 B/op\t       3 allocs/op",
            "extra": "214486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 481.6,
            "unit": "ns/op",
            "extra": "214486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "214486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "214486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 51052,
            "unit": "ns/op\t   19080 B/op\t      11 allocs/op",
            "extra": "2206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 51052,
            "unit": "ns/op",
            "extra": "2206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 19080,
            "unit": "B/op",
            "extra": "2206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 882.5,
            "unit": "ns/op\t     656 B/op\t      11 allocs/op",
            "extra": "125170 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 882.5,
            "unit": "ns/op",
            "extra": "125170 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 656,
            "unit": "B/op",
            "extra": "125170 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "125170 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 56090,
            "unit": "ns/op\t   22448 B/op\t      54 allocs/op",
            "extra": "1984 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 56090,
            "unit": "ns/op",
            "extra": "1984 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 22448,
            "unit": "B/op",
            "extra": "1984 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1984 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 4503,
            "unit": "ns/op\t    4080 B/op\t      54 allocs/op",
            "extra": "24074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 4503,
            "unit": "ns/op",
            "extra": "24074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4080,
            "unit": "B/op",
            "extra": "24074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "24074 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 85730,
            "unit": "ns/op\t   48880 B/op\t     390 allocs/op",
            "extra": "1378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 85730,
            "unit": "ns/op",
            "extra": "1378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 48880,
            "unit": "B/op",
            "extra": "1378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 31787,
            "unit": "ns/op\t   30960 B/op\t     390 allocs/op",
            "extra": "3595 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 31787,
            "unit": "ns/op",
            "extra": "3595 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30960,
            "unit": "B/op",
            "extra": "3595 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3595 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1808,
            "unit": "ns/op\t    1832 B/op\t      21 allocs/op",
            "extra": "63024 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1808,
            "unit": "ns/op",
            "extra": "63024 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1832,
            "unit": "B/op",
            "extra": "63024 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "63024 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1699,
            "unit": "ns/op\t    1344 B/op\t      16 allocs/op",
            "extra": "72547 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1699,
            "unit": "ns/op",
            "extra": "72547 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1344,
            "unit": "B/op",
            "extra": "72547 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "72547 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10179,
            "unit": "ns/op\t    8776 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10179,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8776,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10604,
            "unit": "ns/op\t    8288 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10604,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8288,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37254,
            "unit": "ns/op\t   32584 B/op\t     393 allocs/op",
            "extra": "3290 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37254,
            "unit": "ns/op",
            "extra": "3290 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32584,
            "unit": "B/op",
            "extra": "3290 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3290 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41693,
            "unit": "ns/op\t   32096 B/op\t     388 allocs/op",
            "extra": "3069 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 41693,
            "unit": "ns/op",
            "extra": "3069 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32096,
            "unit": "B/op",
            "extra": "3069 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "3069 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41232,
            "unit": "ns/op\t     344 B/op\t       9 allocs/op",
            "extra": "2804 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 41232,
            "unit": "ns/op",
            "extra": "2804 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "2804 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2804 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1034,
            "unit": "ns/op\t     320 B/op\t       6 allocs/op",
            "extra": "114436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1034,
            "unit": "ns/op",
            "extra": "114436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 320,
            "unit": "B/op",
            "extra": "114436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "114436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 49167,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2391 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 49167,
            "unit": "ns/op",
            "extra": "2391 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2391 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2391 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 459.5,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "255146 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 459.5,
            "unit": "ns/op",
            "extra": "255146 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "255146 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "255146 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 19511,
            "unit": "ns/op\t    5144 B/op\t       5 allocs/op",
            "extra": "5548 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 19511,
            "unit": "ns/op",
            "extra": "5548 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5144,
            "unit": "B/op",
            "extra": "5548 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "5548 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 20120,
            "unit": "ns/op\t    5144 B/op\t       5 allocs/op",
            "extra": "6417 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 20120,
            "unit": "ns/op",
            "extra": "6417 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5144,
            "unit": "B/op",
            "extra": "6417 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6417 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18899,
            "unit": "ns/op\t    4864 B/op\t       1 allocs/op",
            "extra": "5872 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18899,
            "unit": "ns/op",
            "extra": "5872 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4864,
            "unit": "B/op",
            "extra": "5872 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5872 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18965,
            "unit": "ns/op\t    4864 B/op\t       1 allocs/op",
            "extra": "5739 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18965,
            "unit": "ns/op",
            "extra": "5739 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4864,
            "unit": "B/op",
            "extra": "5739 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5739 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 28341,
            "unit": "ns/op\t     268 B/op\t       7 allocs/op",
            "extra": "3958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 28341,
            "unit": "ns/op",
            "extra": "3958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 268,
            "unit": "B/op",
            "extra": "3958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 847.8,
            "unit": "ns/op\t     250 B/op\t       5 allocs/op",
            "extra": "142420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 847.8,
            "unit": "ns/op",
            "extra": "142420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 250,
            "unit": "B/op",
            "extra": "142420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "142420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 26113,
            "unit": "ns/op\t     308 B/op\t       7 allocs/op",
            "extra": "4225 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 26113,
            "unit": "ns/op",
            "extra": "4225 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 308,
            "unit": "B/op",
            "extra": "4225 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4225 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 906.1,
            "unit": "ns/op\t     292 B/op\t       5 allocs/op",
            "extra": "117321 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 906.1,
            "unit": "ns/op",
            "extra": "117321 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 292,
            "unit": "B/op",
            "extra": "117321 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "117321 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 83271,
            "unit": "ns/op\t   30417 B/op\t      30 allocs/op",
            "extra": "1420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 83271,
            "unit": "ns/op",
            "extra": "1420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30417,
            "unit": "B/op",
            "extra": "1420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1420 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2230,
            "unit": "ns/op\t    2008 B/op\t      28 allocs/op",
            "extra": "50708 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2230,
            "unit": "ns/op",
            "extra": "50708 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 2008,
            "unit": "B/op",
            "extra": "50708 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "50708 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 88272,
            "unit": "ns/op\t   33785 B/op\t      73 allocs/op",
            "extra": "1342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 88272,
            "unit": "ns/op",
            "extra": "1342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 33785,
            "unit": "B/op",
            "extra": "1342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6185,
            "unit": "ns/op\t    5432 B/op\t      71 allocs/op",
            "extra": "18217 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6185,
            "unit": "ns/op",
            "extra": "18217 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5432,
            "unit": "B/op",
            "extra": "18217 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "18217 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 133604,
            "unit": "ns/op\t   60217 B/op\t     409 allocs/op",
            "extra": "981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 133604,
            "unit": "ns/op",
            "extra": "981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 60217,
            "unit": "B/op",
            "extra": "981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37251,
            "unit": "ns/op\t   32312 B/op\t     407 allocs/op",
            "extra": "2752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37251,
            "unit": "ns/op",
            "extra": "2752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32312,
            "unit": "B/op",
            "extra": "2752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2752 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 9675,
            "unit": "ns/op\t 846.69 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11661 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 9675,
            "unit": "ns/op",
            "extra": "11661 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 846.69,
            "unit": "MB/s",
            "extra": "11661 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11661 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11661 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 159.6,
            "unit": "ns/op\t1604.07 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "742338 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 159.6,
            "unit": "ns/op",
            "extra": "742338 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 1604.07,
            "unit": "MB/s",
            "extra": "742338 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "742338 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "742338 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal)",
            "value": 369.9,
            "unit": "ns/op\t 692.01 MB/s\t      40 B/op\t       3 allocs/op",
            "extra": "373206 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 369.9,
            "unit": "ns/op",
            "extra": "373206 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - MB/s",
            "value": 692.01,
            "unit": "MB/s",
            "extra": "373206 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "373206 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "373206 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal)",
            "value": 41629313,
            "unit": "ns/op\t 5992354 B/op\t   83380 allocs/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 41629313,
            "unit": "ns/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 5992354,
            "unit": "B/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 83380,
            "unit": "allocs/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/thermos)",
            "value": 22.74,
            "unit": "ns/op",
            "extra": "5295698 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/thermos)",
            "value": 55.43,
            "unit": "ns/op",
            "extra": "2093570 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/thermos)",
            "value": 20.06,
            "unit": "ns/op",
            "extra": "5793097 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/thermos)",
            "value": 22.38,
            "unit": "ns/op",
            "extra": "5284088 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/thermos)",
            "value": 20324238,
            "unit": "ns/op",
            "extra": "5 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/thermos)",
            "value": 83.01,
            "unit": "ns/op",
            "extra": "1443421 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/thermos)",
            "value": 28726,
            "unit": "ns/op",
            "extra": "3948 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 53.8,
            "unit": "ns/op",
            "extra": "2234322 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 182.6,
            "unit": "ns/op",
            "extra": "744853 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 212.1,
            "unit": "ns/op",
            "extra": "627694 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 231.8,
            "unit": "ns/op",
            "extra": "592924 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 39.5,
            "unit": "ns/op",
            "extra": "2987430 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 131.9,
            "unit": "ns/op",
            "extra": "962101 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 147,
            "unit": "ns/op",
            "extra": "788325 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 178.7,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 58.99,
            "unit": "ns/op",
            "extra": "1974638 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 186.5,
            "unit": "ns/op",
            "extra": "691366 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 214,
            "unit": "ns/op",
            "extra": "627254 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 230.7,
            "unit": "ns/op",
            "extra": "600872 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 28.08,
            "unit": "ns/op",
            "extra": "4245084 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 299.9,
            "unit": "ns/op",
            "extra": "394028 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 5474,
            "unit": "ns/op",
            "extra": "21769 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8258,
            "unit": "ns/op",
            "extra": "13977 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 46.91,
            "unit": "ns/op",
            "extra": "2522869 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 88.07,
            "unit": "ns/op",
            "extra": "1370804 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 86.11,
            "unit": "ns/op",
            "extra": "1403478 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 85.17,
            "unit": "ns/op",
            "extra": "1407817 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 11.14,
            "unit": "ns/op",
            "extra": "10776656 times\n4 procs"
          }
        ]
      }
    ]
  }
}