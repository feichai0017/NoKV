window.BENCHMARK_DATA = {
  "lastUpdate": 1777227483831,
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
          "id": "79fc12f83642e7e7dd558592901a3992bab02025",
          "message": "perf(lsm): remaining write/read/compaction optimizations + vlog race fix (#161)\n\n* perf(lsm): add negative cache and ttl cleanup\n\n* docs(lsm): split sharded WAL+memtable design note + cleanup + micro-bench\n\nDocumentation\n─────────────\n- Delete docs/notes/2026-04-26-lsm-engine-throughput-roadmap.md. Roadmap\n  format mixes landed work, code pointers, and TODO wishlist; drifts as\n  items ship and conflates decision record with planning. Each PR-sized\n  change should carry its own design note instead.\n- Add docs/notes/2026-04-27-sharded-wal-memtable.md as the focused\n  design note for the sharding work that landed across PRs #158/#159/\n  #160. Documents the why (wal.Manager.mu profile ceiling), the what\n  (lsmShard struct, end-to-end shardID flow, per-key affinity routing,\n  burst coalesce, per-shard retention, WAL fsync/rotation fence,\n  cross-shard hint cache), the SetBatch atomicity invariant proof,\n  the YCSB and industry baseline numbers, the SPSC rejection record,\n  and the known trade-offs.\n- Update 5 in-code doc references (options.go, db.go, engine/lsm/lsm.go\n  ×2, engine/lsm/shard.go) from the deleted roadmap to the new note.\n\nCleanup\n───────\n- Inline lsm.primaryShard() into the test sites that used it. The\n  helper was dead in production code (write paths route via per-key\n  affinity); tests now reach into lsm.shards[0] directly.\n- Remove primaryShard() from engine/lsm/shard.go.\n- Drop 4 vague TODO comments in engine/lsm/table_builder.go that were\n  aspirational wishlist (encrypt blocks, \"build index more efficiently\",\n  \"estimate sst size\") with no corresponding tracker entries.\n\nMicro-benchmarks\n────────────────\nAdd engine/lsm/sharded_bench_test.go covering hot-path optimizations\nthat previously only had end-to-end YCSB numbers:\n\n- BenchmarkShardedSetBatchByShardCount (N=1/2/4/8) — peak at N=4\n  (294 MB/s, +22% vs N=1); N=8 saturates and matches N=1 due to\n  per-shard WAL goroutine contention with peers.\n- BenchmarkShardedGetWithHint (warm vs cold hint table at N=1/4/8)\n  — quantifies PR #160 benefit: 2.3× speedup at N=4 (417ns vs 972ns),\n  3.2× speedup at N=8 (484ns vs 1540ns); N=1 has no benefit as\n  expected.\n- BenchmarkShardedCrossShardMVCCMerge (N=1/2/4/8) — confirms O(N)\n  cost of the cross-shard memtable walk + max-version selection\n  (~165ns per shard added).\n- BenchmarkShardedNegativeCache (warm vs cold) — confirms negative\n  cache hit short-circuits the full level walk: 151ns warm vs 738µs\n  cold, ~4900× speedup on the not-found path.\n\nAll benches use openShardedBenchLSM helper that mirrors\nopenShardHintTestLSM but takes *testing.B and runs with a 64 MiB\nmemtable so iterations don't trigger flush mid-loop.\n\nVerification\n────────────\ngo vet ./... clean. go test ./engine/lsm/... -count=1 -race green.\n\n* perf(lsm): unblock parallel L0 compaction across all NumCompactors\n\nBench logs at 30M+ data showed the persistent loop:\n  worker=0 level=0 / write slowdown / write stop / worker=0 level=0\nWhile NumCompactors=4, only worker 0 ever drained L0. Code trace found\ntwo hard-coded compaction-suppression points on the L0→L0 fallback path:\n\n1) fillTablesL0ToL0 had `if cd.compactorId != 0 { return false }` —\n   peer workers reached the L0→L0 fallback and were rejected.\n2) PlanForL0ToL0 returned ThisRange/NextRange = InfRange and\n   AddRangeWithTables(0, InfRange, ...) registered an InfRange entry\n   on level 0 — every subsequent PlanForL0ToLbase saw\n   state.Overlaps(0, anything)==true and bailed.\n\nTogether, these caused: when L1+ are also under compaction (state-claimed\nby other workers), L0→Lbase fails, fallback to L0→L0 only worker 0 can\ndo, and worker 0's InfRange blocks any peer L0→Lbase. Result: 1 worker\non L0, 3 idle, 5-second ticker between cycles.\n\nFix: introduce StateEntry.IntraLevel for within-level compactions\n(L0→L0). IntraLevel entries claim by table id only and skip range\nregistration. CompareAndAdd / Delete handle the IntraLevel branch\nwithout touching levelState ranges. PlanForL0ToL0 now:\n  - caps each call at l0ToL0MaxTablesPerWorker (8, matches RocksDB\n    max_subcompactions default) so peer workers find unclaimed tables\n  - uses empty KeyRange (InfRange dropped)\n  - sets IntraLevel = true\nfillTablesL0ToL0 drops the worker-0-only gate and uses\nstate.CompareAndAdd. PlanForL0ToLbase now skips state.HasTable claims\ninside its contiguous-overlap scan so it can find a non-conflicting\nL0 group when L0→L0 holds the prefix.\n\nAlso: bump addSplits cap from hard-coded 5 to max(NumCompactors, 5).\nA compaction with N=8 NumCompactors can now fan out to 8 builder\ngoroutines per task, not 5.\n\nTests (1:1 with source files):\n  - planner_test.go: TestPlanForL0ToL0AllowsConcurrentWorkers,\n    TestPlanForL0ToL0DoesNotBlockL0ToLbase\n  - compaction_test.go: TestStateIntraLevelEntryDeletesCleanly\n\nBenches:\n  - planner_bench_test.go (new): BenchmarkPlanForL0ToL0Concurrent\n    (workers 1/2/4/8), BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure\n  - lsm_bench_test.go (gained from sharded_bench_test.go redistribution):\n    BenchmarkShardedSetBatchByShardCount,\n    BenchmarkShardedCrossShardMVCCMerge\n  - shard_hint_bench_test.go (new): BenchmarkShardedGetWithHint\n  - negative_cache_bench_test.go (new): BenchmarkShardedNegativeCache\n  - sharded_bench_test.go deleted; its content was redistributed to\n    the right per-source-file bench homes (1:1 file mapping)\n\nDesign note: docs/notes/2026-04-27-parallel-l0-compaction.md captures\nthe why/what/decision-log.\n\nVerification:\n  go test -race ./engine/lsm/ -count=1  green\n  PlanForL0ToL0Concurrent at 1/2/4/8 workers + L0→Lbase under L0→L0\n  pressure scale linearly (state mutex contention up, but multiple\n  workers actually return ok=true now — pre-fix only worker 0 did).\n\n* fix(vlog): use monotonic CAS on LogFile.Write to prevent size rewind\n\nvlog/manager.reserve() hands out non-overlapping offset ranges under\nm.filesLock, then releases the lock before the caller acquires\nstore.Lock to publish the actual Write. The reserve order and Write\norder therefore decouple: when a writer with a larger reservation\nfinishes first (lf.size = larger end), a later writer with a smaller\nreservation would Store(smaller end) and rewind lf.size below an\nalready-published value pointer, producing spurious EOF on Read.\n\nReproduced reliably with 200k records + 3KB value + conc=16 (failed\nin ~0.79s before this fix). 1M YCSB + 3KB value (vlog path saturated)\nnow passes all six workloads with NoKV first across the board.\n\nSwitch LogFile.Write to a monotonic CAS so lf.size only ever advances.\nNote that this only guarantees lf.size never rewinds; holes within the\nhigh-water mark are still possible (reserved-but-not-yet-written\nranges). The \"reader never observes a hole\" property is upheld by an\nupper-layer invariant (V1): db.vlog.write must complete before\napplyRequests publishes the pointer to the LSM, so by the time a\nreader sees a ValuePtr, the writing batch's Write has finished and\nlf.size covers ptr.offset+ptr.len.\n\nTests added:\n- engine/file/vlog_test.go::TestLogFileWriteSizeMonotonicOutOfOrder\n  drives the reverse-order serialized Write pattern. Without the fix,\n  asserts lfsz < high-water and fails.\n- engine/vlog/manager_test.go::TestManagerConcurrentAppendReadAfterWrite\n  fan-out 16 workers x 64 batches x 8 entries with immediate Read of\n  every published pointer. Covers the upper-layer publish-discipline\n  invariant in addition to the size monotonicity.\n\nTwo existing TestDoneWriting* tests had Equal(int64(len(data)),\nlf.Size()) assertions that depended on the buggy shrinking behavior;\nrelaxed them to GreaterOrEqual since lf.size is no longer the logical\nwritten byte count but a high-water mark over the pre-allocated mmap\nregion.\n\n* style: run go fmt",
          "timestamp": "2026-04-27T04:14:54+10:00",
          "tree_id": "77f7affcc36a621e40880307e1b943150d13d477",
          "url": "https://github.com/feichai0017/NoKV/commit/79fc12f83642e7e7dd558592901a3992bab02025"
        },
        "date": 1777227481217,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 13012,
            "unit": "ns/op\t   2.46 MB/s\t     467 B/op\t      12 allocs/op",
            "extra": "8175 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 13012,
            "unit": "ns/op",
            "extra": "8175 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 2.46,
            "unit": "MB/s",
            "extra": "8175 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 467,
            "unit": "B/op",
            "extra": "8175 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 12,
            "unit": "allocs/op",
            "extra": "8175 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 16279,
            "unit": "ns/op\t 251.61 MB/s\t     441 B/op\t      19 allocs/op",
            "extra": "7468 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 16279,
            "unit": "ns/op",
            "extra": "7468 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 251.61,
            "unit": "MB/s",
            "extra": "7468 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 441,
            "unit": "B/op",
            "extra": "7468 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 19,
            "unit": "allocs/op",
            "extra": "7468 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 984.8,
            "unit": "ns/op\t  64.99 MB/s\t     267 B/op\t       5 allocs/op",
            "extra": "105316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 984.8,
            "unit": "ns/op",
            "extra": "105316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 64.99,
            "unit": "MB/s",
            "extra": "105316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 267,
            "unit": "B/op",
            "extra": "105316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "105316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 3246,
            "unit": "ns/op\t1261.68 MB/s\t    9163 B/op\t       7 allocs/op",
            "extra": "34944 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 3246,
            "unit": "ns/op",
            "extra": "34944 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1261.68,
            "unit": "MB/s",
            "extra": "34944 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 9163,
            "unit": "B/op",
            "extra": "34944 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "34944 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 133813,
            "unit": "ns/op\t 122.44 MB/s\t   48778 B/op\t     231 allocs/op",
            "extra": "829 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 133813,
            "unit": "ns/op",
            "extra": "829 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 122.44,
            "unit": "MB/s",
            "extra": "829 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 48778,
            "unit": "B/op",
            "extra": "829 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 231,
            "unit": "allocs/op",
            "extra": "829 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 555339,
            "unit": "ns/op\t  29.50 MB/s\t   49543 B/op\t     259 allocs/op",
            "extra": "202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 555339,
            "unit": "ns/op",
            "extra": "202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 29.5,
            "unit": "MB/s",
            "extra": "202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 49543,
            "unit": "B/op",
            "extra": "202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 259,
            "unit": "allocs/op",
            "extra": "202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 581372,
            "unit": "ns/op\t  28.18 MB/s\t   49692 B/op\t     262 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 581372,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 28.18,
            "unit": "MB/s",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 49692,
            "unit": "B/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 262,
            "unit": "allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 5825940,
            "unit": "ns/op\t      47 B/op\t       0 allocs/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 5825940,
            "unit": "ns/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 47,
            "unit": "B/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 1373,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "92331 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 1373,
            "unit": "ns/op",
            "extra": "92331 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "92331 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "92331 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 534.1,
            "unit": "ns/op\t 119.84 MB/s\t    1538 B/op\t       0 allocs/op",
            "extra": "189052 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 534.1,
            "unit": "ns/op",
            "extra": "189052 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 119.84,
            "unit": "MB/s",
            "extra": "189052 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 1538,
            "unit": "B/op",
            "extra": "189052 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "189052 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 152.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "778357 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 152.4,
            "unit": "ns/op",
            "extra": "778357 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "778357 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "778357 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 134.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "899533 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 134.5,
            "unit": "ns/op",
            "extra": "899533 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "899533 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "899533 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 57.14,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2113848 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 57.14,
            "unit": "ns/op",
            "extra": "2113848 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2113848 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2113848 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 1021,
            "unit": "ns/op\t  62.70 MB/s\t     158 B/op\t       1 allocs/op",
            "extra": "173646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 1021,
            "unit": "ns/op",
            "extra": "173646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 62.7,
            "unit": "MB/s",
            "extra": "173646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 158,
            "unit": "B/op",
            "extra": "173646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "173646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 468,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "284413 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 468,
            "unit": "ns/op",
            "extra": "284413 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "284413 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "284413 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 384.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "288216 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 384.5,
            "unit": "ns/op",
            "extra": "288216 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "288216 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "288216 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 44.64,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2629310 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 44.64,
            "unit": "ns/op",
            "extra": "2629310 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2629310 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2629310 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index)",
            "value": 425.9,
            "unit": "ns/op\t 150.28 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "370591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 425.9,
            "unit": "ns/op",
            "extra": "370591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 150.28,
            "unit": "MB/s",
            "extra": "370591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "370591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "370591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index)",
            "value": 1152,
            "unit": "ns/op\t  55.53 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "387940 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 1152,
            "unit": "ns/op",
            "extra": "387940 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 55.53,
            "unit": "MB/s",
            "extra": "387940 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "387940 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "387940 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerNilCharge (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2.205,
            "unit": "ns/op",
            "extra": "47612793 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerChargeFromBucket (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41.78,
            "unit": "ns/op",
            "extra": "3317846 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerChargeWithMockSleep (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 42.31,
            "unit": "ns/op",
            "extra": "2828403 times\n4 procs"
          },
          {
            "name": "BenchmarkCompactionPacerStats (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 0.625,
            "unit": "ns/op",
            "extra": "192108865 times\n4 procs"
          },
          {
            "name": "BenchmarkL0SelectTablesForKeyLinear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1042,
            "unit": "ns/op",
            "extra": "109786 times\n4 procs"
          },
          {
            "name": "BenchmarkL0SelectTablesForKeyViaSublevels (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 101.4,
            "unit": "ns/op",
            "extra": "1052038 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 36218,
            "unit": "ns/op\t 226.19 MB/s\t   34911 B/op\t     206 allocs/op",
            "extra": "3258 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 36218,
            "unit": "ns/op",
            "extra": "3258 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 226.19,
            "unit": "MB/s",
            "extra": "3258 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34911,
            "unit": "B/op",
            "extra": "3258 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 206,
            "unit": "allocs/op",
            "extra": "3258 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 46599,
            "unit": "ns/op\t 175.80 MB/s\t   34908 B/op\t     206 allocs/op",
            "extra": "2376 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 46599,
            "unit": "ns/op",
            "extra": "2376 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 175.8,
            "unit": "MB/s",
            "extra": "2376 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34908,
            "unit": "B/op",
            "extra": "2376 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 206,
            "unit": "allocs/op",
            "extra": "2376 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 7382730,
            "unit": "ns/op\t75968602 B/op\t     507 allocs/op",
            "extra": "27 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 7382730,
            "unit": "ns/op",
            "extra": "27 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 75968602,
            "unit": "B/op",
            "extra": "27 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 507,
            "unit": "allocs/op",
            "extra": "27 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 7474622,
            "unit": "ns/op\t75988576 B/op\t     515 allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 7474622,
            "unit": "ns/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 75988576,
            "unit": "B/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 515,
            "unit": "allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 477.1,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "214140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 477.1,
            "unit": "ns/op",
            "extra": "214140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "214140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "214140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 703.6,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "151927 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 703.6,
            "unit": "ns/op",
            "extra": "151927 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "151927 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "151927 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 158.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "681570 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 158.2,
            "unit": "ns/op",
            "extra": "681570 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "681570 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "681570 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 339.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "368232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 339.7,
            "unit": "ns/op",
            "extra": "368232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "368232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "368232 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 35543,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 35543,
            "unit": "ns/op",
            "extra": "3350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 89.37,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1340731 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 89.37,
            "unit": "ns/op",
            "extra": "1340731 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1340731 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1340731 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 17920,
            "unit": "ns/op\t     288 B/op\t       5 allocs/op",
            "extra": "6162 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 17920,
            "unit": "ns/op",
            "extra": "6162 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 288,
            "unit": "B/op",
            "extra": "6162 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6162 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 542,
            "unit": "ns/op\t     280 B/op\t       4 allocs/op",
            "extra": "200264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 542,
            "unit": "ns/op",
            "extra": "200264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 280,
            "unit": "B/op",
            "extra": "200264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "200264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18269,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6457 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18269,
            "unit": "ns/op",
            "extra": "6457 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6457 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6457 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 506.2,
            "unit": "ns/op\t     248 B/op\t       3 allocs/op",
            "extra": "221691 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 506.2,
            "unit": "ns/op",
            "extra": "221691 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "221691 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "221691 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 56805,
            "unit": "ns/op\t   19080 B/op\t      11 allocs/op",
            "extra": "1928 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 56805,
            "unit": "ns/op",
            "extra": "1928 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 19080,
            "unit": "B/op",
            "extra": "1928 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1928 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 928.8,
            "unit": "ns/op\t     656 B/op\t      11 allocs/op",
            "extra": "118857 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 928.8,
            "unit": "ns/op",
            "extra": "118857 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 656,
            "unit": "B/op",
            "extra": "118857 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "118857 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 62228,
            "unit": "ns/op\t   22448 B/op\t      54 allocs/op",
            "extra": "1975 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 62228,
            "unit": "ns/op",
            "extra": "1975 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 22448,
            "unit": "B/op",
            "extra": "1975 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1975 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5070,
            "unit": "ns/op\t    4080 B/op\t      54 allocs/op",
            "extra": "23559 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5070,
            "unit": "ns/op",
            "extra": "23559 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4080,
            "unit": "B/op",
            "extra": "23559 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "23559 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 92027,
            "unit": "ns/op\t   48880 B/op\t     390 allocs/op",
            "extra": "1206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 92027,
            "unit": "ns/op",
            "extra": "1206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 48880,
            "unit": "B/op",
            "extra": "1206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1206 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 36979,
            "unit": "ns/op\t   30960 B/op\t     390 allocs/op",
            "extra": "3422 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 36979,
            "unit": "ns/op",
            "extra": "3422 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30960,
            "unit": "B/op",
            "extra": "3422 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3422 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2052,
            "unit": "ns/op\t    1832 B/op\t      21 allocs/op",
            "extra": "57576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2052,
            "unit": "ns/op",
            "extra": "57576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1832,
            "unit": "B/op",
            "extra": "57576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "57576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1840,
            "unit": "ns/op\t    1344 B/op\t      16 allocs/op",
            "extra": "56684 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1840,
            "unit": "ns/op",
            "extra": "56684 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1344,
            "unit": "B/op",
            "extra": "56684 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "56684 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10647,
            "unit": "ns/op\t    8776 B/op\t     105 allocs/op",
            "extra": "10676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10647,
            "unit": "ns/op",
            "extra": "10676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8776,
            "unit": "B/op",
            "extra": "10676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "10676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 11215,
            "unit": "ns/op\t    8288 B/op\t     100 allocs/op",
            "extra": "9765 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 11215,
            "unit": "ns/op",
            "extra": "9765 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8288,
            "unit": "B/op",
            "extra": "9765 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "9765 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 40087,
            "unit": "ns/op\t   32584 B/op\t     393 allocs/op",
            "extra": "2904 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 40087,
            "unit": "ns/op",
            "extra": "2904 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32584,
            "unit": "B/op",
            "extra": "2904 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "2904 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 44001,
            "unit": "ns/op\t   32096 B/op\t     388 allocs/op",
            "extra": "2691 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 44001,
            "unit": "ns/op",
            "extra": "2691 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32096,
            "unit": "B/op",
            "extra": "2691 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2691 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 46094,
            "unit": "ns/op\t     344 B/op\t       9 allocs/op",
            "extra": "2511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 46094,
            "unit": "ns/op",
            "extra": "2511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "2511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1098,
            "unit": "ns/op\t     320 B/op\t       6 allocs/op",
            "extra": "102709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1098,
            "unit": "ns/op",
            "extra": "102709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 320,
            "unit": "B/op",
            "extra": "102709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "102709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 80.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1493662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 80.3,
            "unit": "ns/op",
            "extra": "1493662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1493662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1493662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 79.83,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1496846 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 79.83,
            "unit": "ns/op",
            "extra": "1496846 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1496846 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1496846 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21998,
            "unit": "ns/op\t    5144 B/op\t       5 allocs/op",
            "extra": "5008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21998,
            "unit": "ns/op",
            "extra": "5008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5144,
            "unit": "B/op",
            "extra": "5008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "5008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 22092,
            "unit": "ns/op\t    5144 B/op\t       5 allocs/op",
            "extra": "5289 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 22092,
            "unit": "ns/op",
            "extra": "5289 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5144,
            "unit": "B/op",
            "extra": "5289 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "5289 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 20899,
            "unit": "ns/op\t    4864 B/op\t       1 allocs/op",
            "extra": "5388 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 20899,
            "unit": "ns/op",
            "extra": "5388 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4864,
            "unit": "B/op",
            "extra": "5388 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5388 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21318,
            "unit": "ns/op\t    4864 B/op\t       1 allocs/op",
            "extra": "5250 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21318,
            "unit": "ns/op",
            "extra": "5250 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4864,
            "unit": "B/op",
            "extra": "5250 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "5250 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18646,
            "unit": "ns/op\t     252 B/op\t       6 allocs/op",
            "extra": "6594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18646,
            "unit": "ns/op",
            "extra": "6594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 252,
            "unit": "B/op",
            "extra": "6594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "6594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 798.6,
            "unit": "ns/op\t     240 B/op\t       4 allocs/op",
            "extra": "140359 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 798.6,
            "unit": "ns/op",
            "extra": "140359 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "140359 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "140359 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 24566,
            "unit": "ns/op\t     302 B/op\t       7 allocs/op",
            "extra": "4874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 24566,
            "unit": "ns/op",
            "extra": "4874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 302,
            "unit": "B/op",
            "extra": "4874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 958.6,
            "unit": "ns/op\t     288 B/op\t       5 allocs/op",
            "extra": "114130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 958.6,
            "unit": "ns/op",
            "extra": "114130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 288,
            "unit": "B/op",
            "extra": "114130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "114130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 89735,
            "unit": "ns/op\t   30416 B/op\t      30 allocs/op",
            "extra": "1324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 89735,
            "unit": "ns/op",
            "extra": "1324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30416,
            "unit": "B/op",
            "extra": "1324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2537,
            "unit": "ns/op\t    2008 B/op\t      28 allocs/op",
            "extra": "50060 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2537,
            "unit": "ns/op",
            "extra": "50060 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 2008,
            "unit": "B/op",
            "extra": "50060 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "50060 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 92878,
            "unit": "ns/op\t   33784 B/op\t      73 allocs/op",
            "extra": "1209 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 92878,
            "unit": "ns/op",
            "extra": "1209 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 33784,
            "unit": "B/op",
            "extra": "1209 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1209 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6687,
            "unit": "ns/op\t    5432 B/op\t      71 allocs/op",
            "extra": "17678 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6687,
            "unit": "ns/op",
            "extra": "17678 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5432,
            "unit": "B/op",
            "extra": "17678 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "17678 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 126588,
            "unit": "ns/op\t   60216 B/op\t     409 allocs/op",
            "extra": "885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 126588,
            "unit": "ns/op",
            "extra": "885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 60216,
            "unit": "B/op",
            "extra": "885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 43684,
            "unit": "ns/op\t   32312 B/op\t     407 allocs/op",
            "extra": "2683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 43684,
            "unit": "ns/op",
            "extra": "2683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 32312,
            "unit": "B/op",
            "extra": "2683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2683 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_1 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 33384,
            "unit": "ns/op\t 245.39 MB/s\t   25058 B/op\t      76 allocs/op",
            "extra": "3645 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 33384,
            "unit": "ns/op",
            "extra": "3645 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 245.39,
            "unit": "MB/s",
            "extra": "3645 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25058,
            "unit": "B/op",
            "extra": "3645 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 76,
            "unit": "allocs/op",
            "extra": "3645 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_2 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37661,
            "unit": "ns/op\t 217.52 MB/s\t   25054 B/op\t      76 allocs/op",
            "extra": "3498 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37661,
            "unit": "ns/op",
            "extra": "3498 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 217.52,
            "unit": "MB/s",
            "extra": "3498 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25054,
            "unit": "B/op",
            "extra": "3498 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 76,
            "unit": "allocs/op",
            "extra": "3498 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_4 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 35816,
            "unit": "ns/op\t 228.72 MB/s\t   25056 B/op\t      76 allocs/op",
            "extra": "3027 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 35816,
            "unit": "ns/op",
            "extra": "3027 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 228.72,
            "unit": "MB/s",
            "extra": "3027 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25056,
            "unit": "B/op",
            "extra": "3027 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 76,
            "unit": "allocs/op",
            "extra": "3027 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_8 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 38390,
            "unit": "ns/op\t 213.39 MB/s\t   25050 B/op\t      76 allocs/op",
            "extra": "2985 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 38390,
            "unit": "ns/op",
            "extra": "2985 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 213.39,
            "unit": "MB/s",
            "extra": "2985 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25050,
            "unit": "B/op",
            "extra": "2985 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedSetBatchByShardCount/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 76,
            "unit": "allocs/op",
            "extra": "2985 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_1 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 362.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "328646 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 362.3,
            "unit": "ns/op",
            "extra": "328646 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "328646 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_1 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "328646 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_2 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 567.2,
            "unit": "ns/op\t      56 B/op\t       3 allocs/op",
            "extra": "208291 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 567.2,
            "unit": "ns/op",
            "extra": "208291 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 56,
            "unit": "B/op",
            "extra": "208291 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_2 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "208291 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_4 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 873.7,
            "unit": "ns/op\t      88 B/op\t       4 allocs/op",
            "extra": "134976 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 873.7,
            "unit": "ns/op",
            "extra": "134976 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "134976 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_4 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "134976 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_8 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1563,
            "unit": "ns/op\t     152 B/op\t       5 allocs/op",
            "extra": "82270 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1563,
            "unit": "ns/op",
            "extra": "82270 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 152,
            "unit": "B/op",
            "extra": "82270 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedCrossShardMVCCMerge/shards_8 (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "82270 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/warm_neg (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 288.2,
            "unit": "ns/op\t      72 B/op\t       1 allocs/op",
            "extra": "424998 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/warm_neg (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 288.2,
            "unit": "ns/op",
            "extra": "424998 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/warm_neg (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 72,
            "unit": "B/op",
            "extra": "424998 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/warm_neg (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "424998 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/cold_neg (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1558415,
            "unit": "ns/op\t     191 B/op\t       4 allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/cold_neg (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1558415,
            "unit": "ns/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/cold_neg (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 191,
            "unit": "B/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedNegativeCache/cold_neg (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkPlanForL0ToL0Concurrent/workers_1 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2899,
            "unit": "ns/op",
            "extra": "39411 times\n4 procs"
          },
          {
            "name": "BenchmarkPlanForL0ToL0Concurrent/workers_2 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 7396,
            "unit": "ns/op",
            "extra": "16226 times\n4 procs"
          },
          {
            "name": "BenchmarkPlanForL0ToL0Concurrent/workers_4 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 14782,
            "unit": "ns/op",
            "extra": "6892 times\n4 procs"
          },
          {
            "name": "BenchmarkPlanForL0ToL0Concurrent/workers_8 (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 33360,
            "unit": "ns/op",
            "extra": "3318 times\n4 procs"
          },
          {
            "name": "BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 15396,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/warm_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 504.2,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "236338 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 504.2,
            "unit": "ns/op",
            "extra": "236338 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "236338 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "236338 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/cold_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 482.1,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "242011 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 482.1,
            "unit": "ns/op",
            "extra": "242011 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "242011 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_1/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "242011 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/warm_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 525.5,
            "unit": "ns/op\t      41 B/op\t       2 allocs/op",
            "extra": "229192 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 525.5,
            "unit": "ns/op",
            "extra": "229192 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 41,
            "unit": "B/op",
            "extra": "229192 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "229192 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/cold_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1521,
            "unit": "ns/op\t      88 B/op\t       4 allocs/op",
            "extra": "75223 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1521,
            "unit": "ns/op",
            "extra": "75223 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 88,
            "unit": "B/op",
            "extra": "75223 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_4/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "75223 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/warm_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 683.6,
            "unit": "ns/op\t      43 B/op\t       2 allocs/op",
            "extra": "178770 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 683.6,
            "unit": "ns/op",
            "extra": "178770 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 43,
            "unit": "B/op",
            "extra": "178770 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/warm_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "178770 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/cold_hint (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2528,
            "unit": "ns/op\t     152 B/op\t       5 allocs/op",
            "extra": "43959 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2528,
            "unit": "ns/op",
            "extra": "43959 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 152,
            "unit": "B/op",
            "extra": "43959 times\n4 procs"
          },
          {
            "name": "BenchmarkShardedGetWithHint/shards_8/cold_hint (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "43959 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 11016,
            "unit": "ns/op\t 743.64 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "10246 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 11016,
            "unit": "ns/op",
            "extra": "10246 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 743.64,
            "unit": "MB/s",
            "extra": "10246 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "10246 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "10246 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 170.9,
            "unit": "ns/op\t1498.09 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "694820 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 170.9,
            "unit": "ns/op",
            "extra": "694820 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 1498.09,
            "unit": "MB/s",
            "extra": "694820 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "694820 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "694820 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal)",
            "value": 414.4,
            "unit": "ns/op\t 617.80 MB/s\t      40 B/op\t       3 allocs/op",
            "extra": "262180 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 414.4,
            "unit": "ns/op",
            "extra": "262180 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - MB/s",
            "value": 617.8,
            "unit": "MB/s",
            "extra": "262180 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "262180 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "262180 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal)",
            "value": 36335631,
            "unit": "ns/op\t 5991722 B/op\t   83379 allocs/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 36335631,
            "unit": "ns/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 5991722,
            "unit": "B/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 83379,
            "unit": "allocs/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/thermos)",
            "value": 23.98,
            "unit": "ns/op",
            "extra": "4869496 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/thermos)",
            "value": 56.82,
            "unit": "ns/op",
            "extra": "2176140 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/thermos)",
            "value": 20.53,
            "unit": "ns/op",
            "extra": "5884149 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/thermos)",
            "value": 16.56,
            "unit": "ns/op",
            "extra": "7258761 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/thermos)",
            "value": 22876147,
            "unit": "ns/op",
            "extra": "5 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/thermos)",
            "value": 75.96,
            "unit": "ns/op",
            "extra": "1575476 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/thermos)",
            "value": 56822,
            "unit": "ns/op",
            "extra": "2031 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 53.05,
            "unit": "ns/op",
            "extra": "2380896 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 148,
            "unit": "ns/op",
            "extra": "807346 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 167,
            "unit": "ns/op",
            "extra": "834200 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 188.8,
            "unit": "ns/op",
            "extra": "677695 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 41.68,
            "unit": "ns/op",
            "extra": "2886068 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 118.9,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 132.4,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 159.8,
            "unit": "ns/op",
            "extra": "871740 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 60.69,
            "unit": "ns/op",
            "extra": "1991824 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 169.2,
            "unit": "ns/op",
            "extra": "748825 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 177.9,
            "unit": "ns/op",
            "extra": "770612 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 200.2,
            "unit": "ns/op",
            "extra": "873640 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.1,
            "unit": "ns/op",
            "extra": "4614633 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 266.8,
            "unit": "ns/op",
            "extra": "422437 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 5155,
            "unit": "ns/op",
            "extra": "24834 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8412,
            "unit": "ns/op",
            "extra": "14217 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 51.43,
            "unit": "ns/op",
            "extra": "2324710 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 74.61,
            "unit": "ns/op",
            "extra": "1643344 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 72.4,
            "unit": "ns/op",
            "extra": "1676442 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 73,
            "unit": "ns/op",
            "extra": "1661383 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 9.96,
            "unit": "ns/op",
            "extra": "11997342 times\n4 procs"
          }
        ]
      }
    ]
  }
}