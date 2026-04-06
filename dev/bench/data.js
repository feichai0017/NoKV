window.BENCHMARK_DATA = {
  "lastUpdate": 1775484491082,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "eric_song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "songguocheng348@gmail.com",
            "name": "eric_song",
            "username": "feichai0017"
          },
          "distinct": true,
          "id": "a40e381ccbd9c9e22af31eb5fbe74255e210bbb3",
          "message": "bench: compare art and skiplist memtables",
          "timestamp": "2026-04-06T22:04:53+08:00",
          "tree_id": "3e01e04fc40d4d21c1ac9c2ffa034191a376d354",
          "url": "https://github.com/feichai0017/NoKV/commit/a40e381ccbd9c9e22af31eb5fbe74255e210bbb3"
        },
        "date": 1775484489767,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 5361,
            "unit": "ns/op\t   5.97 MB/s\t     425 B/op\t      13 allocs/op",
            "extra": "20319 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 5361,
            "unit": "ns/op",
            "extra": "20319 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 5.97,
            "unit": "MB/s",
            "extra": "20319 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 425,
            "unit": "B/op",
            "extra": "20319 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "20319 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 19533,
            "unit": "ns/op\t 209.70 MB/s\t     402 B/op\t      20 allocs/op",
            "extra": "5400 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 19533,
            "unit": "ns/op",
            "extra": "5400 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 209.7,
            "unit": "MB/s",
            "extra": "5400 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 402,
            "unit": "B/op",
            "extra": "5400 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "5400 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1035,
            "unit": "ns/op\t  61.82 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "120728 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1035,
            "unit": "ns/op",
            "extra": "120728 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 61.82,
            "unit": "MB/s",
            "extra": "120728 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "120728 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "120728 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 2660,
            "unit": "ns/op\t1539.99 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "45255 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 2660,
            "unit": "ns/op",
            "extra": "45255 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1539.99,
            "unit": "MB/s",
            "extra": "45255 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "45255 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "45255 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 65169,
            "unit": "ns/op\t 251.41 MB/s\t  171568 B/op\t     147 allocs/op",
            "extra": "2145 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 65169,
            "unit": "ns/op",
            "extra": "2145 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 251.41,
            "unit": "MB/s",
            "extra": "2145 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 171568,
            "unit": "B/op",
            "extra": "2145 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "2145 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 511729,
            "unit": "ns/op\t  32.02 MB/s\t   46541 B/op\t     147 allocs/op",
            "extra": "214 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 511729,
            "unit": "ns/op",
            "extra": "214 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 32.02,
            "unit": "MB/s",
            "extra": "214 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46541,
            "unit": "B/op",
            "extra": "214 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "214 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 495916,
            "unit": "ns/op\t  33.04 MB/s\t   46664 B/op\t     149 allocs/op",
            "extra": "247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 495916,
            "unit": "ns/op",
            "extra": "247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 33.04,
            "unit": "MB/s",
            "extra": "247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46664,
            "unit": "B/op",
            "extra": "247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1646712,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "70 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1646712,
            "unit": "ns/op",
            "extra": "70 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "70 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "70 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 274.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "437548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 274.3,
            "unit": "ns/op",
            "extra": "437548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "437548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "437548 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.96,
            "unit": "ns/op",
            "extra": "5007118 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 84.91,
            "unit": "ns/op",
            "extra": "1449345 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 22.94,
            "unit": "ns/op",
            "extra": "5256140 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 16.55,
            "unit": "ns/op",
            "extra": "7398172 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 19537534,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 76.23,
            "unit": "ns/op",
            "extra": "1566615 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 55405,
            "unit": "ns/op",
            "extra": "1982 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 26458,
            "unit": "ns/op\t 309.63 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4227 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 26458,
            "unit": "ns/op",
            "extra": "4227 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 309.63,
            "unit": "MB/s",
            "extra": "4227 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4227 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4227 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 36408,
            "unit": "ns/op\t 225.00 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3793 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36408,
            "unit": "ns/op",
            "extra": "3793 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 225,
            "unit": "MB/s",
            "extra": "3793 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3793 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3793 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 6672404,
            "unit": "ns/op\t71593398 B/op\t     476 allocs/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6672404,
            "unit": "ns/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593398,
            "unit": "B/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 6573389,
            "unit": "ns/op\t71593335 B/op\t     474 allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6573389,
            "unit": "ns/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593335,
            "unit": "B/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 474,
            "unit": "allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 327,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "339963 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 327,
            "unit": "ns/op",
            "extra": "339963 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "339963 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "339963 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 543.2,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "219946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 543.2,
            "unit": "ns/op",
            "extra": "219946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "219946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "219946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 164.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "706142 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 164.6,
            "unit": "ns/op",
            "extra": "706142 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "706142 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "706142 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 352.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "346521 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 352.6,
            "unit": "ns/op",
            "extra": "346521 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "346521 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "346521 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 35572,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3374 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35572,
            "unit": "ns/op",
            "extra": "3374 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3374 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3374 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 92.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1297172 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 92.6,
            "unit": "ns/op",
            "extra": "1297172 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1297172 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1297172 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17643,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6219 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17643,
            "unit": "ns/op",
            "extra": "6219 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6219 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6219 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 507.5,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "212625 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 507.5,
            "unit": "ns/op",
            "extra": "212625 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "212625 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "212625 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17400,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6841 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17400,
            "unit": "ns/op",
            "extra": "6841 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6841 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6841 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 475.2,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "258744 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 475.2,
            "unit": "ns/op",
            "extra": "258744 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "258744 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "258744 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53882,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "1977 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53882,
            "unit": "ns/op",
            "extra": "1977 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "1977 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1977 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 879.4,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "123956 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 879.4,
            "unit": "ns/op",
            "extra": "123956 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "123956 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "123956 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 57523,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1947 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 57523,
            "unit": "ns/op",
            "extra": "1947 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1947 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1947 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4596,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "25948 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4596,
            "unit": "ns/op",
            "extra": "25948 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "25948 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "25948 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 87483,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1335 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 87483,
            "unit": "ns/op",
            "extra": "1335 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1335 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1335 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 32734,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 32734,
            "unit": "ns/op",
            "extra": "3528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3528 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 1781,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "65313 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1781,
            "unit": "ns/op",
            "extra": "65313 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "65313 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "65313 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1670,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "61741 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1670,
            "unit": "ns/op",
            "extra": "61741 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "61741 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "61741 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 9592,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "12223 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 9592,
            "unit": "ns/op",
            "extra": "12223 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "12223 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "12223 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 10334,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10334,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 6752,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 36724,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3352 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36724,
            "unit": "ns/op",
            "extra": "3352 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3352 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3352 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 39055,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "3135 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 39055,
            "unit": "ns/op",
            "extra": "3135 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "3135 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "3135 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 45475,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2569 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 45475,
            "unit": "ns/op",
            "extra": "2569 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2569 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2569 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 950.2,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "120654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 950.2,
            "unit": "ns/op",
            "extra": "120654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "120654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "120654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53442,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2236 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53442,
            "unit": "ns/op",
            "extra": "2236 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2236 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2236 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 520.7,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "226075 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 520.7,
            "unit": "ns/op",
            "extra": "226075 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "226075 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "226075 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 20038,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5937 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20038,
            "unit": "ns/op",
            "extra": "5937 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5937 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5937 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 20394,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5368 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20394,
            "unit": "ns/op",
            "extra": "5368 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5368 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5368 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 19888,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6256 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 19888,
            "unit": "ns/op",
            "extra": "6256 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6256 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6256 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 19670,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5917 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 19670,
            "unit": "ns/op",
            "extra": "5917 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5917 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5917 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 31183,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3795 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 31183,
            "unit": "ns/op",
            "extra": "3795 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3795 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3795 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 797.7,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "144368 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 797.7,
            "unit": "ns/op",
            "extra": "144368 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "144368 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "144368 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 28801,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "4072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 28801,
            "unit": "ns/op",
            "extra": "4072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "4072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 867.6,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "133970 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 867.6,
            "unit": "ns/op",
            "extra": "133970 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "133970 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "133970 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 83709,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1401 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 83709,
            "unit": "ns/op",
            "extra": "1401 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1401 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1401 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2167,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "56875 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2167,
            "unit": "ns/op",
            "extra": "56875 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "56875 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "56875 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 89078,
            "unit": "ns/op\t   33400 B/op\t      73 allocs/op",
            "extra": "1390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 89078,
            "unit": "ns/op",
            "extra": "1390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33400,
            "unit": "B/op",
            "extra": "1390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 5957,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "19154 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 5957,
            "unit": "ns/op",
            "extra": "19154 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "19154 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "19154 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 118948,
            "unit": "ns/op\t   57145 B/op\t     409 allocs/op",
            "extra": "980 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 118948,
            "unit": "ns/op",
            "extra": "980 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57145,
            "unit": "B/op",
            "extra": "980 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "980 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 36732,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "3230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36732,
            "unit": "ns/op",
            "extra": "3230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "3230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "3230 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils)",
            "value": 475.3,
            "unit": "ns/op\t 134.64 MB/s\t    1540 B/op\t       0 allocs/op",
            "extra": "220310 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 475.3,
            "unit": "ns/op",
            "extra": "220310 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 134.64,
            "unit": "MB/s",
            "extra": "220310 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 1540,
            "unit": "B/op",
            "extra": "220310 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "220310 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils)",
            "value": 153.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "777790 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 153.3,
            "unit": "ns/op",
            "extra": "777790 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "777790 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "777790 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils)",
            "value": 128.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "906288 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 128.1,
            "unit": "ns/op",
            "extra": "906288 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "906288 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "906288 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 57.59,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2089818 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 57.59,
            "unit": "ns/op",
            "extra": "2089818 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2089818 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2089818 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 55.36,
            "unit": "ns/op",
            "extra": "2028792 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 159.8,
            "unit": "ns/op",
            "extra": "798687 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 166.8,
            "unit": "ns/op",
            "extra": "820465 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 188.2,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 37.01,
            "unit": "ns/op",
            "extra": "3250046 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 117.5,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 130.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 158.4,
            "unit": "ns/op",
            "extra": "868414 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 70.91,
            "unit": "ns/op",
            "extra": "1662596 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 169.4,
            "unit": "ns/op",
            "extra": "761913 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 179.6,
            "unit": "ns/op",
            "extra": "699044 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 199.3,
            "unit": "ns/op",
            "extra": "835147 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.06,
            "unit": "ns/op",
            "extra": "4495018 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 228.4,
            "unit": "ns/op",
            "extra": "439389 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4709,
            "unit": "ns/op",
            "extra": "22854 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 7821,
            "unit": "ns/op",
            "extra": "14916 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 51.85,
            "unit": "ns/op",
            "extra": "2182508 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 76.81,
            "unit": "ns/op",
            "extra": "1570856 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 75.13,
            "unit": "ns/op",
            "extra": "1589614 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 74.2,
            "unit": "ns/op",
            "extra": "1609774 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 9.929,
            "unit": "ns/op",
            "extra": "12102778 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils)",
            "value": 1043,
            "unit": "ns/op\t  61.34 MB/s\t     157 B/op\t       1 allocs/op",
            "extra": "167360 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 1043,
            "unit": "ns/op",
            "extra": "167360 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 61.34,
            "unit": "MB/s",
            "extra": "167360 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 157,
            "unit": "B/op",
            "extra": "167360 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "167360 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils)",
            "value": 435.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "278253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 435.6,
            "unit": "ns/op",
            "extra": "278253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "278253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "278253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils)",
            "value": 385.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "293529 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 385.3,
            "unit": "ns/op",
            "extra": "293529 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "293529 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "293529 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 44.26,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2698215 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 44.26,
            "unit": "ns/op",
            "extra": "2698215 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2698215 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2698215 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils)",
            "value": 420.2,
            "unit": "ns/op\t 152.30 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "348962 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 420.2,
            "unit": "ns/op",
            "extra": "348962 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 152.3,
            "unit": "MB/s",
            "extra": "348962 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "348962 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "348962 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils)",
            "value": 906,
            "unit": "ns/op\t  70.64 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "367894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 906,
            "unit": "ns/op",
            "extra": "367894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 70.64,
            "unit": "MB/s",
            "extra": "367894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "367894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "367894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 9844,
            "unit": "ns/op\t 832.22 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11730 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 9844,
            "unit": "ns/op",
            "extra": "11730 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 832.22,
            "unit": "MB/s",
            "extra": "11730 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11730 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11730 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 178.7,
            "unit": "ns/op\t1432.75 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "655008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 178.7,
            "unit": "ns/op",
            "extra": "655008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1432.75,
            "unit": "MB/s",
            "extra": "655008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "655008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "655008 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 561.9,
            "unit": "ns/op\t 455.62 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "196971 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 561.9,
            "unit": "ns/op",
            "extra": "196971 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 455.62,
            "unit": "MB/s",
            "extra": "196971 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "196971 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "196971 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2493951,
            "unit": "ns/op\t 7476075 B/op\t   40016 allocs/op",
            "extra": "45 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2493951,
            "unit": "ns/op",
            "extra": "45 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7476075,
            "unit": "B/op",
            "extra": "45 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "45 times\n4 procs"
          }
        ]
      }
    ]
  }
}