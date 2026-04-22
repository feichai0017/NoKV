window.BENCHMARK_DATA = {
  "lastUpdate": 1776860728773,
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
          "id": "38a7186aa5a6efb3771b3ddf3ef6e92e4336a275",
          "message": "fix(demo): redirect dashboard root to landing page",
          "timestamp": "2026-04-22T22:22:57+10:00",
          "tree_id": "73e07769ca2a5d933e07baf09a502f1e396395d7",
          "url": "https://github.com/feichai0017/NoKV/commit/38a7186aa5a6efb3771b3ddf3ef6e92e4336a275"
        },
        "date": 1776860725639,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4895,
            "unit": "ns/op\t   6.54 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "26539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4895,
            "unit": "ns/op",
            "extra": "26539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 6.54,
            "unit": "MB/s",
            "extra": "26539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "26539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "26539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 15404,
            "unit": "ns/op\t 265.91 MB/s\t     398 B/op\t      20 allocs/op",
            "extra": "8013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 15404,
            "unit": "ns/op",
            "extra": "8013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 265.91,
            "unit": "MB/s",
            "extra": "8013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 398,
            "unit": "B/op",
            "extra": "8013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "8013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1354,
            "unit": "ns/op\t  47.28 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "90889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1354,
            "unit": "ns/op",
            "extra": "90889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 47.28,
            "unit": "MB/s",
            "extra": "90889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "90889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "90889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 6005,
            "unit": "ns/op\t 682.05 MB/s\t    9160 B/op\t       7 allocs/op",
            "extra": "19275 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 6005,
            "unit": "ns/op",
            "extra": "19275 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 682.05,
            "unit": "MB/s",
            "extra": "19275 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 9160,
            "unit": "B/op",
            "extra": "19275 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "19275 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 131180,
            "unit": "ns/op\t 124.90 MB/s\t  190133 B/op\t     147 allocs/op",
            "extra": "934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 131180,
            "unit": "ns/op",
            "extra": "934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 124.9,
            "unit": "MB/s",
            "extra": "934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 190133,
            "unit": "B/op",
            "extra": "934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 481550,
            "unit": "ns/op\t  34.02 MB/s\t   46533 B/op\t     147 allocs/op",
            "extra": "210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 481550,
            "unit": "ns/op",
            "extra": "210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 34.02,
            "unit": "MB/s",
            "extra": "210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46533,
            "unit": "B/op",
            "extra": "210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 373420,
            "unit": "ns/op\t  43.88 MB/s\t   46636 B/op\t     148 allocs/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 373420,
            "unit": "ns/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 43.88,
            "unit": "MB/s",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46636,
            "unit": "B/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1736598,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1736598,
            "unit": "ns/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 228.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "518883 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 228.7,
            "unit": "ns/op",
            "extra": "518883 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "518883 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "518883 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 444.9,
            "unit": "ns/op\t 143.85 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "254114 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 444.9,
            "unit": "ns/op",
            "extra": "254114 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 143.85,
            "unit": "MB/s",
            "extra": "254114 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "254114 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "254114 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 163.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "759435 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 163.2,
            "unit": "ns/op",
            "extra": "759435 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "759435 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "759435 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 127.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "915028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 127.9,
            "unit": "ns/op",
            "extra": "915028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "915028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "915028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 53.79,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2225073 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 53.79,
            "unit": "ns/op",
            "extra": "2225073 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2225073 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2225073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 892.1,
            "unit": "ns/op\t  71.74 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "170970 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 892.1,
            "unit": "ns/op",
            "extra": "170970 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 71.74,
            "unit": "MB/s",
            "extra": "170970 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "170970 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "170970 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 398.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "279006 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 398.9,
            "unit": "ns/op",
            "extra": "279006 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "279006 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "279006 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 375,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "288192 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 375,
            "unit": "ns/op",
            "extra": "288192 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "288192 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "288192 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 44.39,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2810461 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 44.39,
            "unit": "ns/op",
            "extra": "2810461 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2810461 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2810461 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index)",
            "value": 366.6,
            "unit": "ns/op\t 174.59 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "337084 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 366.6,
            "unit": "ns/op",
            "extra": "337084 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 174.59,
            "unit": "MB/s",
            "extra": "337084 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "337084 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "337084 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index)",
            "value": 904.9,
            "unit": "ns/op\t  70.73 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "360996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 904.9,
            "unit": "ns/op",
            "extra": "360996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 70.73,
            "unit": "MB/s",
            "extra": "360996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "360996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "360996 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 24419,
            "unit": "ns/op\t 335.48 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 24419,
            "unit": "ns/op",
            "extra": "4785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 335.48,
            "unit": "MB/s",
            "extra": "4785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 31527,
            "unit": "ns/op\t 259.84 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 31527,
            "unit": "ns/op",
            "extra": "3926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 259.84,
            "unit": "MB/s",
            "extra": "3926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5092100,
            "unit": "ns/op\t71593329 B/op\t     475 allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5092100,
            "unit": "ns/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593329,
            "unit": "B/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5040994,
            "unit": "ns/op\t71593553 B/op\t     475 allocs/op",
            "extra": "21 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5040994,
            "unit": "ns/op",
            "extra": "21 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593553,
            "unit": "B/op",
            "extra": "21 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "21 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 349.6,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "344911 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 349.6,
            "unit": "ns/op",
            "extra": "344911 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "344911 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "344911 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 500.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "228002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 500.3,
            "unit": "ns/op",
            "extra": "228002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "228002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "228002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 149.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "772294 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 149.5,
            "unit": "ns/op",
            "extra": "772294 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "772294 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "772294 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 343.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "355683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 343.9,
            "unit": "ns/op",
            "extra": "355683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "355683 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "355683 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 29984,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3940 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 29984,
            "unit": "ns/op",
            "extra": "3940 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3940 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3940 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 75.18,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1596297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 75.18,
            "unit": "ns/op",
            "extra": "1596297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1596297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1596297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 15022,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "8008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 15022,
            "unit": "ns/op",
            "extra": "8008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "8008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8008 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 468,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "220177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 468,
            "unit": "ns/op",
            "extra": "220177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "220177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "220177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 14835,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "7302 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 14835,
            "unit": "ns/op",
            "extra": "7302 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "7302 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "7302 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 438.4,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "259086 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 438.4,
            "unit": "ns/op",
            "extra": "259086 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "259086 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "259086 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 50853,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 50853,
            "unit": "ns/op",
            "extra": "2264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2264 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 818.2,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "142606 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 818.2,
            "unit": "ns/op",
            "extra": "142606 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "142606 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "142606 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 59208,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "2156 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 59208,
            "unit": "ns/op",
            "extra": "2156 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "2156 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "2156 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 4112,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "28749 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 4112,
            "unit": "ns/op",
            "extra": "28749 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "28749 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "28749 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 79729,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1492 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 79729,
            "unit": "ns/op",
            "extra": "1492 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1492 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1492 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 28884,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3871 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 28884,
            "unit": "ns/op",
            "extra": "3871 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3871 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3871 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1739,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "62397 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1739,
            "unit": "ns/op",
            "extra": "62397 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "62397 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "62397 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1515,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "80462 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1515,
            "unit": "ns/op",
            "extra": "80462 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "80462 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "80462 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 8824,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "12702 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 8824,
            "unit": "ns/op",
            "extra": "12702 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "12702 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "12702 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 9323,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "12424 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 9323,
            "unit": "ns/op",
            "extra": "12424 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 6752,
            "unit": "B/op",
            "extra": "12424 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "12424 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37017,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3450 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37017,
            "unit": "ns/op",
            "extra": "3450 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3450 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3450 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 35862,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "3238 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 35862,
            "unit": "ns/op",
            "extra": "3238 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "3238 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "3238 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41334,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 41334,
            "unit": "ns/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 915.2,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "122122 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 915.2,
            "unit": "ns/op",
            "extra": "122122 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "122122 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "122122 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 47996,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 47996,
            "unit": "ns/op",
            "extra": "2436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2436 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 460.4,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "252448 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 460.4,
            "unit": "ns/op",
            "extra": "252448 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "252448 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "252448 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18728,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "6177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18728,
            "unit": "ns/op",
            "extra": "6177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "6177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "6177 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18836,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5642 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18836,
            "unit": "ns/op",
            "extra": "5642 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5642 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5642 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18450,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5961 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18450,
            "unit": "ns/op",
            "extra": "5961 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5961 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5961 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 18481,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6662 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 18481,
            "unit": "ns/op",
            "extra": "6662 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6662 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 28367,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "4242 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 28367,
            "unit": "ns/op",
            "extra": "4242 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "4242 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4242 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 758.2,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "138349 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 758.2,
            "unit": "ns/op",
            "extra": "138349 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "138349 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "138349 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 26262,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "4342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 26262,
            "unit": "ns/op",
            "extra": "4342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "4342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4342 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 819.3,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "133618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 819.3,
            "unit": "ns/op",
            "extra": "133618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "133618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "133618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 80838,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1348 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 80838,
            "unit": "ns/op",
            "extra": "1348 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1348 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1348 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2129,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "54319 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2129,
            "unit": "ns/op",
            "extra": "54319 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "54319 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "54319 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 85416,
            "unit": "ns/op\t   33401 B/op\t      73 allocs/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 85416,
            "unit": "ns/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 33401,
            "unit": "B/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 5579,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "19825 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 5579,
            "unit": "ns/op",
            "extra": "19825 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "19825 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "19825 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 115697,
            "unit": "ns/op\t   57145 B/op\t     409 allocs/op",
            "extra": "938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 115697,
            "unit": "ns/op",
            "extra": "938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 57145,
            "unit": "B/op",
            "extra": "938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 33508,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 33508,
            "unit": "ns/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 9670,
            "unit": "ns/op\t 847.14 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11649 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 9670,
            "unit": "ns/op",
            "extra": "11649 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 847.14,
            "unit": "MB/s",
            "extra": "11649 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11649 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11649 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 158.3,
            "unit": "ns/op\t1617.54 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "879471 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 158.3,
            "unit": "ns/op",
            "extra": "879471 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 1617.54,
            "unit": "MB/s",
            "extra": "879471 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "879471 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "879471 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal)",
            "value": 501.9,
            "unit": "ns/op\t 510.02 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "247239 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 501.9,
            "unit": "ns/op",
            "extra": "247239 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - MB/s",
            "value": 510.02,
            "unit": "MB/s",
            "extra": "247239 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "247239 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "247239 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal)",
            "value": 2254133,
            "unit": "ns/op\t 7476006 B/op\t   40016 allocs/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 2254133,
            "unit": "ns/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 7476006,
            "unit": "B/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/thermos)",
            "value": 22.62,
            "unit": "ns/op",
            "extra": "5235818 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/thermos)",
            "value": 54.95,
            "unit": "ns/op",
            "extra": "2150839 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/thermos)",
            "value": 20.12,
            "unit": "ns/op",
            "extra": "5864084 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/thermos)",
            "value": 16.19,
            "unit": "ns/op",
            "extra": "7357917 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/thermos)",
            "value": 19980510,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/thermos)",
            "value": 79.74,
            "unit": "ns/op",
            "extra": "1500044 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/thermos)",
            "value": 32661,
            "unit": "ns/op",
            "extra": "3771 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 52.19,
            "unit": "ns/op",
            "extra": "2298297 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 183.9,
            "unit": "ns/op",
            "extra": "690940 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 219.4,
            "unit": "ns/op",
            "extra": "652672 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 230.3,
            "unit": "ns/op",
            "extra": "571126 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 42.04,
            "unit": "ns/op",
            "extra": "2867342 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 129.9,
            "unit": "ns/op",
            "extra": "987272 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 147.2,
            "unit": "ns/op",
            "extra": "837163 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 174.7,
            "unit": "ns/op",
            "extra": "702148 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 66.44,
            "unit": "ns/op",
            "extra": "1995625 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 183.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 220.1,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 234.8,
            "unit": "ns/op",
            "extra": "556095 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 28.09,
            "unit": "ns/op",
            "extra": "4220152 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 306.1,
            "unit": "ns/op",
            "extra": "363534 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 5656,
            "unit": "ns/op",
            "extra": "21385 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8335,
            "unit": "ns/op",
            "extra": "14311 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 45.87,
            "unit": "ns/op",
            "extra": "2364135 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 91.41,
            "unit": "ns/op",
            "extra": "1314285 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 88.14,
            "unit": "ns/op",
            "extra": "1342522 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 87.85,
            "unit": "ns/op",
            "extra": "1371457 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 11.63,
            "unit": "ns/op",
            "extra": "10770645 times\n4 procs"
          }
        ]
      }
    ]
  }
}