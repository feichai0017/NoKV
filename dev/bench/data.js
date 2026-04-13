window.BENCHMARK_DATA = {
  "lastUpdate": 1776096522181,
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
          "id": "dc2ffccc989bf97a2ba0d0d883e0f51380343b0e",
          "message": "refactor: tighten remaining distributed error handling",
          "timestamp": "2026-04-14T02:06:24+10:00",
          "tree_id": "c8cde9d5bac4706db18cc880c8bf040371d31ddc",
          "url": "https://github.com/feichai0017/NoKV/commit/dc2ffccc989bf97a2ba0d0d883e0f51380343b0e"
        },
        "date": 1776096519587,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 5720,
            "unit": "ns/op\t   5.59 MB/s\t     425 B/op\t      13 allocs/op",
            "extra": "19326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 5720,
            "unit": "ns/op",
            "extra": "19326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 5.59,
            "unit": "MB/s",
            "extra": "19326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 425,
            "unit": "B/op",
            "extra": "19326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "19326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 12294,
            "unit": "ns/op\t 333.17 MB/s\t     400 B/op\t      20 allocs/op",
            "extra": "8244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 12294,
            "unit": "ns/op",
            "extra": "8244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 333.17,
            "unit": "MB/s",
            "extra": "8244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 400,
            "unit": "B/op",
            "extra": "8244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "8244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1392,
            "unit": "ns/op\t  45.96 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "81872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1392,
            "unit": "ns/op",
            "extra": "81872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 45.96,
            "unit": "MB/s",
            "extra": "81872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "81872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "81872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 3799,
            "unit": "ns/op\t1078.31 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "34466 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 3799,
            "unit": "ns/op",
            "extra": "34466 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1078.31,
            "unit": "MB/s",
            "extra": "34466 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "34466 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "34466 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 132721,
            "unit": "ns/op\t 123.45 MB/s\t  202513 B/op\t     147 allocs/op",
            "extra": "860 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 132721,
            "unit": "ns/op",
            "extra": "860 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 123.45,
            "unit": "MB/s",
            "extra": "860 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 202513,
            "unit": "B/op",
            "extra": "860 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "860 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 417166,
            "unit": "ns/op\t  39.27 MB/s\t   46494 B/op\t     147 allocs/op",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 417166,
            "unit": "ns/op",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 39.27,
            "unit": "MB/s",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46494,
            "unit": "B/op",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 391153,
            "unit": "ns/op\t  41.89 MB/s\t   46606 B/op\t     148 allocs/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 391153,
            "unit": "ns/op",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 41.89,
            "unit": "MB/s",
            "extra": "321 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46606,
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
            "value": 2114874,
            "unit": "ns/op\t       6 B/op\t       0 allocs/op",
            "extra": "50 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 2114874,
            "unit": "ns/op",
            "extra": "50 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 6,
            "unit": "B/op",
            "extra": "50 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "50 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 312.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "343959 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 312.3,
            "unit": "ns/op",
            "extra": "343959 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "343959 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "343959 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 30.74,
            "unit": "ns/op",
            "extra": "3849242 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 87.21,
            "unit": "ns/op",
            "extra": "1394532 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 23.88,
            "unit": "ns/op",
            "extra": "5012032 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 14.11,
            "unit": "ns/op",
            "extra": "8460459 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 21142103,
            "unit": "ns/op",
            "extra": "5 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 84.38,
            "unit": "ns/op",
            "extra": "1424295 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 68687,
            "unit": "ns/op",
            "extra": "1774 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index)",
            "value": 613.9,
            "unit": "ns/op\t 104.25 MB/s\t    1541 B/op\t       0 allocs/op",
            "extra": "194835 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 613.9,
            "unit": "ns/op",
            "extra": "194835 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 104.25,
            "unit": "MB/s",
            "extra": "194835 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 1541,
            "unit": "B/op",
            "extra": "194835 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "194835 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index)",
            "value": 128.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "873009 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 128.9,
            "unit": "ns/op",
            "extra": "873009 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "873009 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "873009 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index)",
            "value": 162.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "688086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 162.4,
            "unit": "ns/op",
            "extra": "688086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "688086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "688086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 66.73,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1761210 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 66.73,
            "unit": "ns/op",
            "extra": "1761210 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1761210 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1761210 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index)",
            "value": 975.3,
            "unit": "ns/op\t  65.62 MB/s\t     157 B/op\t       1 allocs/op",
            "extra": "150111 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 975.3,
            "unit": "ns/op",
            "extra": "150111 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 65.62,
            "unit": "MB/s",
            "extra": "150111 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 157,
            "unit": "B/op",
            "extra": "150111 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "150111 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index)",
            "value": 395.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "298615 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 395.9,
            "unit": "ns/op",
            "extra": "298615 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "298615 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "298615 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index)",
            "value": 407.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "312932 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 407.7,
            "unit": "ns/op",
            "extra": "312932 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "312932 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "312932 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 38.33,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2984503 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 38.33,
            "unit": "ns/op",
            "extra": "2984503 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2984503 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2984503 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index)",
            "value": 402.5,
            "unit": "ns/op\t 159.00 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "321879 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 402.5,
            "unit": "ns/op",
            "extra": "321879 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 159,
            "unit": "MB/s",
            "extra": "321879 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "321879 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "321879 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index)",
            "value": 1102,
            "unit": "ns/op\t  58.08 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "329583 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 1102,
            "unit": "ns/op",
            "extra": "329583 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 58.08,
            "unit": "MB/s",
            "extra": "329583 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "329583 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "329583 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 30241,
            "unit": "ns/op\t 270.89 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 30241,
            "unit": "ns/op",
            "extra": "3552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 270.89,
            "unit": "MB/s",
            "extra": "3552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 40362,
            "unit": "ns/op\t 202.96 MB/s\t   34646 B/op\t     210 allocs/op",
            "extra": "2974 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40362,
            "unit": "ns/op",
            "extra": "2974 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 202.96,
            "unit": "MB/s",
            "extra": "2974 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34646,
            "unit": "B/op",
            "extra": "2974 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "2974 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 8975906,
            "unit": "ns/op\t71593428 B/op\t     476 allocs/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 8975906,
            "unit": "ns/op",
            "extra": "34 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593428,
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
            "value": 8958089,
            "unit": "ns/op\t71593537 B/op\t     475 allocs/op",
            "extra": "12 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 8958089,
            "unit": "ns/op",
            "extra": "12 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593537,
            "unit": "B/op",
            "extra": "12 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "12 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 327.2,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "357112 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 327.2,
            "unit": "ns/op",
            "extra": "357112 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "357112 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "357112 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 522.8,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "223482 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 522.8,
            "unit": "ns/op",
            "extra": "223482 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "223482 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "223482 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 153.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "716461 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 153.6,
            "unit": "ns/op",
            "extra": "716461 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "716461 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "716461 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 311.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "385714 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 311.8,
            "unit": "ns/op",
            "extra": "385714 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "385714 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "385714 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 26935,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "4430 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 26935,
            "unit": "ns/op",
            "extra": "4430 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "4430 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "4430 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 87.56,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1378537 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 87.56,
            "unit": "ns/op",
            "extra": "1378537 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1378537 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1378537 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 14127,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "8352 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 14127,
            "unit": "ns/op",
            "extra": "8352 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "8352 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "8352 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 527.8,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "214362 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 527.8,
            "unit": "ns/op",
            "extra": "214362 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "214362 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "214362 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 13881,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "8754 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 13881,
            "unit": "ns/op",
            "extra": "8754 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "8754 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "8754 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 484.7,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "231918 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 484.7,
            "unit": "ns/op",
            "extra": "231918 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "231918 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "231918 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 48923,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2299 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 48923,
            "unit": "ns/op",
            "extra": "2299 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2299 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2299 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 857.4,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "131263 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 857.4,
            "unit": "ns/op",
            "extra": "131263 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "131263 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "131263 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53795,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "2155 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53795,
            "unit": "ns/op",
            "extra": "2155 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "2155 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "2155 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4728,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "21297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4728,
            "unit": "ns/op",
            "extra": "21297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "21297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "21297 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 86820,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1347 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 86820,
            "unit": "ns/op",
            "extra": "1347 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1347 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1347 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 34526,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3057 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 34526,
            "unit": "ns/op",
            "extra": "3057 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3057 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3057 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 2026,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "52834 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2026,
            "unit": "ns/op",
            "extra": "52834 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "52834 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "52834 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1773,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "59146 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1773,
            "unit": "ns/op",
            "extra": "59146 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "59146 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "59146 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 10522,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10522,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 11047,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 11047,
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
            "value": 39386,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3205 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 39386,
            "unit": "ns/op",
            "extra": "3205 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3205 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3205 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 41837,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2894 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 41837,
            "unit": "ns/op",
            "extra": "2894 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2894 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2894 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 36062,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "3111 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36062,
            "unit": "ns/op",
            "extra": "3111 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "3111 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "3111 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 956.6,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "125331 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 956.6,
            "unit": "ns/op",
            "extra": "125331 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "125331 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "125331 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 46136,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 46136,
            "unit": "ns/op",
            "extra": "2494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 537.5,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "213087 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 537.5,
            "unit": "ns/op",
            "extra": "213087 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "213087 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "213087 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 19772,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5827 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 19772,
            "unit": "ns/op",
            "extra": "5827 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5827 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5827 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 19603,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "6064 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 19603,
            "unit": "ns/op",
            "extra": "6064 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "6064 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "6064 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18773,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18773,
            "unit": "ns/op",
            "extra": "6682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 18573,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6586 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18573,
            "unit": "ns/op",
            "extra": "6586 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6586 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6586 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 25929,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "4496 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 25929,
            "unit": "ns/op",
            "extra": "4496 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "4496 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4496 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 809.5,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "144541 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 809.5,
            "unit": "ns/op",
            "extra": "144541 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "144541 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "144541 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 23851,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "4926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 23851,
            "unit": "ns/op",
            "extra": "4926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "4926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4926 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 868.2,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "132766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 868.2,
            "unit": "ns/op",
            "extra": "132766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "132766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "132766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 80406,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1446 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 80406,
            "unit": "ns/op",
            "extra": "1446 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1446 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1446 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2168,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "50816 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2168,
            "unit": "ns/op",
            "extra": "50816 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "50816 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "50816 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 85553,
            "unit": "ns/op\t   33400 B/op\t      73 allocs/op",
            "extra": "1326 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 85553,
            "unit": "ns/op",
            "extra": "1326 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33400,
            "unit": "B/op",
            "extra": "1326 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1326 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 6281,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "18553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6281,
            "unit": "ns/op",
            "extra": "18553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "18553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "18553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 120534,
            "unit": "ns/op\t   57144 B/op\t     409 allocs/op",
            "extra": "972 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 120534,
            "unit": "ns/op",
            "extra": "972 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57144,
            "unit": "B/op",
            "extra": "972 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "972 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 38773,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "2601 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38773,
            "unit": "ns/op",
            "extra": "2601 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "2601 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2601 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 123.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 228.1,
            "unit": "ns/op",
            "extra": "525188 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 254.2,
            "unit": "ns/op",
            "extra": "485443 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 288.7,
            "unit": "ns/op",
            "extra": "454892 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 102.1,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 176.7,
            "unit": "ns/op",
            "extra": "665152 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 212.5,
            "unit": "ns/op",
            "extra": "591999 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 245.7,
            "unit": "ns/op",
            "extra": "486397 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 128.2,
            "unit": "ns/op",
            "extra": "898125 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 240.4,
            "unit": "ns/op",
            "extra": "531291 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 269.2,
            "unit": "ns/op",
            "extra": "520639 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 296.7,
            "unit": "ns/op",
            "extra": "455889 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 74.84,
            "unit": "ns/op",
            "extra": "1600664 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 315.8,
            "unit": "ns/op",
            "extra": "364686 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 9092,
            "unit": "ns/op",
            "extra": "13024 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 12510,
            "unit": "ns/op",
            "extra": "9577 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 58.81,
            "unit": "ns/op",
            "extra": "1981821 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 102.8,
            "unit": "ns/op",
            "extra": "1227326 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 96.9,
            "unit": "ns/op",
            "extra": "1242898 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 95.83,
            "unit": "ns/op",
            "extra": "1211773 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 31.54,
            "unit": "ns/op",
            "extra": "3648934 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 10453,
            "unit": "ns/op\t 783.72 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 10453,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 783.72,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 176.4,
            "unit": "ns/op\t1451.46 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "684673 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 176.4,
            "unit": "ns/op",
            "extra": "684673 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1451.46,
            "unit": "MB/s",
            "extra": "684673 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "684673 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "684673 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 595.4,
            "unit": "ns/op\t 429.97 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "193678 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 595.4,
            "unit": "ns/op",
            "extra": "193678 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 429.97,
            "unit": "MB/s",
            "extra": "193678 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "193678 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "193678 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2541388,
            "unit": "ns/op\t 7475900 B/op\t   40016 allocs/op",
            "extra": "46 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2541388,
            "unit": "ns/op",
            "extra": "46 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7475900,
            "unit": "B/op",
            "extra": "46 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "46 times\n4 procs"
          }
        ]
      }
    ]
  }
}