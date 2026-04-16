window.BENCHMARK_DATA = {
  "lastUpdate": 1776335373092,
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
          "id": "f0c330f1f6c07a4ddec66781aeec8250da149a1e",
          "message": "Merge pull request #149 from feichai0017/research/namespace-listing-layer\n\nfeat: harden namespace listing contract and benchmark tooling",
          "timestamp": "2026-04-16T20:24:34+10:00",
          "tree_id": "7ac560d285bce2e2a0a8096861174689d650dd41",
          "url": "https://github.com/feichai0017/NoKV/commit/f0c330f1f6c07a4ddec66781aeec8250da149a1e"
        },
        "date": 1776335235016,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 6377,
            "unit": "ns/op\t   5.02 MB/s\t     425 B/op\t      13 allocs/op",
            "extra": "16507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 6377,
            "unit": "ns/op",
            "extra": "16507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 5.02,
            "unit": "MB/s",
            "extra": "16507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 425,
            "unit": "B/op",
            "extra": "16507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "16507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 15828,
            "unit": "ns/op\t 258.79 MB/s\t     399 B/op\t      20 allocs/op",
            "extra": "7352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 15828,
            "unit": "ns/op",
            "extra": "7352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 258.79,
            "unit": "MB/s",
            "extra": "7352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "7352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "7352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1454,
            "unit": "ns/op\t  44.02 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "102946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1454,
            "unit": "ns/op",
            "extra": "102946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 44.02,
            "unit": "MB/s",
            "extra": "102946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "102946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "102946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 4389,
            "unit": "ns/op\t 933.27 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "29114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 4389,
            "unit": "ns/op",
            "extra": "29114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 933.27,
            "unit": "MB/s",
            "extra": "29114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "29114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "29114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 69845,
            "unit": "ns/op\t 234.58 MB/s\t  169798 B/op\t     147 allocs/op",
            "extra": "1632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 69845,
            "unit": "ns/op",
            "extra": "1632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 234.58,
            "unit": "MB/s",
            "extra": "1632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 169798,
            "unit": "B/op",
            "extra": "1632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "1632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 498451,
            "unit": "ns/op\t  32.87 MB/s\t   46521 B/op\t     147 allocs/op",
            "extra": "243 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 498451,
            "unit": "ns/op",
            "extra": "243 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 32.87,
            "unit": "MB/s",
            "extra": "243 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46521,
            "unit": "B/op",
            "extra": "243 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "243 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 473330,
            "unit": "ns/op\t  34.61 MB/s\t   46670 B/op\t     149 allocs/op",
            "extra": "268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 473330,
            "unit": "ns/op",
            "extra": "268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 34.61,
            "unit": "MB/s",
            "extra": "268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46670,
            "unit": "B/op",
            "extra": "268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1738409,
            "unit": "ns/op\t      35 B/op\t       0 allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1738409,
            "unit": "ns/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 260.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "396442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 260.4,
            "unit": "ns/op",
            "extra": "396442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "396442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "396442 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.8,
            "unit": "ns/op",
            "extra": "5031456 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 57.16,
            "unit": "ns/op",
            "extra": "2056844 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20,
            "unit": "ns/op",
            "extra": "5967222 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 23.54,
            "unit": "ns/op",
            "extra": "7219969 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 22996157,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 76.44,
            "unit": "ns/op",
            "extra": "1535247 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 53874,
            "unit": "ns/op",
            "extra": "2072 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index)",
            "value": 501.4,
            "unit": "ns/op\t 127.64 MB/s\t    1541 B/op\t       0 allocs/op",
            "extra": "202453 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 501.4,
            "unit": "ns/op",
            "extra": "202453 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 127.64,
            "unit": "MB/s",
            "extra": "202453 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 1541,
            "unit": "B/op",
            "extra": "202453 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "202453 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index)",
            "value": 152.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "790662 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 152.1,
            "unit": "ns/op",
            "extra": "790662 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "790662 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "790662 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index)",
            "value": 132.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "921759 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 132.2,
            "unit": "ns/op",
            "extra": "921759 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "921759 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "921759 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 57.42,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2018914 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 57.42,
            "unit": "ns/op",
            "extra": "2018914 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2018914 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2018914 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index)",
            "value": 977.1,
            "unit": "ns/op\t  65.50 MB/s\t     159 B/op\t       1 allocs/op",
            "extra": "164133 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 977.1,
            "unit": "ns/op",
            "extra": "164133 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 65.5,
            "unit": "MB/s",
            "extra": "164133 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 159,
            "unit": "B/op",
            "extra": "164133 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "164133 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index)",
            "value": 465.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "288208 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 465.6,
            "unit": "ns/op",
            "extra": "288208 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "288208 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "288208 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index)",
            "value": 414.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "292410 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 414.3,
            "unit": "ns/op",
            "extra": "292410 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "292410 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "292410 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 44.79,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2647518 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 44.79,
            "unit": "ns/op",
            "extra": "2647518 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2647518 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2647518 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index)",
            "value": 391.8,
            "unit": "ns/op\t 163.37 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "366073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 391.8,
            "unit": "ns/op",
            "extra": "366073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 163.37,
            "unit": "MB/s",
            "extra": "366073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "366073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "366073 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index)",
            "value": 1013,
            "unit": "ns/op\t  63.21 MB/s\t     159 B/op\t       1 allocs/op",
            "extra": "311546 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 1013,
            "unit": "ns/op",
            "extra": "311546 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 63.21,
            "unit": "MB/s",
            "extra": "311546 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - B/op",
            "value": 159,
            "unit": "B/op",
            "extra": "311546 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "311546 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 26982,
            "unit": "ns/op\t 303.61 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 26982,
            "unit": "ns/op",
            "extra": "4324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 303.61,
            "unit": "MB/s",
            "extra": "4324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 35348,
            "unit": "ns/op\t 231.75 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "2936 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35348,
            "unit": "ns/op",
            "extra": "2936 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 231.75,
            "unit": "MB/s",
            "extra": "2936 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "2936 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "2936 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 6562511,
            "unit": "ns/op\t71593496 B/op\t     477 allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6562511,
            "unit": "ns/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593496,
            "unit": "B/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 477,
            "unit": "allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 6624738,
            "unit": "ns/op\t71593361 B/op\t     474 allocs/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6624738,
            "unit": "ns/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593361,
            "unit": "B/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 474,
            "unit": "allocs/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 325.4,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "349594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 325.4,
            "unit": "ns/op",
            "extra": "349594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "349594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "349594 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 527.6,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "222488 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 527.6,
            "unit": "ns/op",
            "extra": "222488 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "222488 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "222488 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 161.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "732063 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 161.4,
            "unit": "ns/op",
            "extra": "732063 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "732063 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "732063 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 363.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "366422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 363.2,
            "unit": "ns/op",
            "extra": "366422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "366422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "366422 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 35501,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35501,
            "unit": "ns/op",
            "extra": "3192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 92.96,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1294609 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 92.96,
            "unit": "ns/op",
            "extra": "1294609 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1294609 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1294609 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18022,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6279 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18022,
            "unit": "ns/op",
            "extra": "6279 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6279 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6279 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 513.4,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "234246 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 513.4,
            "unit": "ns/op",
            "extra": "234246 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "234246 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "234246 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17329,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6547 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17329,
            "unit": "ns/op",
            "extra": "6547 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6547 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6547 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 478.9,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "243140 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 478.9,
            "unit": "ns/op",
            "extra": "243140 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "243140 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "243140 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 54004,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2082 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 54004,
            "unit": "ns/op",
            "extra": "2082 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2082 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2082 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 891.5,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "136724 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 891.5,
            "unit": "ns/op",
            "extra": "136724 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "136724 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "136724 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 59128,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1966 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 59128,
            "unit": "ns/op",
            "extra": "1966 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1966 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1966 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4608,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "24906 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4608,
            "unit": "ns/op",
            "extra": "24906 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "24906 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "24906 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 88353,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 88353,
            "unit": "ns/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 33519,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3494 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 33519,
            "unit": "ns/op",
            "extra": "3494 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3494 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3494 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 1865,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "61453 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1865,
            "unit": "ns/op",
            "extra": "61453 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "61453 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "61453 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1736,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "61251 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1736,
            "unit": "ns/op",
            "extra": "61251 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "61251 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "61251 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 10325,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10325,
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
            "value": 10948,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10948,
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
            "value": 38758,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3138 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38758,
            "unit": "ns/op",
            "extra": "3138 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3138 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3138 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 41360,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2940 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 41360,
            "unit": "ns/op",
            "extra": "2940 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2940 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2940 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 46614,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2582 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 46614,
            "unit": "ns/op",
            "extra": "2582 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2582 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2582 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 970.7,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "120098 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 970.7,
            "unit": "ns/op",
            "extra": "120098 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "120098 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "120098 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53479,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53479,
            "unit": "ns/op",
            "extra": "2118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 519.7,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "224662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 519.7,
            "unit": "ns/op",
            "extra": "224662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "224662 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "224662 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 21821,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5568 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21821,
            "unit": "ns/op",
            "extra": "5568 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5568 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5568 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 21758,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "4846 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21758,
            "unit": "ns/op",
            "extra": "4846 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "4846 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "4846 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 21268,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5293 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21268,
            "unit": "ns/op",
            "extra": "5293 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5293 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5293 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 21050,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6080 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21050,
            "unit": "ns/op",
            "extra": "6080 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6080 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6080 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 32465,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3466 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 32465,
            "unit": "ns/op",
            "extra": "3466 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3466 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3466 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 820,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "143263 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 820,
            "unit": "ns/op",
            "extra": "143263 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "143263 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "143263 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 30138,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3748 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 30138,
            "unit": "ns/op",
            "extra": "3748 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3748 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3748 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 872.1,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "129056 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 872.1,
            "unit": "ns/op",
            "extra": "129056 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "129056 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "129056 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 84443,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1328 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 84443,
            "unit": "ns/op",
            "extra": "1328 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1328 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1328 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2157,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "52285 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2157,
            "unit": "ns/op",
            "extra": "52285 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "52285 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "52285 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 90697,
            "unit": "ns/op\t   33401 B/op\t      73 allocs/op",
            "extra": "1252 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 90697,
            "unit": "ns/op",
            "extra": "1252 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33401,
            "unit": "B/op",
            "extra": "1252 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1252 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 6062,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "18549 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6062,
            "unit": "ns/op",
            "extra": "18549 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "18549 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "18549 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 121044,
            "unit": "ns/op\t   57144 B/op\t     409 allocs/op",
            "extra": "1028 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 121044,
            "unit": "ns/op",
            "extra": "1028 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57144,
            "unit": "B/op",
            "extra": "1028 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "1028 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 38487,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "2732 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38487,
            "unit": "ns/op",
            "extra": "2732 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "2732 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2732 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 56.86,
            "unit": "ns/op",
            "extra": "2092056 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 154.2,
            "unit": "ns/op",
            "extra": "924004 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 157.5,
            "unit": "ns/op",
            "extra": "867661 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 190.8,
            "unit": "ns/op",
            "extra": "729013 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 44.84,
            "unit": "ns/op",
            "extra": "2700448 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 112.3,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 124.9,
            "unit": "ns/op",
            "extra": "910585 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 162.9,
            "unit": "ns/op",
            "extra": "877227 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 65.83,
            "unit": "ns/op",
            "extra": "1664229 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 164.4,
            "unit": "ns/op",
            "extra": "785402 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 179.3,
            "unit": "ns/op",
            "extra": "697984 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 203.6,
            "unit": "ns/op",
            "extra": "687699 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.48,
            "unit": "ns/op",
            "extra": "4615624 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 268.9,
            "unit": "ns/op",
            "extra": "423574 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4590,
            "unit": "ns/op",
            "extra": "26518 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8177,
            "unit": "ns/op",
            "extra": "14817 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 57.63,
            "unit": "ns/op",
            "extra": "2155255 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 76.8,
            "unit": "ns/op",
            "extra": "1559671 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 75.52,
            "unit": "ns/op",
            "extra": "1591173 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 74.06,
            "unit": "ns/op",
            "extra": "1616668 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 10.06,
            "unit": "ns/op",
            "extra": "12081910 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 10530,
            "unit": "ns/op\t 777.97 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "10723 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 10530,
            "unit": "ns/op",
            "extra": "10723 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 777.97,
            "unit": "MB/s",
            "extra": "10723 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "10723 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "10723 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 174.6,
            "unit": "ns/op\t1466.58 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "679950 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 174.6,
            "unit": "ns/op",
            "extra": "679950 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1466.58,
            "unit": "MB/s",
            "extra": "679950 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "679950 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "679950 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 558.4,
            "unit": "ns/op\t 458.49 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "218068 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 558.4,
            "unit": "ns/op",
            "extra": "218068 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 458.49,
            "unit": "MB/s",
            "extra": "218068 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "218068 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "218068 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2531058,
            "unit": "ns/op\t 7475893 B/op\t   40016 allocs/op",
            "extra": "46 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2531058,
            "unit": "ns/op",
            "extra": "46 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7475893,
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
      },
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
          "id": "49099338738a4650343a1474743f85a0ee42dbb2",
          "message": "Merge pull request #148 from feichai0017/dependabot/go_modules/golang.org/x/sys-0.43.0\n\ndeps(deps): bump golang.org/x/sys from 0.42.0 to 0.43.0",
          "timestamp": "2026-04-16T20:25:07+10:00",
          "tree_id": "60e9f566e2412819b6448e504561a1b95570b47c",
          "url": "https://github.com/feichai0017/NoKV/commit/49099338738a4650343a1474743f85a0ee42dbb2"
        },
        "date": 1776335299926,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4658,
            "unit": "ns/op\t   6.87 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "30610 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4658,
            "unit": "ns/op",
            "extra": "30610 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 6.87,
            "unit": "MB/s",
            "extra": "30610 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "30610 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "30610 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 11045,
            "unit": "ns/op\t 370.85 MB/s\t     397 B/op\t      20 allocs/op",
            "extra": "12112 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 11045,
            "unit": "ns/op",
            "extra": "12112 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 370.85,
            "unit": "MB/s",
            "extra": "12112 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 397,
            "unit": "B/op",
            "extra": "12112 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "12112 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1341,
            "unit": "ns/op\t  47.72 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "118498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1341,
            "unit": "ns/op",
            "extra": "118498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 47.72,
            "unit": "MB/s",
            "extra": "118498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "118498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "118498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 6015,
            "unit": "ns/op\t 680.96 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "22106 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 6015,
            "unit": "ns/op",
            "extra": "22106 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 680.96,
            "unit": "MB/s",
            "extra": "22106 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "22106 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "22106 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 160398,
            "unit": "ns/op\t 102.15 MB/s\t  131511 B/op\t     147 allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 160398,
            "unit": "ns/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 102.15,
            "unit": "MB/s",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 131511,
            "unit": "B/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 1045303,
            "unit": "ns/op\t  15.67 MB/s\t   46696 B/op\t     148 allocs/op",
            "extra": "121 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 1045303,
            "unit": "ns/op",
            "extra": "121 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 15.67,
            "unit": "MB/s",
            "extra": "121 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46696,
            "unit": "B/op",
            "extra": "121 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "121 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 516512,
            "unit": "ns/op\t  31.72 MB/s\t   46682 B/op\t     149 allocs/op",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 516512,
            "unit": "ns/op",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 31.72,
            "unit": "MB/s",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46682,
            "unit": "B/op",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1738949,
            "unit": "ns/op\t      32 B/op\t       0 allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1738949,
            "unit": "ns/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
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
            "value": 258.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "491409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 258.2,
            "unit": "ns/op",
            "extra": "491409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "491409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "491409 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.65,
            "unit": "ns/op",
            "extra": "4856202 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 59.76,
            "unit": "ns/op",
            "extra": "2018029 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20.02,
            "unit": "ns/op",
            "extra": "5755226 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 16.8,
            "unit": "ns/op",
            "extra": "7246014 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 20906510,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 76.45,
            "unit": "ns/op",
            "extra": "1575278 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 51001,
            "unit": "ns/op",
            "extra": "2134 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index)",
            "value": 479.9,
            "unit": "ns/op\t 133.36 MB/s\t    1542 B/op\t       0 allocs/op",
            "extra": "241189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 479.9,
            "unit": "ns/op",
            "extra": "241189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 133.36,
            "unit": "MB/s",
            "extra": "241189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 1542,
            "unit": "B/op",
            "extra": "241189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "241189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index)",
            "value": 149.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "729877 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 149.8,
            "unit": "ns/op",
            "extra": "729877 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "729877 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "729877 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index)",
            "value": 129.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "772846 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 129.6,
            "unit": "ns/op",
            "extra": "772846 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "772846 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "772846 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 57.17,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2070342 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 57.17,
            "unit": "ns/op",
            "extra": "2070342 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2070342 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2070342 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index)",
            "value": 1009,
            "unit": "ns/op\t  63.45 MB/s\t     159 B/op\t       1 allocs/op",
            "extra": "172982 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 1009,
            "unit": "ns/op",
            "extra": "172982 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 63.45,
            "unit": "MB/s",
            "extra": "172982 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 159,
            "unit": "B/op",
            "extra": "172982 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "172982 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index)",
            "value": 427.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "273055 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 427.3,
            "unit": "ns/op",
            "extra": "273055 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "273055 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "273055 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index)",
            "value": 427.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "283120 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 427.5,
            "unit": "ns/op",
            "extra": "283120 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "283120 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "283120 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 44.82,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2639952 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 44.82,
            "unit": "ns/op",
            "extra": "2639952 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2639952 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2639952 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index)",
            "value": 385.7,
            "unit": "ns/op\t 165.94 MB/s\t     159 B/op\t       1 allocs/op",
            "extra": "287587 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 385.7,
            "unit": "ns/op",
            "extra": "287587 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 165.94,
            "unit": "MB/s",
            "extra": "287587 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - B/op",
            "value": 159,
            "unit": "B/op",
            "extra": "287587 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "287587 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index)",
            "value": 1066,
            "unit": "ns/op\t  60.05 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "330946 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 1066,
            "unit": "ns/op",
            "extra": "330946 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 60.05,
            "unit": "MB/s",
            "extra": "330946 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "330946 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "330946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 27131,
            "unit": "ns/op\t 301.94 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3993 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 27131,
            "unit": "ns/op",
            "extra": "3993 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 301.94,
            "unit": "MB/s",
            "extra": "3993 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3993 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3993 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 40223,
            "unit": "ns/op\t 203.67 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40223,
            "unit": "ns/op",
            "extra": "3402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 203.67,
            "unit": "MB/s",
            "extra": "3402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 6936458,
            "unit": "ns/op\t71593367 B/op\t     476 allocs/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6936458,
            "unit": "ns/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593367,
            "unit": "B/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "36 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 6876294,
            "unit": "ns/op\t71593416 B/op\t     475 allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6876294,
            "unit": "ns/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593416,
            "unit": "B/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 330.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "315718 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 330.3,
            "unit": "ns/op",
            "extra": "315718 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "315718 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "315718 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 546.4,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "221500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 546.4,
            "unit": "ns/op",
            "extra": "221500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "221500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "221500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 160.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "748836 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 160.9,
            "unit": "ns/op",
            "extra": "748836 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "748836 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "748836 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 352.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "350576 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 352.1,
            "unit": "ns/op",
            "extra": "350576 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "350576 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "350576 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 35266,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3163 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35266,
            "unit": "ns/op",
            "extra": "3163 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3163 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3163 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 93.09,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1294214 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 93.09,
            "unit": "ns/op",
            "extra": "1294214 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1294214 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1294214 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18001,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6392 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18001,
            "unit": "ns/op",
            "extra": "6392 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6392 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6392 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 509.7,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "200056 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 509.7,
            "unit": "ns/op",
            "extra": "200056 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "200056 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "200056 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17252,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6684 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17252,
            "unit": "ns/op",
            "extra": "6684 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6684 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6684 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 465.9,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "241556 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 465.9,
            "unit": "ns/op",
            "extra": "241556 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "241556 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "241556 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 54844,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2121 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 54844,
            "unit": "ns/op",
            "extra": "2121 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2121 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2121 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 874.3,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "133372 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 874.3,
            "unit": "ns/op",
            "extra": "133372 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "133372 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "133372 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 58842,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1935 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 58842,
            "unit": "ns/op",
            "extra": "1935 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1935 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1935 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4629,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "25185 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4629,
            "unit": "ns/op",
            "extra": "25185 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "25185 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "25185 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 89662,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1287 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 89662,
            "unit": "ns/op",
            "extra": "1287 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1287 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1287 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 35289,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3057 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35289,
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
            "value": 1935,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "52866 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1935,
            "unit": "ns/op",
            "extra": "52866 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "52866 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "52866 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1743,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "64473 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1743,
            "unit": "ns/op",
            "extra": "64473 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "64473 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "64473 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 10092,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10092,
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
            "value": 10937,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10937,
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
            "value": 38026,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "2794 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38026,
            "unit": "ns/op",
            "extra": "2794 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "2794 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "2794 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 40778,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40778,
            "unit": "ns/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 45733,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2526 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 45733,
            "unit": "ns/op",
            "extra": "2526 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2526 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2526 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 977.5,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "118772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 977.5,
            "unit": "ns/op",
            "extra": "118772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "118772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "118772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53822,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53822,
            "unit": "ns/op",
            "extra": "2124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 513.1,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "225580 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 513.1,
            "unit": "ns/op",
            "extra": "225580 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "225580 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "225580 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 21987,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21987,
            "unit": "ns/op",
            "extra": "5314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 22005,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5546 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 22005,
            "unit": "ns/op",
            "extra": "5546 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5546 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5546 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 20551,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5878 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20551,
            "unit": "ns/op",
            "extra": "5878 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5878 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5878 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 20450,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5859 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20450,
            "unit": "ns/op",
            "extra": "5859 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5859 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5859 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 32333,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3262 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 32333,
            "unit": "ns/op",
            "extra": "3262 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3262 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3262 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 805,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "141884 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 805,
            "unit": "ns/op",
            "extra": "141884 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "141884 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "141884 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 29600,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3657 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 29600,
            "unit": "ns/op",
            "extra": "3657 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3657 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3657 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 872.9,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "120212 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 872.9,
            "unit": "ns/op",
            "extra": "120212 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "120212 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "120212 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 86517,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 86517,
            "unit": "ns/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1272 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2209,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "53979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2209,
            "unit": "ns/op",
            "extra": "53979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "53979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "53979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 90880,
            "unit": "ns/op\t   33401 B/op\t      73 allocs/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 90880,
            "unit": "ns/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33401,
            "unit": "B/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 6293,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "17948 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6293,
            "unit": "ns/op",
            "extra": "17948 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "17948 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "17948 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 123346,
            "unit": "ns/op\t   57144 B/op\t     409 allocs/op",
            "extra": "912 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 123346,
            "unit": "ns/op",
            "extra": "912 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57144,
            "unit": "B/op",
            "extra": "912 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "912 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 40601,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "2860 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40601,
            "unit": "ns/op",
            "extra": "2860 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "2860 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2860 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 57.15,
            "unit": "ns/op",
            "extra": "2114103 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 151.3,
            "unit": "ns/op",
            "extra": "815276 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 162.1,
            "unit": "ns/op",
            "extra": "882841 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 181.2,
            "unit": "ns/op",
            "extra": "776118 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 38.49,
            "unit": "ns/op",
            "extra": "3150511 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 113.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 125.5,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 151.2,
            "unit": "ns/op",
            "extra": "946106 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 68.49,
            "unit": "ns/op",
            "extra": "1791948 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 165.5,
            "unit": "ns/op",
            "extra": "798415 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 174.3,
            "unit": "ns/op",
            "extra": "785588 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 189.1,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.22,
            "unit": "ns/op",
            "extra": "4589338 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 274.7,
            "unit": "ns/op",
            "extra": "435424 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 5090,
            "unit": "ns/op",
            "extra": "23071 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8231,
            "unit": "ns/op",
            "extra": "14463 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 48.14,
            "unit": "ns/op",
            "extra": "2219486 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 72.17,
            "unit": "ns/op",
            "extra": "1678676 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 70.32,
            "unit": "ns/op",
            "extra": "1704958 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 69.41,
            "unit": "ns/op",
            "extra": "1719889 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 10.01,
            "unit": "ns/op",
            "extra": "11978070 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 10890,
            "unit": "ns/op\t 752.24 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "10408 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 10890,
            "unit": "ns/op",
            "extra": "10408 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 752.24,
            "unit": "MB/s",
            "extra": "10408 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "10408 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "10408 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 177.2,
            "unit": "ns/op\t1444.54 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "603718 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 177.2,
            "unit": "ns/op",
            "extra": "603718 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1444.54,
            "unit": "MB/s",
            "extra": "603718 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "603718 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "603718 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 584.7,
            "unit": "ns/op\t 437.82 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "209743 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 584.7,
            "unit": "ns/op",
            "extra": "209743 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 437.82,
            "unit": "MB/s",
            "extra": "209743 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "209743 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "209743 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2503466,
            "unit": "ns/op\t 7475887 B/op\t   40016 allocs/op",
            "extra": "42 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2503466,
            "unit": "ns/op",
            "extra": "42 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7475887,
            "unit": "B/op",
            "extra": "42 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "42 times\n4 procs"
          }
        ]
      },
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
          "id": "3f391ed2ca9a039099d173e52a4642b3903e37a9",
          "message": "Merge pull request #147 from feichai0017/dependabot/github_actions/actions/github-script-9\n\nci: bump actions/github-script from 8 to 9",
          "timestamp": "2026-04-16T20:25:18+10:00",
          "tree_id": "5c814e01ea04c561c01ec30533e55ed6f8a10ef6",
          "url": "https://github.com/feichai0017/NoKV/commit/3f391ed2ca9a039099d173e52a4642b3903e37a9"
        },
        "date": 1776335370442,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 5036,
            "unit": "ns/op\t   6.35 MB/s\t     425 B/op\t      13 allocs/op",
            "extra": "22867 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 5036,
            "unit": "ns/op",
            "extra": "22867 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 6.35,
            "unit": "MB/s",
            "extra": "22867 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 425,
            "unit": "B/op",
            "extra": "22867 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "22867 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 15638,
            "unit": "ns/op\t 261.92 MB/s\t     399 B/op\t      20 allocs/op",
            "extra": "6991 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 15638,
            "unit": "ns/op",
            "extra": "6991 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 261.92,
            "unit": "MB/s",
            "extra": "6991 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "6991 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "6991 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1275,
            "unit": "ns/op\t  50.21 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "88158 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1275,
            "unit": "ns/op",
            "extra": "88158 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 50.21,
            "unit": "MB/s",
            "extra": "88158 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "88158 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "88158 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 3793,
            "unit": "ns/op\t1079.85 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "34540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 3793,
            "unit": "ns/op",
            "extra": "34540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1079.85,
            "unit": "MB/s",
            "extra": "34540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "34540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "34540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 124172,
            "unit": "ns/op\t 131.95 MB/s\t  178806 B/op\t     147 allocs/op",
            "extra": "1014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 124172,
            "unit": "ns/op",
            "extra": "1014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 131.95,
            "unit": "MB/s",
            "extra": "1014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 178806,
            "unit": "B/op",
            "extra": "1014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "1014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 1111967,
            "unit": "ns/op\t  14.73 MB/s\t   46706 B/op\t     148 allocs/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 1111967,
            "unit": "ns/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 14.73,
            "unit": "MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46706,
            "unit": "B/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 754816,
            "unit": "ns/op\t  21.71 MB/s\t   46710 B/op\t     149 allocs/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 754816,
            "unit": "ns/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 21.71,
            "unit": "MB/s",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46710,
            "unit": "B/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1717630,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1717630,
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
            "value": 236.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "456224 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 236.6,
            "unit": "ns/op",
            "extra": "456224 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "456224 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "456224 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.67,
            "unit": "ns/op",
            "extra": "4954183 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 58.02,
            "unit": "ns/op",
            "extra": "2108842 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20.04,
            "unit": "ns/op",
            "extra": "5985322 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 16.74,
            "unit": "ns/op",
            "extra": "7153404 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 20949478,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 76.91,
            "unit": "ns/op",
            "extra": "1570390 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 53819,
            "unit": "ns/op",
            "extra": "2002 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index)",
            "value": 503.9,
            "unit": "ns/op\t 127.00 MB/s\t    1542 B/op\t       0 allocs/op",
            "extra": "235039 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 503.9,
            "unit": "ns/op",
            "extra": "235039 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 127,
            "unit": "MB/s",
            "extra": "235039 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 1542,
            "unit": "B/op",
            "extra": "235039 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "235039 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index)",
            "value": 159.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "788893 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 159.2,
            "unit": "ns/op",
            "extra": "788893 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "788893 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "788893 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index)",
            "value": 130.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "889339 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 130.5,
            "unit": "ns/op",
            "extra": "889339 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "889339 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "889339 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 56.18,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2074540 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 56.18,
            "unit": "ns/op",
            "extra": "2074540 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2074540 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2074540 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index)",
            "value": 970,
            "unit": "ns/op\t  65.98 MB/s\t     157 B/op\t       1 allocs/op",
            "extra": "175189 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 970,
            "unit": "ns/op",
            "extra": "175189 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 65.98,
            "unit": "MB/s",
            "extra": "175189 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 157,
            "unit": "B/op",
            "extra": "175189 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "175189 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index)",
            "value": 432.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "291657 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 432.8,
            "unit": "ns/op",
            "extra": "291657 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "291657 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "291657 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index)",
            "value": 424.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "273056 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 424.7,
            "unit": "ns/op",
            "extra": "273056 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "273056 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "273056 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 44.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2649822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 44.4,
            "unit": "ns/op",
            "extra": "2649822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2649822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2649822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index)",
            "value": 414.4,
            "unit": "ns/op\t 154.45 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "366469 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 414.4,
            "unit": "ns/op",
            "extra": "366469 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 154.45,
            "unit": "MB/s",
            "extra": "366469 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "366469 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "366469 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index)",
            "value": 969.8,
            "unit": "ns/op\t  65.99 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "335538 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 969.8,
            "unit": "ns/op",
            "extra": "335538 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 65.99,
            "unit": "MB/s",
            "extra": "335538 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "335538 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "335538 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 26513,
            "unit": "ns/op\t 308.98 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4572 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 26513,
            "unit": "ns/op",
            "extra": "4572 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 308.98,
            "unit": "MB/s",
            "extra": "4572 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4572 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4572 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 36644,
            "unit": "ns/op\t 223.56 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "2737 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36644,
            "unit": "ns/op",
            "extra": "2737 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 223.56,
            "unit": "MB/s",
            "extra": "2737 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "2737 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "2737 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 6864578,
            "unit": "ns/op\t71593442 B/op\t     476 allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6864578,
            "unit": "ns/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593442,
            "unit": "B/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 6840962,
            "unit": "ns/op\t71593372 B/op\t     475 allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6840962,
            "unit": "ns/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593372,
            "unit": "B/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "16 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 329.9,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "317976 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 329.9,
            "unit": "ns/op",
            "extra": "317976 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "317976 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "317976 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 556,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "224172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 556,
            "unit": "ns/op",
            "extra": "224172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "224172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "224172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 160.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "698014 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 160.5,
            "unit": "ns/op",
            "extra": "698014 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "698014 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "698014 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 354.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "345829 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 354.2,
            "unit": "ns/op",
            "extra": "345829 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "345829 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "345829 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 35970,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3268 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35970,
            "unit": "ns/op",
            "extra": "3268 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3268 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3268 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 92.42,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1296585 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 92.42,
            "unit": "ns/op",
            "extra": "1296585 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1296585 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1296585 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18254,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6518 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18254,
            "unit": "ns/op",
            "extra": "6518 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6518 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6518 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 520.7,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "201969 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 520.7,
            "unit": "ns/op",
            "extra": "201969 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "201969 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "201969 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17146,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6723 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17146,
            "unit": "ns/op",
            "extra": "6723 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6723 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6723 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 470.6,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "258303 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 470.6,
            "unit": "ns/op",
            "extra": "258303 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "258303 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "258303 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 54176,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2148 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 54176,
            "unit": "ns/op",
            "extra": "2148 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2148 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2148 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 853.3,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "135566 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 853.3,
            "unit": "ns/op",
            "extra": "135566 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "135566 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "135566 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 58581,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1972 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 58581,
            "unit": "ns/op",
            "extra": "1972 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1972 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1972 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4739,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "24474 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4739,
            "unit": "ns/op",
            "extra": "24474 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "24474 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "24474 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 89971,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 89971,
            "unit": "ns/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 34356,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3444 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 34356,
            "unit": "ns/op",
            "extra": "3444 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3444 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3444 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 1946,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "55544 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1946,
            "unit": "ns/op",
            "extra": "55544 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "55544 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "55544 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1726,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "68857 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1726,
            "unit": "ns/op",
            "extra": "68857 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "68857 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "68857 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 10431,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10431,
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
            "value": 10854,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10854,
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
            "value": 38079,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38079,
            "unit": "ns/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3193 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 41079,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 41079,
            "unit": "ns/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2955 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 45120,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2535 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 45120,
            "unit": "ns/op",
            "extra": "2535 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2535 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2535 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 970.5,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "120938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 970.5,
            "unit": "ns/op",
            "extra": "120938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "120938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "120938 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53141,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53141,
            "unit": "ns/op",
            "extra": "2188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 513.6,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "226208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 513.6,
            "unit": "ns/op",
            "extra": "226208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "226208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "226208 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 21806,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5658 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21806,
            "unit": "ns/op",
            "extra": "5658 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5658 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5658 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 21850,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5209 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21850,
            "unit": "ns/op",
            "extra": "5209 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5209 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5209 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 20628,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20628,
            "unit": "ns/op",
            "extra": "5350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5350 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 20626,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5524 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20626,
            "unit": "ns/op",
            "extra": "5524 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5524 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5524 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 32654,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3418 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 32654,
            "unit": "ns/op",
            "extra": "3418 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3418 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3418 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 803,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "136992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 803,
            "unit": "ns/op",
            "extra": "136992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "136992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "136992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 29965,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3722 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 29965,
            "unit": "ns/op",
            "extra": "3722 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3722 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3722 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 866.3,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "124260 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 866.3,
            "unit": "ns/op",
            "extra": "124260 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "124260 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "124260 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 87473,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 87473,
            "unit": "ns/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1291 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2263,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "55072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2263,
            "unit": "ns/op",
            "extra": "55072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "55072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "55072 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 90555,
            "unit": "ns/op\t   33400 B/op\t      73 allocs/op",
            "extra": "1232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 90555,
            "unit": "ns/op",
            "extra": "1232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33400,
            "unit": "B/op",
            "extra": "1232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1232 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 6225,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "18124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6225,
            "unit": "ns/op",
            "extra": "18124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "18124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "18124 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 122845,
            "unit": "ns/op\t   57145 B/op\t     409 allocs/op",
            "extra": "874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 122845,
            "unit": "ns/op",
            "extra": "874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57145,
            "unit": "B/op",
            "extra": "874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 39312,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "2588 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 39312,
            "unit": "ns/op",
            "extra": "2588 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "2588 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2588 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 59.59,
            "unit": "ns/op",
            "extra": "2056593 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 152.8,
            "unit": "ns/op",
            "extra": "845042 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 162.8,
            "unit": "ns/op",
            "extra": "784644 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 176,
            "unit": "ns/op",
            "extra": "693498 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 40.92,
            "unit": "ns/op",
            "extra": "2957604 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 105.9,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 130.6,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 155.1,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 64.07,
            "unit": "ns/op",
            "extra": "1822513 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 163.1,
            "unit": "ns/op",
            "extra": "821520 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 171.3,
            "unit": "ns/op",
            "extra": "928335 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 192.1,
            "unit": "ns/op",
            "extra": "765230 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 28.58,
            "unit": "ns/op",
            "extra": "4193912 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 267.7,
            "unit": "ns/op",
            "extra": "432844 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4612,
            "unit": "ns/op",
            "extra": "26241 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 7736,
            "unit": "ns/op",
            "extra": "14968 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 51.31,
            "unit": "ns/op",
            "extra": "2763674 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 70.27,
            "unit": "ns/op",
            "extra": "1707022 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 68.81,
            "unit": "ns/op",
            "extra": "1746043 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 68.05,
            "unit": "ns/op",
            "extra": "1762058 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 9.968,
            "unit": "ns/op",
            "extra": "12018722 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 10406,
            "unit": "ns/op\t 787.27 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11124 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 10406,
            "unit": "ns/op",
            "extra": "11124 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 787.27,
            "unit": "MB/s",
            "extra": "11124 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11124 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11124 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 183.1,
            "unit": "ns/op\t1398.33 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "635516 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 183.1,
            "unit": "ns/op",
            "extra": "635516 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1398.33,
            "unit": "MB/s",
            "extra": "635516 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "635516 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "635516 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 567,
            "unit": "ns/op\t 451.52 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "202774 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 567,
            "unit": "ns/op",
            "extra": "202774 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 451.52,
            "unit": "MB/s",
            "extra": "202774 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "202774 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "202774 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2437401,
            "unit": "ns/op\t 7475974 B/op\t   40015 allocs/op",
            "extra": "48 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2437401,
            "unit": "ns/op",
            "extra": "48 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7475974,
            "unit": "B/op",
            "extra": "48 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40015,
            "unit": "allocs/op",
            "extra": "48 times\n4 procs"
          }
        ]
      }
    ]
  }
}