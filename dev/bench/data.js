window.BENCHMARK_DATA = {
  "lastUpdate": 1776095559975,
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
          "id": "1e96d4f5b135653a18ee06bee5b059308af35bf5",
          "message": "refactor: centralize raftstore error surfaces",
          "timestamp": "2026-04-14T01:50:02+10:00",
          "tree_id": "12dacc8cab09556d1745f68c5c27de48f8431bcb",
          "url": "https://github.com/feichai0017/NoKV/commit/1e96d4f5b135653a18ee06bee5b059308af35bf5"
        },
        "date": 1776095557530,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4085,
            "unit": "ns/op\t   7.83 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "30274 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4085,
            "unit": "ns/op",
            "extra": "30274 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 7.83,
            "unit": "MB/s",
            "extra": "30274 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "30274 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "30274 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 17871,
            "unit": "ns/op\t 229.20 MB/s\t     398 B/op\t      20 allocs/op",
            "extra": "7269 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 17871,
            "unit": "ns/op",
            "extra": "7269 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 229.2,
            "unit": "MB/s",
            "extra": "7269 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 398,
            "unit": "B/op",
            "extra": "7269 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "7269 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1051,
            "unit": "ns/op\t  60.90 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "169056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1051,
            "unit": "ns/op",
            "extra": "169056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 60.9,
            "unit": "MB/s",
            "extra": "169056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "169056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "169056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 3971,
            "unit": "ns/op\t1031.46 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "31162 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 3971,
            "unit": "ns/op",
            "extra": "31162 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1031.46,
            "unit": "MB/s",
            "extra": "31162 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "31162 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "31162 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 62121,
            "unit": "ns/op\t 263.74 MB/s\t  178144 B/op\t     147 allocs/op",
            "extra": "2038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 62121,
            "unit": "ns/op",
            "extra": "2038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 263.74,
            "unit": "MB/s",
            "extra": "2038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 178144,
            "unit": "B/op",
            "extra": "2038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "2038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 472322,
            "unit": "ns/op\t  34.69 MB/s\t   46522 B/op\t     147 allocs/op",
            "extra": "230 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 472322,
            "unit": "ns/op",
            "extra": "230 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 34.69,
            "unit": "MB/s",
            "extra": "230 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46522,
            "unit": "B/op",
            "extra": "230 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "230 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 382541,
            "unit": "ns/op\t  42.83 MB/s\t   46607 B/op\t     148 allocs/op",
            "extra": "318 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 382541,
            "unit": "ns/op",
            "extra": "318 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 42.83,
            "unit": "MB/s",
            "extra": "318 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46607,
            "unit": "B/op",
            "extra": "318 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "318 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1743750,
            "unit": "ns/op\t      63 B/op\t       0 allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1743750,
            "unit": "ns/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 63,
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
            "value": 235.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "510900 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 235.6,
            "unit": "ns/op",
            "extra": "510900 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "510900 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "510900 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.09,
            "unit": "ns/op",
            "extra": "5165133 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 55.46,
            "unit": "ns/op",
            "extra": "2205744 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20.03,
            "unit": "ns/op",
            "extra": "5975538 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 17.42,
            "unit": "ns/op",
            "extra": "7377668 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 20256428,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 80.22,
            "unit": "ns/op",
            "extra": "1363870 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 31548,
            "unit": "ns/op",
            "extra": "3619 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index)",
            "value": 446.3,
            "unit": "ns/op\t 143.40 MB/s\t    1539 B/op\t       0 allocs/op",
            "extra": "247142 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 446.3,
            "unit": "ns/op",
            "extra": "247142 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 143.4,
            "unit": "MB/s",
            "extra": "247142 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 1539,
            "unit": "B/op",
            "extra": "247142 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "247142 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index)",
            "value": 157.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "748756 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 157.6,
            "unit": "ns/op",
            "extra": "748756 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "748756 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "748756 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index)",
            "value": 130.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "896703 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 130.6,
            "unit": "ns/op",
            "extra": "896703 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "896703 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "896703 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 56.61,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2235206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 56.61,
            "unit": "ns/op",
            "extra": "2235206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2235206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2235206 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index)",
            "value": 941.2,
            "unit": "ns/op\t  68.00 MB/s\t     157 B/op\t       1 allocs/op",
            "extra": "167112 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 941.2,
            "unit": "ns/op",
            "extra": "167112 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 68,
            "unit": "MB/s",
            "extra": "167112 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - B/op",
            "value": 157,
            "unit": "B/op",
            "extra": "167112 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "167112 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index)",
            "value": 413.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "305095 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 413.9,
            "unit": "ns/op",
            "extra": "305095 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "305095 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "305095 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index)",
            "value": 455.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "299468 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 455.8,
            "unit": "ns/op",
            "extra": "299468 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "299468 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "299468 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index)",
            "value": 42.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2784564 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 42.7,
            "unit": "ns/op",
            "extra": "2784564 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2784564 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2784564 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index)",
            "value": 368,
            "unit": "ns/op\t 173.89 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "376953 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 368,
            "unit": "ns/op",
            "extra": "376953 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 173.89,
            "unit": "MB/s",
            "extra": "376953 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "376953 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "376953 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index)",
            "value": 870,
            "unit": "ns/op\t  73.56 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "315972 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - ns/op",
            "value": 870,
            "unit": "ns/op",
            "extra": "315972 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - MB/s",
            "value": 73.56,
            "unit": "MB/s",
            "extra": "315972 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "315972 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "315972 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm)",
            "value": 24614,
            "unit": "ns/op\t 332.82 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4711 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 24614,
            "unit": "ns/op",
            "extra": "4711 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 332.82,
            "unit": "MB/s",
            "extra": "4711 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4711 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4711 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 31083,
            "unit": "ns/op\t 263.55 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3567 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 31083,
            "unit": "ns/op",
            "extra": "3567 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 263.55,
            "unit": "MB/s",
            "extra": "3567 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3567 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3567 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm)",
            "value": 5166913,
            "unit": "ns/op\t71593332 B/op\t     475 allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 5166913,
            "unit": "ns/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593332,
            "unit": "B/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 475,
            "unit": "allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 5164851,
            "unit": "ns/op\t71593207 B/op\t     473 allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 5164851,
            "unit": "ns/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593207,
            "unit": "B/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 473,
            "unit": "allocs/op",
            "extra": "37 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm)",
            "value": 289.8,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "374515 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 289.8,
            "unit": "ns/op",
            "extra": "374515 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "374515 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "374515 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 446.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "240330 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 446.3,
            "unit": "ns/op",
            "extra": "240330 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "240330 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "240330 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm)",
            "value": 152.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "668984 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 152.9,
            "unit": "ns/op",
            "extra": "668984 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "668984 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "668984 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm)",
            "value": 362.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "351885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 362.4,
            "unit": "ns/op",
            "extra": "351885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "351885 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "351885 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 30399,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3986 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 30399,
            "unit": "ns/op",
            "extra": "3986 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3986 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3986 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 79.53,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1516438 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 79.53,
            "unit": "ns/op",
            "extra": "1516438 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1516438 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1516438 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 14926,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "7605 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 14926,
            "unit": "ns/op",
            "extra": "7605 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "7605 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "7605 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 442.2,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "236528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 442.2,
            "unit": "ns/op",
            "extra": "236528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "236528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "236528 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 14664,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "8044 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 14664,
            "unit": "ns/op",
            "extra": "8044 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "8044 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "8044 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 420.1,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "273500 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 420.1,
            "unit": "ns/op",
            "extra": "273500 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "273500 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "273500 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 51257,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2217 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 51257,
            "unit": "ns/op",
            "extra": "2217 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2217 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2217 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 791.6,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "131630 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 791.6,
            "unit": "ns/op",
            "extra": "131630 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "131630 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "131630 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 55072,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "2065 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 55072,
            "unit": "ns/op",
            "extra": "2065 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "2065 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "2065 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4224,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "27920 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4224,
            "unit": "ns/op",
            "extra": "27920 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "27920 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "27920 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 91289,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1396 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 91289,
            "unit": "ns/op",
            "extra": "1396 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1396 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1396 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 29692,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3909 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 29692,
            "unit": "ns/op",
            "extra": "3909 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3909 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3909 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 1729,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "70676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1729,
            "unit": "ns/op",
            "extra": "70676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "70676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "70676 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1569,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "80266 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1569,
            "unit": "ns/op",
            "extra": "80266 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "80266 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "80266 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 8684,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "12066 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 8684,
            "unit": "ns/op",
            "extra": "12066 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "12066 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "12066 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 9127,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "12338 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 9127,
            "unit": "ns/op",
            "extra": "12338 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 6752,
            "unit": "B/op",
            "extra": "12338 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "12338 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 34327,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3570 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 34327,
            "unit": "ns/op",
            "extra": "3570 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3570 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3570 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 36713,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "3345 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36713,
            "unit": "ns/op",
            "extra": "3345 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "3345 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "3345 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 40959,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2787 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40959,
            "unit": "ns/op",
            "extra": "2787 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2787 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2787 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 915,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "118771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 915,
            "unit": "ns/op",
            "extra": "118771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "118771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "118771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 48720,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 48720,
            "unit": "ns/op",
            "extra": "2412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 463.8,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "252538 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 463.8,
            "unit": "ns/op",
            "extra": "252538 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "252538 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "252538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18997,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "6468 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18997,
            "unit": "ns/op",
            "extra": "6468 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "6468 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "6468 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 18838,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "6610 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18838,
            "unit": "ns/op",
            "extra": "6610 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "6610 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "6610 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 18787,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6618 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18787,
            "unit": "ns/op",
            "extra": "6618 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6618 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6618 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 18362,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5608 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 18362,
            "unit": "ns/op",
            "extra": "5608 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5608 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 28451,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3813 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 28451,
            "unit": "ns/op",
            "extra": "3813 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3813 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3813 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 786.2,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "135344 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 786.2,
            "unit": "ns/op",
            "extra": "135344 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "135344 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "135344 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 26002,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "4106 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 26002,
            "unit": "ns/op",
            "extra": "4106 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "4106 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "4106 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 844.2,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "138188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 844.2,
            "unit": "ns/op",
            "extra": "138188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "138188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "138188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 83245,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1399 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 83245,
            "unit": "ns/op",
            "extra": "1399 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1399 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1399 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2259,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "54898 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2259,
            "unit": "ns/op",
            "extra": "54898 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "54898 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "54898 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 87166,
            "unit": "ns/op\t   33400 B/op\t      73 allocs/op",
            "extra": "1281 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 87166,
            "unit": "ns/op",
            "extra": "1281 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33400,
            "unit": "B/op",
            "extra": "1281 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1281 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 5834,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "19090 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 5834,
            "unit": "ns/op",
            "extra": "19090 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "19090 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "19090 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 116118,
            "unit": "ns/op\t   57145 B/op\t     409 allocs/op",
            "extra": "980 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 116118,
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
            "value": 34632,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "3206 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 34632,
            "unit": "ns/op",
            "extra": "3206 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "3206 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "3206 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 52.48,
            "unit": "ns/op",
            "extra": "2253775 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 190.5,
            "unit": "ns/op",
            "extra": "754334 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 187.6,
            "unit": "ns/op",
            "extra": "675277 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 178,
            "unit": "ns/op",
            "extra": "728468 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 42.41,
            "unit": "ns/op",
            "extra": "2833587 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 125.8,
            "unit": "ns/op",
            "extra": "975657 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 139.3,
            "unit": "ns/op",
            "extra": "789301 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 178.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 55.21,
            "unit": "ns/op",
            "extra": "2196518 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 190.6,
            "unit": "ns/op",
            "extra": "715359 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 220.5,
            "unit": "ns/op",
            "extra": "661023 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 235.1,
            "unit": "ns/op",
            "extra": "561662 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 28.11,
            "unit": "ns/op",
            "extra": "4242796 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 295,
            "unit": "ns/op",
            "extra": "362660 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 5363,
            "unit": "ns/op",
            "extra": "22573 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8157,
            "unit": "ns/op",
            "extra": "14218 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 47.88,
            "unit": "ns/op",
            "extra": "2612067 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 89.26,
            "unit": "ns/op",
            "extra": "1347034 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 87.5,
            "unit": "ns/op",
            "extra": "1381824 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 85.46,
            "unit": "ns/op",
            "extra": "1407710 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 11.18,
            "unit": "ns/op",
            "extra": "10691353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 9829,
            "unit": "ns/op\t 833.46 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11283 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 9829,
            "unit": "ns/op",
            "extra": "11283 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 833.46,
            "unit": "MB/s",
            "extra": "11283 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11283 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11283 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 158.7,
            "unit": "ns/op\t1613.59 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "734559 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 158.7,
            "unit": "ns/op",
            "extra": "734559 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1613.59,
            "unit": "MB/s",
            "extra": "734559 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "734559 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "734559 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 501.2,
            "unit": "ns/op\t 510.75 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "215500 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 501.2,
            "unit": "ns/op",
            "extra": "215500 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 510.75,
            "unit": "MB/s",
            "extra": "215500 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "215500 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "215500 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2100713,
            "unit": "ns/op\t 7475894 B/op\t   40016 allocs/op",
            "extra": "57 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2100713,
            "unit": "ns/op",
            "extra": "57 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7475894,
            "unit": "B/op",
            "extra": "57 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "57 times\n4 procs"
          }
        ]
      }
    ]
  }
}