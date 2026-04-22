window.BENCHMARK_DATA = {
  "lastUpdate": 1776879869171,
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
          "id": "b8337e36743e63a45a24a1298d37610a7c91102c",
          "message": "test: raise coverage and align codecov filtering",
          "timestamp": "2026-04-23T03:40:03+10:00",
          "tree_id": "39f4f4e7d14d8ee6013e6f4e6819ee0c18c2eda9",
          "url": "https://github.com/feichai0017/NoKV/commit/b8337e36743e63a45a24a1298d37610a7c91102c"
        },
        "date": 1776879865990,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4056,
            "unit": "ns/op\t   7.89 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "26239 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4056,
            "unit": "ns/op",
            "extra": "26239 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 7.89,
            "unit": "MB/s",
            "extra": "26239 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "26239 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "26239 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 11197,
            "unit": "ns/op\t 365.80 MB/s\t     397 B/op\t      20 allocs/op",
            "extra": "10452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 11197,
            "unit": "ns/op",
            "extra": "10452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 365.8,
            "unit": "MB/s",
            "extra": "10452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 397,
            "unit": "B/op",
            "extra": "10452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "10452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 447.1,
            "unit": "ns/op\t 143.16 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "239250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 447.1,
            "unit": "ns/op",
            "extra": "239250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 143.16,
            "unit": "MB/s",
            "extra": "239250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "239250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "239250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 2661,
            "unit": "ns/op\t1539.56 MB/s\t    9160 B/op\t       7 allocs/op",
            "extra": "42296 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 2661,
            "unit": "ns/op",
            "extra": "42296 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1539.56,
            "unit": "MB/s",
            "extra": "42296 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 9160,
            "unit": "B/op",
            "extra": "42296 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "42296 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 68550,
            "unit": "ns/op\t 239.01 MB/s\t  178713 B/op\t     147 allocs/op",
            "extra": "1522 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 68550,
            "unit": "ns/op",
            "extra": "1522 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 239.01,
            "unit": "MB/s",
            "extra": "1522 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 178713,
            "unit": "B/op",
            "extra": "1522 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "1522 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 489843,
            "unit": "ns/op\t  33.45 MB/s\t   46544 B/op\t     147 allocs/op",
            "extra": "246 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 489843,
            "unit": "ns/op",
            "extra": "246 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 33.45,
            "unit": "MB/s",
            "extra": "246 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46544,
            "unit": "B/op",
            "extra": "246 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "246 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 538972,
            "unit": "ns/op\t  30.40 MB/s\t   46691 B/op\t     149 allocs/op",
            "extra": "235 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 538972,
            "unit": "ns/op",
            "extra": "235 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 30.4,
            "unit": "MB/s",
            "extra": "235 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46691,
            "unit": "B/op",
            "extra": "235 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "235 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1754866,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "67 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1754866,
            "unit": "ns/op",
            "extra": "67 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "67 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "67 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 252,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "457779 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 252,
            "unit": "ns/op",
            "extra": "457779 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "457779 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "457779 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 478.4,
            "unit": "ns/op\t 133.78 MB/s\t    1540 B/op\t       0 allocs/op",
            "extra": "225153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 478.4,
            "unit": "ns/op",
            "extra": "225153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 133.78,
            "unit": "MB/s",
            "extra": "225153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 1540,
            "unit": "B/op",
            "extra": "225153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "225153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 152,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "787154 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 152,
            "unit": "ns/op",
            "extra": "787154 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "787154 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "787154 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 129.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "855278 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 129.1,
            "unit": "ns/op",
            "extra": "855278 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "855278 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "855278 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 56.03,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2125096 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 56.03,
            "unit": "ns/op",
            "extra": "2125096 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2125096 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2125096 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 1010,
            "unit": "ns/op\t  63.34 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "168802 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 1010,
            "unit": "ns/op",
            "extra": "168802 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 63.34,
            "unit": "MB/s",
            "extra": "168802 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "168802 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "168802 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 438.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "292350 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 438.3,
            "unit": "ns/op",
            "extra": "292350 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "292350 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "292350 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 409.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "284024 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 409.4,
            "unit": "ns/op",
            "extra": "284024 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "284024 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "284024 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 45.05,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2707302 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 45.05,
            "unit": "ns/op",
            "extra": "2707302 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2707302 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2707302 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index)",
            "value": 415,
            "unit": "ns/op\t 154.20 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "299672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 415,
            "unit": "ns/op",
            "extra": "299672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 154.2,
            "unit": "MB/s",
            "extra": "299672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "299672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "299672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index)",
            "value": 1010,
            "unit": "ns/op\t  63.34 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "357648 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 1010,
            "unit": "ns/op",
            "extra": "357648 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 63.34,
            "unit": "MB/s",
            "extra": "357648 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "357648 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "357648 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 25809,
            "unit": "ns/op\t 317.40 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4503 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 25809,
            "unit": "ns/op",
            "extra": "4503 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 317.4,
            "unit": "MB/s",
            "extra": "4503 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4503 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4503 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 36072,
            "unit": "ns/op\t 227.10 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "2888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 36072,
            "unit": "ns/op",
            "extra": "2888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 227.1,
            "unit": "MB/s",
            "extra": "2888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "2888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "2888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 7071045,
            "unit": "ns/op\t71593377 B/op\t     476 allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 7071045,
            "unit": "ns/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593377,
            "unit": "B/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6957486,
            "unit": "ns/op\t71593474 B/op\t     476 allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6957486,
            "unit": "ns/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593474,
            "unit": "B/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 326.3,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "343480 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 326.3,
            "unit": "ns/op",
            "extra": "343480 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "343480 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "343480 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 542.4,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "219756 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 542.4,
            "unit": "ns/op",
            "extra": "219756 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "219756 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "219756 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 157.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "705874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 157.9,
            "unit": "ns/op",
            "extra": "705874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "705874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "705874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 350,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "333930 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 350,
            "unit": "ns/op",
            "extra": "333930 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "333930 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "333930 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 34714,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3090 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 34714,
            "unit": "ns/op",
            "extra": "3090 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3090 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3090 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 90.14,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1329771 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 90.14,
            "unit": "ns/op",
            "extra": "1329771 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1329771 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1329771 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 17969,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6435 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 17969,
            "unit": "ns/op",
            "extra": "6435 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6435 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6435 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 512.1,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "205682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 512.1,
            "unit": "ns/op",
            "extra": "205682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "205682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "205682 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 17435,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "7041 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 17435,
            "unit": "ns/op",
            "extra": "7041 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "7041 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "7041 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 466,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "223982 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 466,
            "unit": "ns/op",
            "extra": "223982 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "223982 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "223982 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 55590,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2070 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 55590,
            "unit": "ns/op",
            "extra": "2070 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2070 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2070 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 895.3,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "124530 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 895.3,
            "unit": "ns/op",
            "extra": "124530 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "124530 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "124530 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 61242,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1840 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 61242,
            "unit": "ns/op",
            "extra": "1840 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1840 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1840 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 4705,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "24538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 4705,
            "unit": "ns/op",
            "extra": "24538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "24538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "24538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 91026,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 91026,
            "unit": "ns/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1320 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 34233,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 34233,
            "unit": "ns/op",
            "extra": "3486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3486 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3486 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1948,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "52080 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1948,
            "unit": "ns/op",
            "extra": "52080 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "52080 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "52080 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1769,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "66141 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1769,
            "unit": "ns/op",
            "extra": "66141 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "66141 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "66141 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 9949,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "11420 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 9949,
            "unit": "ns/op",
            "extra": "11420 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "11420 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "11420 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10665,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10665,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 6752,
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
            "value": 38345,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3134 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 38345,
            "unit": "ns/op",
            "extra": "3134 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3134 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3134 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 41011,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2905 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 41011,
            "unit": "ns/op",
            "extra": "2905 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2905 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2905 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 45313,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 45313,
            "unit": "ns/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 965.3,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "119037 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 965.3,
            "unit": "ns/op",
            "extra": "119037 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "119037 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "119037 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 53541,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2266 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 53541,
            "unit": "ns/op",
            "extra": "2266 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2266 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2266 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 519.5,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "223897 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 519.5,
            "unit": "ns/op",
            "extra": "223897 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "223897 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "223897 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 22186,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5652 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 22186,
            "unit": "ns/op",
            "extra": "5652 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5652 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5652 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 22402,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 22402,
            "unit": "ns/op",
            "extra": "5574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21277,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5463 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21277,
            "unit": "ns/op",
            "extra": "5463 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5463 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5463 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21243,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6006 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21243,
            "unit": "ns/op",
            "extra": "6006 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6006 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6006 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 31381,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3649 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 31381,
            "unit": "ns/op",
            "extra": "3649 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3649 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3649 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 796.2,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "139784 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 796.2,
            "unit": "ns/op",
            "extra": "139784 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "139784 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "139784 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 29734,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3464 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 29734,
            "unit": "ns/op",
            "extra": "3464 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3464 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3464 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 849.5,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "127783 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 849.5,
            "unit": "ns/op",
            "extra": "127783 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "127783 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "127783 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 86750,
            "unit": "ns/op\t   30368 B/op\t      30 allocs/op",
            "extra": "1297 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 86750,
            "unit": "ns/op",
            "extra": "1297 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30368,
            "unit": "B/op",
            "extra": "1297 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1297 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2205,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "50238 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2205,
            "unit": "ns/op",
            "extra": "50238 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "50238 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "50238 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 93211,
            "unit": "ns/op\t   33401 B/op\t      73 allocs/op",
            "extra": "1222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 93211,
            "unit": "ns/op",
            "extra": "1222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 33401,
            "unit": "B/op",
            "extra": "1222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6260,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "17508 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6260,
            "unit": "ns/op",
            "extra": "17508 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "17508 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "17508 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 123502,
            "unit": "ns/op\t   57145 B/op\t     409 allocs/op",
            "extra": "922 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 123502,
            "unit": "ns/op",
            "extra": "922 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 57145,
            "unit": "B/op",
            "extra": "922 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "922 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37271,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "3002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37271,
            "unit": "ns/op",
            "extra": "3002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "3002 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "3002 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 10678,
            "unit": "ns/op\t 767.19 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "10615 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 10678,
            "unit": "ns/op",
            "extra": "10615 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 767.19,
            "unit": "MB/s",
            "extra": "10615 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "10615 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "10615 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 177.5,
            "unit": "ns/op\t1442.64 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "662980 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 177.5,
            "unit": "ns/op",
            "extra": "662980 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 1442.64,
            "unit": "MB/s",
            "extra": "662980 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "662980 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "662980 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal)",
            "value": 570.2,
            "unit": "ns/op\t 448.96 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "211324 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 570.2,
            "unit": "ns/op",
            "extra": "211324 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - MB/s",
            "value": 448.96,
            "unit": "MB/s",
            "extra": "211324 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "211324 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "211324 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal)",
            "value": 2403130,
            "unit": "ns/op\t 7475863 B/op\t   40015 allocs/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 2403130,
            "unit": "ns/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 7475863,
            "unit": "B/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 40015,
            "unit": "allocs/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/thermos)",
            "value": 27.78,
            "unit": "ns/op",
            "extra": "4998938 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/thermos)",
            "value": 57.6,
            "unit": "ns/op",
            "extra": "2039804 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/thermos)",
            "value": 20.56,
            "unit": "ns/op",
            "extra": "5849239 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/thermos)",
            "value": 16.82,
            "unit": "ns/op",
            "extra": "7205366 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/thermos)",
            "value": 19887019,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/thermos)",
            "value": 79.99,
            "unit": "ns/op",
            "extra": "1499016 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/thermos)",
            "value": 56088,
            "unit": "ns/op",
            "extra": "1950 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 54.69,
            "unit": "ns/op",
            "extra": "2316654 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 155.5,
            "unit": "ns/op",
            "extra": "827517 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 165.9,
            "unit": "ns/op",
            "extra": "929552 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 190,
            "unit": "ns/op",
            "extra": "664466 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 43.62,
            "unit": "ns/op",
            "extra": "2611330 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 116.7,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 132.8,
            "unit": "ns/op",
            "extra": "942206 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 158.5,
            "unit": "ns/op",
            "extra": "801728 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 62.66,
            "unit": "ns/op",
            "extra": "1960995 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 168.6,
            "unit": "ns/op",
            "extra": "769492 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 178.6,
            "unit": "ns/op",
            "extra": "738026 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 199.9,
            "unit": "ns/op",
            "extra": "868941 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.15,
            "unit": "ns/op",
            "extra": "4454079 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 283.8,
            "unit": "ns/op",
            "extra": "400353 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4801,
            "unit": "ns/op",
            "extra": "24764 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 8224,
            "unit": "ns/op",
            "extra": "14564 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 54.56,
            "unit": "ns/op",
            "extra": "2895705 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 73.43,
            "unit": "ns/op",
            "extra": "1668948 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 71.57,
            "unit": "ns/op",
            "extra": "1670356 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 71.62,
            "unit": "ns/op",
            "extra": "1684023 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 10.22,
            "unit": "ns/op",
            "extra": "11974993 times\n4 procs"
          }
        ]
      }
    ]
  }
}