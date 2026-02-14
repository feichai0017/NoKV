window.BENCHMARK_DATA = {
  "lastUpdate": 1771085132261,
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
          "id": "9f34c96bc722589c546cdcad5a6cb3fe8d1e6e54",
          "message": "ci: simplify label taxonomy and tighten issue auto-label triggers",
          "timestamp": "2026-02-15T00:04:08+08:00",
          "tree_id": "2bbacbfc1095afe32a44e1c95f40bba212300585",
          "url": "https://github.com/feichai0017/NoKV/commit/9f34c96bc722589c546cdcad5a6cb3fe8d1e6e54"
        },
        "date": 1771085130931,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6905,
            "unit": "ns/op\t   4.63 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "157166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6905,
            "unit": "ns/op",
            "extra": "157166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.63,
            "unit": "MB/s",
            "extra": "157166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "157166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "157166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16520,
            "unit": "ns/op\t 247.94 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "84926 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16520,
            "unit": "ns/op",
            "extra": "84926 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 247.94,
            "unit": "MB/s",
            "extra": "84926 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "84926 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "84926 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7929,
            "unit": "ns/op\t   8.07 MB/s\t   17857 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7929,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.07,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17857,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 12455,
            "unit": "ns/op\t 328.86 MB/s\t   34159 B/op\t      11 allocs/op",
            "extra": "322404 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12455,
            "unit": "ns/op",
            "extra": "322404 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 328.86,
            "unit": "MB/s",
            "extra": "322404 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34159,
            "unit": "B/op",
            "extra": "322404 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "322404 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124009,
            "unit": "ns/op\t 132.12 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124009,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.12,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56848,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 659,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 1492377,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1492377,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 591,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2084252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 591,
            "unit": "ns/op",
            "extra": "2084252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2084252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2084252 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49978,
            "unit": "ns/op\t 163.91 MB/s\t   27782 B/op\t     454 allocs/op",
            "extra": "25065 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49978,
            "unit": "ns/op",
            "extra": "25065 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.91,
            "unit": "MB/s",
            "extra": "25065 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27782,
            "unit": "B/op",
            "extra": "25065 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25065 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6933570,
            "unit": "ns/op\t67523141 B/op\t    2579 allocs/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6933570,
            "unit": "ns/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523141,
            "unit": "B/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 569.5,
            "unit": "ns/op\t 112.37 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2469541 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 569.5,
            "unit": "ns/op",
            "extra": "2469541 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 112.37,
            "unit": "MB/s",
            "extra": "2469541 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2469541 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2469541 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9399826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.8,
            "unit": "ns/op",
            "extra": "9399826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9399826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9399826 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1341,
            "unit": "ns/op\t  47.73 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1341,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47.73,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet",
            "value": 468.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2663547 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 468.6,
            "unit": "ns/op",
            "extra": "2663547 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2663547 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2663547 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26676,
            "unit": "ns/op\t 307.10 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26676,
            "unit": "ns/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.1,
            "unit": "MB/s",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.8,
            "unit": "ns/op\t1643.30 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7663082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.8,
            "unit": "ns/op",
            "extra": "7663082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1643.3,
            "unit": "MB/s",
            "extra": "7663082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7663082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7663082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 687,
            "unit": "ns/op\t 372.64 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3455760 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 687,
            "unit": "ns/op",
            "extra": "3455760 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 372.64,
            "unit": "MB/s",
            "extra": "3455760 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3455760 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3455760 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2029457,
            "unit": "ns/op\t 3064036 B/op\t   40017 allocs/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2029457,
            "unit": "ns/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064036,
            "unit": "B/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "590 times\n4 procs"
          }
        ]
      }
    ]
  }
}