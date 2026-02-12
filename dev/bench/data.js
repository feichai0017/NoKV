window.BENCHMARK_DATA = {
  "lastUpdate": 1770873513128,
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
          "id": "ddcba127dfac486357fbcc54697427884b04534a",
          "message": "refactor: remove cache hot tier and simplify prefetch path",
          "timestamp": "2026-02-12T13:17:21+08:00",
          "tree_id": "a9453308a7dc06813c68930c76d50e0e04624b20",
          "url": "https://github.com/feichai0017/NoKV/commit/ddcba127dfac486357fbcc54697427884b04534a"
        },
        "date": 1770873511303,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7367,
            "unit": "ns/op\t   4.34 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "166639 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7367,
            "unit": "ns/op",
            "extra": "166639 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.34,
            "unit": "MB/s",
            "extra": "166639 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "166639 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "166639 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14179,
            "unit": "ns/op\t 288.89 MB/s\t     641 B/op\t      29 allocs/op",
            "extra": "114190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14179,
            "unit": "ns/op",
            "extra": "114190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 288.89,
            "unit": "MB/s",
            "extra": "114190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 641,
            "unit": "B/op",
            "extra": "114190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "114190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 6984,
            "unit": "ns/op\t   9.16 MB/s\t   17087 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 6984,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 9.16,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17087,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11276,
            "unit": "ns/op\t 363.26 MB/s\t   30169 B/op\t       8 allocs/op",
            "extra": "373998 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11276,
            "unit": "ns/op",
            "extra": "373998 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 363.26,
            "unit": "MB/s",
            "extra": "373998 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 30169,
            "unit": "B/op",
            "extra": "373998 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "373998 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123557,
            "unit": "ns/op\t 132.60 MB/s\t   56856 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123557,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.6,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56856,
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
            "value": 1596197,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "741 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1596197,
            "unit": "ns/op",
            "extra": "741 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "741 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "741 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 572.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2061320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 572.4,
            "unit": "ns/op",
            "extra": "2061320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2061320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2061320 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49412,
            "unit": "ns/op\t 165.79 MB/s\t   27783 B/op\t     454 allocs/op",
            "extra": "25062 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49412,
            "unit": "ns/op",
            "extra": "25062 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.79,
            "unit": "MB/s",
            "extra": "25062 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27783,
            "unit": "B/op",
            "extra": "25062 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25062 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8835146,
            "unit": "ns/op\t67523413 B/op\t    2587 allocs/op",
            "extra": "130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8835146,
            "unit": "ns/op",
            "extra": "130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523413,
            "unit": "B/op",
            "extra": "130 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "130 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 662.3,
            "unit": "ns/op\t  96.64 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1939402 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 662.3,
            "unit": "ns/op",
            "extra": "1939402 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 96.64,
            "unit": "MB/s",
            "extra": "1939402 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1939402 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1939402 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 116,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10455423 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 116,
            "unit": "ns/op",
            "extra": "10455423 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10455423 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10455423 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1301,
            "unit": "ns/op\t  49.19 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1301,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 49.19,
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
            "value": 472.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2418261 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 472.2,
            "unit": "ns/op",
            "extra": "2418261 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2418261 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2418261 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 23107,
            "unit": "ns/op\t 354.53 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "99628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 23107,
            "unit": "ns/op",
            "extra": "99628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 354.53,
            "unit": "MB/s",
            "extra": "99628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "99628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "99628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 151.1,
            "unit": "ns/op\t1693.97 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7912527 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 151.1,
            "unit": "ns/op",
            "extra": "7912527 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1693.97,
            "unit": "MB/s",
            "extra": "7912527 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7912527 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7912527 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 618.8,
            "unit": "ns/op\t 413.68 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3916144 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 618.8,
            "unit": "ns/op",
            "extra": "3916144 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 413.68,
            "unit": "MB/s",
            "extra": "3916144 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3916144 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3916144 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2064695,
            "unit": "ns/op\t 3064048 B/op\t   40019 allocs/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2064695,
            "unit": "ns/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064048,
            "unit": "B/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "577 times\n4 procs"
          }
        ]
      }
    ]
  }
}