window.BENCHMARK_DATA = {
  "lastUpdate": 1771151324256,
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
          "id": "7d8d92ac9ca7e0f3a8d859df40ea03794a649565",
          "message": "refactor: unify fault injection policy and make file descriptor optional",
          "timestamp": "2026-02-15T18:27:29+08:00",
          "tree_id": "b1f771850ad713cfcfe052029fcbe0a4eb84d08d",
          "url": "https://github.com/feichai0017/NoKV/commit/7d8d92ac9ca7e0f3a8d859df40ea03794a649565"
        },
        "date": 1771151323434,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8201,
            "unit": "ns/op\t   3.90 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "163268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8201,
            "unit": "ns/op",
            "extra": "163268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.9,
            "unit": "MB/s",
            "extra": "163268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "163268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "163268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16844,
            "unit": "ns/op\t 243.17 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "69483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16844,
            "unit": "ns/op",
            "extra": "69483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 243.17,
            "unit": "MB/s",
            "extra": "69483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "69483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "69483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8341,
            "unit": "ns/op\t   7.67 MB/s\t   18114 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8341,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.67,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18114,
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
            "value": 12144,
            "unit": "ns/op\t 337.28 MB/s\t   33952 B/op\t      11 allocs/op",
            "extra": "339387 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12144,
            "unit": "ns/op",
            "extra": "339387 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.28,
            "unit": "MB/s",
            "extra": "339387 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33952,
            "unit": "B/op",
            "extra": "339387 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "339387 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124780,
            "unit": "ns/op\t 131.30 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124780,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 131.3,
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
            "value": 1489322,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1489322,
            "unit": "ns/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 579.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2043579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 579.2,
            "unit": "ns/op",
            "extra": "2043579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2043579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2043579 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 47622,
            "unit": "ns/op\t 172.02 MB/s\t   27463 B/op\t     454 allocs/op",
            "extra": "25830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47622,
            "unit": "ns/op",
            "extra": "25830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 172.02,
            "unit": "MB/s",
            "extra": "25830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27463,
            "unit": "B/op",
            "extra": "25830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6263471,
            "unit": "ns/op\t67522995 B/op\t    2579 allocs/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6263471,
            "unit": "ns/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67522995,
            "unit": "B/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 521.7,
            "unit": "ns/op\t 122.69 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2182789 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 521.7,
            "unit": "ns/op",
            "extra": "2182789 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 122.69,
            "unit": "MB/s",
            "extra": "2182789 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2182789 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2182789 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 131.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9215928 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 131.2,
            "unit": "ns/op",
            "extra": "9215928 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9215928 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9215928 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1413,
            "unit": "ns/op\t  45.28 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1413,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.28,
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
            "value": 462.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2440642 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 462.1,
            "unit": "ns/op",
            "extra": "2440642 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2440642 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2440642 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27023,
            "unit": "ns/op\t 303.15 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73875 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27023,
            "unit": "ns/op",
            "extra": "73875 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.15,
            "unit": "MB/s",
            "extra": "73875 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73875 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73875 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.1,
            "unit": "ns/op\t1629.70 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7506159 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.1,
            "unit": "ns/op",
            "extra": "7506159 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1629.7,
            "unit": "MB/s",
            "extra": "7506159 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7506159 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7506159 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 691.6,
            "unit": "ns/op\t 370.18 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3438819 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 691.6,
            "unit": "ns/op",
            "extra": "3438819 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.18,
            "unit": "MB/s",
            "extra": "3438819 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3438819 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3438819 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2025558,
            "unit": "ns/op\t 3064031 B/op\t   40017 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2025558,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064031,
            "unit": "B/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "592 times\n4 procs"
          }
        ]
      }
    ]
  }
}