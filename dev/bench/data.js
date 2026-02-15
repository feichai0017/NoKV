window.BENCHMARK_DATA = {
  "lastUpdate": 1771151765991,
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
          "id": "e7503188813218b2a8d55e38505798bfd2da7595",
          "message": "docs: add vfs design note and flow comparison",
          "timestamp": "2026-02-15T18:34:43+08:00",
          "tree_id": "511cc766514f3606070d656b93a6c582740333c0",
          "url": "https://github.com/feichai0017/NoKV/commit/e7503188813218b2a8d55e38505798bfd2da7595"
        },
        "date": 1771151764603,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8078,
            "unit": "ns/op\t   3.96 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "144079 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8078,
            "unit": "ns/op",
            "extra": "144079 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.96,
            "unit": "MB/s",
            "extra": "144079 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "144079 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "144079 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18440,
            "unit": "ns/op\t 222.13 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "66236 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18440,
            "unit": "ns/op",
            "extra": "66236 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.13,
            "unit": "MB/s",
            "extra": "66236 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "66236 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "66236 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8583,
            "unit": "ns/op\t   7.46 MB/s\t   19982 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8583,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.46,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19982,
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
            "value": 12076,
            "unit": "ns/op\t 339.19 MB/s\t   33599 B/op\t      11 allocs/op",
            "extra": "327133 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12076,
            "unit": "ns/op",
            "extra": "327133 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 339.19,
            "unit": "MB/s",
            "extra": "327133 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33599,
            "unit": "B/op",
            "extra": "327133 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "327133 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120756,
            "unit": "ns/op\t 135.68 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120756,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.68,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56847,
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
            "value": 1493284,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1493284,
            "unit": "ns/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 604.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1938560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 604.1,
            "unit": "ns/op",
            "extra": "1938560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1938560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1938560 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49180,
            "unit": "ns/op\t 166.57 MB/s\t   27781 B/op\t     454 allocs/op",
            "extra": "25068 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49180,
            "unit": "ns/op",
            "extra": "25068 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.57,
            "unit": "MB/s",
            "extra": "25068 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27781,
            "unit": "B/op",
            "extra": "25068 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25068 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6617942,
            "unit": "ns/op\t67523064 B/op\t    2579 allocs/op",
            "extra": "158 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6617942,
            "unit": "ns/op",
            "extra": "158 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523064,
            "unit": "B/op",
            "extra": "158 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "158 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 601.2,
            "unit": "ns/op\t 106.45 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1999501 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 601.2,
            "unit": "ns/op",
            "extra": "1999501 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 106.45,
            "unit": "MB/s",
            "extra": "1999501 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1999501 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1999501 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9337465 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.1,
            "unit": "ns/op",
            "extra": "9337465 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9337465 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9337465 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1411,
            "unit": "ns/op\t  45.36 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1411,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.36,
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
            "value": 465.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2582014 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 465.1,
            "unit": "ns/op",
            "extra": "2582014 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2582014 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2582014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27017,
            "unit": "ns/op\t 303.21 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27017,
            "unit": "ns/op",
            "extra": "73504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.21,
            "unit": "MB/s",
            "extra": "73504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.5,
            "unit": "ns/op\t1645.99 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7545476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.5,
            "unit": "ns/op",
            "extra": "7545476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1645.99,
            "unit": "MB/s",
            "extra": "7545476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7545476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7545476 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.7,
            "unit": "ns/op\t 367.95 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3417955 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.7,
            "unit": "ns/op",
            "extra": "3417955 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 367.95,
            "unit": "MB/s",
            "extra": "3417955 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3417955 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3417955 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2018217,
            "unit": "ns/op\t 3064038 B/op\t   40018 allocs/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2018217,
            "unit": "ns/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064038,
            "unit": "B/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "585 times\n4 procs"
          }
        ]
      }
    ]
  }
}