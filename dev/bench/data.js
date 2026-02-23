window.BENCHMARK_DATA = {
  "lastUpdate": 1771848227341,
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
          "id": "d261e03d9b96b1c3cba04f7e4ee307aeb9e47f40",
          "message": "test: harden refcount underflow tests",
          "timestamp": "2026-02-23T20:02:12+08:00",
          "tree_id": "c03d8e3dd3d04e1a91556d75b6d0d0f383ceede0",
          "url": "https://github.com/feichai0017/NoKV/commit/d261e03d9b96b1c3cba04f7e4ee307aeb9e47f40"
        },
        "date": 1771848226018,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6757,
            "unit": "ns/op\t   4.74 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "204050 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6757,
            "unit": "ns/op",
            "extra": "204050 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.74,
            "unit": "MB/s",
            "extra": "204050 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "204050 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "204050 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16978,
            "unit": "ns/op\t 241.25 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "66763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16978,
            "unit": "ns/op",
            "extra": "66763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 241.25,
            "unit": "MB/s",
            "extra": "66763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "66763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "66763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7780,
            "unit": "ns/op\t   8.23 MB/s\t   17380 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7780,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.23,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17380,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11942,
            "unit": "ns/op\t 342.98 MB/s\t   34491 B/op\t      11 allocs/op",
            "extra": "332740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11942,
            "unit": "ns/op",
            "extra": "332740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 342.98,
            "unit": "MB/s",
            "extra": "332740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34491,
            "unit": "B/op",
            "extra": "332740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "332740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122872,
            "unit": "ns/op\t 133.34 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122872,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.34,
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
            "value": 1536306,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1536306,
            "unit": "ns/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 615.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1923247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 615.6,
            "unit": "ns/op",
            "extra": "1923247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1923247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1923247 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49497,
            "unit": "ns/op\t 165.50 MB/s\t   28129 B/op\t     454 allocs/op",
            "extra": "24282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49497,
            "unit": "ns/op",
            "extra": "24282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.5,
            "unit": "MB/s",
            "extra": "24282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28129,
            "unit": "B/op",
            "extra": "24282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6605352,
            "unit": "ns/op\t67523164 B/op\t    2578 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6605352,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523164,
            "unit": "B/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 582.8,
            "unit": "ns/op\t 109.81 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2059449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 582.8,
            "unit": "ns/op",
            "extra": "2059449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 109.81,
            "unit": "MB/s",
            "extra": "2059449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2059449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2059449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9406186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.5,
            "unit": "ns/op",
            "extra": "9406186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9406186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9406186 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1402,
            "unit": "ns/op\t  45.63 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1402,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.63,
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
            "value": 522.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2561557 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 522.8,
            "unit": "ns/op",
            "extra": "2561557 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2561557 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2561557 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25711,
            "unit": "ns/op\t 318.62 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77803 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25711,
            "unit": "ns/op",
            "extra": "77803 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 318.62,
            "unit": "MB/s",
            "extra": "77803 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77803 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77803 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 163.4,
            "unit": "ns/op\t1567.16 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7337302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 163.4,
            "unit": "ns/op",
            "extra": "7337302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1567.16,
            "unit": "MB/s",
            "extra": "7337302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7337302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7337302 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 687.8,
            "unit": "ns/op\t 372.22 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3422277 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 687.8,
            "unit": "ns/op",
            "extra": "3422277 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 372.22,
            "unit": "MB/s",
            "extra": "3422277 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3422277 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3422277 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1966261,
            "unit": "ns/op\t 3064027 B/op\t   40017 allocs/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1966261,
            "unit": "ns/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064027,
            "unit": "B/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "604 times\n4 procs"
          }
        ]
      }
    ]
  }
}