window.BENCHMARK_DATA = {
  "lastUpdate": 1769964692062,
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
          "id": "8e3bf1c998ea3d85b451d06b104f5d8a84276412",
          "message": "Update docs",
          "timestamp": "2026-02-02T00:50:27+08:00",
          "tree_id": "6ce0db80325485c06f7f5dca7417c395a5bc9141",
          "url": "https://github.com/feichai0017/NoKV/commit/8e3bf1c998ea3d85b451d06b104f5d8a84276412"
        },
        "date": 1769964691356,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11931,
            "unit": "ns/op\t   2.68 MB/s\t     617 B/op\t      24 allocs/op",
            "extra": "125448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11931,
            "unit": "ns/op",
            "extra": "125448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.68,
            "unit": "MB/s",
            "extra": "125448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 617,
            "unit": "B/op",
            "extra": "125448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "125448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15534,
            "unit": "ns/op\t 263.68 MB/s\t     694 B/op\t      27 allocs/op",
            "extra": "87181 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15534,
            "unit": "ns/op",
            "extra": "87181 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 263.68,
            "unit": "MB/s",
            "extra": "87181 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 694,
            "unit": "B/op",
            "extra": "87181 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "87181 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11666,
            "unit": "ns/op\t   5.49 MB/s\t   18656 B/op\t       5 allocs/op",
            "extra": "697065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11666,
            "unit": "ns/op",
            "extra": "697065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.49,
            "unit": "MB/s",
            "extra": "697065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18656,
            "unit": "B/op",
            "extra": "697065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "697065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10325,
            "unit": "ns/op\t 396.70 MB/s\t   18337 B/op\t       7 allocs/op",
            "extra": "234955 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10325,
            "unit": "ns/op",
            "extra": "234955 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 396.7,
            "unit": "MB/s",
            "extra": "234955 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18337,
            "unit": "B/op",
            "extra": "234955 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "234955 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 182651,
            "unit": "ns/op\t  89.70 MB/s\t   61400 B/op\t     671 allocs/op",
            "extra": "9648 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 182651,
            "unit": "ns/op",
            "extra": "9648 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 89.7,
            "unit": "MB/s",
            "extra": "9648 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 61400,
            "unit": "B/op",
            "extra": "9648 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 671,
            "unit": "allocs/op",
            "extra": "9648 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2274578,
            "unit": "ns/op\t    9246 B/op\t       0 allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2274578,
            "unit": "ns/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9246,
            "unit": "B/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1004,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1004,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49997,
            "unit": "ns/op\t 163.85 MB/s\t   25709 B/op\t     454 allocs/op",
            "extra": "23286 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49997,
            "unit": "ns/op",
            "extra": "23286 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.85,
            "unit": "MB/s",
            "extra": "23286 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25709,
            "unit": "B/op",
            "extra": "23286 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23286 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6861811,
            "unit": "ns/op\t67523443 B/op\t    2586 allocs/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6861811,
            "unit": "ns/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523443,
            "unit": "B/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 632.8,
            "unit": "ns/op\t 101.15 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2119430 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 632.8,
            "unit": "ns/op",
            "extra": "2119430 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 101.15,
            "unit": "MB/s",
            "extra": "2119430 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2119430 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2119430 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9362239 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.5,
            "unit": "ns/op",
            "extra": "9362239 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9362239 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9362239 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1408,
            "unit": "ns/op\t  45.47 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1408,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.47,
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
            "value": 479.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2093404 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 479.4,
            "unit": "ns/op",
            "extra": "2093404 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2093404 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2093404 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25950,
            "unit": "ns/op\t 315.69 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "75741 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25950,
            "unit": "ns/op",
            "extra": "75741 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.69,
            "unit": "MB/s",
            "extra": "75741 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "75741 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75741 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 147.9,
            "unit": "ns/op\t1730.88 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8036960 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 147.9,
            "unit": "ns/op",
            "extra": "8036960 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1730.88,
            "unit": "MB/s",
            "extra": "8036960 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8036960 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8036960 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 733.7,
            "unit": "ns/op\t 348.91 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3187425 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 733.7,
            "unit": "ns/op",
            "extra": "3187425 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 348.91,
            "unit": "MB/s",
            "extra": "3187425 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3187425 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3187425 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2025099,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2025099,
            "unit": "ns/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "586 times\n4 procs"
          }
        ]
      }
    ]
  }
}