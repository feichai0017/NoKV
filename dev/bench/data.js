window.BENCHMARK_DATA = {
  "lastUpdate": 1770368867733,
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
          "id": "39657d9e9e7a62cadfec7e18d721355d13490f94",
          "message": "feat: add rotating hotring integration",
          "timestamp": "2026-02-06T17:05:50+08:00",
          "tree_id": "28fbb9ad23761f6e778e2a00472056f1c23848a4",
          "url": "https://github.com/feichai0017/NoKV/commit/39657d9e9e7a62cadfec7e18d721355d13490f94"
        },
        "date": 1770368865964,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 14620,
            "unit": "ns/op\t   2.19 MB/s\t     536 B/op\t      20 allocs/op",
            "extra": "117559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 14620,
            "unit": "ns/op",
            "extra": "117559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.19,
            "unit": "MB/s",
            "extra": "117559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 536,
            "unit": "B/op",
            "extra": "117559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "117559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19400,
            "unit": "ns/op\t 211.14 MB/s\t     774 B/op\t      31 allocs/op",
            "extra": "74251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19400,
            "unit": "ns/op",
            "extra": "74251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 211.14,
            "unit": "MB/s",
            "extra": "74251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 774,
            "unit": "B/op",
            "extra": "74251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "74251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12639,
            "unit": "ns/op\t   5.06 MB/s\t   20039 B/op\t       5 allocs/op",
            "extra": "637003 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12639,
            "unit": "ns/op",
            "extra": "637003 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.06,
            "unit": "MB/s",
            "extra": "637003 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20039,
            "unit": "B/op",
            "extra": "637003 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "637003 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10830,
            "unit": "ns/op\t 378.20 MB/s\t   18443 B/op\t       7 allocs/op",
            "extra": "216234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10830,
            "unit": "ns/op",
            "extra": "216234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 378.2,
            "unit": "MB/s",
            "extra": "216234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18443,
            "unit": "B/op",
            "extra": "216234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "216234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 179196,
            "unit": "ns/op\t  91.43 MB/s\t   61657 B/op\t     681 allocs/op",
            "extra": "9046 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 179196,
            "unit": "ns/op",
            "extra": "9046 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 91.43,
            "unit": "MB/s",
            "extra": "9046 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 61657,
            "unit": "B/op",
            "extra": "9046 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 681,
            "unit": "allocs/op",
            "extra": "9046 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2191122,
            "unit": "ns/op\t       2 B/op\t       0 allocs/op",
            "extra": "501 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2191122,
            "unit": "ns/op",
            "extra": "501 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 2,
            "unit": "B/op",
            "extra": "501 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "501 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1029,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1029,
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
            "value": 48391,
            "unit": "ns/op\t 169.29 MB/s\t   25772 B/op\t     454 allocs/op",
            "extra": "23120 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48391,
            "unit": "ns/op",
            "extra": "23120 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.29,
            "unit": "MB/s",
            "extra": "23120 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25772,
            "unit": "B/op",
            "extra": "23120 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23120 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7271999,
            "unit": "ns/op\t67523390 B/op\t    2587 allocs/op",
            "extra": "153 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7271999,
            "unit": "ns/op",
            "extra": "153 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523390,
            "unit": "B/op",
            "extra": "153 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "153 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 626.4,
            "unit": "ns/op\t 102.18 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1978323 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 626.4,
            "unit": "ns/op",
            "extra": "1978323 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 102.18,
            "unit": "MB/s",
            "extra": "1978323 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1978323 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1978323 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9448509 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.6,
            "unit": "ns/op",
            "extra": "9448509 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9448509 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9448509 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1449,
            "unit": "ns/op\t  44.16 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1449,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.16,
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
            "value": 497.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2495996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 497.9,
            "unit": "ns/op",
            "extra": "2495996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2495996 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2495996 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26147,
            "unit": "ns/op\t 313.31 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76248 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26147,
            "unit": "ns/op",
            "extra": "76248 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 313.31,
            "unit": "MB/s",
            "extra": "76248 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76248 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76248 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 147.2,
            "unit": "ns/op\t1738.59 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8081076 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 147.2,
            "unit": "ns/op",
            "extra": "8081076 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1738.59,
            "unit": "MB/s",
            "extra": "8081076 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8081076 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8081076 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 754.5,
            "unit": "ns/op\t 339.28 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3095942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 754.5,
            "unit": "ns/op",
            "extra": "3095942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 339.28,
            "unit": "MB/s",
            "extra": "3095942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3095942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3095942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2030874,
            "unit": "ns/op\t 3064033 B/op\t   40019 allocs/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2030874,
            "unit": "ns/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064033,
            "unit": "B/op",
            "extra": "585 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "585 times\n4 procs"
          }
        ]
      }
    ]
  }
}