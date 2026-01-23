window.BENCHMARK_DATA = {
  "lastUpdate": 1769199910871,
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
          "id": "664fd71457750d94732910edf78b74239afc7224",
          "message": "bench: add micro benchmarks and workflow",
          "timestamp": "2026-01-24T04:23:56+08:00",
          "tree_id": "59c662c37d260df625e77b1b17fd229dff385cdc",
          "url": "https://github.com/feichai0017/NoKV/commit/664fd71457750d94732910edf78b74239afc7224"
        },
        "date": 1769199910194,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13274,
            "unit": "ns/op\t   2.41 MB/s\t     634 B/op\t      24 allocs/op",
            "extra": "122280 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13274,
            "unit": "ns/op",
            "extra": "122280 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.41,
            "unit": "MB/s",
            "extra": "122280 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 634,
            "unit": "B/op",
            "extra": "122280 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "122280 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15916,
            "unit": "ns/op\t 257.36 MB/s\t     664 B/op\t      27 allocs/op",
            "extra": "89041 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15916,
            "unit": "ns/op",
            "extra": "89041 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 257.36,
            "unit": "MB/s",
            "extra": "89041 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 664,
            "unit": "B/op",
            "extra": "89041 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "89041 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12262,
            "unit": "ns/op\t   5.22 MB/s\t   20072 B/op\t       5 allocs/op",
            "extra": "659402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12262,
            "unit": "ns/op",
            "extra": "659402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.22,
            "unit": "MB/s",
            "extra": "659402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20072,
            "unit": "B/op",
            "extra": "659402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "659402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10227,
            "unit": "ns/op\t 400.49 MB/s\t   18202 B/op\t       7 allocs/op",
            "extra": "236827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10227,
            "unit": "ns/op",
            "extra": "236827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 400.49,
            "unit": "MB/s",
            "extra": "236827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18202,
            "unit": "B/op",
            "extra": "236827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "236827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 177608,
            "unit": "ns/op\t  92.25 MB/s\t   59839 B/op\t     664 allocs/op",
            "extra": "9978 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 177608,
            "unit": "ns/op",
            "extra": "9978 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 92.25,
            "unit": "MB/s",
            "extra": "9978 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 59839,
            "unit": "B/op",
            "extra": "9978 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 664,
            "unit": "allocs/op",
            "extra": "9978 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2286820,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "494 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2286820,
            "unit": "ns/op",
            "extra": "494 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "494 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "494 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1078,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1078,
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
            "value": 51267,
            "unit": "ns/op\t 159.79 MB/s\t   26097 B/op\t     454 allocs/op",
            "extra": "22290 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51267,
            "unit": "ns/op",
            "extra": "22290 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 159.79,
            "unit": "MB/s",
            "extra": "22290 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 26097,
            "unit": "B/op",
            "extra": "22290 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "22290 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6741203,
            "unit": "ns/op\t67523164 B/op\t    2586 allocs/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6741203,
            "unit": "ns/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523164,
            "unit": "B/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 607.1,
            "unit": "ns/op\t 105.43 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2043411 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 607.1,
            "unit": "ns/op",
            "extra": "2043411 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 105.43,
            "unit": "MB/s",
            "extra": "2043411 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2043411 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2043411 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9270363 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.2,
            "unit": "ns/op",
            "extra": "9270363 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9270363 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9270363 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1406,
            "unit": "ns/op\t  45.51 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1406,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.51,
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
            "value": 488.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2416028 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 488.3,
            "unit": "ns/op",
            "extra": "2416028 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2416028 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2416028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 28806,
            "unit": "ns/op\t 284.38 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "70165 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 28806,
            "unit": "ns/op",
            "extra": "70165 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 284.38,
            "unit": "MB/s",
            "extra": "70165 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "70165 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "70165 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 153.2,
            "unit": "ns/op\t1670.81 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7276434 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 153.2,
            "unit": "ns/op",
            "extra": "7276434 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1670.81,
            "unit": "MB/s",
            "extra": "7276434 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7276434 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7276434 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 729.7,
            "unit": "ns/op\t 350.84 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3156268 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 729.7,
            "unit": "ns/op",
            "extra": "3156268 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.84,
            "unit": "MB/s",
            "extra": "3156268 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3156268 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3156268 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2011582,
            "unit": "ns/op\t 3064040 B/op\t   40019 allocs/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2011582,
            "unit": "ns/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "591 times\n4 procs"
          }
        ]
      }
    ]
  }
}