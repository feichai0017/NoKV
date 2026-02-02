window.BENCHMARK_DATA = {
  "lastUpdate": 1770004796125,
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
          "id": "0ef2b217351748a76b32bbcd30ccd84e0e44c6ab",
          "message": "Update docs",
          "timestamp": "2026-02-02T11:58:41+08:00",
          "tree_id": "333e8ae68e64a76b98311c167e49d94b6c84a121",
          "url": "https://github.com/feichai0017/NoKV/commit/0ef2b217351748a76b32bbcd30ccd84e0e44c6ab"
        },
        "date": 1770004794576,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 10974,
            "unit": "ns/op\t   2.92 MB/s\t     625 B/op\t      24 allocs/op",
            "extra": "148968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 10974,
            "unit": "ns/op",
            "extra": "148968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.92,
            "unit": "MB/s",
            "extra": "148968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 625,
            "unit": "B/op",
            "extra": "148968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "148968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15302,
            "unit": "ns/op\t 267.68 MB/s\t     675 B/op\t      27 allocs/op",
            "extra": "88509 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15302,
            "unit": "ns/op",
            "extra": "88509 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 267.68,
            "unit": "MB/s",
            "extra": "88509 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 675,
            "unit": "B/op",
            "extra": "88509 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "88509 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12055,
            "unit": "ns/op\t   5.31 MB/s\t   19329 B/op\t       5 allocs/op",
            "extra": "672194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12055,
            "unit": "ns/op",
            "extra": "672194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.31,
            "unit": "MB/s",
            "extra": "672194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19329,
            "unit": "B/op",
            "extra": "672194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "672194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10318,
            "unit": "ns/op\t 396.97 MB/s\t   18582 B/op\t       7 allocs/op",
            "extra": "232711 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10318,
            "unit": "ns/op",
            "extra": "232711 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 396.97,
            "unit": "MB/s",
            "extra": "232711 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18582,
            "unit": "B/op",
            "extra": "232711 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "232711 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 184408,
            "unit": "ns/op\t  88.85 MB/s\t   65099 B/op\t     687 allocs/op",
            "extra": "8946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 184408,
            "unit": "ns/op",
            "extra": "8946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 88.85,
            "unit": "MB/s",
            "extra": "8946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 65099,
            "unit": "B/op",
            "extra": "8946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 687,
            "unit": "allocs/op",
            "extra": "8946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2390564,
            "unit": "ns/op\t   11484 B/op\t       0 allocs/op",
            "extra": "430 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2390564,
            "unit": "ns/op",
            "extra": "430 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 11484,
            "unit": "B/op",
            "extra": "430 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "430 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1011,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1011,
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
            "value": 49695,
            "unit": "ns/op\t 164.85 MB/s\t   28095 B/op\t     454 allocs/op",
            "extra": "24356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49695,
            "unit": "ns/op",
            "extra": "24356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.85,
            "unit": "MB/s",
            "extra": "24356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28095,
            "unit": "B/op",
            "extra": "24356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6752268,
            "unit": "ns/op\t67523344 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6752268,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523344,
            "unit": "B/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 620.3,
            "unit": "ns/op\t 103.18 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1927369 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 620.3,
            "unit": "ns/op",
            "extra": "1927369 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.18,
            "unit": "MB/s",
            "extra": "1927369 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1927369 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1927369 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9318290 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.8,
            "unit": "ns/op",
            "extra": "9318290 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9318290 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9318290 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1424,
            "unit": "ns/op\t  44.95 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1424,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.95,
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
            "value": 476.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2527374 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 476.2,
            "unit": "ns/op",
            "extra": "2527374 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2527374 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2527374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25274,
            "unit": "ns/op\t 324.12 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "77212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25274,
            "unit": "ns/op",
            "extra": "77212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 324.12,
            "unit": "MB/s",
            "extra": "77212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "77212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.6,
            "unit": "ns/op\t1624.66 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7982913 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.6,
            "unit": "ns/op",
            "extra": "7982913 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1624.66,
            "unit": "MB/s",
            "extra": "7982913 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7982913 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7982913 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 725.7,
            "unit": "ns/op\t 352.75 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3172582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 725.7,
            "unit": "ns/op",
            "extra": "3172582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 352.75,
            "unit": "MB/s",
            "extra": "3172582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3172582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3172582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2031160,
            "unit": "ns/op\t 3064050 B/op\t   40019 allocs/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2031160,
            "unit": "ns/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064050,
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