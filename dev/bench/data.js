window.BENCHMARK_DATA = {
  "lastUpdate": 1770917504239,
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
          "id": "a3dace5e2097ff87e22a4359adb902f6bdc3e1d0",
          "message": "chore: bump project go version to 1.26",
          "timestamp": "2026-02-13T01:29:57+08:00",
          "tree_id": "6c9c14da2ffc17a7aae190c559166e9a5d1af3bf",
          "url": "https://github.com/feichai0017/NoKV/commit/a3dace5e2097ff87e22a4359adb902f6bdc3e1d0"
        },
        "date": 1770917503033,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9774,
            "unit": "ns/op\t   3.27 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "172344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9774,
            "unit": "ns/op",
            "extra": "172344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.27,
            "unit": "MB/s",
            "extra": "172344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "172344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "172344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18870,
            "unit": "ns/op\t 217.06 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "62516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18870,
            "unit": "ns/op",
            "extra": "62516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 217.06,
            "unit": "MB/s",
            "extra": "62516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "62516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "62516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8694,
            "unit": "ns/op\t   7.36 MB/s\t   19793 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8694,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.36,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19793,
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
            "value": 11614,
            "unit": "ns/op\t 352.66 MB/s\t   29338 B/op\t       8 allocs/op",
            "extra": "354055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11614,
            "unit": "ns/op",
            "extra": "354055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 352.66,
            "unit": "MB/s",
            "extra": "354055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 29338,
            "unit": "B/op",
            "extra": "354055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "354055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127129,
            "unit": "ns/op\t 128.88 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127129,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.88,
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
            "value": 1510370,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1510370,
            "unit": "ns/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 623.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1906545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 623.2,
            "unit": "ns/op",
            "extra": "1906545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1906545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1906545 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49903,
            "unit": "ns/op\t 164.16 MB/s\t   27971 B/op\t     454 allocs/op",
            "extra": "24633 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49903,
            "unit": "ns/op",
            "extra": "24633 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.16,
            "unit": "MB/s",
            "extra": "24633 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27971,
            "unit": "B/op",
            "extra": "24633 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24633 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6823171,
            "unit": "ns/op\t67523098 B/op\t    2579 allocs/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6823171,
            "unit": "ns/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523098,
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
            "value": 582.2,
            "unit": "ns/op\t 109.92 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2002826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 582.2,
            "unit": "ns/op",
            "extra": "2002826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 109.92,
            "unit": "MB/s",
            "extra": "2002826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2002826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2002826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9460054 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.1,
            "unit": "ns/op",
            "extra": "9460054 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9460054 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9460054 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1461,
            "unit": "ns/op\t  43.80 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1461,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.8,
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
            "value": 458.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2700672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 458.4,
            "unit": "ns/op",
            "extra": "2700672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2700672 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2700672 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27273,
            "unit": "ns/op\t 300.37 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "72115 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27273,
            "unit": "ns/op",
            "extra": "72115 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 300.37,
            "unit": "MB/s",
            "extra": "72115 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "72115 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "72115 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 183.1,
            "unit": "ns/op\t1397.93 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "5850699 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 183.1,
            "unit": "ns/op",
            "extra": "5850699 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1397.93,
            "unit": "MB/s",
            "extra": "5850699 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "5850699 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "5850699 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.4,
            "unit": "ns/op\t 368.13 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3431961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.4,
            "unit": "ns/op",
            "extra": "3431961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.13,
            "unit": "MB/s",
            "extra": "3431961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3431961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3431961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2121823,
            "unit": "ns/op\t 3064046 B/op\t   40018 allocs/op",
            "extra": "520 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2121823,
            "unit": "ns/op",
            "extra": "520 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064046,
            "unit": "B/op",
            "extra": "520 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "520 times\n4 procs"
          }
        ]
      }
    ]
  }
}