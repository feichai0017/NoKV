window.BENCHMARK_DATA = {
  "lastUpdate": 1770361494410,
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
          "id": "cfd450de6f11cf25260fb155a48b1ffd26fc7b7a",
          "message": "chore: bump hotring v0.2.1",
          "timestamp": "2026-02-06T14:52:57+08:00",
          "tree_id": "8a885a33ec970741e9d89fc4e4dfb7a1c3d5d7c7",
          "url": "https://github.com/feichai0017/NoKV/commit/cfd450de6f11cf25260fb155a48b1ffd26fc7b7a"
        },
        "date": 1770361493819,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13029,
            "unit": "ns/op\t   2.46 MB/s\t     594 B/op\t      20 allocs/op",
            "extra": "119911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13029,
            "unit": "ns/op",
            "extra": "119911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.46,
            "unit": "MB/s",
            "extra": "119911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 594,
            "unit": "B/op",
            "extra": "119911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "119911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17276,
            "unit": "ns/op\t 237.10 MB/s\t     830 B/op\t      31 allocs/op",
            "extra": "77529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17276,
            "unit": "ns/op",
            "extra": "77529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 237.1,
            "unit": "MB/s",
            "extra": "77529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 830,
            "unit": "B/op",
            "extra": "77529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "77529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11284,
            "unit": "ns/op\t   5.67 MB/s\t   18816 B/op\t       5 allocs/op",
            "extra": "719821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11284,
            "unit": "ns/op",
            "extra": "719821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.67,
            "unit": "MB/s",
            "extra": "719821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18816,
            "unit": "B/op",
            "extra": "719821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "719821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9958,
            "unit": "ns/op\t 411.34 MB/s\t   18463 B/op\t       7 allocs/op",
            "extra": "242917 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9958,
            "unit": "ns/op",
            "extra": "242917 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 411.34,
            "unit": "MB/s",
            "extra": "242917 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18463,
            "unit": "B/op",
            "extra": "242917 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "242917 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 172055,
            "unit": "ns/op\t  95.23 MB/s\t   56858 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 172055,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.23,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56858,
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
            "value": 2196772,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2196772,
            "unit": "ns/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 976.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1327652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 976.6,
            "unit": "ns/op",
            "extra": "1327652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1327652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1327652 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50095,
            "unit": "ns/op\t 163.53 MB/s\t   25424 B/op\t     454 allocs/op",
            "extra": "24076 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50095,
            "unit": "ns/op",
            "extra": "24076 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.53,
            "unit": "MB/s",
            "extra": "24076 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25424,
            "unit": "B/op",
            "extra": "24076 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24076 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6557289,
            "unit": "ns/op\t67523360 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6557289,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523360,
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
            "value": 682.6,
            "unit": "ns/op\t  93.76 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1846308 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 682.6,
            "unit": "ns/op",
            "extra": "1846308 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 93.76,
            "unit": "MB/s",
            "extra": "1846308 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1846308 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1846308 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9400294 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.2,
            "unit": "ns/op",
            "extra": "9400294 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9400294 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9400294 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1422,
            "unit": "ns/op\t  45.01 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1422,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.01,
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
            "value": 486.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2409670 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 486.5,
            "unit": "ns/op",
            "extra": "2409670 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2409670 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2409670 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26078,
            "unit": "ns/op\t 314.14 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74858 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26078,
            "unit": "ns/op",
            "extra": "74858 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 314.14,
            "unit": "MB/s",
            "extra": "74858 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74858 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74858 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 144.3,
            "unit": "ns/op\t1774.20 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8344924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 144.3,
            "unit": "ns/op",
            "extra": "8344924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1774.2,
            "unit": "MB/s",
            "extra": "8344924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8344924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8344924 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 753.1,
            "unit": "ns/op\t 339.93 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3132768 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 753.1,
            "unit": "ns/op",
            "extra": "3132768 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 339.93,
            "unit": "MB/s",
            "extra": "3132768 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3132768 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3132768 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2031497,
            "unit": "ns/op\t 3064048 B/op\t   40019 allocs/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2031497,
            "unit": "ns/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064048,
            "unit": "B/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "580 times\n4 procs"
          }
        ]
      }
    ]
  }
}