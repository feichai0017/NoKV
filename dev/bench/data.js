window.BENCHMARK_DATA = {
  "lastUpdate": 1769200464034,
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
          "id": "1b44438d143c873a618d11cc6a5882f25f4b7aa5",
          "message": "bench: improve ycsb logging and helper naming",
          "timestamp": "2026-01-24T04:33:10+08:00",
          "tree_id": "081e3879386c305a87bc03cef553dd5419a90f97",
          "url": "https://github.com/feichai0017/NoKV/commit/1b44438d143c873a618d11cc6a5882f25f4b7aa5"
        },
        "date": 1769200462858,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11673,
            "unit": "ns/op\t   2.74 MB/s\t     650 B/op\t      24 allocs/op",
            "extra": "183920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11673,
            "unit": "ns/op",
            "extra": "183920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.74,
            "unit": "MB/s",
            "extra": "183920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 650,
            "unit": "B/op",
            "extra": "183920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "183920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15677,
            "unit": "ns/op\t 261.28 MB/s\t     693 B/op\t      27 allocs/op",
            "extra": "87544 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15677,
            "unit": "ns/op",
            "extra": "87544 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 261.28,
            "unit": "MB/s",
            "extra": "87544 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 693,
            "unit": "B/op",
            "extra": "87544 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "87544 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12361,
            "unit": "ns/op\t   5.18 MB/s\t   19542 B/op\t       5 allocs/op",
            "extra": "639358 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12361,
            "unit": "ns/op",
            "extra": "639358 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.18,
            "unit": "MB/s",
            "extra": "639358 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19542,
            "unit": "B/op",
            "extra": "639358 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "639358 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10137,
            "unit": "ns/op\t 404.08 MB/s\t   17384 B/op\t       7 allocs/op",
            "extra": "241824 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10137,
            "unit": "ns/op",
            "extra": "241824 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 404.08,
            "unit": "MB/s",
            "extra": "241824 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17384,
            "unit": "B/op",
            "extra": "241824 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "241824 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 178664,
            "unit": "ns/op\t  91.70 MB/s\t   59739 B/op\t     663 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 178664,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 91.7,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 59739,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 663,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2307155,
            "unit": "ns/op\t    9514 B/op\t       0 allocs/op",
            "extra": "519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2307155,
            "unit": "ns/op",
            "extra": "519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9514,
            "unit": "B/op",
            "extra": "519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1035,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1035,
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
            "value": 49439,
            "unit": "ns/op\t 165.70 MB/s\t   27793 B/op\t     454 allocs/op",
            "extra": "25040 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49439,
            "unit": "ns/op",
            "extra": "25040 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.7,
            "unit": "MB/s",
            "extra": "25040 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27793,
            "unit": "B/op",
            "extra": "25040 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25040 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6656663,
            "unit": "ns/op\t67523378 B/op\t    2586 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6656663,
            "unit": "ns/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523378,
            "unit": "B/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 671.9,
            "unit": "ns/op\t  95.25 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1871492 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 671.9,
            "unit": "ns/op",
            "extra": "1871492 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 95.25,
            "unit": "MB/s",
            "extra": "1871492 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1871492 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1871492 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "8967837 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.2,
            "unit": "ns/op",
            "extra": "8967837 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "8967837 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8967837 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1418,
            "unit": "ns/op\t  45.15 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1418,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.15,
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
            "extra": "2525151 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 476.2,
            "unit": "ns/op",
            "extra": "2525151 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2525151 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2525151 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25547,
            "unit": "ns/op\t 320.66 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "77460 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25547,
            "unit": "ns/op",
            "extra": "77460 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 320.66,
            "unit": "MB/s",
            "extra": "77460 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "77460 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77460 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149.9,
            "unit": "ns/op\t1707.52 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7856352 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149.9,
            "unit": "ns/op",
            "extra": "7856352 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1707.52,
            "unit": "MB/s",
            "extra": "7856352 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7856352 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7856352 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 723.2,
            "unit": "ns/op\t 354.00 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3158400 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 723.2,
            "unit": "ns/op",
            "extra": "3158400 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 354,
            "unit": "MB/s",
            "extra": "3158400 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3158400 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3158400 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2080458,
            "unit": "ns/op\t 3064039 B/op\t   40019 allocs/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2080458,
            "unit": "ns/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "570 times\n4 procs"
          }
        ]
      }
    ]
  }
}