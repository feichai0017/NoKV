window.BENCHMARK_DATA = {
  "lastUpdate": 1771060434056,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "Guocheng Song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7aa50aa7254b00971071f41186be51619a34c42a",
          "message": "Merge pull request #56 from feichai0017/fix/entry-ownership-structural\n\nfix: unify Entry lifecycle ownership across DB/Txn read paths",
          "timestamp": "2026-02-14T17:12:45+08:00",
          "tree_id": "b55671ed71ec051584c54dcb5e6b9706b09531d5",
          "url": "https://github.com/feichai0017/NoKV/commit/7aa50aa7254b00971071f41186be51619a34c42a"
        },
        "date": 1771060433426,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7188,
            "unit": "ns/op\t   4.45 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "147320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7188,
            "unit": "ns/op",
            "extra": "147320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.45,
            "unit": "MB/s",
            "extra": "147320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "147320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "147320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 20095,
            "unit": "ns/op\t 203.84 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "63030 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 20095,
            "unit": "ns/op",
            "extra": "63030 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 203.84,
            "unit": "MB/s",
            "extra": "63030 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "63030 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "63030 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7973,
            "unit": "ns/op\t   8.03 MB/s\t   17815 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7973,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.03,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17815,
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
            "value": 11887,
            "unit": "ns/op\t 344.57 MB/s\t   33772 B/op\t      11 allocs/op",
            "extra": "351188 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11887,
            "unit": "ns/op",
            "extra": "351188 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 344.57,
            "unit": "MB/s",
            "extra": "351188 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33772,
            "unit": "B/op",
            "extra": "351188 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "351188 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125711,
            "unit": "ns/op\t 130.33 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125711,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.33,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56849,
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
            "value": 1502564,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1502564,
            "unit": "ns/op",
            "extra": "795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 661.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1976277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 661.9,
            "unit": "ns/op",
            "extra": "1976277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1976277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1976277 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50707,
            "unit": "ns/op\t 161.55 MB/s\t   27560 B/op\t     454 allocs/op",
            "extra": "25593 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50707,
            "unit": "ns/op",
            "extra": "25593 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.55,
            "unit": "MB/s",
            "extra": "25593 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27560,
            "unit": "B/op",
            "extra": "25593 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25593 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6345849,
            "unit": "ns/op\t67523073 B/op\t    2578 allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6345849,
            "unit": "ns/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523073,
            "unit": "B/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 569.4,
            "unit": "ns/op\t 112.40 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2180834 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 569.4,
            "unit": "ns/op",
            "extra": "2180834 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 112.4,
            "unit": "MB/s",
            "extra": "2180834 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2180834 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2180834 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9432938 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.8,
            "unit": "ns/op",
            "extra": "9432938 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9432938 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9432938 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1396,
            "unit": "ns/op\t  45.84 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1396,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.84,
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
            "value": 464.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2482894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 464.4,
            "unit": "ns/op",
            "extra": "2482894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2482894 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2482894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26919,
            "unit": "ns/op\t 304.31 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26919,
            "unit": "ns/op",
            "extra": "73448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 304.31,
            "unit": "MB/s",
            "extra": "73448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.7,
            "unit": "ns/op\t1613.22 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "6969706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.7,
            "unit": "ns/op",
            "extra": "6969706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1613.22,
            "unit": "MB/s",
            "extra": "6969706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "6969706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6969706 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 668.2,
            "unit": "ns/op\t 383.10 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3495004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 668.2,
            "unit": "ns/op",
            "extra": "3495004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 383.1,
            "unit": "MB/s",
            "extra": "3495004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3495004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3495004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2042865,
            "unit": "ns/op\t 3064034 B/op\t   40017 allocs/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2042865,
            "unit": "ns/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064034,
            "unit": "B/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "576 times\n4 procs"
          }
        ]
      }
    ]
  }
}