window.BENCHMARK_DATA = {
  "lastUpdate": 1770627624637,
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
          "id": "b911812e7fb58864eaba68754da9399c7bde95b7",
          "message": "docs: align memtable/hotring/config docs with implementation",
          "timestamp": "2026-02-09T16:50:03+08:00",
          "tree_id": "49c0263275d92fba39abddc61f36fecdd6d5c90b",
          "url": "https://github.com/feichai0017/NoKV/commit/b911812e7fb58864eaba68754da9399c7bde95b7"
        },
        "date": 1770627622867,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13056,
            "unit": "ns/op\t   2.45 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "85483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13056,
            "unit": "ns/op",
            "extra": "85483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.45,
            "unit": "MB/s",
            "extra": "85483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "85483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "85483 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 13782,
            "unit": "ns/op\t 297.20 MB/s\t     657 B/op\t      29 allocs/op",
            "extra": "108715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 13782,
            "unit": "ns/op",
            "extra": "108715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 297.2,
            "unit": "MB/s",
            "extra": "108715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 657,
            "unit": "B/op",
            "extra": "108715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "108715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9978,
            "unit": "ns/op\t   6.41 MB/s\t   19018 B/op\t       5 allocs/op",
            "extra": "853825 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9978,
            "unit": "ns/op",
            "extra": "853825 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.41,
            "unit": "MB/s",
            "extra": "853825 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19018,
            "unit": "B/op",
            "extra": "853825 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "853825 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10750,
            "unit": "ns/op\t 381.01 MB/s\t   22444 B/op\t       8 allocs/op",
            "extra": "212674 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10750,
            "unit": "ns/op",
            "extra": "212674 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 381.01,
            "unit": "MB/s",
            "extra": "212674 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 22444,
            "unit": "B/op",
            "extra": "212674 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "212674 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 157478,
            "unit": "ns/op\t 104.04 MB/s\t   58193 B/op\t     665 allocs/op",
            "extra": "9715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 157478,
            "unit": "ns/op",
            "extra": "9715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 104.04,
            "unit": "MB/s",
            "extra": "9715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 58193,
            "unit": "B/op",
            "extra": "9715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 665,
            "unit": "allocs/op",
            "extra": "9715 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2458206,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "492 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2458206,
            "unit": "ns/op",
            "extra": "492 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "492 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "492 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 919.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1377225 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 919.9,
            "unit": "ns/op",
            "extra": "1377225 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1377225 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1377225 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49077,
            "unit": "ns/op\t 166.92 MB/s\t   28186 B/op\t     454 allocs/op",
            "extra": "24156 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49077,
            "unit": "ns/op",
            "extra": "24156 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.92,
            "unit": "MB/s",
            "extra": "24156 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28186,
            "unit": "B/op",
            "extra": "24156 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24156 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8532458,
            "unit": "ns/op\t67523437 B/op\t    2587 allocs/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8532458,
            "unit": "ns/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523437,
            "unit": "B/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 703.8,
            "unit": "ns/op\t  90.94 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1713825 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 703.8,
            "unit": "ns/op",
            "extra": "1713825 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 90.94,
            "unit": "MB/s",
            "extra": "1713825 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1713825 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1713825 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 113.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10551019 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 113.8,
            "unit": "ns/op",
            "extra": "10551019 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10551019 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10551019 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1296,
            "unit": "ns/op\t  49.38 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1296,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 49.38,
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
            "value": 470.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2538574 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 470.4,
            "unit": "ns/op",
            "extra": "2538574 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2538574 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2538574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 24090,
            "unit": "ns/op\t 340.07 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "101774 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 24090,
            "unit": "ns/op",
            "extra": "101774 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 340.07,
            "unit": "MB/s",
            "extra": "101774 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "101774 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "101774 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 150.1,
            "unit": "ns/op\t1705.70 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7918353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 150.1,
            "unit": "ns/op",
            "extra": "7918353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1705.7,
            "unit": "MB/s",
            "extra": "7918353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7918353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7918353 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 596.4,
            "unit": "ns/op\t 429.21 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3871436 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 596.4,
            "unit": "ns/op",
            "extra": "3871436 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 429.21,
            "unit": "MB/s",
            "extra": "3871436 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3871436 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3871436 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1984691,
            "unit": "ns/op\t 3064054 B/op\t   40019 allocs/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1984691,
            "unit": "ns/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064054,
            "unit": "B/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "602 times\n4 procs"
          }
        ]
      }
    ]
  }
}