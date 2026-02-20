window.BENCHMARK_DATA = {
  "lastUpdate": 1771572370337,
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
          "id": "d0a8abf9155ae07f248cb5dd1178867182e261e1",
          "message": "docs: fix stats mermaid diagram rendering",
          "timestamp": "2026-02-20T15:24:23+08:00",
          "tree_id": "792fad3ce123998a2578fd4c69a0fac96348cfa8",
          "url": "https://github.com/feichai0017/NoKV/commit/d0a8abf9155ae07f248cb5dd1178867182e261e1"
        },
        "date": 1771572368835,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7287,
            "unit": "ns/op\t   4.39 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "159159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7287,
            "unit": "ns/op",
            "extra": "159159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.39,
            "unit": "MB/s",
            "extra": "159159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "159159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "159159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16467,
            "unit": "ns/op\t 248.74 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70440 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16467,
            "unit": "ns/op",
            "extra": "70440 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 248.74,
            "unit": "MB/s",
            "extra": "70440 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70440 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70440 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8234,
            "unit": "ns/op\t   7.77 MB/s\t   17586 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8234,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.77,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17586,
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
            "value": 12726,
            "unit": "ns/op\t 321.87 MB/s\t   34082 B/op\t      11 allocs/op",
            "extra": "314590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12726,
            "unit": "ns/op",
            "extra": "314590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 321.87,
            "unit": "MB/s",
            "extra": "314590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34082,
            "unit": "B/op",
            "extra": "314590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "314590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 130889,
            "unit": "ns/op\t 125.18 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 130889,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 125.18,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56847,
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
            "value": 1553899,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1553899,
            "unit": "ns/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 579.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2075478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 579.8,
            "unit": "ns/op",
            "extra": "2075478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2075478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2075478 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 51029,
            "unit": "ns/op\t 160.54 MB/s\t   25667 B/op\t     454 allocs/op",
            "extra": "23400 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51029,
            "unit": "ns/op",
            "extra": "23400 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 160.54,
            "unit": "MB/s",
            "extra": "23400 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25667,
            "unit": "B/op",
            "extra": "23400 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23400 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7597249,
            "unit": "ns/op\t67523177 B/op\t    2579 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7597249,
            "unit": "ns/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523177,
            "unit": "B/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 639.6,
            "unit": "ns/op\t 100.06 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1973336 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 639.6,
            "unit": "ns/op",
            "extra": "1973336 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 100.06,
            "unit": "MB/s",
            "extra": "1973336 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1973336 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1973336 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9294428 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.2,
            "unit": "ns/op",
            "extra": "9294428 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9294428 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9294428 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1440,
            "unit": "ns/op\t  44.44 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1440,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.44,
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
            "value": 446.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2583009 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 446.1,
            "unit": "ns/op",
            "extra": "2583009 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2583009 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2583009 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25569,
            "unit": "ns/op\t 320.39 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77350 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25569,
            "unit": "ns/op",
            "extra": "77350 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 320.39,
            "unit": "MB/s",
            "extra": "77350 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77350 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77350 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 162.9,
            "unit": "ns/op\t1571.62 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7418139 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 162.9,
            "unit": "ns/op",
            "extra": "7418139 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1571.62,
            "unit": "MB/s",
            "extra": "7418139 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7418139 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7418139 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 701.7,
            "unit": "ns/op\t 364.83 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3368862 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 701.7,
            "unit": "ns/op",
            "extra": "3368862 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 364.83,
            "unit": "MB/s",
            "extra": "3368862 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3368862 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3368862 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1988855,
            "unit": "ns/op\t 3064034 B/op\t   40017 allocs/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1988855,
            "unit": "ns/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064034,
            "unit": "B/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "594 times\n4 procs"
          }
        ]
      }
    ]
  }
}