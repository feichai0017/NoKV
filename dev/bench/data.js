window.BENCHMARK_DATA = {
  "lastUpdate": 1770829455610,
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
          "id": "b143c892f3191ce31a57fedb8049424d486436cf",
          "message": "Merge pull request #50 from feichai0017/raftstore-architecture-refactor\n\nrefactor: split raftstore store services and reduce duplication",
          "timestamp": "2026-02-12T01:03:08+08:00",
          "tree_id": "b7c4ecbb8c5ddba6dbafd481b1921153f4cff6c1",
          "url": "https://github.com/feichai0017/NoKV/commit/b143c892f3191ce31a57fedb8049424d486436cf"
        },
        "date": 1770829454639,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8149,
            "unit": "ns/op\t   3.93 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "157941 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8149,
            "unit": "ns/op",
            "extra": "157941 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.93,
            "unit": "MB/s",
            "extra": "157941 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "157941 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "157941 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17289,
            "unit": "ns/op\t 236.91 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "76616 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17289,
            "unit": "ns/op",
            "extra": "76616 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 236.91,
            "unit": "MB/s",
            "extra": "76616 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "76616 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "76616 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7524,
            "unit": "ns/op\t   8.51 MB/s\t   16911 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7524,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.51,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16911,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10430,
            "unit": "ns/op\t 392.70 MB/s\t   27100 B/op\t       8 allocs/op",
            "extra": "417982 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10430,
            "unit": "ns/op",
            "extra": "417982 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 392.7,
            "unit": "MB/s",
            "extra": "417982 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 27100,
            "unit": "B/op",
            "extra": "417982 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "417982 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127202,
            "unit": "ns/op\t 128.80 MB/s\t   56855 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127202,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.8,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56855,
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
            "value": 1494434,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1494434,
            "unit": "ns/op",
            "extra": "804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
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
            "value": 624.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1859403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 624.3,
            "unit": "ns/op",
            "extra": "1859403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1859403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1859403 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50524,
            "unit": "ns/op\t 162.14 MB/s\t   27763 B/op\t     454 allocs/op",
            "extra": "25110 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50524,
            "unit": "ns/op",
            "extra": "25110 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.14,
            "unit": "MB/s",
            "extra": "25110 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27763,
            "unit": "B/op",
            "extra": "25110 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25110 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6656197,
            "unit": "ns/op\t67523306 B/op\t    2586 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6656197,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523306,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 725.8,
            "unit": "ns/op\t  88.18 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1822204 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 725.8,
            "unit": "ns/op",
            "extra": "1822204 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 88.18,
            "unit": "MB/s",
            "extra": "1822204 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1822204 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1822204 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9419572 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.8,
            "unit": "ns/op",
            "extra": "9419572 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9419572 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9419572 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1403,
            "unit": "ns/op\t  45.62 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1403,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.62,
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
            "value": 500.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2508278 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 500.5,
            "unit": "ns/op",
            "extra": "2508278 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2508278 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2508278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26049,
            "unit": "ns/op\t 314.48 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76957 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26049,
            "unit": "ns/op",
            "extra": "76957 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 314.48,
            "unit": "MB/s",
            "extra": "76957 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76957 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76957 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 161.5,
            "unit": "ns/op\t1585.56 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7667733 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 161.5,
            "unit": "ns/op",
            "extra": "7667733 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1585.56,
            "unit": "MB/s",
            "extra": "7667733 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7667733 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7667733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 730.8,
            "unit": "ns/op\t 350.31 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3186578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 730.8,
            "unit": "ns/op",
            "extra": "3186578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.31,
            "unit": "MB/s",
            "extra": "3186578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3186578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3186578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2111962,
            "unit": "ns/op\t 3064041 B/op\t   40019 allocs/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2111962,
            "unit": "ns/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064041,
            "unit": "B/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "567 times\n4 procs"
          }
        ]
      }
    ]
  }
}