window.BENCHMARK_DATA = {
  "lastUpdate": 1771163018857,
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
          "id": "2008ec917521f6470c79b582fa740eb3849e5e87",
          "message": "refactor: centralize SyncDir in vfs and sync docs",
          "timestamp": "2026-02-15T21:42:06+08:00",
          "tree_id": "245b336ddea0df61fa9a40506b247576c9ef6996",
          "url": "https://github.com/feichai0017/NoKV/commit/2008ec917521f6470c79b582fa740eb3849e5e87"
        },
        "date": 1771163017959,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7387,
            "unit": "ns/op\t   4.33 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "175830 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7387,
            "unit": "ns/op",
            "extra": "175830 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.33,
            "unit": "MB/s",
            "extra": "175830 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "175830 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "175830 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18470,
            "unit": "ns/op\t 221.76 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "72679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18470,
            "unit": "ns/op",
            "extra": "72679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 221.76,
            "unit": "MB/s",
            "extra": "72679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "72679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "72679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7848,
            "unit": "ns/op\t   8.16 MB/s\t   17965 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7848,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.16,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17965,
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
            "value": 12130,
            "unit": "ns/op\t 337.67 MB/s\t   34162 B/op\t      11 allocs/op",
            "extra": "337388 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12130,
            "unit": "ns/op",
            "extra": "337388 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.67,
            "unit": "MB/s",
            "extra": "337388 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34162,
            "unit": "B/op",
            "extra": "337388 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "337388 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121682,
            "unit": "ns/op\t 134.65 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121682,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.65,
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
            "value": 1499353,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1499353,
            "unit": "ns/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 587.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2122244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 587.8,
            "unit": "ns/op",
            "extra": "2122244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2122244 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2122244 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50659,
            "unit": "ns/op\t 161.71 MB/s\t   27633 B/op\t     454 allocs/op",
            "extra": "25416 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50659,
            "unit": "ns/op",
            "extra": "25416 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.71,
            "unit": "MB/s",
            "extra": "25416 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27633,
            "unit": "B/op",
            "extra": "25416 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25416 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6494617,
            "unit": "ns/op\t67523156 B/op\t    2579 allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6494617,
            "unit": "ns/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523156,
            "unit": "B/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 626.3,
            "unit": "ns/op\t 102.19 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2470598 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 626.3,
            "unit": "ns/op",
            "extra": "2470598 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 102.19,
            "unit": "MB/s",
            "extra": "2470598 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2470598 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2470598 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9309148 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.4,
            "unit": "ns/op",
            "extra": "9309148 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9309148 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9309148 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1345,
            "unit": "ns/op\t  47.60 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1345,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47.6,
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
            "value": 466.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2488502 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 466.7,
            "unit": "ns/op",
            "extra": "2488502 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2488502 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2488502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26866,
            "unit": "ns/op\t 304.92 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74122 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26866,
            "unit": "ns/op",
            "extra": "74122 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 304.92,
            "unit": "MB/s",
            "extra": "74122 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74122 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74122 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 152.3,
            "unit": "ns/op\t1681.02 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7730397 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 152.3,
            "unit": "ns/op",
            "extra": "7730397 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1681.02,
            "unit": "MB/s",
            "extra": "7730397 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7730397 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7730397 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 701,
            "unit": "ns/op\t 365.21 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3408750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 701,
            "unit": "ns/op",
            "extra": "3408750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 365.21,
            "unit": "MB/s",
            "extra": "3408750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3408750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3408750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2020504,
            "unit": "ns/op\t 3064032 B/op\t   40018 allocs/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2020504,
            "unit": "ns/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "588 times\n4 procs"
          }
        ]
      }
    ]
  }
}