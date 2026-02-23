window.BENCHMARK_DATA = {
  "lastUpdate": 1771850021219,
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
          "id": "e6d4c0e64bafea415aaea5c8fc0118ef6fc80d34",
          "message": "docs: align docs with current benchmark and code reality",
          "timestamp": "2026-02-23T20:32:28+08:00",
          "tree_id": "66490998e5e1241f858db894d832ed5ebc826e35",
          "url": "https://github.com/feichai0017/NoKV/commit/e6d4c0e64bafea415aaea5c8fc0118ef6fc80d34"
        },
        "date": 1771850020159,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6541,
            "unit": "ns/op\t   4.89 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "170769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6541,
            "unit": "ns/op",
            "extra": "170769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.89,
            "unit": "MB/s",
            "extra": "170769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "170769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "170769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16676,
            "unit": "ns/op\t 245.63 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "74359 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16676,
            "unit": "ns/op",
            "extra": "74359 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 245.63,
            "unit": "MB/s",
            "extra": "74359 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "74359 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "74359 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8794,
            "unit": "ns/op\t   7.28 MB/s\t   20117 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8794,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.28,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20117,
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
            "value": 12560,
            "unit": "ns/op\t 326.12 MB/s\t   35364 B/op\t      11 allocs/op",
            "extra": "317070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12560,
            "unit": "ns/op",
            "extra": "317070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 326.12,
            "unit": "MB/s",
            "extra": "317070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35364,
            "unit": "B/op",
            "extra": "317070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "317070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122065,
            "unit": "ns/op\t 134.22 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122065,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.22,
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
            "value": 1483292,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1483292,
            "unit": "ns/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 601.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2110257 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 601.2,
            "unit": "ns/op",
            "extra": "2110257 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2110257 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2110257 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49421,
            "unit": "ns/op\t 165.76 MB/s\t   28001 B/op\t     454 allocs/op",
            "extra": "24564 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49421,
            "unit": "ns/op",
            "extra": "24564 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.76,
            "unit": "MB/s",
            "extra": "24564 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28001,
            "unit": "B/op",
            "extra": "24564 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24564 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6490401,
            "unit": "ns/op\t67523190 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6490401,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523190,
            "unit": "B/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 601.8,
            "unit": "ns/op\t 106.35 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1913179 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 601.8,
            "unit": "ns/op",
            "extra": "1913179 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 106.35,
            "unit": "MB/s",
            "extra": "1913179 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1913179 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1913179 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9384127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.1,
            "unit": "ns/op",
            "extra": "9384127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9384127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9384127 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1403,
            "unit": "ns/op\t  45.61 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 45.61,
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
            "value": 466.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2671592 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 466.5,
            "unit": "ns/op",
            "extra": "2671592 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2671592 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2671592 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25228,
            "unit": "ns/op\t 324.72 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77744 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25228,
            "unit": "ns/op",
            "extra": "77744 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 324.72,
            "unit": "MB/s",
            "extra": "77744 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77744 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77744 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 165.1,
            "unit": "ns/op\t1550.44 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7206476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 165.1,
            "unit": "ns/op",
            "extra": "7206476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1550.44,
            "unit": "MB/s",
            "extra": "7206476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7206476 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7206476 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 691.8,
            "unit": "ns/op\t 370.04 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3405154 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 691.8,
            "unit": "ns/op",
            "extra": "3405154 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.04,
            "unit": "MB/s",
            "extra": "3405154 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3405154 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3405154 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1955540,
            "unit": "ns/op\t 3064022 B/op\t   40017 allocs/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1955540,
            "unit": "ns/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064022,
            "unit": "B/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "609 times\n4 procs"
          }
        ]
      }
    ]
  }
}