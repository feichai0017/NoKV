window.BENCHMARK_DATA = {
  "lastUpdate": 1771566290282,
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
          "id": "b948aa5ba995e173f1cc37efc423b901368de017",
          "message": "chore: apply go fix cleanups",
          "timestamp": "2026-02-20T13:35:02+08:00",
          "tree_id": "2a0faa3ee1c00e5b4861d1adb9fe37da9daf84b8",
          "url": "https://github.com/feichai0017/NoKV/commit/b948aa5ba995e173f1cc37efc423b901368de017"
        },
        "date": 1771566288839,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7172,
            "unit": "ns/op\t   4.46 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "177558 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7172,
            "unit": "ns/op",
            "extra": "177558 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.46,
            "unit": "MB/s",
            "extra": "177558 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "177558 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "177558 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16279,
            "unit": "ns/op\t 251.62 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "73194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16279,
            "unit": "ns/op",
            "extra": "73194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 251.62,
            "unit": "MB/s",
            "extra": "73194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "73194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "73194 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8001,
            "unit": "ns/op\t   8.00 MB/s\t   18131 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8001,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18131,
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
            "value": 12643,
            "unit": "ns/op\t 323.99 MB/s\t   34224 B/op\t      11 allocs/op",
            "extra": "316596 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12643,
            "unit": "ns/op",
            "extra": "316596 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 323.99,
            "unit": "MB/s",
            "extra": "316596 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34224,
            "unit": "B/op",
            "extra": "316596 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "316596 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122333,
            "unit": "ns/op\t 133.93 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122333,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.93,
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
            "value": 1494741,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1494741,
            "unit": "ns/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 592.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1984887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 592.7,
            "unit": "ns/op",
            "extra": "1984887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1984887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1984887 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49114,
            "unit": "ns/op\t 166.80 MB/s\t   25434 B/op\t     454 allocs/op",
            "extra": "24046 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49114,
            "unit": "ns/op",
            "extra": "24046 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.8,
            "unit": "MB/s",
            "extra": "24046 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25434,
            "unit": "B/op",
            "extra": "24046 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24046 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6642239,
            "unit": "ns/op\t67523217 B/op\t    2579 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6642239,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523217,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 504.9,
            "unit": "ns/op\t 126.75 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2098202 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 504.9,
            "unit": "ns/op",
            "extra": "2098202 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 126.75,
            "unit": "MB/s",
            "extra": "2098202 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2098202 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2098202 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9388040 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.6,
            "unit": "ns/op",
            "extra": "9388040 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9388040 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9388040 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1455,
            "unit": "ns/op\t  43.97 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1455,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.97,
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
            "value": 456,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2601550 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 456,
            "unit": "ns/op",
            "extra": "2601550 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2601550 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2601550 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26598,
            "unit": "ns/op\t 308.00 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77773 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26598,
            "unit": "ns/op",
            "extra": "77773 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308,
            "unit": "MB/s",
            "extra": "77773 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77773 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77773 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 163.2,
            "unit": "ns/op\t1568.24 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7353628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 163.2,
            "unit": "ns/op",
            "extra": "7353628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1568.24,
            "unit": "MB/s",
            "extra": "7353628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7353628 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7353628 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 694.7,
            "unit": "ns/op\t 368.52 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3386769 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 694.7,
            "unit": "ns/op",
            "extra": "3386769 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.52,
            "unit": "MB/s",
            "extra": "3386769 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3386769 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3386769 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1988179,
            "unit": "ns/op\t 3064027 B/op\t   40017 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1988179,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064027,
            "unit": "B/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "589 times\n4 procs"
          }
        ]
      }
    ]
  }
}