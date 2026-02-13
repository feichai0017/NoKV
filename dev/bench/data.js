window.BENCHMARK_DATA = {
  "lastUpdate": 1770971601102,
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
          "id": "777e7ef9c95689764fbb60c9e31227667ec5f04a",
          "message": "Merge pull request #53 from zzzzwc/zwc-fix-lock-resolve\n\nfix: commit secondary when primary already committed",
          "timestamp": "2026-02-13T16:32:13+08:00",
          "tree_id": "f256f14bb1da1040f98b79ac3219f42f85751137",
          "url": "https://github.com/feichai0017/NoKV/commit/777e7ef9c95689764fbb60c9e31227667ec5f04a"
        },
        "date": 1770971600514,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6642,
            "unit": "ns/op\t   4.82 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "170755 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6642,
            "unit": "ns/op",
            "extra": "170755 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.82,
            "unit": "MB/s",
            "extra": "170755 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "170755 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "170755 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16096,
            "unit": "ns/op\t 254.47 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "81968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16096,
            "unit": "ns/op",
            "extra": "81968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 254.47,
            "unit": "MB/s",
            "extra": "81968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "81968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "81968 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8082,
            "unit": "ns/op\t   7.92 MB/s\t   18823 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8082,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.92,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18823,
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
            "value": 10001,
            "unit": "ns/op\t 409.54 MB/s\t   26947 B/op\t       8 allocs/op",
            "extra": "435252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10001,
            "unit": "ns/op",
            "extra": "435252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 409.54,
            "unit": "MB/s",
            "extra": "435252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26947,
            "unit": "B/op",
            "extra": "435252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "435252 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121336,
            "unit": "ns/op\t 135.03 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121336,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.03,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56846,
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
            "value": 1483130,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "817 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1483130,
            "unit": "ns/op",
            "extra": "817 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "817 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "817 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 593.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2019408 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 593.8,
            "unit": "ns/op",
            "extra": "2019408 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2019408 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2019408 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 58461,
            "unit": "ns/op\t 140.13 MB/s\t   28089 B/op\t     454 allocs/op",
            "extra": "24370 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 58461,
            "unit": "ns/op",
            "extra": "24370 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 140.13,
            "unit": "MB/s",
            "extra": "24370 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28089,
            "unit": "B/op",
            "extra": "24370 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24370 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6370938,
            "unit": "ns/op\t67523032 B/op\t    2578 allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6370938,
            "unit": "ns/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523032,
            "unit": "B/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 619.3,
            "unit": "ns/op\t 103.35 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1919367 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 619.3,
            "unit": "ns/op",
            "extra": "1919367 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.35,
            "unit": "MB/s",
            "extra": "1919367 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1919367 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1919367 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9511016 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.1,
            "unit": "ns/op",
            "extra": "9511016 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9511016 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9511016 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1387,
            "unit": "ns/op\t  46.13 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1387,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.13,
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
            "value": 452.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2458048 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 452.7,
            "unit": "ns/op",
            "extra": "2458048 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2458048 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2458048 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26408,
            "unit": "ns/op\t 310.21 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73483 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26408,
            "unit": "ns/op",
            "extra": "73483 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 310.21,
            "unit": "MB/s",
            "extra": "73483 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73483 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73483 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 165.1,
            "unit": "ns/op\t1550.52 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7185910 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 165.1,
            "unit": "ns/op",
            "extra": "7185910 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1550.52,
            "unit": "MB/s",
            "extra": "7185910 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7185910 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7185910 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 678.1,
            "unit": "ns/op\t 377.53 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3453942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 678.1,
            "unit": "ns/op",
            "extra": "3453942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 377.53,
            "unit": "MB/s",
            "extra": "3453942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3453942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3453942 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2013715,
            "unit": "ns/op\t 3064040 B/op\t   40018 allocs/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2013715,
            "unit": "ns/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "577 times\n4 procs"
          }
        ]
      }
    ]
  }
}