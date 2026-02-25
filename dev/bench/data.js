window.BENCHMARK_DATA = {
  "lastUpdate": 1772003977660,
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
          "id": "0a6eb9222236bcae08de82e461962c5300382cac",
          "message": "Merge pull request #77 from zzzzwc/zwc-read-op-resolve-locks\n\nfix: resolve locks during read operations",
          "timestamp": "2026-02-25T15:18:27+08:00",
          "tree_id": "bd25a3b6c787c28dd591054e99c9ef1036b46bfa",
          "url": "https://github.com/feichai0017/NoKV/commit/0a6eb9222236bcae08de82e461962c5300382cac"
        },
        "date": 1772003976496,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7024,
            "unit": "ns/op\t   4.56 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "183928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7024,
            "unit": "ns/op",
            "extra": "183928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.56,
            "unit": "MB/s",
            "extra": "183928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "183928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "183928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18404,
            "unit": "ns/op\t 222.57 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "62010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18404,
            "unit": "ns/op",
            "extra": "62010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.57,
            "unit": "MB/s",
            "extra": "62010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "62010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "62010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8607,
            "unit": "ns/op\t   7.44 MB/s\t   20244 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8607,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.44,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20244,
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
            "value": 12240,
            "unit": "ns/op\t 334.63 MB/s\t   34232 B/op\t      11 allocs/op",
            "extra": "324045 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12240,
            "unit": "ns/op",
            "extra": "324045 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 334.63,
            "unit": "MB/s",
            "extra": "324045 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34232,
            "unit": "B/op",
            "extra": "324045 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "324045 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 129275,
            "unit": "ns/op\t 126.74 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 129275,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 126.74,
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
            "value": 1494461,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1494461,
            "unit": "ns/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 615.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2115804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 615.8,
            "unit": "ns/op",
            "extra": "2115804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2115804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2115804 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48777,
            "unit": "ns/op\t 167.95 MB/s\t   28021 B/op\t     454 allocs/op",
            "extra": "24519 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48777,
            "unit": "ns/op",
            "extra": "24519 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 167.95,
            "unit": "MB/s",
            "extra": "24519 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28021,
            "unit": "B/op",
            "extra": "24519 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24519 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6266271,
            "unit": "ns/op\t67523157 B/op\t    2578 allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6266271,
            "unit": "ns/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523157,
            "unit": "B/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 614.4,
            "unit": "ns/op\t 104.17 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2017582 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 614.4,
            "unit": "ns/op",
            "extra": "2017582 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 104.17,
            "unit": "MB/s",
            "extra": "2017582 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2017582 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2017582 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 131.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9330766 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 131.9,
            "unit": "ns/op",
            "extra": "9330766 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9330766 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9330766 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1383,
            "unit": "ns/op\t  46.26 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1383,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.26,
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
            "value": 475.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2465142 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 475.8,
            "unit": "ns/op",
            "extra": "2465142 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2465142 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2465142 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25455,
            "unit": "ns/op\t 321.82 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "78039 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25455,
            "unit": "ns/op",
            "extra": "78039 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 321.82,
            "unit": "MB/s",
            "extra": "78039 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "78039 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "78039 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164.2,
            "unit": "ns/op\t1559.47 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7301354 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164.2,
            "unit": "ns/op",
            "extra": "7301354 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1559.47,
            "unit": "MB/s",
            "extra": "7301354 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7301354 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7301354 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 674.7,
            "unit": "ns/op\t 379.42 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3406580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 674.7,
            "unit": "ns/op",
            "extra": "3406580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 379.42,
            "unit": "MB/s",
            "extra": "3406580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3406580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3406580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1987658,
            "unit": "ns/op\t 3064035 B/op\t   40017 allocs/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1987658,
            "unit": "ns/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064035,
            "unit": "B/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "602 times\n4 procs"
          }
        ]
      }
    ]
  }
}