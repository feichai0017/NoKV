window.BENCHMARK_DATA = {
  "lastUpdate": 1772007964729,
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
          "id": "2510d44302b9351cd018c6983152ec2f09226ee3",
          "message": "Merge pull request #67 from zqr10159/fix-issue-58\n\nfix: add underflow check in DecrRef method and corresponding test",
          "timestamp": "2026-02-25T16:24:55+08:00",
          "tree_id": "05da87776fc4922dab75a1d1b69dc85cd7f6866c",
          "url": "https://github.com/feichai0017/NoKV/commit/2510d44302b9351cd018c6983152ec2f09226ee3"
        },
        "date": 1772007963754,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7400,
            "unit": "ns/op\t   4.32 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "150934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7400,
            "unit": "ns/op",
            "extra": "150934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.32,
            "unit": "MB/s",
            "extra": "150934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "150934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "150934 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16700,
            "unit": "ns/op\t 245.27 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "67581 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16700,
            "unit": "ns/op",
            "extra": "67581 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 245.27,
            "unit": "MB/s",
            "extra": "67581 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "67581 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "67581 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7918,
            "unit": "ns/op\t   8.08 MB/s\t   17823 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7918,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.08,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17823,
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
            "value": 12232,
            "unit": "ns/op\t 334.87 MB/s\t   31648 B/op\t      11 allocs/op",
            "extra": "318622 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12232,
            "unit": "ns/op",
            "extra": "318622 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 334.87,
            "unit": "MB/s",
            "extra": "318622 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 31648,
            "unit": "B/op",
            "extra": "318622 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "318622 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123457,
            "unit": "ns/op\t 132.71 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123457,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.71,
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
            "value": 1536611,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1536611,
            "unit": "ns/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 629.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1901421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 629.7,
            "unit": "ns/op",
            "extra": "1901421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1901421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1901421 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50570,
            "unit": "ns/op\t 161.99 MB/s\t   25662 B/op\t     454 allocs/op",
            "extra": "23414 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50570,
            "unit": "ns/op",
            "extra": "23414 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.99,
            "unit": "MB/s",
            "extra": "23414 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25662,
            "unit": "B/op",
            "extra": "23414 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23414 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6634253,
            "unit": "ns/op\t67523155 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6634253,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523155,
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
            "value": 623.4,
            "unit": "ns/op\t 102.66 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1903574 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 623.4,
            "unit": "ns/op",
            "extra": "1903574 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 102.66,
            "unit": "MB/s",
            "extra": "1903574 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1903574 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1903574 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9457561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.2,
            "unit": "ns/op",
            "extra": "9457561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9457561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9457561 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1429,
            "unit": "ns/op\t  44.78 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1429,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.78,
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
            "value": 488.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2506333 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 488.4,
            "unit": "ns/op",
            "extra": "2506333 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2506333 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2506333 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26833,
            "unit": "ns/op\t 305.30 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26833,
            "unit": "ns/op",
            "extra": "77698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 305.3,
            "unit": "MB/s",
            "extra": "77698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164,
            "unit": "ns/op\t1561.12 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7238166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164,
            "unit": "ns/op",
            "extra": "7238166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1561.12,
            "unit": "MB/s",
            "extra": "7238166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7238166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7238166 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 691.9,
            "unit": "ns/op\t 370.00 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3404707 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 691.9,
            "unit": "ns/op",
            "extra": "3404707 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370,
            "unit": "MB/s",
            "extra": "3404707 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3404707 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3404707 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1990966,
            "unit": "ns/op\t 3064046 B/op\t   40018 allocs/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1990966,
            "unit": "ns/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064046,
            "unit": "B/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "595 times\n4 procs"
          }
        ]
      }
    ]
  }
}