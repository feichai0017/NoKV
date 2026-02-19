window.BENCHMARK_DATA = {
  "lastUpdate": 1771525225589,
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
          "id": "407f26f15d69b3e9c37e6abfcb6064aef25fc17a",
          "message": "ci: pin issue-labeler action to v3.4",
          "timestamp": "2026-02-20T02:19:05+08:00",
          "tree_id": "d711066af5185246b21541dfff0c007213b7ac33",
          "url": "https://github.com/feichai0017/NoKV/commit/407f26f15d69b3e9c37e6abfcb6064aef25fc17a"
        },
        "date": 1771525224120,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6709,
            "unit": "ns/op\t   4.77 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "187464 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6709,
            "unit": "ns/op",
            "extra": "187464 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.77,
            "unit": "MB/s",
            "extra": "187464 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "187464 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "187464 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15045,
            "unit": "ns/op\t 272.24 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "76536 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15045,
            "unit": "ns/op",
            "extra": "76536 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 272.24,
            "unit": "MB/s",
            "extra": "76536 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "76536 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "76536 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8361,
            "unit": "ns/op\t   7.65 MB/s\t   18988 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8361,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.65,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18988,
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
            "value": 12399,
            "unit": "ns/op\t 330.35 MB/s\t   34545 B/op\t      11 allocs/op",
            "extra": "317130 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12399,
            "unit": "ns/op",
            "extra": "317130 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 330.35,
            "unit": "MB/s",
            "extra": "317130 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34545,
            "unit": "B/op",
            "extra": "317130 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "317130 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123464,
            "unit": "ns/op\t 132.70 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123464,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.7,
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
            "value": 1485473,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1485473,
            "unit": "ns/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 586.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1951080 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 586.5,
            "unit": "ns/op",
            "extra": "1951080 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1951080 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1951080 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48895,
            "unit": "ns/op\t 167.54 MB/s\t   27598 B/op\t     454 allocs/op",
            "extra": "25500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48895,
            "unit": "ns/op",
            "extra": "25500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 167.54,
            "unit": "MB/s",
            "extra": "25500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27598,
            "unit": "B/op",
            "extra": "25500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25500 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6322123,
            "unit": "ns/op\t67523126 B/op\t    2579 allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6322123,
            "unit": "ns/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523126,
            "unit": "B/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 541.8,
            "unit": "ns/op\t 118.13 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2495038 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 541.8,
            "unit": "ns/op",
            "extra": "2495038 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 118.13,
            "unit": "MB/s",
            "extra": "2495038 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2495038 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2495038 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9350906 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.5,
            "unit": "ns/op",
            "extra": "9350906 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9350906 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9350906 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1392,
            "unit": "ns/op\t  45.99 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1392,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.99,
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
            "value": 482.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2603394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 482.8,
            "unit": "ns/op",
            "extra": "2603394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2603394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2603394 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25629,
            "unit": "ns/op\t 319.64 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "78008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25629,
            "unit": "ns/op",
            "extra": "78008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 319.64,
            "unit": "MB/s",
            "extra": "78008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "78008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "78008 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164.3,
            "unit": "ns/op\t1558.57 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7366710 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164.3,
            "unit": "ns/op",
            "extra": "7366710 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1558.57,
            "unit": "MB/s",
            "extra": "7366710 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7366710 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7366710 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 686.2,
            "unit": "ns/op\t 373.05 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3423510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 686.2,
            "unit": "ns/op",
            "extra": "3423510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 373.05,
            "unit": "MB/s",
            "extra": "3423510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3423510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3423510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1969068,
            "unit": "ns/op\t 3064041 B/op\t   40018 allocs/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1969068,
            "unit": "ns/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064041,
            "unit": "B/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "602 times\n4 procs"
          }
        ]
      }
    ]
  }
}