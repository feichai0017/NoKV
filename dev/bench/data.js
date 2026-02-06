window.BENCHMARK_DATA = {
  "lastUpdate": 1770374438617,
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
          "id": "390fa54dda1f6c293712d53d65165621e7edf1ad",
          "message": "feat: tune hotring defaults",
          "timestamp": "2026-02-06T18:38:27+08:00",
          "tree_id": "18af96cc7db98a14be497ad84cc9b4f3dd520dba",
          "url": "https://github.com/feichai0017/NoKV/commit/390fa54dda1f6c293712d53d65165621e7edf1ad"
        },
        "date": 1770374436847,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11854,
            "unit": "ns/op\t   2.70 MB/s\t     576 B/op\t      20 allocs/op",
            "extra": "215258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11854,
            "unit": "ns/op",
            "extra": "215258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.7,
            "unit": "MB/s",
            "extra": "215258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 576,
            "unit": "B/op",
            "extra": "215258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "215258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16996,
            "unit": "ns/op\t 241.00 MB/s\t     818 B/op\t      31 allocs/op",
            "extra": "90058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16996,
            "unit": "ns/op",
            "extra": "90058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 241,
            "unit": "MB/s",
            "extra": "90058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 818,
            "unit": "B/op",
            "extra": "90058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "90058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11995,
            "unit": "ns/op\t   5.34 MB/s\t   18212 B/op\t       5 allocs/op",
            "extra": "669942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11995,
            "unit": "ns/op",
            "extra": "669942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.34,
            "unit": "MB/s",
            "extra": "669942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18212,
            "unit": "B/op",
            "extra": "669942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "669942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10782,
            "unit": "ns/op\t 379.89 MB/s\t   17647 B/op\t       7 allocs/op",
            "extra": "221326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10782,
            "unit": "ns/op",
            "extra": "221326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 379.89,
            "unit": "MB/s",
            "extra": "221326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17647,
            "unit": "B/op",
            "extra": "221326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "221326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 171127,
            "unit": "ns/op\t  95.74 MB/s\t   58153 B/op\t     665 allocs/op",
            "extra": "9723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 171127,
            "unit": "ns/op",
            "extra": "9723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.74,
            "unit": "MB/s",
            "extra": "9723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 58153,
            "unit": "B/op",
            "extra": "9723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 665,
            "unit": "allocs/op",
            "extra": "9723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2124304,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2124304,
            "unit": "ns/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 949.8,
            "unit": "ns/op\t      35 B/op\t       1 allocs/op",
            "extra": "1230354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 949.8,
            "unit": "ns/op",
            "extra": "1230354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "1230354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1230354 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 51015,
            "unit": "ns/op\t 160.58 MB/s\t   27805 B/op\t     454 allocs/op",
            "extra": "25012 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51015,
            "unit": "ns/op",
            "extra": "25012 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 160.58,
            "unit": "MB/s",
            "extra": "25012 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27805,
            "unit": "B/op",
            "extra": "25012 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25012 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6947125,
            "unit": "ns/op\t67523226 B/op\t    2587 allocs/op",
            "extra": "166 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6947125,
            "unit": "ns/op",
            "extra": "166 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523226,
            "unit": "B/op",
            "extra": "166 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "166 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 627.8,
            "unit": "ns/op\t 101.94 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1877028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 627.8,
            "unit": "ns/op",
            "extra": "1877028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 101.94,
            "unit": "MB/s",
            "extra": "1877028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1877028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1877028 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 131.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9110924 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 131.7,
            "unit": "ns/op",
            "extra": "9110924 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9110924 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9110924 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1431,
            "unit": "ns/op\t  44.73 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1431,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.73,
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
            "value": 488.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2528174 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 488.6,
            "unit": "ns/op",
            "extra": "2528174 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2528174 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2528174 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25931,
            "unit": "ns/op\t 315.92 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76062 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25931,
            "unit": "ns/op",
            "extra": "76062 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.92,
            "unit": "MB/s",
            "extra": "76062 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76062 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76062 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 146.2,
            "unit": "ns/op\t1751.52 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8072972 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 146.2,
            "unit": "ns/op",
            "extra": "8072972 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1751.52,
            "unit": "MB/s",
            "extra": "8072972 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8072972 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8072972 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 742,
            "unit": "ns/op\t 345.02 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3122580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 742,
            "unit": "ns/op",
            "extra": "3122580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 345.02,
            "unit": "MB/s",
            "extra": "3122580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3122580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3122580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2029296,
            "unit": "ns/op\t 3064041 B/op\t   40019 allocs/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2029296,
            "unit": "ns/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064041,
            "unit": "B/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "594 times\n4 procs"
          }
        ]
      }
    ]
  }
}