window.BENCHMARK_DATA = {
  "lastUpdate": 1769239689179,
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
          "id": "28b430922edb43f53a9ccb3acb606211b205bb01",
          "message": "docs: update readme md",
          "timestamp": "2026-01-24T15:27:01+08:00",
          "tree_id": "352344cb2dfdf48e890c97fee9774bdf913b72e7",
          "url": "https://github.com/feichai0017/NoKV/commit/28b430922edb43f53a9ccb3acb606211b205bb01"
        },
        "date": 1769239688478,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11931,
            "unit": "ns/op\t   2.68 MB/s\t     653 B/op\t      24 allocs/op",
            "extra": "134326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11931,
            "unit": "ns/op",
            "extra": "134326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.68,
            "unit": "MB/s",
            "extra": "134326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 653,
            "unit": "B/op",
            "extra": "134326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "134326 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16157,
            "unit": "ns/op\t 253.52 MB/s\t     665 B/op\t      27 allocs/op",
            "extra": "87013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16157,
            "unit": "ns/op",
            "extra": "87013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 253.52,
            "unit": "MB/s",
            "extra": "87013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 665,
            "unit": "B/op",
            "extra": "87013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "87013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10731,
            "unit": "ns/op\t   5.96 MB/s\t   16489 B/op\t       5 allocs/op",
            "extra": "584418 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10731,
            "unit": "ns/op",
            "extra": "584418 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.96,
            "unit": "MB/s",
            "extra": "584418 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16489,
            "unit": "B/op",
            "extra": "584418 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "584418 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10591,
            "unit": "ns/op\t 386.74 MB/s\t   18909 B/op\t       7 allocs/op",
            "extra": "221889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10591,
            "unit": "ns/op",
            "extra": "221889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 386.74,
            "unit": "MB/s",
            "extra": "221889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18909,
            "unit": "B/op",
            "extra": "221889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "221889 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 183812,
            "unit": "ns/op\t  89.13 MB/s\t   62108 B/op\t     674 allocs/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 183812,
            "unit": "ns/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 89.13,
            "unit": "MB/s",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62108,
            "unit": "B/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 674,
            "unit": "allocs/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2313574,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2313574,
            "unit": "ns/op",
            "extra": "548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "548 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1085,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1085,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50201,
            "unit": "ns/op\t 163.18 MB/s\t   28052 B/op\t     454 allocs/op",
            "extra": "24451 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50201,
            "unit": "ns/op",
            "extra": "24451 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.18,
            "unit": "MB/s",
            "extra": "24451 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28052,
            "unit": "B/op",
            "extra": "24451 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24451 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6696543,
            "unit": "ns/op\t67523329 B/op\t    2586 allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6696543,
            "unit": "ns/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523329,
            "unit": "B/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 665.1,
            "unit": "ns/op\t  96.23 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1796180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 665.1,
            "unit": "ns/op",
            "extra": "1796180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 96.23,
            "unit": "MB/s",
            "extra": "1796180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1796180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1796180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9213526 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.3,
            "unit": "ns/op",
            "extra": "9213526 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9213526 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9213526 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1444,
            "unit": "ns/op\t  44.34 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1444,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.34,
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
            "value": 489,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2654107 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 489,
            "unit": "ns/op",
            "extra": "2654107 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2654107 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2654107 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25976,
            "unit": "ns/op\t 315.37 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "75532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25976,
            "unit": "ns/op",
            "extra": "75532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.37,
            "unit": "MB/s",
            "extra": "75532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "75532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148.5,
            "unit": "ns/op\t1723.34 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8018551 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.5,
            "unit": "ns/op",
            "extra": "8018551 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1723.34,
            "unit": "MB/s",
            "extra": "8018551 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8018551 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8018551 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 736.7,
            "unit": "ns/op\t 347.50 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3170467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 736.7,
            "unit": "ns/op",
            "extra": "3170467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 347.5,
            "unit": "MB/s",
            "extra": "3170467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3170467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3170467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2050057,
            "unit": "ns/op\t 3064051 B/op\t   40019 allocs/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2050057,
            "unit": "ns/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064051,
            "unit": "B/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "573 times\n4 procs"
          }
        ]
      }
    ]
  }
}