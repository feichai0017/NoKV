window.BENCHMARK_DATA = {
  "lastUpdate": 1770905057070,
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
          "id": "0a5a0780814be40cddef6238734796b99935bdb2",
          "message": "refactor: centralize min_commit_ts check in commitKey",
          "timestamp": "2026-02-12T22:02:33+08:00",
          "tree_id": "1034133f367ed93e547d51829636a5ac3acf4f12",
          "url": "https://github.com/feichai0017/NoKV/commit/0a5a0780814be40cddef6238734796b99935bdb2"
        },
        "date": 1770905055239,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7248,
            "unit": "ns/op\t   4.41 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "180477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7248,
            "unit": "ns/op",
            "extra": "180477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.41,
            "unit": "MB/s",
            "extra": "180477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "180477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "180477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14163,
            "unit": "ns/op\t 289.21 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "84304 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14163,
            "unit": "ns/op",
            "extra": "84304 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 289.21,
            "unit": "MB/s",
            "extra": "84304 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "84304 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "84304 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7431,
            "unit": "ns/op\t   8.61 MB/s\t   18744 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7431,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.61,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18744,
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
            "value": 11329,
            "unit": "ns/op\t 361.54 MB/s\t   31854 B/op\t       8 allocs/op",
            "extra": "372682 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11329,
            "unit": "ns/op",
            "extra": "372682 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 361.54,
            "unit": "MB/s",
            "extra": "372682 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 31854,
            "unit": "B/op",
            "extra": "372682 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "372682 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123675,
            "unit": "ns/op\t 132.48 MB/s\t   56858 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123675,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.48,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56858,
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
            "value": 1609504,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1609504,
            "unit": "ns/op",
            "extra": "740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 559.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2180821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 559.4,
            "unit": "ns/op",
            "extra": "2180821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2180821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2180821 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48200,
            "unit": "ns/op\t 169.96 MB/s\t   27715 B/op\t     454 allocs/op",
            "extra": "25222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48200,
            "unit": "ns/op",
            "extra": "25222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.96,
            "unit": "MB/s",
            "extra": "25222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27715,
            "unit": "B/op",
            "extra": "25222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25222 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8364079,
            "unit": "ns/op\t67523340 B/op\t    2586 allocs/op",
            "extra": "139 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8364079,
            "unit": "ns/op",
            "extra": "139 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523340,
            "unit": "B/op",
            "extra": "139 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "139 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 665.5,
            "unit": "ns/op\t  96.17 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1827795 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 665.5,
            "unit": "ns/op",
            "extra": "1827795 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 96.17,
            "unit": "MB/s",
            "extra": "1827795 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1827795 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1827795 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 114.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10474624 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 114.9,
            "unit": "ns/op",
            "extra": "10474624 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10474624 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10474624 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1257,
            "unit": "ns/op\t  50.92 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1257,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 50.92,
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
            "value": 465.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2523636 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 465.5,
            "unit": "ns/op",
            "extra": "2523636 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2523636 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2523636 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 22746,
            "unit": "ns/op\t 360.15 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "102784 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 22746,
            "unit": "ns/op",
            "extra": "102784 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 360.15,
            "unit": "MB/s",
            "extra": "102784 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "102784 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "102784 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148.7,
            "unit": "ns/op\t1721.42 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7973720 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.7,
            "unit": "ns/op",
            "extra": "7973720 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1721.42,
            "unit": "MB/s",
            "extra": "7973720 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7973720 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7973720 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 588,
            "unit": "ns/op\t 435.39 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3953115 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 588,
            "unit": "ns/op",
            "extra": "3953115 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 435.39,
            "unit": "MB/s",
            "extra": "3953115 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3953115 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3953115 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2045147,
            "unit": "ns/op\t 3064050 B/op\t   40019 allocs/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2045147,
            "unit": "ns/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064050,
            "unit": "B/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "582 times\n4 procs"
          }
        ]
      }
    ]
  }
}