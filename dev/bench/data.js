window.BENCHMARK_DATA = {
  "lastUpdate": 1770273281424,
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
          "id": "0f05f254d73dd803a0c71a325880a16dced9a4ca",
          "message": "docs: merge vlog notes with expanded design",
          "timestamp": "2026-02-05T14:33:28+08:00",
          "tree_id": "d1d547494570e7d0660f430af6c66ac2b22b57b2",
          "url": "https://github.com/feichai0017/NoKV/commit/0f05f254d73dd803a0c71a325880a16dced9a4ca"
        },
        "date": 1770273279954,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12297,
            "unit": "ns/op\t   2.60 MB/s\t     577 B/op\t      20 allocs/op",
            "extra": "107738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12297,
            "unit": "ns/op",
            "extra": "107738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.6,
            "unit": "MB/s",
            "extra": "107738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 577,
            "unit": "B/op",
            "extra": "107738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "107738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14649,
            "unit": "ns/op\t 279.60 MB/s\t     833 B/op\t      31 allocs/op",
            "extra": "112764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14649,
            "unit": "ns/op",
            "extra": "112764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 279.6,
            "unit": "MB/s",
            "extra": "112764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 833,
            "unit": "B/op",
            "extra": "112764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "112764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10676,
            "unit": "ns/op\t   5.99 MB/s\t   18998 B/op\t       5 allocs/op",
            "extra": "855702 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10676,
            "unit": "ns/op",
            "extra": "855702 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.99,
            "unit": "MB/s",
            "extra": "855702 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18998,
            "unit": "B/op",
            "extra": "855702 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "855702 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10167,
            "unit": "ns/op\t 402.87 MB/s\t   19598 B/op\t       7 allocs/op",
            "extra": "239247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10167,
            "unit": "ns/op",
            "extra": "239247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 402.87,
            "unit": "MB/s",
            "extra": "239247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19598,
            "unit": "B/op",
            "extra": "239247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "239247 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 161461,
            "unit": "ns/op\t 101.47 MB/s\t   56860 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 161461,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 101.47,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56860,
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
            "value": 2300559,
            "unit": "ns/op\t    9247 B/op\t       0 allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2300559,
            "unit": "ns/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9247,
            "unit": "B/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 875.2,
            "unit": "ns/op\t      35 B/op\t       1 allocs/op",
            "extra": "1451131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 875.2,
            "unit": "ns/op",
            "extra": "1451131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "1451131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1451131 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48649,
            "unit": "ns/op\t 168.39 MB/s\t   27727 B/op\t     454 allocs/op",
            "extra": "25192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48649,
            "unit": "ns/op",
            "extra": "25192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.39,
            "unit": "MB/s",
            "extra": "25192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27727,
            "unit": "B/op",
            "extra": "25192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 10576671,
            "unit": "ns/op\t67523536 B/op\t    2588 allocs/op",
            "extra": "133 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 10576671,
            "unit": "ns/op",
            "extra": "133 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523536,
            "unit": "B/op",
            "extra": "133 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2588,
            "unit": "allocs/op",
            "extra": "133 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 741.5,
            "unit": "ns/op\t  86.31 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1777390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 741.5,
            "unit": "ns/op",
            "extra": "1777390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 86.31,
            "unit": "MB/s",
            "extra": "1777390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1777390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1777390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 112.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10579974 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 112.9,
            "unit": "ns/op",
            "extra": "10579974 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10579974 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10579974 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1346,
            "unit": "ns/op\t  47.56 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1346,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47.56,
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
            "value": 471.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2506974 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 471.9,
            "unit": "ns/op",
            "extra": "2506974 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2506974 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2506974 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 23595,
            "unit": "ns/op\t 347.19 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "98863 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 23595,
            "unit": "ns/op",
            "extra": "98863 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 347.19,
            "unit": "MB/s",
            "extra": "98863 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "98863 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "98863 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 151.3,
            "unit": "ns/op\t1691.81 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7811593 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 151.3,
            "unit": "ns/op",
            "extra": "7811593 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1691.81,
            "unit": "MB/s",
            "extra": "7811593 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7811593 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7811593 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 610.6,
            "unit": "ns/op\t 419.28 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3914467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 610.6,
            "unit": "ns/op",
            "extra": "3914467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 419.28,
            "unit": "MB/s",
            "extra": "3914467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3914467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3914467 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2034012,
            "unit": "ns/op\t 3064040 B/op\t   40019 allocs/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2034012,
            "unit": "ns/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "588 times\n4 procs"
          }
        ]
      }
    ]
  }
}