window.BENCHMARK_DATA = {
  "lastUpdate": 1771210881316,
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
          "id": "7bf314e5421fe26cf083bdee6d8ee5dd0566b8d6",
          "message": "refactor: remove regionutil and consolidate bloom logic",
          "timestamp": "2026-02-16T10:59:37+08:00",
          "tree_id": "9af13274db97d392454273a90bbf333ea92b02d1",
          "url": "https://github.com/feichai0017/NoKV/commit/7bf314e5421fe26cf083bdee6d8ee5dd0566b8d6"
        },
        "date": 1771210880373,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7851,
            "unit": "ns/op\t   4.08 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "150945 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7851,
            "unit": "ns/op",
            "extra": "150945 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.08,
            "unit": "MB/s",
            "extra": "150945 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "150945 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "150945 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16526,
            "unit": "ns/op\t 247.85 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "96921 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16526,
            "unit": "ns/op",
            "extra": "96921 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 247.85,
            "unit": "MB/s",
            "extra": "96921 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "96921 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "96921 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7844,
            "unit": "ns/op\t   8.16 MB/s\t   17118 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7844,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.16,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17118,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11967,
            "unit": "ns/op\t 342.28 MB/s\t   32482 B/op\t      11 allocs/op",
            "extra": "333679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11967,
            "unit": "ns/op",
            "extra": "333679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 342.28,
            "unit": "MB/s",
            "extra": "333679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32482,
            "unit": "B/op",
            "extra": "333679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "333679 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122367,
            "unit": "ns/op\t 133.89 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122367,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.89,
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
            "value": 1506889,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "675 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1506889,
            "unit": "ns/op",
            "extra": "675 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "675 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "675 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 602.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2010704 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 602.9,
            "unit": "ns/op",
            "extra": "2010704 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2010704 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2010704 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50130,
            "unit": "ns/op\t 163.41 MB/s\t   27889 B/op\t     454 allocs/op",
            "extra": "24818 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50130,
            "unit": "ns/op",
            "extra": "24818 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.41,
            "unit": "MB/s",
            "extra": "24818 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27889,
            "unit": "B/op",
            "extra": "24818 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24818 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6568966,
            "unit": "ns/op\t67523129 B/op\t    2579 allocs/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6568966,
            "unit": "ns/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523129,
            "unit": "B/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 590.7,
            "unit": "ns/op\t 108.34 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2094715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 590.7,
            "unit": "ns/op",
            "extra": "2094715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 108.34,
            "unit": "MB/s",
            "extra": "2094715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2094715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2094715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9331182 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "9331182 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9331182 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9331182 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1430,
            "unit": "ns/op\t  44.77 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1430,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.77,
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
            "value": 493.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2588119 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 493.8,
            "unit": "ns/op",
            "extra": "2588119 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2588119 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2588119 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26899,
            "unit": "ns/op\t 304.54 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73839 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26899,
            "unit": "ns/op",
            "extra": "73839 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 304.54,
            "unit": "MB/s",
            "extra": "73839 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73839 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73839 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 153.8,
            "unit": "ns/op\t1664.29 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7760148 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 153.8,
            "unit": "ns/op",
            "extra": "7760148 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1664.29,
            "unit": "MB/s",
            "extra": "7760148 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7760148 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7760148 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 689.2,
            "unit": "ns/op\t 371.44 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3416810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 689.2,
            "unit": "ns/op",
            "extra": "3416810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 371.44,
            "unit": "MB/s",
            "extra": "3416810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3416810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3416810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1996713,
            "unit": "ns/op\t 3064028 B/op\t   40017 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1996713,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064028,
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