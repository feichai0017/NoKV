window.BENCHMARK_DATA = {
  "lastUpdate": 1770904175837,
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
          "id": "ba573fc74a202917a3c94b8fb2d3a74b065fecfe",
          "message": "Merge pull request #51 from zzzzwc/zwc-commit-ts-expired-check\n\nfeat: support min_commit_ts validation for percolator commit",
          "timestamp": "2026-02-12T21:48:24+08:00",
          "tree_id": "e7d99878b1389790373209c2fbf82cc980640224",
          "url": "https://github.com/feichai0017/NoKV/commit/ba573fc74a202917a3c94b8fb2d3a74b065fecfe"
        },
        "date": 1770904173777,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6950,
            "unit": "ns/op\t   4.60 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "144516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6950,
            "unit": "ns/op",
            "extra": "144516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.6,
            "unit": "MB/s",
            "extra": "144516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "144516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "144516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17056,
            "unit": "ns/op\t 240.15 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "73202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17056,
            "unit": "ns/op",
            "extra": "73202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 240.15,
            "unit": "MB/s",
            "extra": "73202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "73202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "73202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7275,
            "unit": "ns/op\t   8.80 MB/s\t   16861 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7275,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.8,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16861,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10621,
            "unit": "ns/op\t 385.65 MB/s\t   28074 B/op\t       8 allocs/op",
            "extra": "403065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10621,
            "unit": "ns/op",
            "extra": "403065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 385.65,
            "unit": "MB/s",
            "extra": "403065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 28074,
            "unit": "B/op",
            "extra": "403065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "403065 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127275,
            "unit": "ns/op\t 128.73 MB/s\t   56856 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127275,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.73,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56856,
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
            "value": 1489178,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1489178,
            "unit": "ns/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 622.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2014652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 622.4,
            "unit": "ns/op",
            "extra": "2014652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2014652 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2014652 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49081,
            "unit": "ns/op\t 166.91 MB/s\t   25563 B/op\t     454 allocs/op",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49081,
            "unit": "ns/op",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.91,
            "unit": "MB/s",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25563,
            "unit": "B/op",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6637476,
            "unit": "ns/op\t67523213 B/op\t    2585 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6637476,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523213,
            "unit": "B/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2585,
            "unit": "allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 685.1,
            "unit": "ns/op\t  93.42 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1805652 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 685.1,
            "unit": "ns/op",
            "extra": "1805652 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 93.42,
            "unit": "MB/s",
            "extra": "1805652 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1805652 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1805652 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9335715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.4,
            "unit": "ns/op",
            "extra": "9335715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9335715 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9335715 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1462,
            "unit": "ns/op\t  43.78 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1462,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.78,
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
            "value": 487,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2536920 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 487,
            "unit": "ns/op",
            "extra": "2536920 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2536920 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2536920 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25382,
            "unit": "ns/op\t 322.75 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25382,
            "unit": "ns/op",
            "extra": "77190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 322.75,
            "unit": "MB/s",
            "extra": "77190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149,
            "unit": "ns/op\t1718.16 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8003014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149,
            "unit": "ns/op",
            "extra": "8003014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1718.16,
            "unit": "MB/s",
            "extra": "8003014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8003014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8003014 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 727.2,
            "unit": "ns/op\t 352.02 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3187790 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 727.2,
            "unit": "ns/op",
            "extra": "3187790 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 352.02,
            "unit": "MB/s",
            "extra": "3187790 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3187790 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3187790 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2078938,
            "unit": "ns/op\t 3064043 B/op\t   40019 allocs/op",
            "extra": "566 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2078938,
            "unit": "ns/op",
            "extra": "566 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064043,
            "unit": "B/op",
            "extra": "566 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "566 times\n4 procs"
          }
        ]
      }
    ]
  }
}