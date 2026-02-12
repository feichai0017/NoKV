window.BENCHMARK_DATA = {
  "lastUpdate": 1770904342934,
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
          "id": "43d9e1176eb0ef990f0ba09ce3d24f9d231575c5",
          "message": "fix: enforce min_commit_ts in ResolveLock path",
          "timestamp": "2026-02-12T21:51:13+08:00",
          "tree_id": "dce02e7ab4827c910697dca757d6854c1b9eb46b",
          "url": "https://github.com/feichai0017/NoKV/commit/43d9e1176eb0ef990f0ba09ce3d24f9d231575c5"
        },
        "date": 1770904341689,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 5667,
            "unit": "ns/op\t   5.65 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "210810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 5667,
            "unit": "ns/op",
            "extra": "210810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 5.65,
            "unit": "MB/s",
            "extra": "210810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "210810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "210810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14885,
            "unit": "ns/op\t 275.18 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "79062 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14885,
            "unit": "ns/op",
            "extra": "79062 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 275.18,
            "unit": "MB/s",
            "extra": "79062 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "79062 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "79062 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7234,
            "unit": "ns/op\t   8.85 MB/s\t   16928 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7234,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.85,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16928,
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
            "value": 10549,
            "unit": "ns/op\t 388.30 MB/s\t   28161 B/op\t       8 allocs/op",
            "extra": "405032 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10549,
            "unit": "ns/op",
            "extra": "405032 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 388.3,
            "unit": "MB/s",
            "extra": "405032 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 28161,
            "unit": "B/op",
            "extra": "405032 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "405032 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124033,
            "unit": "ns/op\t 132.09 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124033,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.09,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56857,
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
            "value": 1515025,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1515025,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 592.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2090200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 592.5,
            "unit": "ns/op",
            "extra": "2090200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2090200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2090200 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48076,
            "unit": "ns/op\t 170.40 MB/s\t   25458 B/op\t     454 allocs/op",
            "extra": "23979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48076,
            "unit": "ns/op",
            "extra": "23979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 170.4,
            "unit": "MB/s",
            "extra": "23979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25458,
            "unit": "B/op",
            "extra": "23979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23979 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6415831,
            "unit": "ns/op\t67523318 B/op\t    2586 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6415831,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523318,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 567.1,
            "unit": "ns/op\t 112.85 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2116539 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 567.1,
            "unit": "ns/op",
            "extra": "2116539 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 112.85,
            "unit": "MB/s",
            "extra": "2116539 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2116539 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2116539 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9391261 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "9391261 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9391261 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9391261 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1402,
            "unit": "ns/op\t  45.64 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1402,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.64,
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
            "value": 479.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2609022 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 479.1,
            "unit": "ns/op",
            "extra": "2609022 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2609022 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2609022 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25726,
            "unit": "ns/op\t 318.43 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76262 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25726,
            "unit": "ns/op",
            "extra": "76262 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 318.43,
            "unit": "MB/s",
            "extra": "76262 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76262 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76262 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.5,
            "unit": "ns/op\t1759.78 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8186607 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.5,
            "unit": "ns/op",
            "extra": "8186607 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1759.78,
            "unit": "MB/s",
            "extra": "8186607 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8186607 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8186607 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 709.2,
            "unit": "ns/op\t 360.97 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3222218 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 709.2,
            "unit": "ns/op",
            "extra": "3222218 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 360.97,
            "unit": "MB/s",
            "extra": "3222218 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3222218 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3222218 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2074992,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "572 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2074992,
            "unit": "ns/op",
            "extra": "572 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "572 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "572 times\n4 procs"
          }
        ]
      }
    ]
  }
}