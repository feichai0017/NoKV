window.BENCHMARK_DATA = {
  "lastUpdate": 1769241321256,
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
          "id": "7e8c453a8b8b7e2eb756e49df28bf843d5fc413f",
          "message": "docs: fix mermaid diagram syntax error",
          "timestamp": "2026-01-24T15:54:13+08:00",
          "tree_id": "3d51c0e2b42d514628c9c649eb96d7b7f0406d71",
          "url": "https://github.com/feichai0017/NoKV/commit/7e8c453a8b8b7e2eb756e49df28bf843d5fc413f"
        },
        "date": 1769241320242,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12846,
            "unit": "ns/op\t   2.49 MB/s\t     623 B/op\t      24 allocs/op",
            "extra": "138607 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12846,
            "unit": "ns/op",
            "extra": "138607 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.49,
            "unit": "MB/s",
            "extra": "138607 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 623,
            "unit": "B/op",
            "extra": "138607 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "138607 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16139,
            "unit": "ns/op\t 253.80 MB/s\t     652 B/op\t      27 allocs/op",
            "extra": "84048 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16139,
            "unit": "ns/op",
            "extra": "84048 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 253.8,
            "unit": "MB/s",
            "extra": "84048 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 652,
            "unit": "B/op",
            "extra": "84048 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "84048 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12750,
            "unit": "ns/op\t   5.02 MB/s\t   20546 B/op\t       5 allocs/op",
            "extra": "624363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12750,
            "unit": "ns/op",
            "extra": "624363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.02,
            "unit": "MB/s",
            "extra": "624363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20546,
            "unit": "B/op",
            "extra": "624363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "624363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10222,
            "unit": "ns/op\t 400.72 MB/s\t   18516 B/op\t       8 allocs/op",
            "extra": "239061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10222,
            "unit": "ns/op",
            "extra": "239061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 400.72,
            "unit": "MB/s",
            "extra": "239061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18516,
            "unit": "B/op",
            "extra": "239061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "239061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 175631,
            "unit": "ns/op\t  93.29 MB/s\t   53755 B/op\t     636 allocs/op",
            "extra": "5760 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 175631,
            "unit": "ns/op",
            "extra": "5760 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 93.29,
            "unit": "MB/s",
            "extra": "5760 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 53755,
            "unit": "B/op",
            "extra": "5760 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 636,
            "unit": "allocs/op",
            "extra": "5760 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2277416,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2277416,
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
            "value": 1020,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1020,
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
            "value": 49643,
            "unit": "ns/op\t 165.02 MB/s\t   25465 B/op\t     454 allocs/op",
            "extra": "23958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49643,
            "unit": "ns/op",
            "extra": "23958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.02,
            "unit": "MB/s",
            "extra": "23958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25465,
            "unit": "B/op",
            "extra": "23958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6679196,
            "unit": "ns/op\t67523275 B/op\t    2586 allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6679196,
            "unit": "ns/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523275,
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
            "value": 672.1,
            "unit": "ns/op\t  95.22 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1804176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 672.1,
            "unit": "ns/op",
            "extra": "1804176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 95.22,
            "unit": "MB/s",
            "extra": "1804176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1804176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1804176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "8992248 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.2,
            "unit": "ns/op",
            "extra": "8992248 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "8992248 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8992248 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1448,
            "unit": "ns/op\t  44.19 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1448,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.19,
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
            "value": 467.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2542316 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 467.8,
            "unit": "ns/op",
            "extra": "2542316 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2542316 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2542316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27172,
            "unit": "ns/op\t 301.48 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "76894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27172,
            "unit": "ns/op",
            "extra": "76894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 301.48,
            "unit": "MB/s",
            "extra": "76894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "76894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76894 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 146.5,
            "unit": "ns/op\t1746.91 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8071874 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 146.5,
            "unit": "ns/op",
            "extra": "8071874 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1746.91,
            "unit": "MB/s",
            "extra": "8071874 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8071874 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8071874 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 736.6,
            "unit": "ns/op\t 347.56 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3160171 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 736.6,
            "unit": "ns/op",
            "extra": "3160171 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 347.56,
            "unit": "MB/s",
            "extra": "3160171 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3160171 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3160171 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2034941,
            "unit": "ns/op\t 3064044 B/op\t   40019 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2034941,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064044,
            "unit": "B/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "589 times\n4 procs"
          }
        ]
      }
    ]
  }
}