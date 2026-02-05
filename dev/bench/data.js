window.BENCHMARK_DATA = {
  "lastUpdate": 1770278281184,
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
          "id": "af2991252a7f7aa283fc65f9c31b67efd581457c",
          "message": "docs: expand compaction ingest research notes",
          "timestamp": "2026-02-05T15:51:14+08:00",
          "tree_id": "5dc30d2db639aa8abf76abac066bcfe72c37e797",
          "url": "https://github.com/feichai0017/NoKV/commit/af2991252a7f7aa283fc65f9c31b67efd581457c"
        },
        "date": 1770278279887,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11229,
            "unit": "ns/op\t   2.85 MB/s\t     579 B/op\t      20 allocs/op",
            "extra": "149210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11229,
            "unit": "ns/op",
            "extra": "149210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.85,
            "unit": "MB/s",
            "extra": "149210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 579,
            "unit": "B/op",
            "extra": "149210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "149210 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17503,
            "unit": "ns/op\t 234.01 MB/s\t     820 B/op\t      31 allocs/op",
            "extra": "75980 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17503,
            "unit": "ns/op",
            "extra": "75980 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 234.01,
            "unit": "MB/s",
            "extra": "75980 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 820,
            "unit": "B/op",
            "extra": "75980 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "75980 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11811,
            "unit": "ns/op\t   5.42 MB/s\t   19428 B/op\t       5 allocs/op",
            "extra": "688197 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11811,
            "unit": "ns/op",
            "extra": "688197 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.42,
            "unit": "MB/s",
            "extra": "688197 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19428,
            "unit": "B/op",
            "extra": "688197 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "688197 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9894,
            "unit": "ns/op\t 413.99 MB/s\t   18735 B/op\t       8 allocs/op",
            "extra": "249445 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9894,
            "unit": "ns/op",
            "extra": "249445 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 413.99,
            "unit": "MB/s",
            "extra": "249445 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18735,
            "unit": "B/op",
            "extra": "249445 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "249445 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 169355,
            "unit": "ns/op\t  96.74 MB/s\t   63729 B/op\t     690 allocs/op",
            "extra": "8688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 169355,
            "unit": "ns/op",
            "extra": "8688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 96.74,
            "unit": "MB/s",
            "extra": "8688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 63729,
            "unit": "B/op",
            "extra": "8688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 690,
            "unit": "allocs/op",
            "extra": "8688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2170872,
            "unit": "ns/op\t    9387 B/op\t       0 allocs/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2170872,
            "unit": "ns/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9387,
            "unit": "B/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 979.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1233523 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 979.3,
            "unit": "ns/op",
            "extra": "1233523 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1233523 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1233523 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49585,
            "unit": "ns/op\t 165.21 MB/s\t   28110 B/op\t     454 allocs/op",
            "extra": "24324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49585,
            "unit": "ns/op",
            "extra": "24324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.21,
            "unit": "MB/s",
            "extra": "24324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28110,
            "unit": "B/op",
            "extra": "24324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24324 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6534169,
            "unit": "ns/op\t67523364 B/op\t    2586 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6534169,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523364,
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
            "value": 688.2,
            "unit": "ns/op\t  92.99 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1850389 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 688.2,
            "unit": "ns/op",
            "extra": "1850389 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 92.99,
            "unit": "MB/s",
            "extra": "1850389 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1850389 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1850389 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 126.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9451564 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 126.8,
            "unit": "ns/op",
            "extra": "9451564 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9451564 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9451564 times\n4 procs"
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
            "value": 487.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2573196 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 487.5,
            "unit": "ns/op",
            "extra": "2573196 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2573196 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2573196 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 24965,
            "unit": "ns/op\t 328.13 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 24965,
            "unit": "ns/op",
            "extra": "77360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 328.13,
            "unit": "MB/s",
            "extra": "77360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.8,
            "unit": "ns/op\t1756.10 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8235732 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.8,
            "unit": "ns/op",
            "extra": "8235732 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1756.1,
            "unit": "MB/s",
            "extra": "8235732 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8235732 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8235732 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 730.5,
            "unit": "ns/op\t 350.47 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3156858 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 730.5,
            "unit": "ns/op",
            "extra": "3156858 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.47,
            "unit": "MB/s",
            "extra": "3156858 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3156858 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3156858 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2025618,
            "unit": "ns/op\t 3064061 B/op\t   40019 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2025618,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064061,
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