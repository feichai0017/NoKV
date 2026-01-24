window.BENCHMARK_DATA = {
  "lastUpdate": 1769233307824,
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
          "id": "05d40aace703506f42aa36f8fe5dc6bad8b66d57",
          "message": "docs: revamp overview page with features, benchmarks, and architecture diagram",
          "timestamp": "2026-01-24T13:40:39+08:00",
          "tree_id": "fc2d1368d6d205d932cb3f09be1d5a58f0a45e72",
          "url": "https://github.com/feichai0017/NoKV/commit/05d40aace703506f42aa36f8fe5dc6bad8b66d57"
        },
        "date": 1769233307212,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12507,
            "unit": "ns/op\t   2.56 MB/s\t     619 B/op\t      24 allocs/op",
            "extra": "88152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12507,
            "unit": "ns/op",
            "extra": "88152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.56,
            "unit": "MB/s",
            "extra": "88152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 619,
            "unit": "B/op",
            "extra": "88152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "88152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14821,
            "unit": "ns/op\t 276.36 MB/s\t     653 B/op\t      27 allocs/op",
            "extra": "89580 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14821,
            "unit": "ns/op",
            "extra": "89580 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 276.36,
            "unit": "MB/s",
            "extra": "89580 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 653,
            "unit": "B/op",
            "extra": "89580 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "89580 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9577,
            "unit": "ns/op\t   6.68 MB/s\t   15871 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9577,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.68,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 15871,
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
            "value": 10484,
            "unit": "ns/op\t 390.68 MB/s\t   20726 B/op\t       7 allocs/op",
            "extra": "225124 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10484,
            "unit": "ns/op",
            "extra": "225124 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 390.68,
            "unit": "MB/s",
            "extra": "225124 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 20726,
            "unit": "B/op",
            "extra": "225124 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "225124 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 178059,
            "unit": "ns/op\t  92.01 MB/s\t   72362 B/op\t     719 allocs/op",
            "extra": "7804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 178059,
            "unit": "ns/op",
            "extra": "7804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 92.01,
            "unit": "MB/s",
            "extra": "7804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 72362,
            "unit": "B/op",
            "extra": "7804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 719,
            "unit": "allocs/op",
            "extra": "7804 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2308585,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2308585,
            "unit": "ns/op",
            "extra": "510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1091,
            "unit": "ns/op\t      35 B/op\t       1 allocs/op",
            "extra": "1261356 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1091,
            "unit": "ns/op",
            "extra": "1261356 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "1261356 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1261356 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49831,
            "unit": "ns/op\t 164.39 MB/s\t   25442 B/op\t     454 allocs/op",
            "extra": "24024 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49831,
            "unit": "ns/op",
            "extra": "24024 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.39,
            "unit": "MB/s",
            "extra": "24024 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25442,
            "unit": "B/op",
            "extra": "24024 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24024 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6343114,
            "unit": "ns/op\t67523312 B/op\t    2586 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6343114,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523312,
            "unit": "B/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 645.6,
            "unit": "ns/op\t  99.13 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1869738 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 645.6,
            "unit": "ns/op",
            "extra": "1869738 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 99.13,
            "unit": "MB/s",
            "extra": "1869738 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1869738 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1869738 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 126.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9536434 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 126.3,
            "unit": "ns/op",
            "extra": "9536434 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9536434 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9536434 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1366,
            "unit": "ns/op\t  46.87 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1366,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.87,
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
            "value": 447.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2290317 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 447.5,
            "unit": "ns/op",
            "extra": "2290317 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2290317 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2290317 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 24815,
            "unit": "ns/op\t 330.12 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "77376 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 24815,
            "unit": "ns/op",
            "extra": "77376 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 330.12,
            "unit": "MB/s",
            "extra": "77376 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "77376 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77376 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 147.2,
            "unit": "ns/op\t1738.54 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8024281 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 147.2,
            "unit": "ns/op",
            "extra": "8024281 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1738.54,
            "unit": "MB/s",
            "extra": "8024281 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8024281 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8024281 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 717.3,
            "unit": "ns/op\t 356.87 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3188104 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 717.3,
            "unit": "ns/op",
            "extra": "3188104 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 356.87,
            "unit": "MB/s",
            "extra": "3188104 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3188104 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3188104 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2000689,
            "unit": "ns/op\t 3064049 B/op\t   40019 allocs/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2000689,
            "unit": "ns/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064049,
            "unit": "B/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "597 times\n4 procs"
          }
        ]
      }
    ]
  }
}