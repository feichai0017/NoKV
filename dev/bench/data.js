window.BENCHMARK_DATA = {
  "lastUpdate": 1769962214049,
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
          "id": "4eefeb05348a10d9085e92a49ce949bf0b2f517d",
          "message": "Update docs",
          "timestamp": "2026-02-02T00:08:54+08:00",
          "tree_id": "55ffc9036c772614d4c3d84a9d97e8b29b989456",
          "url": "https://github.com/feichai0017/NoKV/commit/4eefeb05348a10d9085e92a49ce949bf0b2f517d"
        },
        "date": 1769962212218,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9564,
            "unit": "ns/op\t   3.35 MB/s\t     634 B/op\t      24 allocs/op",
            "extra": "127970 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9564,
            "unit": "ns/op",
            "extra": "127970 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.35,
            "unit": "MB/s",
            "extra": "127970 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 634,
            "unit": "B/op",
            "extra": "127970 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "127970 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14623,
            "unit": "ns/op\t 280.10 MB/s\t     705 B/op\t      27 allocs/op",
            "extra": "89409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14623,
            "unit": "ns/op",
            "extra": "89409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 280.1,
            "unit": "MB/s",
            "extra": "89409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 705,
            "unit": "B/op",
            "extra": "89409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "89409 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11559,
            "unit": "ns/op\t   5.54 MB/s\t   18985 B/op\t       5 allocs/op",
            "extra": "701988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11559,
            "unit": "ns/op",
            "extra": "701988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.54,
            "unit": "MB/s",
            "extra": "701988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18985,
            "unit": "B/op",
            "extra": "701988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "701988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10231,
            "unit": "ns/op\t 400.35 MB/s\t   18950 B/op\t       7 allocs/op",
            "extra": "232746 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10231,
            "unit": "ns/op",
            "extra": "232746 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 400.35,
            "unit": "MB/s",
            "extra": "232746 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18950,
            "unit": "B/op",
            "extra": "232746 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "232746 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 171882,
            "unit": "ns/op\t  95.32 MB/s\t   65042 B/op\t     687 allocs/op",
            "extra": "8956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 171882,
            "unit": "ns/op",
            "extra": "8956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.32,
            "unit": "MB/s",
            "extra": "8956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 65042,
            "unit": "B/op",
            "extra": "8956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 687,
            "unit": "allocs/op",
            "extra": "8956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2188981,
            "unit": "ns/op\t    9110 B/op\t       0 allocs/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2188981,
            "unit": "ns/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9110,
            "unit": "B/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 978,
            "unit": "ns/op\t      36 B/op\t       1 allocs/op",
            "extra": "1213411 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 978,
            "unit": "ns/op",
            "extra": "1213411 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "1213411 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1213411 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48902,
            "unit": "ns/op\t 167.52 MB/s\t   25732 B/op\t     454 allocs/op",
            "extra": "23226 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48902,
            "unit": "ns/op",
            "extra": "23226 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 167.52,
            "unit": "MB/s",
            "extra": "23226 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25732,
            "unit": "B/op",
            "extra": "23226 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23226 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6572910,
            "unit": "ns/op\t67523287 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6572910,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523287,
            "unit": "B/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 637.7,
            "unit": "ns/op\t 100.36 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1846538 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 637.7,
            "unit": "ns/op",
            "extra": "1846538 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 100.36,
            "unit": "MB/s",
            "extra": "1846538 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1846538 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1846538 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9345482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.5,
            "unit": "ns/op",
            "extra": "9345482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9345482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9345482 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1416,
            "unit": "ns/op\t  45.21 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1416,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.21,
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
            "value": 454.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2442872 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 454.8,
            "unit": "ns/op",
            "extra": "2442872 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2442872 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2442872 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25833,
            "unit": "ns/op\t 317.12 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "76918 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25833,
            "unit": "ns/op",
            "extra": "76918 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 317.12,
            "unit": "MB/s",
            "extra": "76918 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "76918 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76918 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 147.9,
            "unit": "ns/op\t1731.19 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8053224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 147.9,
            "unit": "ns/op",
            "extra": "8053224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1731.19,
            "unit": "MB/s",
            "extra": "8053224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8053224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8053224 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 730.8,
            "unit": "ns/op\t 350.30 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3190621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 730.8,
            "unit": "ns/op",
            "extra": "3190621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.3,
            "unit": "MB/s",
            "extra": "3190621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3190621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3190621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2007500,
            "unit": "ns/op\t 3064041 B/op\t   40019 allocs/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2007500,
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