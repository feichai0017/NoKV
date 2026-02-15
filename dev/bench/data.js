window.BENCHMARK_DATA = {
  "lastUpdate": 1771158654078,
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
          "id": "540354f19c3ea32e4b11a30237d3bcb5964bf692",
          "message": "benchmark: add pebble engine and isolate benchmark deps",
          "timestamp": "2026-02-15T20:29:12+08:00",
          "tree_id": "155d5e47f24f16dd4100709d607781cbcd1e8f4c",
          "url": "https://github.com/feichai0017/NoKV/commit/540354f19c3ea32e4b11a30237d3bcb5964bf692"
        },
        "date": 1771158652543,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7134,
            "unit": "ns/op\t   4.49 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "171872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7134,
            "unit": "ns/op",
            "extra": "171872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.49,
            "unit": "MB/s",
            "extra": "171872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "171872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "171872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16847,
            "unit": "ns/op\t 243.13 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "81493 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16847,
            "unit": "ns/op",
            "extra": "81493 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 243.13,
            "unit": "MB/s",
            "extra": "81493 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "81493 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "81493 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8988,
            "unit": "ns/op\t   7.12 MB/s\t   21227 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8988,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.12,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 21227,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 8638,
            "unit": "ns/op\t 474.17 MB/s\t   26003 B/op\t      11 allocs/op",
            "extra": "289898 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8638,
            "unit": "ns/op",
            "extra": "289898 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 474.17,
            "unit": "MB/s",
            "extra": "289898 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26003,
            "unit": "B/op",
            "extra": "289898 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "289898 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123263,
            "unit": "ns/op\t 132.92 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123263,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.92,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56848,
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
            "value": 1493466,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1493466,
            "unit": "ns/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "800 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 597,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2071833 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 597,
            "unit": "ns/op",
            "extra": "2071833 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2071833 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2071833 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49572,
            "unit": "ns/op\t 165.25 MB/s\t   27969 B/op\t     454 allocs/op",
            "extra": "24636 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49572,
            "unit": "ns/op",
            "extra": "24636 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.25,
            "unit": "MB/s",
            "extra": "24636 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27969,
            "unit": "B/op",
            "extra": "24636 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24636 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6915260,
            "unit": "ns/op\t67523185 B/op\t    2579 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6915260,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523185,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 620.5,
            "unit": "ns/op\t 103.15 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1953958 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 620.5,
            "unit": "ns/op",
            "extra": "1953958 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.15,
            "unit": "MB/s",
            "extra": "1953958 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1953958 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1953958 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9345544 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.1,
            "unit": "ns/op",
            "extra": "9345544 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9345544 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9345544 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1416,
            "unit": "ns/op\t  45.20 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 45.2,
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
            "value": 450.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2552240 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 450.4,
            "unit": "ns/op",
            "extra": "2552240 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2552240 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2552240 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26728,
            "unit": "ns/op\t 306.50 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26728,
            "unit": "ns/op",
            "extra": "73666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 306.5,
            "unit": "MB/s",
            "extra": "73666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 165.1,
            "unit": "ns/op\t1550.95 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "6400802 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 165.1,
            "unit": "ns/op",
            "extra": "6400802 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1550.95,
            "unit": "MB/s",
            "extra": "6400802 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "6400802 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6400802 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 712.8,
            "unit": "ns/op\t 359.12 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3289958 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 712.8,
            "unit": "ns/op",
            "extra": "3289958 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 359.12,
            "unit": "MB/s",
            "extra": "3289958 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3289958 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3289958 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2023632,
            "unit": "ns/op\t 3064034 B/op\t   40018 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2023632,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064034,
            "unit": "B/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "589 times\n4 procs"
          }
        ]
      }
    ]
  }
}