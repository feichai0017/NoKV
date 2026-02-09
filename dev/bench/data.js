window.BENCHMARK_DATA = {
  "lastUpdate": 1770634908553,
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
          "id": "3e7bdbda655664a4c7126191b1462da06ac1cfdc",
          "message": "docs: refine note naming and expand skiplist design walkthrough",
          "timestamp": "2026-02-09T19:00:31+08:00",
          "tree_id": "878bb6f991077e4983160eaddeac897abb482da6",
          "url": "https://github.com/feichai0017/NoKV/commit/3e7bdbda655664a4c7126191b1462da06ac1cfdc"
        },
        "date": 1770634907158,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8594,
            "unit": "ns/op\t   3.72 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "162730 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8594,
            "unit": "ns/op",
            "extra": "162730 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.72,
            "unit": "MB/s",
            "extra": "162730 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "162730 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "162730 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16313,
            "unit": "ns/op\t 251.09 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "95294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16313,
            "unit": "ns/op",
            "extra": "95294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 251.09,
            "unit": "MB/s",
            "extra": "95294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "95294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "95294 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9562,
            "unit": "ns/op\t   6.69 MB/s\t   16761 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9562,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.69,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16761,
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
            "value": 12149,
            "unit": "ns/op\t 337.16 MB/s\t   24736 B/op\t       8 allocs/op",
            "extra": "323120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12149,
            "unit": "ns/op",
            "extra": "323120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.16,
            "unit": "MB/s",
            "extra": "323120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 24736,
            "unit": "B/op",
            "extra": "323120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "323120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127280,
            "unit": "ns/op\t 128.72 MB/s\t   56855 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127280,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.72,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56855,
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
            "value": 1563458,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1563458,
            "unit": "ns/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 625.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1812975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 625.8,
            "unit": "ns/op",
            "extra": "1812975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1812975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1812975 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49693,
            "unit": "ns/op\t 164.85 MB/s\t   25626 B/op\t     454 allocs/op",
            "extra": "23511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49693,
            "unit": "ns/op",
            "extra": "23511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.85,
            "unit": "MB/s",
            "extra": "23511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25626,
            "unit": "B/op",
            "extra": "23511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23511 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7726868,
            "unit": "ns/op\t67523229 B/op\t    2586 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7726868,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523229,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 616.7,
            "unit": "ns/op\t 103.78 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1935128 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 616.7,
            "unit": "ns/op",
            "extra": "1935128 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.78,
            "unit": "MB/s",
            "extra": "1935128 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1935128 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1935128 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 126.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9452778 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 126.3,
            "unit": "ns/op",
            "extra": "9452778 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9452778 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9452778 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1402,
            "unit": "ns/op\t  45.65 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 45.65,
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
            "value": 473,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2617702 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 473,
            "unit": "ns/op",
            "extra": "2617702 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2617702 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2617702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25935,
            "unit": "ns/op\t 315.87 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77103 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25935,
            "unit": "ns/op",
            "extra": "77103 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.87,
            "unit": "MB/s",
            "extra": "77103 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77103 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77103 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 144.7,
            "unit": "ns/op\t1768.93 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8318325 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 144.7,
            "unit": "ns/op",
            "extra": "8318325 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1768.93,
            "unit": "MB/s",
            "extra": "8318325 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8318325 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8318325 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 733.2,
            "unit": "ns/op\t 349.15 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3148876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 733.2,
            "unit": "ns/op",
            "extra": "3148876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 349.15,
            "unit": "MB/s",
            "extra": "3148876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3148876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3148876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2043313,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2043313,
            "unit": "ns/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "583 times\n4 procs"
          }
        ]
      }
    ]
  }
}