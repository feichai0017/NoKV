window.BENCHMARK_DATA = {
  "lastUpdate": 1770359130742,
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
          "id": "5ffe0ecc40d467dc63d8c866009eded62ca698aa",
          "message": "refactor: use external hotring module",
          "timestamp": "2026-02-06T14:23:56+08:00",
          "tree_id": "8a49cf35a67d560b1bde144d6cacfb483db14a27",
          "url": "https://github.com/feichai0017/NoKV/commit/5ffe0ecc40d467dc63d8c866009eded62ca698aa"
        },
        "date": 1770359129618,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13163,
            "unit": "ns/op\t   2.43 MB/s\t     591 B/op\t      20 allocs/op",
            "extra": "76707 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13163,
            "unit": "ns/op",
            "extra": "76707 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.43,
            "unit": "MB/s",
            "extra": "76707 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 591,
            "unit": "B/op",
            "extra": "76707 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "76707 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 21592,
            "unit": "ns/op\t 189.70 MB/s\t     836 B/op\t      31 allocs/op",
            "extra": "54352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 21592,
            "unit": "ns/op",
            "extra": "54352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 189.7,
            "unit": "MB/s",
            "extra": "54352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 836,
            "unit": "B/op",
            "extra": "54352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "54352 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11631,
            "unit": "ns/op\t   5.50 MB/s\t   20679 B/op\t       5 allocs/op",
            "extra": "794451 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11631,
            "unit": "ns/op",
            "extra": "794451 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.5,
            "unit": "MB/s",
            "extra": "794451 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20679,
            "unit": "B/op",
            "extra": "794451 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "794451 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10382,
            "unit": "ns/op\t 394.53 MB/s\t   20165 B/op\t       7 allocs/op",
            "extra": "225452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10382,
            "unit": "ns/op",
            "extra": "225452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 394.53,
            "unit": "MB/s",
            "extra": "225452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 20165,
            "unit": "B/op",
            "extra": "225452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "225452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 165887,
            "unit": "ns/op\t  98.77 MB/s\t   59227 B/op\t     670 allocs/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 165887,
            "unit": "ns/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 98.77,
            "unit": "MB/s",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 59227,
            "unit": "B/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 670,
            "unit": "allocs/op",
            "extra": "9505 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2188791,
            "unit": "ns/op\t    8801 B/op\t       0 allocs/op",
            "extra": "561 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2188791,
            "unit": "ns/op",
            "extra": "561 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 8801,
            "unit": "B/op",
            "extra": "561 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "561 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1048,
            "unit": "ns/op\t      36 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1048,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 36,
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
            "value": 49376,
            "unit": "ns/op\t 165.91 MB/s\t   25603 B/op\t     454 allocs/op",
            "extra": "23574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49376,
            "unit": "ns/op",
            "extra": "23574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.91,
            "unit": "MB/s",
            "extra": "23574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25603,
            "unit": "B/op",
            "extra": "23574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6392576,
            "unit": "ns/op\t67523309 B/op\t    2586 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6392576,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523309,
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
            "value": 641.6,
            "unit": "ns/op\t  99.74 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1972033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 641.6,
            "unit": "ns/op",
            "extra": "1972033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 99.74,
            "unit": "MB/s",
            "extra": "1972033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1972033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1972033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9400033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.8,
            "unit": "ns/op",
            "extra": "9400033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9400033 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9400033 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1403,
            "unit": "ns/op\t  45.63 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1403,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.63,
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
            "value": 507.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2561595 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 507.9,
            "unit": "ns/op",
            "extra": "2561595 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2561595 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2561595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26770,
            "unit": "ns/op\t 306.02 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77042 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26770,
            "unit": "ns/op",
            "extra": "77042 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 306.02,
            "unit": "MB/s",
            "extra": "77042 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77042 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77042 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.8,
            "unit": "ns/op\t1755.27 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8119382 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.8,
            "unit": "ns/op",
            "extra": "8119382 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1755.27,
            "unit": "MB/s",
            "extra": "8119382 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8119382 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8119382 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 727.1,
            "unit": "ns/op\t 352.09 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3134550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 727.1,
            "unit": "ns/op",
            "extra": "3134550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 352.09,
            "unit": "MB/s",
            "extra": "3134550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3134550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3134550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2044273,
            "unit": "ns/op\t 3064038 B/op\t   40019 allocs/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2044273,
            "unit": "ns/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064038,
            "unit": "B/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "579 times\n4 procs"
          }
        ]
      }
    ]
  }
}