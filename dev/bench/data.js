window.BENCHMARK_DATA = {
  "lastUpdate": 1770283922523,
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
          "id": "93263936b0b2199021a4e104b9317f161c7a97bb",
          "message": "docs: update hotring",
          "timestamp": "2026-02-05T17:30:49+08:00",
          "tree_id": "c78abfbfb606c546ce77b51d22218b4cbe3afa61",
          "url": "https://github.com/feichai0017/NoKV/commit/93263936b0b2199021a4e104b9317f161c7a97bb"
        },
        "date": 1770283921512,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13184,
            "unit": "ns/op\t   2.43 MB/s\t     601 B/op\t      20 allocs/op",
            "extra": "147000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13184,
            "unit": "ns/op",
            "extra": "147000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.43,
            "unit": "MB/s",
            "extra": "147000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 601,
            "unit": "B/op",
            "extra": "147000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "147000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18083,
            "unit": "ns/op\t 226.51 MB/s\t     821 B/op\t      31 allocs/op",
            "extra": "75288 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18083,
            "unit": "ns/op",
            "extra": "75288 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 226.51,
            "unit": "MB/s",
            "extra": "75288 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 821,
            "unit": "B/op",
            "extra": "75288 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "75288 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11634,
            "unit": "ns/op\t   5.50 MB/s\t   17847 B/op\t       5 allocs/op",
            "extra": "701732 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11634,
            "unit": "ns/op",
            "extra": "701732 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.5,
            "unit": "MB/s",
            "extra": "701732 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17847,
            "unit": "B/op",
            "extra": "701732 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "701732 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11274,
            "unit": "ns/op\t 363.31 MB/s\t   18178 B/op\t       7 allocs/op",
            "extra": "206355 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11274,
            "unit": "ns/op",
            "extra": "206355 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 363.31,
            "unit": "MB/s",
            "extra": "206355 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18178,
            "unit": "B/op",
            "extra": "206355 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "206355 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 180903,
            "unit": "ns/op\t  90.57 MB/s\t   60176 B/op\t     674 allocs/op",
            "extra": "9320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 180903,
            "unit": "ns/op",
            "extra": "9320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 90.57,
            "unit": "MB/s",
            "extra": "9320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 60176,
            "unit": "B/op",
            "extra": "9320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 674,
            "unit": "allocs/op",
            "extra": "9320 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2235121,
            "unit": "ns/op\t       7 B/op\t       0 allocs/op",
            "extra": "504 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2235121,
            "unit": "ns/op",
            "extra": "504 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 7,
            "unit": "B/op",
            "extra": "504 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "504 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1017,
            "unit": "ns/op\t      36 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1017,
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
            "value": 54518,
            "unit": "ns/op\t 150.26 MB/s\t   26379 B/op\t     454 allocs/op",
            "extra": "21618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 54518,
            "unit": "ns/op",
            "extra": "21618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 150.26,
            "unit": "MB/s",
            "extra": "21618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 26379,
            "unit": "B/op",
            "extra": "21618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "21618 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8748852,
            "unit": "ns/op\t67523541 B/op\t    2588 allocs/op",
            "extra": "138 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8748852,
            "unit": "ns/op",
            "extra": "138 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523541,
            "unit": "B/op",
            "extra": "138 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2588,
            "unit": "allocs/op",
            "extra": "138 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 756.4,
            "unit": "ns/op\t  84.61 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1861282 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 756.4,
            "unit": "ns/op",
            "extra": "1861282 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 84.61,
            "unit": "MB/s",
            "extra": "1861282 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1861282 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1861282 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9318333 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.9,
            "unit": "ns/op",
            "extra": "9318333 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9318333 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9318333 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1604,
            "unit": "ns/op\t  39.90 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1604,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 39.9,
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
            "value": 486.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2521516 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 486.5,
            "unit": "ns/op",
            "extra": "2521516 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2521516 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2521516 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 28351,
            "unit": "ns/op\t 288.95 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "69226 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 28351,
            "unit": "ns/op",
            "extra": "69226 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 288.95,
            "unit": "MB/s",
            "extra": "69226 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "69226 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "69226 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 160,
            "unit": "ns/op\t1599.72 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7508961 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 160,
            "unit": "ns/op",
            "extra": "7508961 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1599.72,
            "unit": "MB/s",
            "extra": "7508961 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7508961 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7508961 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 683.4,
            "unit": "ns/op\t 374.60 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "2879686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 683.4,
            "unit": "ns/op",
            "extra": "2879686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.6,
            "unit": "MB/s",
            "extra": "2879686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "2879686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "2879686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2178388,
            "unit": "ns/op\t 3064032 B/op\t   40019 allocs/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2178388,
            "unit": "ns/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "550 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "550 times\n4 procs"
          }
        ]
      }
    ]
  }
}