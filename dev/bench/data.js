window.BENCHMARK_DATA = {
  "lastUpdate": 1770005395249,
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
          "id": "2e88e27734b39bc53cd45df029430e6fc46ead2a",
          "message": "Refine design notes: clarify WAL IO, HotRing concurrency, and Ingest modes",
          "timestamp": "2026-02-02T12:07:57+08:00",
          "tree_id": "83a96da1309e2061aa3da4fb62c7642e0dce1bb4",
          "url": "https://github.com/feichai0017/NoKV/commit/2e88e27734b39bc53cd45df029430e6fc46ead2a"
        },
        "date": 1770005394558,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11178,
            "unit": "ns/op\t   2.86 MB/s\t     618 B/op\t      24 allocs/op",
            "extra": "135267 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11178,
            "unit": "ns/op",
            "extra": "135267 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.86,
            "unit": "MB/s",
            "extra": "135267 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 618,
            "unit": "B/op",
            "extra": "135267 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "135267 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15821,
            "unit": "ns/op\t 258.90 MB/s\t     660 B/op\t      27 allocs/op",
            "extra": "89412 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15821,
            "unit": "ns/op",
            "extra": "89412 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 258.9,
            "unit": "MB/s",
            "extra": "89412 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 660,
            "unit": "B/op",
            "extra": "89412 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "89412 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11000,
            "unit": "ns/op\t   5.82 MB/s\t   17842 B/op\t       5 allocs/op",
            "extra": "924498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11000,
            "unit": "ns/op",
            "extra": "924498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.82,
            "unit": "MB/s",
            "extra": "924498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17842,
            "unit": "B/op",
            "extra": "924498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "924498 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10020,
            "unit": "ns/op\t 408.77 MB/s\t   18198 B/op\t       7 allocs/op",
            "extra": "244693 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10020,
            "unit": "ns/op",
            "extra": "244693 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 408.77,
            "unit": "MB/s",
            "extra": "244693 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18198,
            "unit": "B/op",
            "extra": "244693 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "244693 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 178438,
            "unit": "ns/op\t  91.82 MB/s\t   62124 B/op\t     674 allocs/op",
            "extra": "9502 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 178438,
            "unit": "ns/op",
            "extra": "9502 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 91.82,
            "unit": "MB/s",
            "extra": "9502 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62124,
            "unit": "B/op",
            "extra": "9502 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 674,
            "unit": "allocs/op",
            "extra": "9502 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2293088,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2293088,
            "unit": "ns/op",
            "extra": "541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1058,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1058,
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
            "value": 50826,
            "unit": "ns/op\t 161.18 MB/s\t   26389 B/op\t     454 allocs/op",
            "extra": "21597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50826,
            "unit": "ns/op",
            "extra": "21597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.18,
            "unit": "MB/s",
            "extra": "21597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 26389,
            "unit": "B/op",
            "extra": "21597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "21597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8264148,
            "unit": "ns/op\t67523235 B/op\t    2586 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8264148,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523235,
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
            "value": 693.5,
            "unit": "ns/op\t  92.28 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1870228 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 693.5,
            "unit": "ns/op",
            "extra": "1870228 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 92.28,
            "unit": "MB/s",
            "extra": "1870228 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1870228 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1870228 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "8938646 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.7,
            "unit": "ns/op",
            "extra": "8938646 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "8938646 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8938646 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1424,
            "unit": "ns/op\t  44.96 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1424,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.96,
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
            "value": 482.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2554370 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 482.5,
            "unit": "ns/op",
            "extra": "2554370 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2554370 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2554370 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25756,
            "unit": "ns/op\t 318.06 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "75014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25756,
            "unit": "ns/op",
            "extra": "75014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 318.06,
            "unit": "MB/s",
            "extra": "75014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "75014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75014 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 147.2,
            "unit": "ns/op\t1739.00 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7928480 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 147.2,
            "unit": "ns/op",
            "extra": "7928480 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1739,
            "unit": "MB/s",
            "extra": "7928480 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7928480 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7928480 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 722.5,
            "unit": "ns/op\t 354.31 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3189406 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 722.5,
            "unit": "ns/op",
            "extra": "3189406 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 354.31,
            "unit": "MB/s",
            "extra": "3189406 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3189406 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3189406 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2020658,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2020658,
            "unit": "ns/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "584 times\n4 procs"
          }
        ]
      }
    ]
  }
}