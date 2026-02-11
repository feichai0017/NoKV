window.BENCHMARK_DATA = {
  "lastUpdate": 1770788137442,
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
          "id": "6a9f38662e403c21b314d1faeecdb749d4871801",
          "message": "chore: include ycsb workload E in default benchmark set",
          "timestamp": "2026-02-11T12:30:26+08:00",
          "tree_id": "b4e2621ee2db292c858639e489153c9d132f49b2",
          "url": "https://github.com/feichai0017/NoKV/commit/6a9f38662e403c21b314d1faeecdb749d4871801"
        },
        "date": 1770784313549,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7486,
            "unit": "ns/op\t   4.27 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "163060 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7486,
            "unit": "ns/op",
            "extra": "163060 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.27,
            "unit": "MB/s",
            "extra": "163060 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "163060 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "163060 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16883,
            "unit": "ns/op\t 242.61 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "89316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16883,
            "unit": "ns/op",
            "extra": "89316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 242.61,
            "unit": "MB/s",
            "extra": "89316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "89316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "89316 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7642,
            "unit": "ns/op\t   8.37 MB/s\t   17934 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7642,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.37,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17934,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9921,
            "unit": "ns/op\t 412.87 MB/s\t   26364 B/op\t       8 allocs/op",
            "extra": "441196 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9921,
            "unit": "ns/op",
            "extra": "441196 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 412.87,
            "unit": "MB/s",
            "extra": "441196 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26364,
            "unit": "B/op",
            "extra": "441196 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "441196 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 128497,
            "unit": "ns/op\t 127.51 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 128497,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 127.51,
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
            "value": 1480903,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1480903,
            "unit": "ns/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "801 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 580.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2045880 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 580.2,
            "unit": "ns/op",
            "extra": "2045880 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2045880 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2045880 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49398,
            "unit": "ns/op\t 165.84 MB/s\t   27604 B/op\t     454 allocs/op",
            "extra": "25486 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49398,
            "unit": "ns/op",
            "extra": "25486 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.84,
            "unit": "MB/s",
            "extra": "25486 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27604,
            "unit": "B/op",
            "extra": "25486 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25486 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6358393,
            "unit": "ns/op\t67523222 B/op\t    2585 allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6358393,
            "unit": "ns/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523222,
            "unit": "B/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2585,
            "unit": "allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 633.5,
            "unit": "ns/op\t 101.03 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2076801 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 633.5,
            "unit": "ns/op",
            "extra": "2076801 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 101.03,
            "unit": "MB/s",
            "extra": "2076801 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2076801 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2076801 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9400826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.4,
            "unit": "ns/op",
            "extra": "9400826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9400826 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9400826 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1376,
            "unit": "ns/op\t  46.51 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1376,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.51,
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
            "value": 464.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2440210 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 464.2,
            "unit": "ns/op",
            "extra": "2440210 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2440210 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2440210 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25614,
            "unit": "ns/op\t 319.82 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77748 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25614,
            "unit": "ns/op",
            "extra": "77748 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 319.82,
            "unit": "MB/s",
            "extra": "77748 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77748 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77748 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 150.2,
            "unit": "ns/op\t1704.13 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7892611 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 150.2,
            "unit": "ns/op",
            "extra": "7892611 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1704.13,
            "unit": "MB/s",
            "extra": "7892611 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7892611 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7892611 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 721.8,
            "unit": "ns/op\t 354.67 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3167542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 721.8,
            "unit": "ns/op",
            "extra": "3167542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 354.67,
            "unit": "MB/s",
            "extra": "3167542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3167542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3167542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2110630,
            "unit": "ns/op\t 3064038 B/op\t   40019 allocs/op",
            "extra": "564 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2110630,
            "unit": "ns/op",
            "extra": "564 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064038,
            "unit": "B/op",
            "extra": "564 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "564 times\n4 procs"
          }
        ]
      },
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
          "id": "6a9f38662e403c21b314d1faeecdb749d4871801",
          "message": "chore: include ycsb workload E in default benchmark set",
          "timestamp": "2026-02-11T12:30:26+08:00",
          "tree_id": "b4e2621ee2db292c858639e489153c9d132f49b2",
          "url": "https://github.com/feichai0017/NoKV/commit/6a9f38662e403c21b314d1faeecdb749d4871801"
        },
        "date": 1770788135477,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6893,
            "unit": "ns/op\t   4.64 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "185846 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6893,
            "unit": "ns/op",
            "extra": "185846 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.64,
            "unit": "MB/s",
            "extra": "185846 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "185846 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "185846 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18524,
            "unit": "ns/op\t 221.12 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "62942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18524,
            "unit": "ns/op",
            "extra": "62942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 221.12,
            "unit": "MB/s",
            "extra": "62942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "62942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "62942 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7438,
            "unit": "ns/op\t   8.60 MB/s\t   17142 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7438,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.6,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17142,
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
            "value": 10093,
            "unit": "ns/op\t 405.83 MB/s\t   26669 B/op\t       8 allocs/op",
            "extra": "429956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10093,
            "unit": "ns/op",
            "extra": "429956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 405.83,
            "unit": "MB/s",
            "extra": "429956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26669,
            "unit": "B/op",
            "extra": "429956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "429956 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126545,
            "unit": "ns/op\t 129.47 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126545,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.47,
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
            "value": 1521747,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1521747,
            "unit": "ns/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 588.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1997516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 588.7,
            "unit": "ns/op",
            "extra": "1997516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1997516 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1997516 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50232,
            "unit": "ns/op\t 163.08 MB/s\t   28070 B/op\t     454 allocs/op",
            "extra": "24412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50232,
            "unit": "ns/op",
            "extra": "24412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.08,
            "unit": "MB/s",
            "extra": "24412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28070,
            "unit": "B/op",
            "extra": "24412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24412 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6737220,
            "unit": "ns/op\t67523317 B/op\t    2586 allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6737220,
            "unit": "ns/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523317,
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
            "value": 682.9,
            "unit": "ns/op\t  93.72 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1966250 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 682.9,
            "unit": "ns/op",
            "extra": "1966250 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 93.72,
            "unit": "MB/s",
            "extra": "1966250 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1966250 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1966250 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9313800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130,
            "unit": "ns/op",
            "extra": "9313800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9313800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9313800 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1354,
            "unit": "ns/op\t  47.27 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1354,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47.27,
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
            "value": 463.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2622426 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 463.3,
            "unit": "ns/op",
            "extra": "2622426 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2622426 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2622426 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25454,
            "unit": "ns/op\t 321.84 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25454,
            "unit": "ns/op",
            "extra": "77595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 321.84,
            "unit": "MB/s",
            "extra": "77595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77595 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149,
            "unit": "ns/op\t1718.16 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7891572 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149,
            "unit": "ns/op",
            "extra": "7891572 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1718.16,
            "unit": "MB/s",
            "extra": "7891572 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7891572 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7891572 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 721.5,
            "unit": "ns/op\t 354.83 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3161872 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 721.5,
            "unit": "ns/op",
            "extra": "3161872 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 354.83,
            "unit": "MB/s",
            "extra": "3161872 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3161872 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3161872 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2090772,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "574 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2090772,
            "unit": "ns/op",
            "extra": "574 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "574 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "574 times\n4 procs"
          }
        ]
      }
    ]
  }
}