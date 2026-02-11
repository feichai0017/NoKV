window.BENCHMARK_DATA = {
  "lastUpdate": 1770797884163,
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
          "id": "4cbede2b44c75b6aa65200bb22e6eb0507bf8ef0",
          "message": "refactor: always isolate ycsb workloads per run",
          "timestamp": "2026-02-11T15:13:46+08:00",
          "tree_id": "471ace4ee6ba501c14c1d505e0aa2bf90912cc6c",
          "url": "https://github.com/feichai0017/NoKV/commit/4cbede2b44c75b6aa65200bb22e6eb0507bf8ef0"
        },
        "date": 1770794107922,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8039,
            "unit": "ns/op\t   3.98 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "149283 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8039,
            "unit": "ns/op",
            "extra": "149283 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.98,
            "unit": "MB/s",
            "extra": "149283 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "149283 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "149283 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16544,
            "unit": "ns/op\t 247.59 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "80564 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16544,
            "unit": "ns/op",
            "extra": "80564 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 247.59,
            "unit": "MB/s",
            "extra": "80564 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "80564 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "80564 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8030,
            "unit": "ns/op\t   7.97 MB/s\t   17998 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8030,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.97,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17998,
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
            "value": 11120,
            "unit": "ns/op\t 368.35 MB/s\t   29114 B/op\t       8 allocs/op",
            "extra": "383305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11120,
            "unit": "ns/op",
            "extra": "383305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 368.35,
            "unit": "MB/s",
            "extra": "383305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 29114,
            "unit": "B/op",
            "extra": "383305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "383305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126222,
            "unit": "ns/op\t 129.80 MB/s\t   56858 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126222,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.8,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56858,
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
            "value": 1560784,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1560784,
            "unit": "ns/op",
            "extra": "790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 613.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1976649 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 613.7,
            "unit": "ns/op",
            "extra": "1976649 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1976649 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1976649 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50133,
            "unit": "ns/op\t 163.41 MB/s\t   25512 B/op\t     454 allocs/op",
            "extra": "23827 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50133,
            "unit": "ns/op",
            "extra": "23827 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.41,
            "unit": "MB/s",
            "extra": "23827 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25512,
            "unit": "B/op",
            "extra": "23827 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23827 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6712236,
            "unit": "ns/op\t67523339 B/op\t    2586 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6712236,
            "unit": "ns/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523339,
            "unit": "B/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 654.3,
            "unit": "ns/op\t  97.81 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1802446 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 654.3,
            "unit": "ns/op",
            "extra": "1802446 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 97.81,
            "unit": "MB/s",
            "extra": "1802446 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1802446 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1802446 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9324404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.7,
            "unit": "ns/op",
            "extra": "9324404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9324404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9324404 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1409,
            "unit": "ns/op\t  45.42 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1409,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.42,
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
            "value": 448.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2533258 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 448.3,
            "unit": "ns/op",
            "extra": "2533258 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2533258 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2533258 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27929,
            "unit": "ns/op\t 293.31 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76455 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27929,
            "unit": "ns/op",
            "extra": "76455 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 293.31,
            "unit": "MB/s",
            "extra": "76455 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76455 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76455 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148.3,
            "unit": "ns/op\t1725.65 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8102022 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.3,
            "unit": "ns/op",
            "extra": "8102022 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1725.65,
            "unit": "MB/s",
            "extra": "8102022 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8102022 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8102022 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 728.4,
            "unit": "ns/op\t 351.45 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3149900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 728.4,
            "unit": "ns/op",
            "extra": "3149900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 351.45,
            "unit": "MB/s",
            "extra": "3149900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3149900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3149900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2083936,
            "unit": "ns/op\t 3064052 B/op\t   40019 allocs/op",
            "extra": "564 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2083936,
            "unit": "ns/op",
            "extra": "564 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064052,
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
          "id": "4cbede2b44c75b6aa65200bb22e6eb0507bf8ef0",
          "message": "refactor: always isolate ycsb workloads per run",
          "timestamp": "2026-02-11T15:13:46+08:00",
          "tree_id": "471ace4ee6ba501c14c1d505e0aa2bf90912cc6c",
          "url": "https://github.com/feichai0017/NoKV/commit/4cbede2b44c75b6aa65200bb22e6eb0507bf8ef0"
        },
        "date": 1770797883297,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7747,
            "unit": "ns/op\t   4.13 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "162668 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7747,
            "unit": "ns/op",
            "extra": "162668 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.13,
            "unit": "MB/s",
            "extra": "162668 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "162668 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "162668 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16445,
            "unit": "ns/op\t 249.07 MB/s\t     641 B/op\t      29 allocs/op",
            "extra": "82908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16445,
            "unit": "ns/op",
            "extra": "82908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 249.07,
            "unit": "MB/s",
            "extra": "82908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 641,
            "unit": "B/op",
            "extra": "82908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "82908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7725,
            "unit": "ns/op\t   8.29 MB/s\t   16826 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7725,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.29,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16826,
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
            "value": 10668,
            "unit": "ns/op\t 383.96 MB/s\t   26602 B/op\t       8 allocs/op",
            "extra": "402775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10668,
            "unit": "ns/op",
            "extra": "402775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 383.96,
            "unit": "MB/s",
            "extra": "402775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26602,
            "unit": "B/op",
            "extra": "402775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "402775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 200738,
            "unit": "ns/op\t  81.62 MB/s\t   56858 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 200738,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 81.62,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56858,
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
            "value": 1501018,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1501018,
            "unit": "ns/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 623.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1921783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 623.3,
            "unit": "ns/op",
            "extra": "1921783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1921783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1921783 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48237,
            "unit": "ns/op\t 169.83 MB/s\t   25524 B/op\t     454 allocs/op",
            "extra": "23791 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48237,
            "unit": "ns/op",
            "extra": "23791 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.83,
            "unit": "MB/s",
            "extra": "23791 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25524,
            "unit": "B/op",
            "extra": "23791 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23791 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6737979,
            "unit": "ns/op\t67523458 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6737979,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523458,
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
            "value": 702.9,
            "unit": "ns/op\t  91.06 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1832172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 702.9,
            "unit": "ns/op",
            "extra": "1832172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 91.06,
            "unit": "MB/s",
            "extra": "1832172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1832172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1832172 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9152256 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.5,
            "unit": "ns/op",
            "extra": "9152256 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9152256 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9152256 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1412,
            "unit": "ns/op\t  45.33 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1412,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.33,
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
            "value": 470.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2538530 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 470.4,
            "unit": "ns/op",
            "extra": "2538530 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2538530 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2538530 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26221,
            "unit": "ns/op\t 312.42 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "75502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26221,
            "unit": "ns/op",
            "extra": "75502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 312.42,
            "unit": "MB/s",
            "extra": "75502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "75502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75502 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.8,
            "unit": "ns/op\t1612.44 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8003640 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.8,
            "unit": "ns/op",
            "extra": "8003640 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1612.44,
            "unit": "MB/s",
            "extra": "8003640 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8003640 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8003640 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 728.9,
            "unit": "ns/op\t 351.20 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3137604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 728.9,
            "unit": "ns/op",
            "extra": "3137604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 351.2,
            "unit": "MB/s",
            "extra": "3137604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3137604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3137604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2145238,
            "unit": "ns/op\t 3064039 B/op\t   40019 allocs/op",
            "extra": "554 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2145238,
            "unit": "ns/op",
            "extra": "554 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "554 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "554 times\n4 procs"
          }
        ]
      }
    ]
  }
}