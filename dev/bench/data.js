window.BENCHMARK_DATA = {
  "lastUpdate": 1770482920536,
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
          "id": "97b38758a93af87c7d8fb38c1f292ed45748317f",
          "message": "feat: load options from toml",
          "timestamp": "2026-02-08T00:31:04+08:00",
          "tree_id": "2b16b38a160a5012730ee58b3c4da474411e08a3",
          "url": "https://github.com/feichai0017/NoKV/commit/97b38758a93af87c7d8fb38c1f292ed45748317f"
        },
        "date": 1770481968730,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12925,
            "unit": "ns/op\t   2.48 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "201403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12925,
            "unit": "ns/op",
            "extra": "201403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.48,
            "unit": "MB/s",
            "extra": "201403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "201403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "201403 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17234,
            "unit": "ns/op\t 237.67 MB/s\t     658 B/op\t      29 allocs/op",
            "extra": "79762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17234,
            "unit": "ns/op",
            "extra": "79762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 237.67,
            "unit": "MB/s",
            "extra": "79762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 658,
            "unit": "B/op",
            "extra": "79762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "79762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12058,
            "unit": "ns/op\t   5.31 MB/s\t   19219 B/op\t       5 allocs/op",
            "extra": "838322 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12058,
            "unit": "ns/op",
            "extra": "838322 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.31,
            "unit": "MB/s",
            "extra": "838322 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19219,
            "unit": "B/op",
            "extra": "838322 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "838322 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10554,
            "unit": "ns/op\t 388.10 MB/s\t   18593 B/op\t       7 allocs/op",
            "extra": "222241 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10554,
            "unit": "ns/op",
            "extra": "222241 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 388.1,
            "unit": "MB/s",
            "extra": "222241 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18593,
            "unit": "B/op",
            "extra": "222241 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "222241 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 172455,
            "unit": "ns/op\t  95.00 MB/s\t   56859 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 172455,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56859,
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
            "value": 2197125,
            "unit": "ns/op\t       5 B/op\t       0 allocs/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2197125,
            "unit": "ns/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 5,
            "unit": "B/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1153,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1153,
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
            "value": 49628,
            "unit": "ns/op\t 165.07 MB/s\t   25747 B/op\t     454 allocs/op",
            "extra": "23185 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49628,
            "unit": "ns/op",
            "extra": "23185 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.07,
            "unit": "MB/s",
            "extra": "23185 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25747,
            "unit": "B/op",
            "extra": "23185 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23185 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7117099,
            "unit": "ns/op\t67523148 B/op\t    2586 allocs/op",
            "extra": "169 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7117099,
            "unit": "ns/op",
            "extra": "169 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523148,
            "unit": "B/op",
            "extra": "169 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "169 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 617.9,
            "unit": "ns/op\t 103.58 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1922449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 617.9,
            "unit": "ns/op",
            "extra": "1922449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.58,
            "unit": "MB/s",
            "extra": "1922449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1922449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1922449 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 131.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9116086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 131.6,
            "unit": "ns/op",
            "extra": "9116086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9116086 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9116086 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1469,
            "unit": "ns/op\t  43.56 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1469,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.56,
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
            "value": 471,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2455232 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 471,
            "unit": "ns/op",
            "extra": "2455232 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2455232 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2455232 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26526,
            "unit": "ns/op\t 308.83 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74862 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26526,
            "unit": "ns/op",
            "extra": "74862 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.83,
            "unit": "MB/s",
            "extra": "74862 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74862 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74862 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148,
            "unit": "ns/op\t1729.35 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8012643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148,
            "unit": "ns/op",
            "extra": "8012643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1729.35,
            "unit": "MB/s",
            "extra": "8012643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8012643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8012643 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 744.9,
            "unit": "ns/op\t 343.67 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3092362 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 744.9,
            "unit": "ns/op",
            "extra": "3092362 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 343.67,
            "unit": "MB/s",
            "extra": "3092362 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3092362 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3092362 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2023992,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2023992,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
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
          "id": "97b38758a93af87c7d8fb38c1f292ed45748317f",
          "message": "feat: load options from toml",
          "timestamp": "2026-02-08T00:31:04+08:00",
          "tree_id": "2b16b38a160a5012730ee58b3c4da474411e08a3",
          "url": "https://github.com/feichai0017/NoKV/commit/97b38758a93af87c7d8fb38c1f292ed45748317f"
        },
        "date": 1770482919543,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 10343,
            "unit": "ns/op\t   3.09 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "110656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 10343,
            "unit": "ns/op",
            "extra": "110656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.09,
            "unit": "MB/s",
            "extra": "110656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "110656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "110656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 13958,
            "unit": "ns/op\t 293.46 MB/s\t     657 B/op\t      29 allocs/op",
            "extra": "111904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 13958,
            "unit": "ns/op",
            "extra": "111904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 293.46,
            "unit": "MB/s",
            "extra": "111904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 657,
            "unit": "B/op",
            "extra": "111904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "111904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10726,
            "unit": "ns/op\t   5.97 MB/s\t   20223 B/op\t       5 allocs/op",
            "extra": "758685 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10726,
            "unit": "ns/op",
            "extra": "758685 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.97,
            "unit": "MB/s",
            "extra": "758685 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20223,
            "unit": "B/op",
            "extra": "758685 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "758685 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9919,
            "unit": "ns/op\t 412.97 MB/s\t   20132 B/op\t       8 allocs/op",
            "extra": "242853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9919,
            "unit": "ns/op",
            "extra": "242853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 412.97,
            "unit": "MB/s",
            "extra": "242853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 20132,
            "unit": "B/op",
            "extra": "242853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "242853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 164347,
            "unit": "ns/op\t  99.69 MB/s\t   58805 B/op\t     668 allocs/op",
            "extra": "9590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 164347,
            "unit": "ns/op",
            "extra": "9590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 99.69,
            "unit": "MB/s",
            "extra": "9590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 58805,
            "unit": "B/op",
            "extra": "9590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 668,
            "unit": "allocs/op",
            "extra": "9590 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2457938,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "490 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2457938,
            "unit": "ns/op",
            "extra": "490 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "490 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "490 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 900.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1355487 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 900.7,
            "unit": "ns/op",
            "extra": "1355487 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1355487 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1355487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50546,
            "unit": "ns/op\t 162.07 MB/s\t   28178 B/op\t     454 allocs/op",
            "extra": "24174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50546,
            "unit": "ns/op",
            "extra": "24174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.07,
            "unit": "MB/s",
            "extra": "24174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28178,
            "unit": "B/op",
            "extra": "24174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8643705,
            "unit": "ns/op\t67523538 B/op\t    2587 allocs/op",
            "extra": "135 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8643705,
            "unit": "ns/op",
            "extra": "135 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523538,
            "unit": "B/op",
            "extra": "135 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "135 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 725.5,
            "unit": "ns/op\t  88.22 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1769912 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 725.5,
            "unit": "ns/op",
            "extra": "1769912 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 88.22,
            "unit": "MB/s",
            "extra": "1769912 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1769912 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1769912 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 113.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9690960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 113.1,
            "unit": "ns/op",
            "extra": "9690960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9690960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9690960 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1317,
            "unit": "ns/op\t  48.60 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1317,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 48.6,
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
            "value": 487.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2592914 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 487.1,
            "unit": "ns/op",
            "extra": "2592914 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2592914 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2592914 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 22780,
            "unit": "ns/op\t 359.61 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "101019 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 22780,
            "unit": "ns/op",
            "extra": "101019 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 359.61,
            "unit": "MB/s",
            "extra": "101019 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "101019 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "101019 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 152,
            "unit": "ns/op\t1684.69 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7872405 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 152,
            "unit": "ns/op",
            "extra": "7872405 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1684.69,
            "unit": "MB/s",
            "extra": "7872405 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7872405 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7872405 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 605.3,
            "unit": "ns/op\t 422.95 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3887686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 605.3,
            "unit": "ns/op",
            "extra": "3887686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 422.95,
            "unit": "MB/s",
            "extra": "3887686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3887686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3887686 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2020201,
            "unit": "ns/op\t 3064048 B/op\t   40019 allocs/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2020201,
            "unit": "ns/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064048,
            "unit": "B/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "596 times\n4 procs"
          }
        ]
      }
    ]
  }
}