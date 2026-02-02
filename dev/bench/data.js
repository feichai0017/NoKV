window.BENCHMARK_DATA = {
  "lastUpdate": 1770015117475,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "Guocheng Song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4fd859b04655fbf22115c049b24595904c8f7ec7",
          "message": "Merge pull request #32 from feichai0017/alert-autofix-11\n\nPotential fix for code scanning alert no. 11: Workflow does not contain permissions",
          "timestamp": "2026-02-02T14:50:49+08:00",
          "tree_id": "c34fddc2ce6fc81b0f28daa0056fa9e69c6751fd",
          "url": "https://github.com/feichai0017/NoKV/commit/4fd859b04655fbf22115c049b24595904c8f7ec7"
        },
        "date": 1770015116399,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11716,
            "unit": "ns/op\t   2.73 MB/s\t     654 B/op\t      24 allocs/op",
            "extra": "99708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11716,
            "unit": "ns/op",
            "extra": "99708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.73,
            "unit": "MB/s",
            "extra": "99708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 654,
            "unit": "B/op",
            "extra": "99708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "99708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14577,
            "unit": "ns/op\t 280.99 MB/s\t     674 B/op\t      27 allocs/op",
            "extra": "94633 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14577,
            "unit": "ns/op",
            "extra": "94633 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 280.99,
            "unit": "MB/s",
            "extra": "94633 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 674,
            "unit": "B/op",
            "extra": "94633 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "94633 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10133,
            "unit": "ns/op\t   6.32 MB/s\t   16822 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10133,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.32,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16822,
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
            "value": 9021,
            "unit": "ns/op\t 454.05 MB/s\t   17511 B/op\t       7 allocs/op",
            "extra": "287557 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9021,
            "unit": "ns/op",
            "extra": "287557 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 454.05,
            "unit": "MB/s",
            "extra": "287557 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17511,
            "unit": "B/op",
            "extra": "287557 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "287557 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 171614,
            "unit": "ns/op\t  95.47 MB/s\t   62138 B/op\t     674 allocs/op",
            "extra": "9499 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 171614,
            "unit": "ns/op",
            "extra": "9499 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.47,
            "unit": "MB/s",
            "extra": "9499 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62138,
            "unit": "B/op",
            "extra": "9499 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 674,
            "unit": "allocs/op",
            "extra": "9499 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2199945,
            "unit": "ns/op\t    8994 B/op\t       0 allocs/op",
            "extra": "549 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2199945,
            "unit": "ns/op",
            "extra": "549 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 8994,
            "unit": "B/op",
            "extra": "549 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "549 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1005,
            "unit": "ns/op\t      36 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1005,
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
            "value": 50952,
            "unit": "ns/op\t 160.78 MB/s\t   27816 B/op\t     454 allocs/op",
            "extra": "24987 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50952,
            "unit": "ns/op",
            "extra": "24987 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 160.78,
            "unit": "MB/s",
            "extra": "24987 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27816,
            "unit": "B/op",
            "extra": "24987 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24987 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6762430,
            "unit": "ns/op\t67523339 B/op\t    2586 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6762430,
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
            "value": 612,
            "unit": "ns/op\t 104.58 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2124082 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 612,
            "unit": "ns/op",
            "extra": "2124082 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 104.58,
            "unit": "MB/s",
            "extra": "2124082 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2124082 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2124082 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "8984390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "8984390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "8984390 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8984390 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1394,
            "unit": "ns/op\t  45.92 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1394,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.92,
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
            "value": 467.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2464117 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 467.9,
            "unit": "ns/op",
            "extra": "2464117 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2464117 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2464117 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25549,
            "unit": "ns/op\t 320.63 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "76419 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25549,
            "unit": "ns/op",
            "extra": "76419 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 320.63,
            "unit": "MB/s",
            "extra": "76419 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "76419 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76419 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 153.1,
            "unit": "ns/op\t1671.88 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7741117 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 153.1,
            "unit": "ns/op",
            "extra": "7741117 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1671.88,
            "unit": "MB/s",
            "extra": "7741117 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7741117 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7741117 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 729.6,
            "unit": "ns/op\t 350.90 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3188004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 729.6,
            "unit": "ns/op",
            "extra": "3188004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.9,
            "unit": "MB/s",
            "extra": "3188004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3188004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3188004 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2017446,
            "unit": "ns/op\t 3064044 B/op\t   40019 allocs/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2017446,
            "unit": "ns/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064044,
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