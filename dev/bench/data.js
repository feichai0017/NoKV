window.BENCHMARK_DATA = {
  "lastUpdate": 1770704944138,
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
          "id": "46ee8c658423c7f82161b34e40c13954faf42fb7",
          "message": "Merge pull request #38 from feichai0017/metrics-stats-full-refactor\n\nrefactor: unify metrics collection and stats export",
          "timestamp": "2026-02-10T14:27:50+08:00",
          "tree_id": "795e294f68dbaebdef0510dcf2087a3cae70999b",
          "url": "https://github.com/feichai0017/NoKV/commit/46ee8c658423c7f82161b34e40c13954faf42fb7"
        },
        "date": 1770704942332,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7752,
            "unit": "ns/op\t   4.13 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "169626 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7752,
            "unit": "ns/op",
            "extra": "169626 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.13,
            "unit": "MB/s",
            "extra": "169626 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "169626 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "169626 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16642,
            "unit": "ns/op\t 246.13 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "81085 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16642,
            "unit": "ns/op",
            "extra": "81085 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 246.13,
            "unit": "MB/s",
            "extra": "81085 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "81085 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "81085 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7369,
            "unit": "ns/op\t   8.68 MB/s\t   16618 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7369,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.68,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16618,
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
            "value": 10182,
            "unit": "ns/op\t 402.27 MB/s\t   26674 B/op\t       8 allocs/op",
            "extra": "427545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10182,
            "unit": "ns/op",
            "extra": "427545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 402.27,
            "unit": "MB/s",
            "extra": "427545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26674,
            "unit": "B/op",
            "extra": "427545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "427545 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122098,
            "unit": "ns/op\t 134.19 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122098,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.19,
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
            "value": 1476863,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1476863,
            "unit": "ns/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "805 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 595.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1997871 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 595.7,
            "unit": "ns/op",
            "extra": "1997871 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1997871 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1997871 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49950,
            "unit": "ns/op\t 164.00 MB/s\t   28144 B/op\t     454 allocs/op",
            "extra": "24249 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49950,
            "unit": "ns/op",
            "extra": "24249 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164,
            "unit": "MB/s",
            "extra": "24249 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28144,
            "unit": "B/op",
            "extra": "24249 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24249 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6660974,
            "unit": "ns/op\t67523370 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6660974,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523370,
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
            "value": 698.7,
            "unit": "ns/op\t  91.59 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2200659 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 698.7,
            "unit": "ns/op",
            "extra": "2200659 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 91.59,
            "unit": "MB/s",
            "extra": "2200659 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2200659 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2200659 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 132.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9260124 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 132.2,
            "unit": "ns/op",
            "extra": "9260124 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9260124 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9260124 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1401,
            "unit": "ns/op\t  45.67 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1401,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.67,
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
            "value": 455.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2570067 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 455.4,
            "unit": "ns/op",
            "extra": "2570067 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2570067 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2570067 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26299,
            "unit": "ns/op\t 311.50 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74997 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26299,
            "unit": "ns/op",
            "extra": "74997 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 311.5,
            "unit": "MB/s",
            "extra": "74997 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74997 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74997 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149.6,
            "unit": "ns/op\t1711.37 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7981912 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149.6,
            "unit": "ns/op",
            "extra": "7981912 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1711.37,
            "unit": "MB/s",
            "extra": "7981912 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7981912 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7981912 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 717.9,
            "unit": "ns/op\t 356.58 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3177573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 717.9,
            "unit": "ns/op",
            "extra": "3177573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 356.58,
            "unit": "MB/s",
            "extra": "3177573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3177573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3177573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2110506,
            "unit": "ns/op\t 3064042 B/op\t   40019 allocs/op",
            "extra": "571 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2110506,
            "unit": "ns/op",
            "extra": "571 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064042,
            "unit": "B/op",
            "extra": "571 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "571 times\n4 procs"
          }
        ]
      }
    ]
  }
}