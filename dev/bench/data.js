window.BENCHMARK_DATA = {
  "lastUpdate": 1770809513457,
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
          "id": "6c19cc4eba2e829efe347936cf896a872c4bf47b",
          "message": "Update benchmark config",
          "timestamp": "2026-02-11T19:30:37+08:00",
          "tree_id": "cdd4aeed7cd66919296d2addca225c057a673235",
          "url": "https://github.com/feichai0017/NoKV/commit/6c19cc4eba2e829efe347936cf896a872c4bf47b"
        },
        "date": 1770809511735,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8420,
            "unit": "ns/op\t   3.80 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "136920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8420,
            "unit": "ns/op",
            "extra": "136920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.8,
            "unit": "MB/s",
            "extra": "136920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "136920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "136920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15864,
            "unit": "ns/op\t 258.20 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "83618 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15864,
            "unit": "ns/op",
            "extra": "83618 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 258.2,
            "unit": "MB/s",
            "extra": "83618 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "83618 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "83618 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7649,
            "unit": "ns/op\t   8.37 MB/s\t   16903 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7649,
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
            "value": 16903,
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
            "value": 11881,
            "unit": "ns/op\t 344.74 MB/s\t   29082 B/op\t       8 allocs/op",
            "extra": "346437 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11881,
            "unit": "ns/op",
            "extra": "346437 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 344.74,
            "unit": "MB/s",
            "extra": "346437 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 29082,
            "unit": "B/op",
            "extra": "346437 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "346437 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126769,
            "unit": "ns/op\t 129.24 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126769,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.24,
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
            "value": 1562407,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1562407,
            "unit": "ns/op",
            "extra": "766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 624.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1854597 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 624.6,
            "unit": "ns/op",
            "extra": "1854597 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1854597 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1854597 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49308,
            "unit": "ns/op\t 166.14 MB/s\t   28028 B/op\t     454 allocs/op",
            "extra": "24505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49308,
            "unit": "ns/op",
            "extra": "24505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.14,
            "unit": "MB/s",
            "extra": "24505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28028,
            "unit": "B/op",
            "extra": "24505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6952202,
            "unit": "ns/op\t67523235 B/op\t    2586 allocs/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6952202,
            "unit": "ns/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523235,
            "unit": "B/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "168 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 614.7,
            "unit": "ns/op\t 104.12 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2014532 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 614.7,
            "unit": "ns/op",
            "extra": "2014532 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 104.12,
            "unit": "MB/s",
            "extra": "2014532 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2014532 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2014532 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9316591 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130,
            "unit": "ns/op",
            "extra": "9316591 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9316591 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9316591 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1454,
            "unit": "ns/op\t  44.01 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1454,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.01,
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
            "value": 482.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2437431 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 482.3,
            "unit": "ns/op",
            "extra": "2437431 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2437431 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2437431 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25879,
            "unit": "ns/op\t 316.55 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74893 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25879,
            "unit": "ns/op",
            "extra": "74893 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 316.55,
            "unit": "MB/s",
            "extra": "74893 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74893 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74893 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149.1,
            "unit": "ns/op\t1717.53 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8005806 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149.1,
            "unit": "ns/op",
            "extra": "8005806 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1717.53,
            "unit": "MB/s",
            "extra": "8005806 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8005806 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8005806 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 743,
            "unit": "ns/op\t 344.55 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3110469 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 743,
            "unit": "ns/op",
            "extra": "3110469 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 344.55,
            "unit": "MB/s",
            "extra": "3110469 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3110469 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3110469 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2186470,
            "unit": "ns/op\t 3064043 B/op\t   40019 allocs/op",
            "extra": "554 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2186470,
            "unit": "ns/op",
            "extra": "554 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064043,
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