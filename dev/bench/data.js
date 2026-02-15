window.BENCHMARK_DATA = {
  "lastUpdate": 1771125510734,
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
          "id": "d13573fe71345477444d5022a33237110adafd03",
          "message": "Merge pull request #64 from zqr10159/fix-issue-45\n\nfix(txn): decrement active txn counter for empty commits",
          "timestamp": "2026-02-15T11:17:17+08:00",
          "tree_id": "9438b8bf3c0f48884ce39920f3308e0b53fc3f1d",
          "url": "https://github.com/feichai0017/NoKV/commit/d13573fe71345477444d5022a33237110adafd03"
        },
        "date": 1771125509246,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7238,
            "unit": "ns/op\t   4.42 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "162931 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7238,
            "unit": "ns/op",
            "extra": "162931 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.42,
            "unit": "MB/s",
            "extra": "162931 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "162931 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "162931 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17275,
            "unit": "ns/op\t 237.10 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "67738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17275,
            "unit": "ns/op",
            "extra": "67738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 237.1,
            "unit": "MB/s",
            "extra": "67738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "67738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "67738 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7965,
            "unit": "ns/op\t   8.04 MB/s\t   17919 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7965,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.04,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17919,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 12123,
            "unit": "ns/op\t 337.87 MB/s\t   34058 B/op\t      11 allocs/op",
            "extra": "318910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12123,
            "unit": "ns/op",
            "extra": "318910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.87,
            "unit": "MB/s",
            "extra": "318910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34058,
            "unit": "B/op",
            "extra": "318910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "318910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121880,
            "unit": "ns/op\t 134.43 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121880,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.43,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56848,
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
            "value": 1507998,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1507998,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 686.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2081559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 686.1,
            "unit": "ns/op",
            "extra": "2081559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2081559 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2081559 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50365,
            "unit": "ns/op\t 162.65 MB/s\t   28060 B/op\t     454 allocs/op",
            "extra": "24434 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50365,
            "unit": "ns/op",
            "extra": "24434 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.65,
            "unit": "MB/s",
            "extra": "24434 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28060,
            "unit": "B/op",
            "extra": "24434 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24434 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6392575,
            "unit": "ns/op\t67523130 B/op\t    2579 allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6392575,
            "unit": "ns/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523130,
            "unit": "B/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 536.7,
            "unit": "ns/op\t 119.26 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2227089 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 536.7,
            "unit": "ns/op",
            "extra": "2227089 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 119.26,
            "unit": "MB/s",
            "extra": "2227089 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2227089 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2227089 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9417184 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.5,
            "unit": "ns/op",
            "extra": "9417184 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9417184 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9417184 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1365,
            "unit": "ns/op\t  46.88 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1365,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.88,
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
            "value": 459,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2711479 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 459,
            "unit": "ns/op",
            "extra": "2711479 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2711479 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2711479 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26333,
            "unit": "ns/op\t 311.09 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26333,
            "unit": "ns/op",
            "extra": "74448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 311.09,
            "unit": "MB/s",
            "extra": "74448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74448 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.3,
            "unit": "ns/op\t1638.07 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7693006 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.3,
            "unit": "ns/op",
            "extra": "7693006 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1638.07,
            "unit": "MB/s",
            "extra": "7693006 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7693006 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7693006 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 682.8,
            "unit": "ns/op\t 374.93 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3490758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 682.8,
            "unit": "ns/op",
            "extra": "3490758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.93,
            "unit": "MB/s",
            "extra": "3490758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3490758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3490758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2045517,
            "unit": "ns/op\t 3064046 B/op\t   40018 allocs/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2045517,
            "unit": "ns/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064046,
            "unit": "B/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "580 times\n4 procs"
          }
        ]
      }
    ]
  }
}