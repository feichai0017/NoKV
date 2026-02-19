window.BENCHMARK_DATA = {
  "lastUpdate": 1771513003896,
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
          "id": "d802e3b9d22e91ea3b5390a7d2c211f615890003",
          "message": "Merge pull request #70 from ByteByteUp/feat/badger-batch-insert\n\nfeat: add batch insert support for badger and pure insert workload",
          "timestamp": "2026-02-19T22:55:28+08:00",
          "tree_id": "8fcbafff3714b6f5e20eed48470e783de5544cd3",
          "url": "https://github.com/feichai0017/NoKV/commit/d802e3b9d22e91ea3b5390a7d2c211f615890003"
        },
        "date": 1771513002996,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7721,
            "unit": "ns/op\t   4.14 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "140911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7721,
            "unit": "ns/op",
            "extra": "140911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.14,
            "unit": "MB/s",
            "extra": "140911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "140911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "140911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16288,
            "unit": "ns/op\t 251.47 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "62185 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16288,
            "unit": "ns/op",
            "extra": "62185 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 251.47,
            "unit": "MB/s",
            "extra": "62185 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "62185 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "62185 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8249,
            "unit": "ns/op\t   7.76 MB/s\t   18871 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8249,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.76,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18871,
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
            "value": 11881,
            "unit": "ns/op\t 344.76 MB/s\t   32834 B/op\t      11 allocs/op",
            "extra": "344629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11881,
            "unit": "ns/op",
            "extra": "344629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 344.76,
            "unit": "MB/s",
            "extra": "344629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32834,
            "unit": "B/op",
            "extra": "344629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "344629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125642,
            "unit": "ns/op\t 130.40 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125642,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.4,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56847,
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
            "value": 1515289,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1515289,
            "unit": "ns/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "787 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 593.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2096512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 593.9,
            "unit": "ns/op",
            "extra": "2096512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2096512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2096512 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48338,
            "unit": "ns/op\t 169.47 MB/s\t   27667 B/op\t     454 allocs/op",
            "extra": "25336 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48338,
            "unit": "ns/op",
            "extra": "25336 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.47,
            "unit": "MB/s",
            "extra": "25336 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27667,
            "unit": "B/op",
            "extra": "25336 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25336 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6616833,
            "unit": "ns/op\t67523119 B/op\t    2578 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6616833,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523119,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 545.9,
            "unit": "ns/op\t 117.24 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2682469 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 545.9,
            "unit": "ns/op",
            "extra": "2682469 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 117.24,
            "unit": "MB/s",
            "extra": "2682469 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2682469 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2682469 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9246614 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.5,
            "unit": "ns/op",
            "extra": "9246614 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9246614 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9246614 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1372,
            "unit": "ns/op\t  46.64 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1372,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.64,
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
            "value": 452.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2667655 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 452.3,
            "unit": "ns/op",
            "extra": "2667655 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2667655 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2667655 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26467,
            "unit": "ns/op\t 309.52 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73627 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26467,
            "unit": "ns/op",
            "extra": "73627 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 309.52,
            "unit": "MB/s",
            "extra": "73627 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73627 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73627 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 179.6,
            "unit": "ns/op\t1425.06 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7133220 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 179.6,
            "unit": "ns/op",
            "extra": "7133220 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1425.06,
            "unit": "MB/s",
            "extra": "7133220 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7133220 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7133220 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 683.4,
            "unit": "ns/op\t 374.62 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3440142 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 683.4,
            "unit": "ns/op",
            "extra": "3440142 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.62,
            "unit": "MB/s",
            "extra": "3440142 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3440142 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3440142 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1992038,
            "unit": "ns/op\t 3064032 B/op\t   40017 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1992038,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "592 times\n4 procs"
          }
        ]
      }
    ]
  }
}