window.BENCHMARK_DATA = {
  "lastUpdate": 1770633399227,
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
          "id": "902cf7c46acc394738fd84be61b3e9adc598e981",
          "message": "Merge pull request #34 from feichai0017/dependabot/go_modules/golang.org/x/sys-0.41.0\n\ndeps(deps): bump golang.org/x/sys from 0.40.0 to 0.41.0",
          "timestamp": "2026-02-09T18:35:07+08:00",
          "tree_id": "3e325ba684e78320ba560078f7fad1dd94076730",
          "url": "https://github.com/feichai0017/NoKV/commit/902cf7c46acc394738fd84be61b3e9adc598e981"
        },
        "date": 1770633397235,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13630,
            "unit": "ns/op\t   2.35 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "117550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13630,
            "unit": "ns/op",
            "extra": "117550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.35,
            "unit": "MB/s",
            "extra": "117550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "117550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "117550 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16890,
            "unit": "ns/op\t 242.51 MB/s\t     657 B/op\t      29 allocs/op",
            "extra": "83103 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16890,
            "unit": "ns/op",
            "extra": "83103 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 242.51,
            "unit": "MB/s",
            "extra": "83103 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 657,
            "unit": "B/op",
            "extra": "83103 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "83103 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10922,
            "unit": "ns/op\t   5.86 MB/s\t   18736 B/op\t       5 allocs/op",
            "extra": "752312 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10922,
            "unit": "ns/op",
            "extra": "752312 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.86,
            "unit": "MB/s",
            "extra": "752312 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18736,
            "unit": "B/op",
            "extra": "752312 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "752312 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10830,
            "unit": "ns/op\t 378.20 MB/s\t   20656 B/op\t       8 allocs/op",
            "extra": "210508 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10830,
            "unit": "ns/op",
            "extra": "210508 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 378.2,
            "unit": "MB/s",
            "extra": "210508 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 20656,
            "unit": "B/op",
            "extra": "210508 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "210508 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 168038,
            "unit": "ns/op\t  97.50 MB/s\t   57515 B/op\t     662 allocs/op",
            "extra": "9858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 168038,
            "unit": "ns/op",
            "extra": "9858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 97.5,
            "unit": "MB/s",
            "extra": "9858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 57515,
            "unit": "B/op",
            "extra": "9858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 662,
            "unit": "allocs/op",
            "extra": "9858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2159936,
            "unit": "ns/op\t       5 B/op\t       0 allocs/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2159936,
            "unit": "ns/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 5,
            "unit": "B/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1062,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1211078 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1062,
            "unit": "ns/op",
            "extra": "1211078 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1211078 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1211078 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 51741,
            "unit": "ns/op\t 158.33 MB/s\t   25587 B/op\t     454 allocs/op",
            "extra": "23619 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51741,
            "unit": "ns/op",
            "extra": "23619 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 158.33,
            "unit": "MB/s",
            "extra": "23619 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25587,
            "unit": "B/op",
            "extra": "23619 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23619 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6683535,
            "unit": "ns/op\t67523333 B/op\t    2586 allocs/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6683535,
            "unit": "ns/op",
            "extra": "175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523333,
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
            "value": 735,
            "unit": "ns/op\t  87.07 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1842597 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 735,
            "unit": "ns/op",
            "extra": "1842597 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 87.07,
            "unit": "MB/s",
            "extra": "1842597 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1842597 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1842597 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9276579 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.3,
            "unit": "ns/op",
            "extra": "9276579 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9276579 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9276579 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1469,
            "unit": "ns/op\t  43.55 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 43.55,
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
            "value": 469,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2609871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 469,
            "unit": "ns/op",
            "extra": "2609871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2609871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2609871 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25651,
            "unit": "ns/op\t 319.37 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76410 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25651,
            "unit": "ns/op",
            "extra": "76410 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 319.37,
            "unit": "MB/s",
            "extra": "76410 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76410 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76410 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.6,
            "unit": "ns/op\t1758.02 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8173364 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.6,
            "unit": "ns/op",
            "extra": "8173364 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1758.02,
            "unit": "MB/s",
            "extra": "8173364 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8173364 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8173364 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 746.8,
            "unit": "ns/op\t 342.81 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3068852 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 746.8,
            "unit": "ns/op",
            "extra": "3068852 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 342.81,
            "unit": "MB/s",
            "extra": "3068852 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3068852 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3068852 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2052709,
            "unit": "ns/op\t 3064048 B/op\t   40019 allocs/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2052709,
            "unit": "ns/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064048,
            "unit": "B/op",
            "extra": "580 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "580 times\n4 procs"
          }
        ]
      }
    ]
  }
}