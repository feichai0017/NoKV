window.BENCHMARK_DATA = {
  "lastUpdate": 1770284343649,
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
          "id": "cfb9399079d8030e59eb857cf228c67c0c4c3428",
          "message": "docs: note hotring usage in vlog routing",
          "timestamp": "2026-02-05T17:37:36+08:00",
          "tree_id": "05ca75b10948e39b1d50bd13f3a536de3edc82a8",
          "url": "https://github.com/feichai0017/NoKV/commit/cfb9399079d8030e59eb857cf228c67c0c4c3428"
        },
        "date": 1770284342030,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12207,
            "unit": "ns/op\t   2.62 MB/s\t     568 B/op\t      20 allocs/op",
            "extra": "113025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12207,
            "unit": "ns/op",
            "extra": "113025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.62,
            "unit": "MB/s",
            "extra": "113025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 568,
            "unit": "B/op",
            "extra": "113025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "113025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18778,
            "unit": "ns/op\t 218.12 MB/s\t     868 B/op\t      31 allocs/op",
            "extra": "75729 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18778,
            "unit": "ns/op",
            "extra": "75729 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 218.12,
            "unit": "MB/s",
            "extra": "75729 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 868,
            "unit": "B/op",
            "extra": "75729 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "75729 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11120,
            "unit": "ns/op\t   5.76 MB/s\t   18229 B/op\t       5 allocs/op",
            "extra": "738795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11120,
            "unit": "ns/op",
            "extra": "738795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.76,
            "unit": "MB/s",
            "extra": "738795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18229,
            "unit": "B/op",
            "extra": "738795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "738795 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10311,
            "unit": "ns/op\t 397.25 MB/s\t   18620 B/op\t       7 allocs/op",
            "extra": "234532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10311,
            "unit": "ns/op",
            "extra": "234532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 397.25,
            "unit": "MB/s",
            "extra": "234532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18620,
            "unit": "B/op",
            "extra": "234532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "234532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 181730,
            "unit": "ns/op\t  90.16 MB/s\t   60032 B/op\t     673 allocs/op",
            "extra": "9348 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 181730,
            "unit": "ns/op",
            "extra": "9348 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 90.16,
            "unit": "MB/s",
            "extra": "9348 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 60032,
            "unit": "B/op",
            "extra": "9348 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 673,
            "unit": "allocs/op",
            "extra": "9348 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2241624,
            "unit": "ns/op\t    8994 B/op\t       0 allocs/op",
            "extra": "549 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2241624,
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
            "value": 1050,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1050,
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
            "value": 47989,
            "unit": "ns/op\t 170.71 MB/s\t   25440 B/op\t     454 allocs/op",
            "extra": "24031 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47989,
            "unit": "ns/op",
            "extra": "24031 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 170.71,
            "unit": "MB/s",
            "extra": "24031 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25440,
            "unit": "B/op",
            "extra": "24031 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24031 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6714165,
            "unit": "ns/op\t67523331 B/op\t    2586 allocs/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6714165,
            "unit": "ns/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523331,
            "unit": "B/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 643.1,
            "unit": "ns/op\t  99.52 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1854051 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 643.1,
            "unit": "ns/op",
            "extra": "1854051 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 99.52,
            "unit": "MB/s",
            "extra": "1854051 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1854051 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1854051 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9249472 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.5,
            "unit": "ns/op",
            "extra": "9249472 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9249472 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9249472 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1417,
            "unit": "ns/op\t  45.16 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1417,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.16,
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
            "value": 488.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2695104 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 488.2,
            "unit": "ns/op",
            "extra": "2695104 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2695104 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2695104 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25863,
            "unit": "ns/op\t 316.74 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76851 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25863,
            "unit": "ns/op",
            "extra": "76851 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 316.74,
            "unit": "MB/s",
            "extra": "76851 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76851 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76851 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.3,
            "unit": "ns/op\t1762.24 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8187038 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.3,
            "unit": "ns/op",
            "extra": "8187038 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1762.24,
            "unit": "MB/s",
            "extra": "8187038 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8187038 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8187038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 738.2,
            "unit": "ns/op\t 346.77 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3152522 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 738.2,
            "unit": "ns/op",
            "extra": "3152522 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 346.77,
            "unit": "MB/s",
            "extra": "3152522 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3152522 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3152522 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2053456,
            "unit": "ns/op\t 3064050 B/op\t   40019 allocs/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2053456,
            "unit": "ns/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064050,
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