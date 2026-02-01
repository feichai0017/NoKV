window.BENCHMARK_DATA = {
  "lastUpdate": 1769963957483,
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
          "id": "3af8a11be75871788bb0ad0cc3bf206c714653b0",
          "message": "Update docs",
          "timestamp": "2026-02-02T00:38:03+08:00",
          "tree_id": "65672b067a56dd91c307bae3225d9850033085e3",
          "url": "https://github.com/feichai0017/NoKV/commit/3af8a11be75871788bb0ad0cc3bf206c714653b0"
        },
        "date": 1769963956991,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 10765,
            "unit": "ns/op\t   2.97 MB/s\t     624 B/op\t      24 allocs/op",
            "extra": "159842 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 10765,
            "unit": "ns/op",
            "extra": "159842 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.97,
            "unit": "MB/s",
            "extra": "159842 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 624,
            "unit": "B/op",
            "extra": "159842 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "159842 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14609,
            "unit": "ns/op\t 280.37 MB/s\t     655 B/op\t      27 allocs/op",
            "extra": "91298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14609,
            "unit": "ns/op",
            "extra": "91298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 280.37,
            "unit": "MB/s",
            "extra": "91298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 655,
            "unit": "B/op",
            "extra": "91298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "91298 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12488,
            "unit": "ns/op\t   5.12 MB/s\t   20244 B/op\t       5 allocs/op",
            "extra": "750790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12488,
            "unit": "ns/op",
            "extra": "750790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.12,
            "unit": "MB/s",
            "extra": "750790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20244,
            "unit": "B/op",
            "extra": "750790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "750790 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9934,
            "unit": "ns/op\t 412.33 MB/s\t   17385 B/op\t       7 allocs/op",
            "extra": "248986 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9934,
            "unit": "ns/op",
            "extra": "248986 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 412.33,
            "unit": "MB/s",
            "extra": "248986 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17385,
            "unit": "B/op",
            "extra": "248986 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "248986 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 172373,
            "unit": "ns/op\t  95.05 MB/s\t   59738 B/op\t     663 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 172373,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.05,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 59738,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 663,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2249193,
            "unit": "ns/op\t    9248 B/op\t       0 allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2249193,
            "unit": "ns/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9248,
            "unit": "B/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "534 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 956.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1249788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 956.4,
            "unit": "ns/op",
            "extra": "1249788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1249788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1249788 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50143,
            "unit": "ns/op\t 163.37 MB/s\t   25592 B/op\t     454 allocs/op",
            "extra": "23604 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50143,
            "unit": "ns/op",
            "extra": "23604 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.37,
            "unit": "MB/s",
            "extra": "23604 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25592,
            "unit": "B/op",
            "extra": "23604 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23604 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6992574,
            "unit": "ns/op\t67523225 B/op\t    2587 allocs/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6992574,
            "unit": "ns/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523225,
            "unit": "B/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 648.2,
            "unit": "ns/op\t  98.73 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1968590 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 648.2,
            "unit": "ns/op",
            "extra": "1968590 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 98.73,
            "unit": "MB/s",
            "extra": "1968590 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1968590 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1968590 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9512121 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.2,
            "unit": "ns/op",
            "extra": "9512121 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9512121 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9512121 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1417,
            "unit": "ns/op\t  45.17 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 45.17,
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
            "value": 452.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2453698 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 452.9,
            "unit": "ns/op",
            "extra": "2453698 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2453698 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2453698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26672,
            "unit": "ns/op\t 307.14 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "73065 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26672,
            "unit": "ns/op",
            "extra": "73065 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.14,
            "unit": "MB/s",
            "extra": "73065 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "73065 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73065 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149.6,
            "unit": "ns/op\t1711.34 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7998804 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149.6,
            "unit": "ns/op",
            "extra": "7998804 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1711.34,
            "unit": "MB/s",
            "extra": "7998804 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7998804 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7998804 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 732.7,
            "unit": "ns/op\t 349.41 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3137587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 732.7,
            "unit": "ns/op",
            "extra": "3137587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 349.41,
            "unit": "MB/s",
            "extra": "3137587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3137587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3137587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2116308,
            "unit": "ns/op\t 3064037 B/op\t   40019 allocs/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2116308,
            "unit": "ns/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "576 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "576 times\n4 procs"
          }
        ]
      }
    ]
  }
}