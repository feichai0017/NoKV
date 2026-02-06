window.BENCHMARK_DATA = {
  "lastUpdate": 1770360501561,
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
          "id": "cec6e73cb0e6b97795632c3fea893ee68a935ea7",
          "message": "feat: wire hotring stats into observability",
          "timestamp": "2026-02-06T14:46:02+08:00",
          "tree_id": "26e4e8c20002a9be7775e6257281c0ee9eca013e",
          "url": "https://github.com/feichai0017/NoKV/commit/cec6e73cb0e6b97795632c3fea893ee68a935ea7"
        },
        "date": 1770360500174,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 15188,
            "unit": "ns/op\t   2.11 MB/s\t     629 B/op\t      20 allocs/op",
            "extra": "130587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 15188,
            "unit": "ns/op",
            "extra": "130587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.11,
            "unit": "MB/s",
            "extra": "130587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 629,
            "unit": "B/op",
            "extra": "130587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "130587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17957,
            "unit": "ns/op\t 228.11 MB/s\t     822 B/op\t      31 allocs/op",
            "extra": "83012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17957,
            "unit": "ns/op",
            "extra": "83012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 228.11,
            "unit": "MB/s",
            "extra": "83012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 822,
            "unit": "B/op",
            "extra": "83012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "83012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12013,
            "unit": "ns/op\t   5.33 MB/s\t   19347 B/op\t       5 allocs/op",
            "extra": "676010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12013,
            "unit": "ns/op",
            "extra": "676010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.33,
            "unit": "MB/s",
            "extra": "676010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19347,
            "unit": "B/op",
            "extra": "676010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "676010 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9881,
            "unit": "ns/op\t 414.54 MB/s\t   17432 B/op\t       7 allocs/op",
            "extra": "251512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9881,
            "unit": "ns/op",
            "extra": "251512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 414.54,
            "unit": "MB/s",
            "extra": "251512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17432,
            "unit": "B/op",
            "extra": "251512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "251512 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 169832,
            "unit": "ns/op\t  96.47 MB/s\t   61773 B/op\t     681 allocs/op",
            "extra": "9025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 169832,
            "unit": "ns/op",
            "extra": "9025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 96.47,
            "unit": "MB/s",
            "extra": "9025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 61773,
            "unit": "B/op",
            "extra": "9025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 681,
            "unit": "allocs/op",
            "extra": "9025 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2102391,
            "unit": "ns/op\t    8786 B/op\t       0 allocs/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2102391,
            "unit": "ns/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 8786,
            "unit": "B/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1001,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1001,
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
            "value": 50232,
            "unit": "ns/op\t 163.08 MB/s\t   25768 B/op\t     454 allocs/op",
            "extra": "23128 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50232,
            "unit": "ns/op",
            "extra": "23128 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.08,
            "unit": "MB/s",
            "extra": "23128 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25768,
            "unit": "B/op",
            "extra": "23128 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23128 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6850529,
            "unit": "ns/op\t67523249 B/op\t    2587 allocs/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6850529,
            "unit": "ns/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523249,
            "unit": "B/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2587,
            "unit": "allocs/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 711.9,
            "unit": "ns/op\t  89.90 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1877427 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 711.9,
            "unit": "ns/op",
            "extra": "1877427 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 89.9,
            "unit": "MB/s",
            "extra": "1877427 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1877427 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1877427 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9317521 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.6,
            "unit": "ns/op",
            "extra": "9317521 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9317521 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9317521 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1421,
            "unit": "ns/op\t  45.05 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1421,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.05,
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
            "value": 468.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2449213 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 468.3,
            "unit": "ns/op",
            "extra": "2449213 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2449213 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2449213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26886,
            "unit": "ns/op\t 304.69 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26886,
            "unit": "ns/op",
            "extra": "76278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 304.69,
            "unit": "MB/s",
            "extra": "76278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76278 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 144,
            "unit": "ns/op\t1777.94 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8272140 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 144,
            "unit": "ns/op",
            "extra": "8272140 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1777.94,
            "unit": "MB/s",
            "extra": "8272140 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8272140 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8272140 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 737,
            "unit": "ns/op\t 347.34 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3148587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 737,
            "unit": "ns/op",
            "extra": "3148587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 347.34,
            "unit": "MB/s",
            "extra": "3148587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3148587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3148587 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2050354,
            "unit": "ns/op\t 3064042 B/op\t   40019 allocs/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2050354,
            "unit": "ns/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064042,
            "unit": "B/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "578 times\n4 procs"
          }
        ]
      }
    ]
  }
}