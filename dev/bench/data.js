window.BENCHMARK_DATA = {
  "lastUpdate": 1771144459616,
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
          "id": "c3dc13968dfc26819030d903ddd9699a784fd8b5",
          "message": "Merge pull request #66 from feichai0017/feature/vfs-fault-injection\n\nrefactor: unify storage IO on vfs and add file-level fault injection",
          "timestamp": "2026-02-15T16:33:09+08:00",
          "tree_id": "e5507132434faa2bf6b2bef51cc27d27b793c086",
          "url": "https://github.com/feichai0017/NoKV/commit/c3dc13968dfc26819030d903ddd9699a784fd8b5"
        },
        "date": 1771144458458,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7274,
            "unit": "ns/op\t   4.40 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "165740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7274,
            "unit": "ns/op",
            "extra": "165740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.4,
            "unit": "MB/s",
            "extra": "165740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "165740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "165740 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18935,
            "unit": "ns/op\t 216.32 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "58725 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18935,
            "unit": "ns/op",
            "extra": "58725 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 216.32,
            "unit": "MB/s",
            "extra": "58725 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "58725 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "58725 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8001,
            "unit": "ns/op\t   8.00 MB/s\t   18492 B/op\t       8 allocs/op",
            "extra": "952279 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8001,
            "unit": "ns/op",
            "extra": "952279 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8,
            "unit": "MB/s",
            "extra": "952279 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18492,
            "unit": "B/op",
            "extra": "952279 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "952279 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11377,
            "unit": "ns/op\t 360.03 MB/s\t   33517 B/op\t      11 allocs/op",
            "extra": "367708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11377,
            "unit": "ns/op",
            "extra": "367708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 360.03,
            "unit": "MB/s",
            "extra": "367708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33517,
            "unit": "B/op",
            "extra": "367708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "367708 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120998,
            "unit": "ns/op\t 135.41 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120998,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.41,
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
            "value": 1489619,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1489619,
            "unit": "ns/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 595.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2086240 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 595.9,
            "unit": "ns/op",
            "extra": "2086240 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2086240 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2086240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 47809,
            "unit": "ns/op\t 171.35 MB/s\t   27445 B/op\t     454 allocs/op",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47809,
            "unit": "ns/op",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 171.35,
            "unit": "MB/s",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27445,
            "unit": "B/op",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6174519,
            "unit": "ns/op\t67523192 B/op\t    2579 allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6174519,
            "unit": "ns/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523192,
            "unit": "B/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 554,
            "unit": "ns/op\t 115.52 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1954206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 554,
            "unit": "ns/op",
            "extra": "1954206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 115.52,
            "unit": "MB/s",
            "extra": "1954206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1954206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1954206 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9391060 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.1,
            "unit": "ns/op",
            "extra": "9391060 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9391060 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9391060 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1324,
            "unit": "ns/op\t  48.34 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1324,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 48.34,
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
            "value": 492.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2695202 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 492.1,
            "unit": "ns/op",
            "extra": "2695202 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2695202 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2695202 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27043,
            "unit": "ns/op\t 302.93 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "72776 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27043,
            "unit": "ns/op",
            "extra": "72776 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 302.93,
            "unit": "MB/s",
            "extra": "72776 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "72776 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "72776 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 177.8,
            "unit": "ns/op\t1439.43 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7269236 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 177.8,
            "unit": "ns/op",
            "extra": "7269236 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1439.43,
            "unit": "MB/s",
            "extra": "7269236 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7269236 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7269236 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 667.3,
            "unit": "ns/op\t 383.66 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3504692 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 667.3,
            "unit": "ns/op",
            "extra": "3504692 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 383.66,
            "unit": "MB/s",
            "extra": "3504692 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3504692 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3504692 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1927936,
            "unit": "ns/op\t 3064030 B/op\t   40017 allocs/op",
            "extra": "621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1927936,
            "unit": "ns/op",
            "extra": "621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064030,
            "unit": "B/op",
            "extra": "621 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "621 times\n4 procs"
          }
        ]
      }
    ]
  }
}