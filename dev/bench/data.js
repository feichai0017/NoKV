window.BENCHMARK_DATA = {
  "lastUpdate": 1770277319872,
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
          "id": "aabd46963df14b5cd6020d59557064a71ae7001d",
          "message": "docs: add paper links and deepen vlog note",
          "timestamp": "2026-02-05T15:40:13+08:00",
          "tree_id": "81f21fb3ea2e27e46f89c700be7465d52a6c2f24",
          "url": "https://github.com/feichai0017/NoKV/commit/aabd46963df14b5cd6020d59557064a71ae7001d"
        },
        "date": 1770277318062,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12607,
            "unit": "ns/op\t   2.54 MB/s\t     576 B/op\t      20 allocs/op",
            "extra": "89875 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12607,
            "unit": "ns/op",
            "extra": "89875 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.54,
            "unit": "MB/s",
            "extra": "89875 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 576,
            "unit": "B/op",
            "extra": "89875 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "89875 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16443,
            "unit": "ns/op\t 249.11 MB/s\t     822 B/op\t      31 allocs/op",
            "extra": "82300 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16443,
            "unit": "ns/op",
            "extra": "82300 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 249.11,
            "unit": "MB/s",
            "extra": "82300 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 822,
            "unit": "B/op",
            "extra": "82300 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "82300 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11932,
            "unit": "ns/op\t   5.36 MB/s\t   21075 B/op\t       5 allocs/op",
            "extra": "752524 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11932,
            "unit": "ns/op",
            "extra": "752524 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.36,
            "unit": "MB/s",
            "extra": "752524 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 21075,
            "unit": "B/op",
            "extra": "752524 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "752524 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9708,
            "unit": "ns/op\t 421.90 MB/s\t   19437 B/op\t       7 allocs/op",
            "extra": "250653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9708,
            "unit": "ns/op",
            "extra": "250653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 421.9,
            "unit": "MB/s",
            "extra": "250653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19437,
            "unit": "B/op",
            "extra": "250653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "250653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 163200,
            "unit": "ns/op\t 100.39 MB/s\t   56859 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 163200,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 100.39,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56859,
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
            "value": 2090851,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2090851,
            "unit": "ns/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 909.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1352671 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 909.8,
            "unit": "ns/op",
            "extra": "1352671 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1352671 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1352671 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 51443,
            "unit": "ns/op\t 159.24 MB/s\t   28007 B/op\t     454 allocs/op",
            "extra": "24552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51443,
            "unit": "ns/op",
            "extra": "24552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 159.24,
            "unit": "MB/s",
            "extra": "24552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28007,
            "unit": "B/op",
            "extra": "24552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24552 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6461149,
            "unit": "ns/op\t67523285 B/op\t    2586 allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6461149,
            "unit": "ns/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523285,
            "unit": "B/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 558.6,
            "unit": "ns/op\t 114.58 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2173279 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 558.6,
            "unit": "ns/op",
            "extra": "2173279 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 114.58,
            "unit": "MB/s",
            "extra": "2173279 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2173279 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2173279 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9379561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.7,
            "unit": "ns/op",
            "extra": "9379561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9379561 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9379561 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1379,
            "unit": "ns/op\t  46.40 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1379,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.4,
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
            "extra": "2449609 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 482.3,
            "unit": "ns/op",
            "extra": "2449609 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2449609 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2449609 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26374,
            "unit": "ns/op\t 310.61 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76815 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26374,
            "unit": "ns/op",
            "extra": "76815 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 310.61,
            "unit": "MB/s",
            "extra": "76815 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76815 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76815 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.5,
            "unit": "ns/op\t1759.88 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8340921 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.5,
            "unit": "ns/op",
            "extra": "8340921 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1759.88,
            "unit": "MB/s",
            "extra": "8340921 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8340921 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8340921 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 726.8,
            "unit": "ns/op\t 352.22 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3177676 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 726.8,
            "unit": "ns/op",
            "extra": "3177676 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 352.22,
            "unit": "MB/s",
            "extra": "3177676 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3177676 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3177676 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2004382,
            "unit": "ns/op\t 3064039 B/op\t   40019 allocs/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2004382,
            "unit": "ns/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "595 times\n4 procs"
          }
        ]
      }
    ]
  }
}