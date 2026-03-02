window.BENCHMARK_DATA = {
  "lastUpdate": 1772412149310,
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
          "id": "e7acd967db5e023380010653b6d97b2e6d17875a",
          "message": "docs: simplify contributing guide and remove template placeholders",
          "timestamp": "2026-03-02T11:40:18+11:00",
          "tree_id": "18c4542584318c7abaa22e0a85fc578c34091e0e",
          "url": "https://github.com/feichai0017/NoKV/commit/e7acd967db5e023380010653b6d97b2e6d17875a"
        },
        "date": 1772412147722,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7549,
            "unit": "ns/op\t   4.24 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "153913 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7549,
            "unit": "ns/op",
            "extra": "153913 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.24,
            "unit": "MB/s",
            "extra": "153913 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "153913 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "153913 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18764,
            "unit": "ns/op\t 218.29 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "69904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18764,
            "unit": "ns/op",
            "extra": "69904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 218.29,
            "unit": "MB/s",
            "extra": "69904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "69904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "69904 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8592,
            "unit": "ns/op\t   7.45 MB/s\t   19113 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8592,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.45,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19113,
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
            "value": 11938,
            "unit": "ns/op\t 343.10 MB/s\t   33338 B/op\t      11 allocs/op",
            "extra": "338920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11938,
            "unit": "ns/op",
            "extra": "338920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 343.1,
            "unit": "MB/s",
            "extra": "338920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33338,
            "unit": "B/op",
            "extra": "338920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "338920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121271,
            "unit": "ns/op\t 135.10 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121271,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.1,
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
            "value": 1552518,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1552518,
            "unit": "ns/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 592.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1881650 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 592.9,
            "unit": "ns/op",
            "extra": "1881650 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1881650 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1881650 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.82,
            "unit": "ns/op",
            "extra": "48833346 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 56.96,
            "unit": "ns/op",
            "extra": "21037347 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.96,
            "unit": "ns/op",
            "extra": "59993190 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.71,
            "unit": "ns/op",
            "extra": "70741502 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20874048,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.1,
            "unit": "ns/op",
            "extra": "15905602 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50228,
            "unit": "ns/op",
            "extra": "23512 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49993,
            "unit": "ns/op\t 163.86 MB/s\t   27568 B/op\t     454 allocs/op",
            "extra": "25574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49993,
            "unit": "ns/op",
            "extra": "25574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.86,
            "unit": "MB/s",
            "extra": "25574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27568,
            "unit": "B/op",
            "extra": "25574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25574 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6443215,
            "unit": "ns/op\t67523263 B/op\t    2578 allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6443215,
            "unit": "ns/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523263,
            "unit": "B/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26363,
            "unit": "ns/op\t 310.74 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74097 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26363,
            "unit": "ns/op",
            "extra": "74097 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 310.74,
            "unit": "MB/s",
            "extra": "74097 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74097 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74097 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157,
            "unit": "ns/op\t1630.09 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7606742 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157,
            "unit": "ns/op",
            "extra": "7606742 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1630.09,
            "unit": "MB/s",
            "extra": "7606742 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7606742 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7606742 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 686.5,
            "unit": "ns/op\t 372.91 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3490267 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 686.5,
            "unit": "ns/op",
            "extra": "3490267 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 372.91,
            "unit": "MB/s",
            "extra": "3490267 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3490267 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3490267 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1991786,
            "unit": "ns/op\t 3064050 B/op\t   40018 allocs/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1991786,
            "unit": "ns/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064050,
            "unit": "B/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "604 times\n4 procs"
          }
        ]
      }
    ]
  }
}