window.BENCHMARK_DATA = {
  "lastUpdate": 1772079949885,
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
          "id": "454273e5513f89469baba04c93ea313e5e724cd1",
          "message": "Merge pull request #75 from nothiny/fix/revarse-iterator\n\nfix [Bug/Feature] Iterator Does Not Support Reverse Iteration",
          "timestamp": "2026-02-26T12:24:19+08:00",
          "tree_id": "f42e72f3c359b03f52c1acdbea3ba31c1e51d2af",
          "url": "https://github.com/feichai0017/NoKV/commit/454273e5513f89469baba04c93ea313e5e724cd1"
        },
        "date": 1772079947846,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7976,
            "unit": "ns/op\t   4.01 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "144472 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7976,
            "unit": "ns/op",
            "extra": "144472 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.01,
            "unit": "MB/s",
            "extra": "144472 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "144472 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "144472 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16066,
            "unit": "ns/op\t 254.95 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "75642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16066,
            "unit": "ns/op",
            "extra": "75642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 254.95,
            "unit": "MB/s",
            "extra": "75642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "75642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "75642 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7917,
            "unit": "ns/op\t   8.08 MB/s\t   17789 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7917,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.08,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17789,
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
            "value": 11906,
            "unit": "ns/op\t 344.02 MB/s\t   32551 B/op\t      11 allocs/op",
            "extra": "352435 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11906,
            "unit": "ns/op",
            "extra": "352435 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 344.02,
            "unit": "MB/s",
            "extra": "352435 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32551,
            "unit": "B/op",
            "extra": "352435 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "352435 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124085,
            "unit": "ns/op\t 132.04 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124085,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.04,
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
            "value": 1537141,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1537141,
            "unit": "ns/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 587.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1908928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 587.1,
            "unit": "ns/op",
            "extra": "1908928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1908928 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1908928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49931,
            "unit": "ns/op\t 164.06 MB/s\t   27849 B/op\t     454 allocs/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49931,
            "unit": "ns/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.06,
            "unit": "MB/s",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27849,
            "unit": "B/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6598776,
            "unit": "ns/op\t67523255 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6598776,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523255,
            "unit": "B/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": null,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": null,
            "unit": "ns/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27495,
            "unit": "ns/op\t 297.94 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27495,
            "unit": "ns/op",
            "extra": "74745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 297.94,
            "unit": "MB/s",
            "extra": "74745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 154.7,
            "unit": "ns/op\t1655.24 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7591656 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 154.7,
            "unit": "ns/op",
            "extra": "7591656 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1655.24,
            "unit": "MB/s",
            "extra": "7591656 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7591656 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7591656 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 693.7,
            "unit": "ns/op\t 369.01 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3389560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 693.7,
            "unit": "ns/op",
            "extra": "3389560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 369.01,
            "unit": "MB/s",
            "extra": "3389560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3389560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3389560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2222676,
            "unit": "ns/op\t 3064036 B/op\t   40018 allocs/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2222676,
            "unit": "ns/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064036,
            "unit": "B/op",
            "extra": "577 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "577 times\n4 procs"
          }
        ]
      }
    ]
  }
}