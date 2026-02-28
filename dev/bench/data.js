window.BENCHMARK_DATA = {
  "lastUpdate": 1772300435414,
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
          "id": "84de610025875294bec90aa2d79ce4b0dcc8d293",
          "message": "Merge pull request #100 from feichai0017/feature/pd-lite-design\n\nfeat(pd): integrate PD-lite control plane with persistence and config-driven bootstrap",
          "timestamp": "2026-03-01T01:39:21+08:00",
          "tree_id": "f64dfff9c6c33b00caf6a4782901f5d9efc0f5b2",
          "url": "https://github.com/feichai0017/NoKV/commit/84de610025875294bec90aa2d79ce4b0dcc8d293"
        },
        "date": 1772300434421,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9906,
            "unit": "ns/op\t   3.23 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "127028 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9906,
            "unit": "ns/op",
            "extra": "127028 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.23,
            "unit": "MB/s",
            "extra": "127028 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "127028 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "127028 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19560,
            "unit": "ns/op\t 209.41 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "55809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19560,
            "unit": "ns/op",
            "extra": "55809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 209.41,
            "unit": "MB/s",
            "extra": "55809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "55809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "55809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8576,
            "unit": "ns/op\t   7.46 MB/s\t   19689 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8576,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.46,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19689,
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
            "value": 12354,
            "unit": "ns/op\t 331.56 MB/s\t   33411 B/op\t      11 allocs/op",
            "extra": "332742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12354,
            "unit": "ns/op",
            "extra": "332742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 331.56,
            "unit": "MB/s",
            "extra": "332742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33411,
            "unit": "B/op",
            "extra": "332742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "332742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124046,
            "unit": "ns/op\t 132.08 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124046,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.08,
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
            "value": 1496725,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1496725,
            "unit": "ns/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 650.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1853665 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 650.4,
            "unit": "ns/op",
            "extra": "1853665 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1853665 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1853665 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 24.47,
            "unit": "ns/op",
            "extra": "49911294 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.63,
            "unit": "ns/op",
            "extra": "18618516 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.12,
            "unit": "ns/op",
            "extra": "59962564 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.28,
            "unit": "ns/op",
            "extra": "71981012 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 24546885,
            "unit": "ns/op",
            "extra": "56 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 76.71,
            "unit": "ns/op",
            "extra": "14500066 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51331,
            "unit": "ns/op",
            "extra": "23380 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49972,
            "unit": "ns/op\t 163.93 MB/s\t   27802 B/op\t     454 allocs/op",
            "extra": "25017 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49972,
            "unit": "ns/op",
            "extra": "25017 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.93,
            "unit": "MB/s",
            "extra": "25017 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27802,
            "unit": "B/op",
            "extra": "25017 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25017 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6741143,
            "unit": "ns/op\t67523123 B/op\t    2579 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6741143,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523123,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "183 times\n4 procs"
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
            "value": 26966,
            "unit": "ns/op\t 303.79 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73400 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26966,
            "unit": "ns/op",
            "extra": "73400 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.79,
            "unit": "MB/s",
            "extra": "73400 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73400 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73400 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.7,
            "unit": "ns/op\t1633.62 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7568046 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.7,
            "unit": "ns/op",
            "extra": "7568046 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1633.62,
            "unit": "MB/s",
            "extra": "7568046 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7568046 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7568046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 680.4,
            "unit": "ns/op\t 376.27 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3391623 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 680.4,
            "unit": "ns/op",
            "extra": "3391623 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 376.27,
            "unit": "MB/s",
            "extra": "3391623 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3391623 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3391623 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2020914,
            "unit": "ns/op\t 3064039 B/op\t   40017 allocs/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2020914,
            "unit": "ns/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "582 times\n4 procs"
          }
        ]
      }
    ]
  }
}