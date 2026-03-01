window.BENCHMARK_DATA = {
  "lastUpdate": 1772340891739,
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
          "id": "1a0c4613007731957cc6c11612e560423431ef5e",
          "message": "Merge pull request #101 from CyberSleeper/feature/iterator-bounds\n\nfeat: add iterator bounds to IteratorOptions",
          "timestamp": "2026-03-01T12:53:34+08:00",
          "tree_id": "bb95d674c01bd2d826fe6ff718d35eccfac3e946",
          "url": "https://github.com/feichai0017/NoKV/commit/1a0c4613007731957cc6c11612e560423431ef5e"
        },
        "date": 1772340890194,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7922,
            "unit": "ns/op\t   4.04 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "139495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7922,
            "unit": "ns/op",
            "extra": "139495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.04,
            "unit": "MB/s",
            "extra": "139495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "139495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "139495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18101,
            "unit": "ns/op\t 226.28 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "60237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18101,
            "unit": "ns/op",
            "extra": "60237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 226.28,
            "unit": "MB/s",
            "extra": "60237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "60237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "60237 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8104,
            "unit": "ns/op\t   7.90 MB/s\t   18854 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8104,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.9,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18854,
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
            "value": 12797,
            "unit": "ns/op\t 320.07 MB/s\t   36423 B/op\t      11 allocs/op",
            "extra": "316378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12797,
            "unit": "ns/op",
            "extra": "316378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 320.07,
            "unit": "MB/s",
            "extra": "316378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 36423,
            "unit": "B/op",
            "extra": "316378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "316378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127051,
            "unit": "ns/op\t 128.96 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127051,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.96,
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
            "value": 1540630,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1540630,
            "unit": "ns/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 608.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1988146 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 608.2,
            "unit": "ns/op",
            "extra": "1988146 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1988146 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1988146 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 24.14,
            "unit": "ns/op",
            "extra": "50392242 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 59.37,
            "unit": "ns/op",
            "extra": "19969993 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.95,
            "unit": "ns/op",
            "extra": "60004862 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.58,
            "unit": "ns/op",
            "extra": "72555408 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20560463,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.24,
            "unit": "ns/op",
            "extra": "15761244 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50828,
            "unit": "ns/op",
            "extra": "22920 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49173,
            "unit": "ns/op\t 166.59 MB/s\t   27937 B/op\t     454 allocs/op",
            "extra": "24709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49173,
            "unit": "ns/op",
            "extra": "24709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.59,
            "unit": "MB/s",
            "extra": "24709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27937,
            "unit": "B/op",
            "extra": "24709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24709 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6279966,
            "unit": "ns/op\t67523222 B/op\t    2578 allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6279966,
            "unit": "ns/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523222,
            "unit": "B/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26607,
            "unit": "ns/op\t 307.89 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73669 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26607,
            "unit": "ns/op",
            "extra": "73669 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.89,
            "unit": "MB/s",
            "extra": "73669 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73669 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73669 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.2,
            "unit": "ns/op\t1629.00 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7568935 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.2,
            "unit": "ns/op",
            "extra": "7568935 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1629,
            "unit": "MB/s",
            "extra": "7568935 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7568935 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7568935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 689.4,
            "unit": "ns/op\t 371.33 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3458041 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 689.4,
            "unit": "ns/op",
            "extra": "3458041 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 371.33,
            "unit": "MB/s",
            "extra": "3458041 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3458041 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3458041 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1987249,
            "unit": "ns/op\t 3064022 B/op\t   40017 allocs/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1987249,
            "unit": "ns/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064022,
            "unit": "B/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "603 times\n4 procs"
          }
        ]
      }
    ]
  }
}