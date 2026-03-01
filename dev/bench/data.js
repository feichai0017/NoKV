window.BENCHMARK_DATA = {
  "lastUpdate": 1772401227679,
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
          "id": "a88242bdf0988ac582514549b4628a2d61e4ffdb",
          "message": "Remove unused options toml",
          "timestamp": "2026-03-02T08:39:09+11:00",
          "tree_id": "626ed0862901cb192528d108309c0b386376dd99",
          "url": "https://github.com/feichai0017/NoKV/commit/a88242bdf0988ac582514549b4628a2d61e4ffdb"
        },
        "date": 1772401226445,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8075,
            "unit": "ns/op\t   3.96 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "140584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8075,
            "unit": "ns/op",
            "extra": "140584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.96,
            "unit": "MB/s",
            "extra": "140584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "140584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "140584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18412,
            "unit": "ns/op\t 222.46 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "68305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18412,
            "unit": "ns/op",
            "extra": "68305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.46,
            "unit": "MB/s",
            "extra": "68305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "68305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "68305 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8573,
            "unit": "ns/op\t   7.47 MB/s\t   20022 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8573,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.47,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20022,
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
            "value": 12538,
            "unit": "ns/op\t 326.69 MB/s\t   35219 B/op\t      11 allocs/op",
            "extra": "318531 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12538,
            "unit": "ns/op",
            "extra": "318531 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 326.69,
            "unit": "MB/s",
            "extra": "318531 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35219,
            "unit": "B/op",
            "extra": "318531 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "318531 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121964,
            "unit": "ns/op\t 134.33 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121964,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.33,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56846,
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
            "value": 1586415,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "747 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1586415,
            "unit": "ns/op",
            "extra": "747 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "747 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "747 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 617,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2000338 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 617,
            "unit": "ns/op",
            "extra": "2000338 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2000338 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2000338 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.72,
            "unit": "ns/op",
            "extra": "50410870 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.57,
            "unit": "ns/op",
            "extra": "19714734 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.17,
            "unit": "ns/op",
            "extra": "60001460 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.9,
            "unit": "ns/op",
            "extra": "72503298 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21245772,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 76.13,
            "unit": "ns/op",
            "extra": "15096000 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50726,
            "unit": "ns/op",
            "extra": "23360 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49835,
            "unit": "ns/op\t 164.38 MB/s\t   27792 B/op\t     454 allocs/op",
            "extra": "25042 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49835,
            "unit": "ns/op",
            "extra": "25042 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.38,
            "unit": "MB/s",
            "extra": "25042 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27792,
            "unit": "B/op",
            "extra": "25042 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25042 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6450994,
            "unit": "ns/op\t67523331 B/op\t    2579 allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6450994,
            "unit": "ns/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523331,
            "unit": "B/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "187 times\n4 procs"
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
            "value": 26698,
            "unit": "ns/op\t 306.84 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74494 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26698,
            "unit": "ns/op",
            "extra": "74494 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 306.84,
            "unit": "MB/s",
            "extra": "74494 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74494 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74494 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.5,
            "unit": "ns/op\t1635.31 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7683747 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.5,
            "unit": "ns/op",
            "extra": "7683747 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1635.31,
            "unit": "MB/s",
            "extra": "7683747 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7683747 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7683747 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 675.2,
            "unit": "ns/op\t 379.17 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3429056 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 675.2,
            "unit": "ns/op",
            "extra": "3429056 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 379.17,
            "unit": "MB/s",
            "extra": "3429056 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3429056 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3429056 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2018718,
            "unit": "ns/op\t 3064033 B/op\t   40017 allocs/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2018718,
            "unit": "ns/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064033,
            "unit": "B/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "600 times\n4 procs"
          }
        ]
      }
    ]
  }
}