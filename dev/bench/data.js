window.BENCHMARK_DATA = {
  "lastUpdate": 1772345654160,
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
          "id": "97c1c1ae6105810f4935108df529bb2ee51d884d",
          "message": "fix(iterator): prevent resurrection after out-of-range seek",
          "timestamp": "2026-03-01T17:12:53+11:00",
          "tree_id": "2df7928a622c697990dcc7b901c19e86e53a8052",
          "url": "https://github.com/feichai0017/NoKV/commit/97c1c1ae6105810f4935108df529bb2ee51d884d"
        },
        "date": 1772345653118,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8710,
            "unit": "ns/op\t   3.67 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "129268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8710,
            "unit": "ns/op",
            "extra": "129268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.67,
            "unit": "MB/s",
            "extra": "129268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "129268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "129268 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18348,
            "unit": "ns/op\t 223.24 MB/s\t     537 B/op\t      23 allocs/op",
            "extra": "81402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18348,
            "unit": "ns/op",
            "extra": "81402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 223.24,
            "unit": "MB/s",
            "extra": "81402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 537,
            "unit": "B/op",
            "extra": "81402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "81402 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8532,
            "unit": "ns/op\t   7.50 MB/s\t   19605 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8532,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.5,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19605,
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
            "value": 11806,
            "unit": "ns/op\t 346.93 MB/s\t   33073 B/op\t      11 allocs/op",
            "extra": "349114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11806,
            "unit": "ns/op",
            "extra": "349114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 346.93,
            "unit": "MB/s",
            "extra": "349114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33073,
            "unit": "B/op",
            "extra": "349114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "349114 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125467,
            "unit": "ns/op\t 130.58 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125467,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.58,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56849,
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
            "value": 1545800,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1545800,
            "unit": "ns/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 621.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1994122 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 621.1,
            "unit": "ns/op",
            "extra": "1994122 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1994122 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1994122 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.82,
            "unit": "ns/op",
            "extra": "49384609 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.22,
            "unit": "ns/op",
            "extra": "20339427 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.05,
            "unit": "ns/op",
            "extra": "59971335 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 32.38,
            "unit": "ns/op",
            "extra": "34230180 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 22212228,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 76.45,
            "unit": "ns/op",
            "extra": "15674773 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 52577,
            "unit": "ns/op",
            "extra": "22339 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50985,
            "unit": "ns/op\t 160.67 MB/s\t   27726 B/op\t     454 allocs/op",
            "extra": "25196 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50985,
            "unit": "ns/op",
            "extra": "25196 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 160.67,
            "unit": "MB/s",
            "extra": "25196 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27726,
            "unit": "B/op",
            "extra": "25196 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25196 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6528646,
            "unit": "ns/op\t67523333 B/op\t    2579 allocs/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6528646,
            "unit": "ns/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523333,
            "unit": "B/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "182 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26600,
            "unit": "ns/op\t 307.97 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74098 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26600,
            "unit": "ns/op",
            "extra": "74098 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.97,
            "unit": "MB/s",
            "extra": "74098 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74098 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74098 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 171.2,
            "unit": "ns/op\t1495.19 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "6108543 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 171.2,
            "unit": "ns/op",
            "extra": "6108543 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1495.19,
            "unit": "MB/s",
            "extra": "6108543 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "6108543 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6108543 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 694,
            "unit": "ns/op\t 368.87 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3363645 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 694,
            "unit": "ns/op",
            "extra": "3363645 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.87,
            "unit": "MB/s",
            "extra": "3363645 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3363645 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3363645 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2000248,
            "unit": "ns/op\t 3064039 B/op\t   40017 allocs/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2000248,
            "unit": "ns/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "596 times\n4 procs"
          }
        ]
      }
    ]
  }
}