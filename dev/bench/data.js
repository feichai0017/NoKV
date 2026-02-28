window.BENCHMARK_DATA = {
  "lastUpdate": 1772258973895,
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
          "id": "cb2128aff81c2aa4558a8c065c9af94d322c94ad",
          "message": "refactor: centralize level table sort semantics",
          "timestamp": "2026-02-28T17:08:04+11:00",
          "tree_id": "6f2879d091d7dcd71f520d7877a877c293da3d48",
          "url": "https://github.com/feichai0017/NoKV/commit/cb2128aff81c2aa4558a8c065c9af94d322c94ad"
        },
        "date": 1772258972796,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7687,
            "unit": "ns/op\t   4.16 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "175056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7687,
            "unit": "ns/op",
            "extra": "175056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.16,
            "unit": "MB/s",
            "extra": "175056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "175056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "175056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18557,
            "unit": "ns/op\t 220.72 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "74068 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18557,
            "unit": "ns/op",
            "extra": "74068 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 220.72,
            "unit": "MB/s",
            "extra": "74068 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "74068 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "74068 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8485,
            "unit": "ns/op\t   7.54 MB/s\t   19217 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8485,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.54,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19217,
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
            "value": 12125,
            "unit": "ns/op\t 337.80 MB/s\t   35318 B/op\t      11 allocs/op",
            "extra": "339510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12125,
            "unit": "ns/op",
            "extra": "339510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.8,
            "unit": "MB/s",
            "extra": "339510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35318,
            "unit": "B/op",
            "extra": "339510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "339510 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125674,
            "unit": "ns/op\t 130.37 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125674,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.37,
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
            "value": 1559561,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1559561,
            "unit": "ns/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 607.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1979313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 607.8,
            "unit": "ns/op",
            "extra": "1979313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1979313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1979313 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.82,
            "unit": "ns/op",
            "extra": "50243395 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 60.56,
            "unit": "ns/op",
            "extra": "19913436 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 21.03,
            "unit": "ns/op",
            "extra": "56561917 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.85,
            "unit": "ns/op",
            "extra": "72516714 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 24258184,
            "unit": "ns/op",
            "extra": "57 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.18,
            "unit": "ns/op",
            "extra": "15903394 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51409,
            "unit": "ns/op",
            "extra": "22995 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49715,
            "unit": "ns/op\t 164.78 MB/s\t   25658 B/op\t     454 allocs/op",
            "extra": "23422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49715,
            "unit": "ns/op",
            "extra": "23422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.78,
            "unit": "MB/s",
            "extra": "23422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25658,
            "unit": "B/op",
            "extra": "23422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23422 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6597522,
            "unit": "ns/op\t67523276 B/op\t    2579 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6597522,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523276,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26504,
            "unit": "ns/op\t 309.09 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26504,
            "unit": "ns/op",
            "extra": "73213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 309.09,
            "unit": "MB/s",
            "extra": "73213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73213 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164.5,
            "unit": "ns/op\t1556.13 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7031234 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164.5,
            "unit": "ns/op",
            "extra": "7031234 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1556.13,
            "unit": "MB/s",
            "extra": "7031234 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7031234 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7031234 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 692.6,
            "unit": "ns/op\t 369.60 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3389750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 692.6,
            "unit": "ns/op",
            "extra": "3389750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 369.6,
            "unit": "MB/s",
            "extra": "3389750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3389750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3389750 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2049682,
            "unit": "ns/op\t 3064032 B/op\t   40017 allocs/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2049682,
            "unit": "ns/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "570 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "570 times\n4 procs"
          }
        ]
      }
    ]
  }
}