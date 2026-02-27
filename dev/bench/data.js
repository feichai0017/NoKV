window.BENCHMARK_DATA = {
  "lastUpdate": 1772154656812,
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
          "id": "b0eec92b1b47c5365d4dde163c15eebee8c0e32d",
          "message": "Optimize with go fix",
          "timestamp": "2026-02-27T09:09:44+08:00",
          "tree_id": "113107c649c08c632a673c944b4347c2f859820b",
          "url": "https://github.com/feichai0017/NoKV/commit/b0eec92b1b47c5365d4dde163c15eebee8c0e32d"
        },
        "date": 1772154655817,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6969,
            "unit": "ns/op\t   4.59 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "157694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6969,
            "unit": "ns/op",
            "extra": "157694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.59,
            "unit": "MB/s",
            "extra": "157694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "157694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "157694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17594,
            "unit": "ns/op\t 232.81 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17594,
            "unit": "ns/op",
            "extra": "70975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 232.81,
            "unit": "MB/s",
            "extra": "70975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70975 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7849,
            "unit": "ns/op\t   8.15 MB/s\t   17849 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7849,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.15,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17849,
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
            "value": 11467,
            "unit": "ns/op\t 357.19 MB/s\t   32484 B/op\t      11 allocs/op",
            "extra": "365653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11467,
            "unit": "ns/op",
            "extra": "365653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 357.19,
            "unit": "MB/s",
            "extra": "365653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32484,
            "unit": "B/op",
            "extra": "365653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "365653 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121546,
            "unit": "ns/op\t 134.80 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121546,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.8,
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
            "value": 1489319,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1489319,
            "unit": "ns/op",
            "extra": "810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 657.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2037818 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 657.2,
            "unit": "ns/op",
            "extra": "2037818 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2037818 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2037818 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.66,
            "unit": "ns/op",
            "extra": "50451354 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 60.05,
            "unit": "ns/op",
            "extra": "19619352 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.98,
            "unit": "ns/op",
            "extra": "60029770 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.89,
            "unit": "ns/op",
            "extra": "71919472 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20510619,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.19,
            "unit": "ns/op",
            "extra": "15933068 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50588,
            "unit": "ns/op",
            "extra": "23684 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49000,
            "unit": "ns/op\t 167.18 MB/s\t   27697 B/op\t     454 allocs/op",
            "extra": "25264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49000,
            "unit": "ns/op",
            "extra": "25264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 167.18,
            "unit": "MB/s",
            "extra": "25264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27697,
            "unit": "B/op",
            "extra": "25264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6273717,
            "unit": "ns/op\t67523137 B/op\t    2578 allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6273717,
            "unit": "ns/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523137,
            "unit": "B/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26674,
            "unit": "ns/op\t 307.12 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26674,
            "unit": "ns/op",
            "extra": "73082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.12,
            "unit": "MB/s",
            "extra": "73082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73082 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164.8,
            "unit": "ns/op\t1553.05 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7406817 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164.8,
            "unit": "ns/op",
            "extra": "7406817 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1553.05,
            "unit": "MB/s",
            "extra": "7406817 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7406817 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7406817 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 709.1,
            "unit": "ns/op\t 361.01 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3309156 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 709.1,
            "unit": "ns/op",
            "extra": "3309156 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 361.01,
            "unit": "MB/s",
            "extra": "3309156 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3309156 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3309156 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1982492,
            "unit": "ns/op\t 3064041 B/op\t   40018 allocs/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1982492,
            "unit": "ns/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064041,
            "unit": "B/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "601 times\n4 procs"
          }
        ]
      }
    ]
  }
}