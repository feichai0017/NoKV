window.BENCHMARK_DATA = {
  "lastUpdate": 1772467912870,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "committer": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "distinct": true,
          "id": "1447824b62f2ac9fb8451ae2f2edc4d783966283",
          "message": "refactor: streamline arena helpers and pool builder buffers",
          "timestamp": "2026-03-03T03:10:34+11:00",
          "tree_id": "1322a90a58e4dc0fe7bd266fe62331b023bd12e0",
          "url": "https://github.com/feichai0017/NoKV/commit/1447824b62f2ac9fb8451ae2f2edc4d783966283"
        },
        "date": 1772467911555,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8377,
            "unit": "ns/op\t   3.82 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "166467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8377,
            "unit": "ns/op",
            "extra": "166467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.82,
            "unit": "MB/s",
            "extra": "166467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "166467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "166467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15293,
            "unit": "ns/op\t 267.83 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "90307 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15293,
            "unit": "ns/op",
            "extra": "90307 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 267.83,
            "unit": "MB/s",
            "extra": "90307 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "90307 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "90307 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7108,
            "unit": "ns/op\t   9.00 MB/s\t   16857 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7108,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 9,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16857,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 8959,
            "unit": "ns/op\t 457.18 MB/s\t   26748 B/op\t      11 allocs/op",
            "extra": "283887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8959,
            "unit": "ns/op",
            "extra": "283887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 457.18,
            "unit": "MB/s",
            "extra": "283887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26748,
            "unit": "B/op",
            "extra": "283887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "283887 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126333,
            "unit": "ns/op\t 129.69 MB/s\t   50500 B/op\t     661 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126333,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.69,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 50500,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 661,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 1655326,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "734 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1655326,
            "unit": "ns/op",
            "extra": "734 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "734 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "734 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 560.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2113450 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 560.5,
            "unit": "ns/op",
            "extra": "2113450 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2113450 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2113450 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 31.06,
            "unit": "ns/op",
            "extra": "37676266 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 83.76,
            "unit": "ns/op",
            "extra": "14048114 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 24.02,
            "unit": "ns/op",
            "extra": "50406517 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 14.2,
            "unit": "ns/op",
            "extra": "84758062 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 22248832,
            "unit": "ns/op",
            "extra": "52 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 82.85,
            "unit": "ns/op",
            "extra": "14423331 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 68769,
            "unit": "ns/op",
            "extra": "16998 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 47570,
            "unit": "ns/op\t 172.21 MB/s\t   27728 B/op\t     454 allocs/op",
            "extra": "25189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47570,
            "unit": "ns/op",
            "extra": "25189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 172.21,
            "unit": "MB/s",
            "extra": "25189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27728,
            "unit": "B/op",
            "extra": "25189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8713505,
            "unit": "ns/op\t67443849 B/op\t    2583 allocs/op",
            "extra": "134 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8713505,
            "unit": "ns/op",
            "extra": "134 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67443849,
            "unit": "B/op",
            "extra": "134 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2583,
            "unit": "allocs/op",
            "extra": "134 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 24109,
            "unit": "ns/op\t 339.80 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "100393 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 24109,
            "unit": "ns/op",
            "extra": "100393 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 339.8,
            "unit": "MB/s",
            "extra": "100393 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "100393 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "100393 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 162.7,
            "unit": "ns/op\t1573.38 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7266532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 162.7,
            "unit": "ns/op",
            "extra": "7266532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1573.38,
            "unit": "MB/s",
            "extra": "7266532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7266532 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7266532 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 636.1,
            "unit": "ns/op\t 402.43 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "4160997 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 636.1,
            "unit": "ns/op",
            "extra": "4160997 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 402.43,
            "unit": "MB/s",
            "extra": "4160997 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "4160997 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4160997 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2079952,
            "unit": "ns/op\t 3064040 B/op\t   40018 allocs/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2079952,
            "unit": "ns/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "573 times\n4 procs"
          }
        ]
      }
    ]
  }
}