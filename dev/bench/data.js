window.BENCHMARK_DATA = {
  "lastUpdate": 1772336771127,
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
          "id": "8cda57fe06182bca1b19a05ac8299c1490317e52",
          "message": "bump version to 1.26.0",
          "timestamp": "2026-03-01T14:44:24+11:00",
          "tree_id": "74bf9e09ada80199a4e8bc6bf65881a22d872ae5",
          "url": "https://github.com/feichai0017/NoKV/commit/8cda57fe06182bca1b19a05ac8299c1490317e52"
        },
        "date": 1772336769579,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7983,
            "unit": "ns/op\t   4.01 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "160256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7983,
            "unit": "ns/op",
            "extra": "160256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.01,
            "unit": "MB/s",
            "extra": "160256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "160256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "160256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18449,
            "unit": "ns/op\t 222.02 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "64539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18449,
            "unit": "ns/op",
            "extra": "64539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.02,
            "unit": "MB/s",
            "extra": "64539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "64539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "64539 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8660,
            "unit": "ns/op\t   7.39 MB/s\t   19939 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8660,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.39,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19939,
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
            "value": 11459,
            "unit": "ns/op\t 357.43 MB/s\t   32669 B/op\t      11 allocs/op",
            "extra": "359438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11459,
            "unit": "ns/op",
            "extra": "359438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 357.43,
            "unit": "MB/s",
            "extra": "359438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32669,
            "unit": "B/op",
            "extra": "359438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "359438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123989,
            "unit": "ns/op\t 132.14 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123989,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.14,
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
            "value": 1500658,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "793 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1500658,
            "unit": "ns/op",
            "extra": "793 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "793 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "793 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 583.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1891142 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 583.7,
            "unit": "ns/op",
            "extra": "1891142 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1891142 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1891142 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.81,
            "unit": "ns/op",
            "extra": "49556203 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.8,
            "unit": "ns/op",
            "extra": "20556496 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.07,
            "unit": "ns/op",
            "extra": "59793067 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.08,
            "unit": "ns/op",
            "extra": "72327812 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21801586,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.37,
            "unit": "ns/op",
            "extra": "15949080 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51547,
            "unit": "ns/op",
            "extra": "22970 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49891,
            "unit": "ns/op\t 164.20 MB/s\t   28016 B/op\t     454 allocs/op",
            "extra": "24531 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49891,
            "unit": "ns/op",
            "extra": "24531 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.2,
            "unit": "MB/s",
            "extra": "24531 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28016,
            "unit": "B/op",
            "extra": "24531 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24531 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6302438,
            "unit": "ns/op\t67523192 B/op\t    2579 allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6302438,
            "unit": "ns/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523192,
            "unit": "B/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27004,
            "unit": "ns/op\t 303.36 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73351 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27004,
            "unit": "ns/op",
            "extra": "73351 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.36,
            "unit": "MB/s",
            "extra": "73351 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73351 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73351 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157,
            "unit": "ns/op\t1630.53 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7575147 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157,
            "unit": "ns/op",
            "extra": "7575147 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1630.53,
            "unit": "MB/s",
            "extra": "7575147 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7575147 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7575147 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 702.8,
            "unit": "ns/op\t 364.24 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3381616 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 702.8,
            "unit": "ns/op",
            "extra": "3381616 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 364.24,
            "unit": "MB/s",
            "extra": "3381616 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3381616 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3381616 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1988136,
            "unit": "ns/op\t 3064030 B/op\t   40018 allocs/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1988136,
            "unit": "ns/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064030,
            "unit": "B/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "594 times\n4 procs"
          }
        ]
      }
    ]
  }
}