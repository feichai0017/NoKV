window.BENCHMARK_DATA = {
  "lastUpdate": 1772335034632,
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
          "id": "e25b6f9cce02fa77991638864a090d97764931cb",
          "message": "fix: remove hardcoded .0 suffix from GOTOOLCHAIN",
          "timestamp": "2026-03-01T14:15:52+11:00",
          "tree_id": "81132987bdb1a08876d7078f75b38f1ce131432d",
          "url": "https://github.com/feichai0017/NoKV/commit/e25b6f9cce02fa77991638864a090d97764931cb"
        },
        "date": 1772335033773,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8460,
            "unit": "ns/op\t   3.78 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "151605 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8460,
            "unit": "ns/op",
            "extra": "151605 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.78,
            "unit": "MB/s",
            "extra": "151605 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "151605 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "151605 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19145,
            "unit": "ns/op\t 213.94 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "61983 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19145,
            "unit": "ns/op",
            "extra": "61983 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 213.94,
            "unit": "MB/s",
            "extra": "61983 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "61983 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "61983 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10619,
            "unit": "ns/op\t   6.03 MB/s\t   24863 B/op\t       8 allocs/op",
            "extra": "690844 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10619,
            "unit": "ns/op",
            "extra": "690844 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.03,
            "unit": "MB/s",
            "extra": "690844 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 24863,
            "unit": "B/op",
            "extra": "690844 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "690844 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11771,
            "unit": "ns/op\t 347.96 MB/s\t   32746 B/op\t      11 allocs/op",
            "extra": "355720 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11771,
            "unit": "ns/op",
            "extra": "355720 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 347.96,
            "unit": "MB/s",
            "extra": "355720 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32746,
            "unit": "B/op",
            "extra": "355720 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "355720 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125903,
            "unit": "ns/op\t 130.13 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125903,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.13,
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
            "value": 1507743,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1507743,
            "unit": "ns/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 575.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1755375 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 575.2,
            "unit": "ns/op",
            "extra": "1755375 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1755375 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1755375 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 28.04,
            "unit": "ns/op",
            "extra": "41926626 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.64,
            "unit": "ns/op",
            "extra": "20687217 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.99,
            "unit": "ns/op",
            "extra": "60035018 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.87,
            "unit": "ns/op",
            "extra": "72406528 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21631402,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.07,
            "unit": "ns/op",
            "extra": "15656068 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49915,
            "unit": "ns/op",
            "extra": "23865 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48627,
            "unit": "ns/op\t 168.47 MB/s\t   25552 B/op\t     454 allocs/op",
            "extra": "23715 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48627,
            "unit": "ns/op",
            "extra": "23715 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.47,
            "unit": "MB/s",
            "extra": "23715 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25552,
            "unit": "B/op",
            "extra": "23715 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23715 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6445058,
            "unit": "ns/op\t67523140 B/op\t    2579 allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6445058,
            "unit": "ns/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523140,
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
            "name": "BenchmarkVLogAppendEntries",
            "value": 26960,
            "unit": "ns/op\t 303.86 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73836 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26960,
            "unit": "ns/op",
            "extra": "73836 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.86,
            "unit": "MB/s",
            "extra": "73836 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73836 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73836 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.9,
            "unit": "ns/op\t1611.56 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7476504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.9,
            "unit": "ns/op",
            "extra": "7476504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1611.56,
            "unit": "MB/s",
            "extra": "7476504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7476504 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7476504 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 689.1,
            "unit": "ns/op\t 371.48 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3437194 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 689.1,
            "unit": "ns/op",
            "extra": "3437194 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 371.48,
            "unit": "MB/s",
            "extra": "3437194 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3437194 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3437194 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1994600,
            "unit": "ns/op\t 3064026 B/op\t   40017 allocs/op",
            "extra": "607 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1994600,
            "unit": "ns/op",
            "extra": "607 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064026,
            "unit": "B/op",
            "extra": "607 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "607 times\n4 procs"
          }
        ]
      }
    ]
  }
}