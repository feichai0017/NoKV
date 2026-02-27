window.BENCHMARK_DATA = {
  "lastUpdate": 1772154565709,
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
          "id": "5c734e60852ce8d546c85b0d62019f0914e733f0",
          "message": "refactor: vendor hotring as in-repo package\n\n- add local hotring package (implementation + tests)\n\n- switch NoKV imports from external hotring module to local package\n\n- remove external hotring dependency from root/benchmark modules\n\n- update docs to reference in-repo hotring package",
          "timestamp": "2026-02-27T09:07:21+08:00",
          "tree_id": "6eae612c5afbf80e79172b58df7bc3b59fe46827",
          "url": "https://github.com/feichai0017/NoKV/commit/5c734e60852ce8d546c85b0d62019f0914e733f0"
        },
        "date": 1772154564490,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7641,
            "unit": "ns/op\t   4.19 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "146454 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7641,
            "unit": "ns/op",
            "extra": "146454 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.19,
            "unit": "MB/s",
            "extra": "146454 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "146454 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "146454 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18384,
            "unit": "ns/op\t 222.80 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18384,
            "unit": "ns/op",
            "extra": "70784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.8,
            "unit": "MB/s",
            "extra": "70784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7518,
            "unit": "ns/op\t   8.51 MB/s\t   17201 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7518,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.51,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17201,
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
            "value": 11756,
            "unit": "ns/op\t 348.41 MB/s\t   33000 B/op\t      11 allocs/op",
            "extra": "356541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11756,
            "unit": "ns/op",
            "extra": "356541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 348.41,
            "unit": "MB/s",
            "extra": "356541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33000,
            "unit": "B/op",
            "extra": "356541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "356541 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126112,
            "unit": "ns/op\t 129.92 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126112,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.92,
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
            "value": 1509699,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "810 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1509699,
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
            "value": 580.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2042163 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 580.8,
            "unit": "ns/op",
            "extra": "2042163 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2042163 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2042163 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.59,
            "unit": "ns/op",
            "extra": "50404563 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 65.91,
            "unit": "ns/op",
            "extra": "20560095 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.93,
            "unit": "ns/op",
            "extra": "59720103 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.61,
            "unit": "ns/op",
            "extra": "68390814 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20285865,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.92,
            "unit": "ns/op",
            "extra": "15958000 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49616,
            "unit": "ns/op",
            "extra": "24147 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48512,
            "unit": "ns/op\t 168.86 MB/s\t   27828 B/op\t     454 allocs/op",
            "extra": "24958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48512,
            "unit": "ns/op",
            "extra": "24958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.86,
            "unit": "MB/s",
            "extra": "24958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27828,
            "unit": "B/op",
            "extra": "24958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24958 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6233487,
            "unit": "ns/op\t67523036 B/op\t    2578 allocs/op",
            "extra": "198 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6233487,
            "unit": "ns/op",
            "extra": "198 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523036,
            "unit": "B/op",
            "extra": "198 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "198 times\n4 procs"
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
            "value": 26548,
            "unit": "ns/op\t 308.57 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "75594 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26548,
            "unit": "ns/op",
            "extra": "75594 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.57,
            "unit": "MB/s",
            "extra": "75594 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "75594 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75594 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.3,
            "unit": "ns/op\t1627.05 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7552749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.3,
            "unit": "ns/op",
            "extra": "7552749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1627.05,
            "unit": "MB/s",
            "extra": "7552749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7552749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7552749 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.7,
            "unit": "ns/op\t 367.97 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3374984 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.7,
            "unit": "ns/op",
            "extra": "3374984 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 367.97,
            "unit": "MB/s",
            "extra": "3374984 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3374984 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3374984 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1974523,
            "unit": "ns/op\t 3064046 B/op\t   40018 allocs/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1974523,
            "unit": "ns/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064046,
            "unit": "B/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "603 times\n4 procs"
          }
        ]
      }
    ]
  }
}