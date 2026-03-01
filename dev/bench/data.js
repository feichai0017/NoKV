window.BENCHMARK_DATA = {
  "lastUpdate": 1772374246313,
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
          "id": "9211ad50de402e743460de5d2b3f78848aed2e89",
          "message": "refactor: drop region sink mirror and remove scheduler CLI",
          "timestamp": "2026-03-02T01:06:07+11:00",
          "tree_id": "8edb3a336d2c8239572a4256e00e99a29665261f",
          "url": "https://github.com/feichai0017/NoKV/commit/9211ad50de402e743460de5d2b3f78848aed2e89"
        },
        "date": 1772374245113,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8089,
            "unit": "ns/op\t   3.96 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "146467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8089,
            "unit": "ns/op",
            "extra": "146467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.96,
            "unit": "MB/s",
            "extra": "146467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "146467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "146467 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19161,
            "unit": "ns/op\t 213.77 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "67443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19161,
            "unit": "ns/op",
            "extra": "67443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 213.77,
            "unit": "MB/s",
            "extra": "67443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "67443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "67443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8103,
            "unit": "ns/op\t   7.90 MB/s\t   18336 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8103,
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
            "value": 18336,
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
            "value": 8458,
            "unit": "ns/op\t 484.28 MB/s\t   24816 B/op\t      10 allocs/op",
            "extra": "300856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8458,
            "unit": "ns/op",
            "extra": "300856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 484.28,
            "unit": "MB/s",
            "extra": "300856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 24816,
            "unit": "B/op",
            "extra": "300856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 10,
            "unit": "allocs/op",
            "extra": "300856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127410,
            "unit": "ns/op\t 128.59 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127410,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.59,
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
            "value": 1567291,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1567291,
            "unit": "ns/op",
            "extra": "763 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
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
            "value": 598.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1970635 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 598.2,
            "unit": "ns/op",
            "extra": "1970635 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1970635 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1970635 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.68,
            "unit": "ns/op",
            "extra": "50242366 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 60.89,
            "unit": "ns/op",
            "extra": "20426805 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.97,
            "unit": "ns/op",
            "extra": "59838056 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.84,
            "unit": "ns/op",
            "extra": "72543652 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 23130116,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.17,
            "unit": "ns/op",
            "extra": "15901964 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49295,
            "unit": "ns/op",
            "extra": "23366 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49699,
            "unit": "ns/op\t 164.83 MB/s\t   27801 B/op\t     454 allocs/op",
            "extra": "25020 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49699,
            "unit": "ns/op",
            "extra": "25020 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.83,
            "unit": "MB/s",
            "extra": "25020 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27801,
            "unit": "B/op",
            "extra": "25020 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25020 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6467720,
            "unit": "ns/op\t67523331 B/op\t    2579 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6467720,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523331,
            "unit": "B/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26551,
            "unit": "ns/op\t 308.54 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26551,
            "unit": "ns/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.54,
            "unit": "MB/s",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74342 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.4,
            "unit": "ns/op\t1626.55 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7593812 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.4,
            "unit": "ns/op",
            "extra": "7593812 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1626.55,
            "unit": "MB/s",
            "extra": "7593812 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7593812 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7593812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 691.8,
            "unit": "ns/op\t 370.03 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3425418 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 691.8,
            "unit": "ns/op",
            "extra": "3425418 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.03,
            "unit": "MB/s",
            "extra": "3425418 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3425418 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3425418 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2024262,
            "unit": "ns/op\t 3064016 B/op\t   40017 allocs/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2024262,
            "unit": "ns/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064016,
            "unit": "B/op",
            "extra": "590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "590 times\n4 procs"
          }
        ]
      }
    ]
  }
}