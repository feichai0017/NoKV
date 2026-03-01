window.BENCHMARK_DATA = {
  "lastUpdate": 1772350217255,
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
          "id": "a553a55faa06f941d6182082856324819c44e7ed",
          "message": "docs: align architecture docs with PD-first control plane",
          "timestamp": "2026-03-01T18:27:40+11:00",
          "tree_id": "b4e54b93080faafdf009c57d811ec58fb97d25dc",
          "url": "https://github.com/feichai0017/NoKV/commit/a553a55faa06f941d6182082856324819c44e7ed"
        },
        "date": 1772350216301,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7655,
            "unit": "ns/op\t   4.18 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "164911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7655,
            "unit": "ns/op",
            "extra": "164911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.18,
            "unit": "MB/s",
            "extra": "164911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "164911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "164911 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18422,
            "unit": "ns/op\t 222.34 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70488 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18422,
            "unit": "ns/op",
            "extra": "70488 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 222.34,
            "unit": "MB/s",
            "extra": "70488 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70488 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70488 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8667,
            "unit": "ns/op\t   7.38 MB/s\t   20178 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8667,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.38,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20178,
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
            "value": 11863,
            "unit": "ns/op\t 345.28 MB/s\t   33508 B/op\t      11 allocs/op",
            "extra": "352438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11863,
            "unit": "ns/op",
            "extra": "352438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 345.28,
            "unit": "MB/s",
            "extra": "352438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33508,
            "unit": "B/op",
            "extra": "352438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "352438 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120998,
            "unit": "ns/op\t 135.41 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120998,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.41,
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
            "value": 1539932,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "777 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1539932,
            "unit": "ns/op",
            "extra": "777 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "777 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "777 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 599.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1846993 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 599.7,
            "unit": "ns/op",
            "extra": "1846993 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1846993 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1846993 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.72,
            "unit": "ns/op",
            "extra": "50564923 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.41,
            "unit": "ns/op",
            "extra": "20693293 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 21.08,
            "unit": "ns/op",
            "extra": "56595740 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.87,
            "unit": "ns/op",
            "extra": "72584768 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21112364,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.61,
            "unit": "ns/op",
            "extra": "15729618 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50452,
            "unit": "ns/op",
            "extra": "23248 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50160,
            "unit": "ns/op\t 163.32 MB/s\t   27787 B/op\t     454 allocs/op",
            "extra": "25054 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50160,
            "unit": "ns/op",
            "extra": "25054 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.32,
            "unit": "MB/s",
            "extra": "25054 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27787,
            "unit": "B/op",
            "extra": "25054 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25054 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6635442,
            "unit": "ns/op\t67523316 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6635442,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523316,
            "unit": "B/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26732,
            "unit": "ns/op\t 306.45 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26732,
            "unit": "ns/op",
            "extra": "74643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 306.45,
            "unit": "MB/s",
            "extra": "74643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74643 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 170.2,
            "unit": "ns/op\t1503.86 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7672828 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 170.2,
            "unit": "ns/op",
            "extra": "7672828 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1503.86,
            "unit": "MB/s",
            "extra": "7672828 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7672828 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7672828 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.3,
            "unit": "ns/op\t 368.21 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3469384 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.3,
            "unit": "ns/op",
            "extra": "3469384 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.21,
            "unit": "MB/s",
            "extra": "3469384 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3469384 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3469384 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1997521,
            "unit": "ns/op\t 3064033 B/op\t   40018 allocs/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1997521,
            "unit": "ns/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064033,
            "unit": "B/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "598 times\n4 procs"
          }
        ]
      }
    ]
  }
}