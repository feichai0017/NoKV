window.BENCHMARK_DATA = {
  "lastUpdate": 1772441468676,
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
          "id": "23cd7658fe012276f7996b2f1cd3901390ff7f88",
          "message": "refactor: use typed atomics in write queue and lsm ids",
          "timestamp": "2026-03-02T19:47:03+11:00",
          "tree_id": "09bc9967927ec287566b81823bfa4a58bb9a8ac5",
          "url": "https://github.com/feichai0017/NoKV/commit/23cd7658fe012276f7996b2f1cd3901390ff7f88"
        },
        "date": 1772441467607,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6803,
            "unit": "ns/op\t   4.70 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6803,
            "unit": "ns/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.7,
            "unit": "MB/s",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16543,
            "unit": "ns/op\t 247.60 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "78007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16543,
            "unit": "ns/op",
            "extra": "78007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 247.6,
            "unit": "MB/s",
            "extra": "78007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "78007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "78007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7801,
            "unit": "ns/op\t   8.20 MB/s\t   17966 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7801,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.2,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17966,
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
            "value": 11630,
            "unit": "ns/op\t 352.18 MB/s\t   33667 B/op\t      11 allocs/op",
            "extra": "362251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11630,
            "unit": "ns/op",
            "extra": "362251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 352.18,
            "unit": "MB/s",
            "extra": "362251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33667,
            "unit": "B/op",
            "extra": "362251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "362251 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120661,
            "unit": "ns/op\t 135.79 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120661,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.79,
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
            "value": 1529321,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "774 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1529321,
            "unit": "ns/op",
            "extra": "774 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "774 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "774 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 613.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2061172 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 613.5,
            "unit": "ns/op",
            "extra": "2061172 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2061172 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2061172 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.6,
            "unit": "ns/op",
            "extra": "50739468 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 56.73,
            "unit": "ns/op",
            "extra": "20913384 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.16,
            "unit": "ns/op",
            "extra": "59956845 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.82,
            "unit": "ns/op",
            "extra": "71225460 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20525716,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.36,
            "unit": "ns/op",
            "extra": "15890349 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50053,
            "unit": "ns/op",
            "extra": "23518 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49547,
            "unit": "ns/op\t 165.34 MB/s\t   28180 B/op\t     454 allocs/op",
            "extra": "24171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49547,
            "unit": "ns/op",
            "extra": "24171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.34,
            "unit": "MB/s",
            "extra": "24171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28180,
            "unit": "B/op",
            "extra": "24171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6344390,
            "unit": "ns/op\t67523239 B/op\t    2579 allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6344390,
            "unit": "ns/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523239,
            "unit": "B/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26890,
            "unit": "ns/op\t 304.65 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73705 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26890,
            "unit": "ns/op",
            "extra": "73705 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 304.65,
            "unit": "MB/s",
            "extra": "73705 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73705 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73705 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.1,
            "unit": "ns/op\t1650.29 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7680924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.1,
            "unit": "ns/op",
            "extra": "7680924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1650.29,
            "unit": "MB/s",
            "extra": "7680924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7680924 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7680924 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 698.4,
            "unit": "ns/op\t 366.56 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3400404 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 698.4,
            "unit": "ns/op",
            "extra": "3400404 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 366.56,
            "unit": "MB/s",
            "extra": "3400404 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3400404 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3400404 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1984346,
            "unit": "ns/op\t 3064035 B/op\t   40018 allocs/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1984346,
            "unit": "ns/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064035,
            "unit": "B/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "600 times\n4 procs"
          }
        ]
      }
    ]
  }
}