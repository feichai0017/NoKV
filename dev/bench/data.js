window.BENCHMARK_DATA = {
  "lastUpdate": 1772344942696,
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
          "id": "7ffeb3e1e7bfb41d39c6af64fd23f0a960096999",
          "message": "fix(test): check iterator close error in bounds test",
          "timestamp": "2026-03-01T17:01:05+11:00",
          "tree_id": "15fcf9333c3fda4649bd6010bd99634a3001b5f7",
          "url": "https://github.com/feichai0017/NoKV/commit/7ffeb3e1e7bfb41d39c6af64fd23f0a960096999"
        },
        "date": 1772344941665,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7820,
            "unit": "ns/op\t   4.09 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "132067 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7820,
            "unit": "ns/op",
            "extra": "132067 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.09,
            "unit": "MB/s",
            "extra": "132067 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "132067 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "132067 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16837,
            "unit": "ns/op\t 243.27 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16837,
            "unit": "ns/op",
            "extra": "70477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 243.27,
            "unit": "MB/s",
            "extra": "70477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8860,
            "unit": "ns/op\t   7.22 MB/s\t   20459 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8860,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.22,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20459,
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
            "value": 11957,
            "unit": "ns/op\t 342.56 MB/s\t   33931 B/op\t      11 allocs/op",
            "extra": "338128 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11957,
            "unit": "ns/op",
            "extra": "338128 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 342.56,
            "unit": "MB/s",
            "extra": "338128 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33931,
            "unit": "B/op",
            "extra": "338128 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "338128 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125526,
            "unit": "ns/op\t 130.52 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125526,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.52,
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
            "value": 1542433,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1542433,
            "unit": "ns/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 639.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2014858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 639.7,
            "unit": "ns/op",
            "extra": "2014858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2014858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2014858 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.83,
            "unit": "ns/op",
            "extra": "42704144 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.77,
            "unit": "ns/op",
            "extra": "21059841 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.98,
            "unit": "ns/op",
            "extra": "60126024 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.26,
            "unit": "ns/op",
            "extra": "72472502 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20559933,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.06,
            "unit": "ns/op",
            "extra": "15993177 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49838,
            "unit": "ns/op",
            "extra": "23401 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 59719,
            "unit": "ns/op\t 137.18 MB/s\t   27851 B/op\t     454 allocs/op",
            "extra": "24904 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 59719,
            "unit": "ns/op",
            "extra": "24904 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 137.18,
            "unit": "MB/s",
            "extra": "24904 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27851,
            "unit": "B/op",
            "extra": "24904 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24904 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6413312,
            "unit": "ns/op\t67523253 B/op\t    2579 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6413312,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523253,
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
            "value": 26488,
            "unit": "ns/op\t 309.27 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74414 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26488,
            "unit": "ns/op",
            "extra": "74414 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 309.27,
            "unit": "MB/s",
            "extra": "74414 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74414 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74414 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156,
            "unit": "ns/op\t1640.54 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7705946 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156,
            "unit": "ns/op",
            "extra": "7705946 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1640.54,
            "unit": "MB/s",
            "extra": "7705946 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7705946 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7705946 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 682,
            "unit": "ns/op\t 375.37 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3450457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 682,
            "unit": "ns/op",
            "extra": "3450457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 375.37,
            "unit": "MB/s",
            "extra": "3450457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3450457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3450457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1999630,
            "unit": "ns/op\t 3064034 B/op\t   40018 allocs/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1999630,
            "unit": "ns/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064034,
            "unit": "B/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "586 times\n4 procs"
          }
        ]
      }
    ]
  }
}