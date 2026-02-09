window.BENCHMARK_DATA = {
  "lastUpdate": 1770638376788,
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
          "id": "ba6caec832470a136178b805b9956860205940a8",
          "message": "docs: enrich note narratives and align note title naming",
          "timestamp": "2026-02-09T19:56:27+08:00",
          "tree_id": "f441ae4eda15fa9ba7100a9c924ed02628211e04",
          "url": "https://github.com/feichai0017/NoKV/commit/ba6caec832470a136178b805b9956860205940a8"
        },
        "date": 1770638327967,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7430,
            "unit": "ns/op\t   4.31 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "142183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7430,
            "unit": "ns/op",
            "extra": "142183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.31,
            "unit": "MB/s",
            "extra": "142183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "142183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "142183 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16378,
            "unit": "ns/op\t 250.10 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "73858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16378,
            "unit": "ns/op",
            "extra": "73858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 250.1,
            "unit": "MB/s",
            "extra": "73858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "73858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "73858 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9371,
            "unit": "ns/op\t   6.83 MB/s\t   16678 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9371,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.83,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16678,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11737,
            "unit": "ns/op\t 348.99 MB/s\t   24345 B/op\t       8 allocs/op",
            "extra": "359366 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11737,
            "unit": "ns/op",
            "extra": "359366 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 348.99,
            "unit": "MB/s",
            "extra": "359366 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 24345,
            "unit": "B/op",
            "extra": "359366 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "359366 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 130495,
            "unit": "ns/op\t 125.55 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 130495,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 125.55,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56857,
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
            "value": 1490295,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1490295,
            "unit": "ns/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
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
            "value": 600.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2043888 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 600.5,
            "unit": "ns/op",
            "extra": "2043888 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2043888 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2043888 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49882,
            "unit": "ns/op\t 164.23 MB/s\t   28137 B/op\t     454 allocs/op",
            "extra": "24264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49882,
            "unit": "ns/op",
            "extra": "24264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.23,
            "unit": "MB/s",
            "extra": "24264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28137,
            "unit": "B/op",
            "extra": "24264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24264 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7467166,
            "unit": "ns/op\t67523305 B/op\t    2586 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7467166,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523305,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 606.3,
            "unit": "ns/op\t 105.56 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2028088 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 606.3,
            "unit": "ns/op",
            "extra": "2028088 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 105.56,
            "unit": "MB/s",
            "extra": "2028088 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2028088 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2028088 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9244960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.7,
            "unit": "ns/op",
            "extra": "9244960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9244960 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9244960 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1414,
            "unit": "ns/op\t  45.25 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1414,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.25,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet",
            "value": 510.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2542263 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 510.3,
            "unit": "ns/op",
            "extra": "2542263 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2542263 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2542263 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26591,
            "unit": "ns/op\t 308.08 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74205 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26591,
            "unit": "ns/op",
            "extra": "74205 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.08,
            "unit": "MB/s",
            "extra": "74205 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74205 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74205 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 153.2,
            "unit": "ns/op\t1671.51 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8006666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 153.2,
            "unit": "ns/op",
            "extra": "8006666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1671.51,
            "unit": "MB/s",
            "extra": "8006666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8006666 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8006666 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 646.8,
            "unit": "ns/op\t 395.79 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "2908038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 646.8,
            "unit": "ns/op",
            "extra": "2908038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 395.79,
            "unit": "MB/s",
            "extra": "2908038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "2908038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "2908038 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2029839,
            "unit": "ns/op\t 3064055 B/op\t   40019 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2029839,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064055,
            "unit": "B/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "592 times\n4 procs"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "Guocheng Song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "917438d29395b1b18e3ff1ea5ba5833162f4daf6",
          "message": "Merge pull request #37 from feichai0017/dependabot/go_modules/github.com/dgraph-io/badger/v4-4.9.1\n\ndeps(deps): bump github.com/dgraph-io/badger/v4 from 4.9.0 to 4.9.1",
          "timestamp": "2026-02-09T19:58:05+08:00",
          "tree_id": "93364aa018d5fade96d202b38363d4416087e768",
          "url": "https://github.com/feichai0017/NoKV/commit/917438d29395b1b18e3ff1ea5ba5833162f4daf6"
        },
        "date": 1770638375535,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9118,
            "unit": "ns/op\t   3.51 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "161014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9118,
            "unit": "ns/op",
            "extra": "161014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.51,
            "unit": "MB/s",
            "extra": "161014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "161014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "161014 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16558,
            "unit": "ns/op\t 247.37 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "84910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16558,
            "unit": "ns/op",
            "extra": "84910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 247.37,
            "unit": "MB/s",
            "extra": "84910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "84910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "84910 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9575,
            "unit": "ns/op\t   6.68 MB/s\t   17203 B/op\t       5 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9575,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 6.68,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17203,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11995,
            "unit": "ns/op\t 341.49 MB/s\t   25444 B/op\t       8 allocs/op",
            "extra": "348309 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11995,
            "unit": "ns/op",
            "extra": "348309 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 341.49,
            "unit": "MB/s",
            "extra": "348309 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 25444,
            "unit": "B/op",
            "extra": "348309 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "348309 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 130705,
            "unit": "ns/op\t 125.35 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 130705,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 125.35,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56857,
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
            "value": 1538457,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1538457,
            "unit": "ns/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 612.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1995612 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 612.5,
            "unit": "ns/op",
            "extra": "1995612 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1995612 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1995612 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49087,
            "unit": "ns/op\t 166.89 MB/s\t   25574 B/op\t     454 allocs/op",
            "extra": "23654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49087,
            "unit": "ns/op",
            "extra": "23654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.89,
            "unit": "MB/s",
            "extra": "23654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25574,
            "unit": "B/op",
            "extra": "23654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23654 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6453485,
            "unit": "ns/op\t67523300 B/op\t    2586 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6453485,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523300,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 616.1,
            "unit": "ns/op\t 103.88 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1957396 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 616.1,
            "unit": "ns/op",
            "extra": "1957396 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.88,
            "unit": "MB/s",
            "extra": "1957396 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1957396 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1957396 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 126.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9499803 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 126.5,
            "unit": "ns/op",
            "extra": "9499803 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9499803 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9499803 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1353,
            "unit": "ns/op\t  47.31 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1353,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47.31,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet",
            "value": 474.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2537253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 474.5,
            "unit": "ns/op",
            "extra": "2537253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2537253 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2537253 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25296,
            "unit": "ns/op\t 323.85 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25296,
            "unit": "ns/op",
            "extra": "76599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 323.85,
            "unit": "MB/s",
            "extra": "76599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 157.4,
            "unit": "ns/op\t1626.72 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7050698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 157.4,
            "unit": "ns/op",
            "extra": "7050698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1626.72,
            "unit": "MB/s",
            "extra": "7050698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7050698 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7050698 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 724.9,
            "unit": "ns/op\t 353.15 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3153734 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 724.9,
            "unit": "ns/op",
            "extra": "3153734 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 353.15,
            "unit": "MB/s",
            "extra": "3153734 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3153734 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3153734 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2033036,
            "unit": "ns/op\t 3064036 B/op\t   40019 allocs/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2033036,
            "unit": "ns/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064036,
            "unit": "B/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "584 times\n4 procs"
          }
        ]
      }
    ]
  }
}