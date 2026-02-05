window.BENCHMARK_DATA = {
  "lastUpdate": 1770272995280,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
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
          "id": "a3ba32578931002fbb086a6e65bd3df48fe9e505",
          "message": "Merge pull request #33 from feichai0017/hashkv-exploration\n\nfeat: introduce bucketed value log (hashkv)",
          "timestamp": "2026-02-05T14:28:41+08:00",
          "tree_id": "141ef471634e1cc0641159610c90593d92d4b502",
          "url": "https://github.com/feichai0017/NoKV/commit/a3ba32578931002fbb086a6e65bd3df48fe9e505"
        },
        "date": 1770272993881,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13161,
            "unit": "ns/op\t   2.43 MB/s\t     577 B/op\t      20 allocs/op",
            "extra": "143611 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13161,
            "unit": "ns/op",
            "extra": "143611 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.43,
            "unit": "MB/s",
            "extra": "143611 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 577,
            "unit": "B/op",
            "extra": "143611 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "143611 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15778,
            "unit": "ns/op\t 259.60 MB/s\t     836 B/op\t      31 allocs/op",
            "extra": "93940 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15778,
            "unit": "ns/op",
            "extra": "93940 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 259.6,
            "unit": "MB/s",
            "extra": "93940 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 836,
            "unit": "B/op",
            "extra": "93940 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "93940 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11829,
            "unit": "ns/op\t   5.41 MB/s\t   20254 B/op\t       5 allocs/op",
            "extra": "689256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11829,
            "unit": "ns/op",
            "extra": "689256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.41,
            "unit": "MB/s",
            "extra": "689256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20254,
            "unit": "B/op",
            "extra": "689256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "689256 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10993,
            "unit": "ns/op\t 372.59 MB/s\t   19655 B/op\t       7 allocs/op",
            "extra": "211070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10993,
            "unit": "ns/op",
            "extra": "211070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 372.59,
            "unit": "MB/s",
            "extra": "211070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19655,
            "unit": "B/op",
            "extra": "211070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "211070 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 181981,
            "unit": "ns/op\t  90.03 MB/s\t   61598 B/op\t     680 allocs/op",
            "extra": "9057 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 181981,
            "unit": "ns/op",
            "extra": "9057 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 90.03,
            "unit": "MB/s",
            "extra": "9057 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 61598,
            "unit": "B/op",
            "extra": "9057 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 680,
            "unit": "allocs/op",
            "extra": "9057 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2407858,
            "unit": "ns/op\t    9196 B/op\t       0 allocs/op",
            "extra": "537 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2407858,
            "unit": "ns/op",
            "extra": "537 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9196,
            "unit": "B/op",
            "extra": "537 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "537 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 912.8,
            "unit": "ns/op\t      35 B/op\t       1 allocs/op",
            "extra": "1311432 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 912.8,
            "unit": "ns/op",
            "extra": "1311432 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "1311432 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1311432 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 54881,
            "unit": "ns/op\t 149.27 MB/s\t   26453 B/op\t     454 allocs/op",
            "extra": "21450 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 54881,
            "unit": "ns/op",
            "extra": "21450 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 149.27,
            "unit": "MB/s",
            "extra": "21450 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 26453,
            "unit": "B/op",
            "extra": "21450 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "21450 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8789808,
            "unit": "ns/op\t67523835 B/op\t    2588 allocs/op",
            "extra": "129 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8789808,
            "unit": "ns/op",
            "extra": "129 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523835,
            "unit": "B/op",
            "extra": "129 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2588,
            "unit": "allocs/op",
            "extra": "129 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 695.2,
            "unit": "ns/op\t  92.06 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1772886 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 695.2,
            "unit": "ns/op",
            "extra": "1772886 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 92.06,
            "unit": "MB/s",
            "extra": "1772886 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1772886 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1772886 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 114.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10614100 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 114.2,
            "unit": "ns/op",
            "extra": "10614100 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10614100 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10614100 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1305,
            "unit": "ns/op\t  49.06 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1305,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 49.06,
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
            "value": 474.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2610279 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 474.3,
            "unit": "ns/op",
            "extra": "2610279 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2610279 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2610279 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 23695,
            "unit": "ns/op\t 345.73 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "100856 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 23695,
            "unit": "ns/op",
            "extra": "100856 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 345.73,
            "unit": "MB/s",
            "extra": "100856 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "100856 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "100856 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 154.8,
            "unit": "ns/op\t1653.26 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7649702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 154.8,
            "unit": "ns/op",
            "extra": "7649702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1653.26,
            "unit": "MB/s",
            "extra": "7649702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7649702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7649702 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 606.4,
            "unit": "ns/op\t 422.16 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3870348 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 606.4,
            "unit": "ns/op",
            "extra": "3870348 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 422.16,
            "unit": "MB/s",
            "extra": "3870348 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3870348 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3870348 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2020917,
            "unit": "ns/op\t 3064039 B/op\t   40019 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2020917,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
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
      }
    ]
  }
}