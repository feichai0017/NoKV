window.BENCHMARK_DATA = {
  "lastUpdate": 1771062901500,
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
          "id": "4f692cd3c0bf8eae48c116c70a6638cf161b26c8",
          "message": "ci: add issue auto-labeler workflow and rules",
          "timestamp": "2026-02-14T17:53:06+08:00",
          "tree_id": "c7354b39164584a52d7fa1e72877f066bec5d37e",
          "url": "https://github.com/feichai0017/NoKV/commit/4f692cd3c0bf8eae48c116c70a6638cf161b26c8"
        },
        "date": 1771062900378,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8054,
            "unit": "ns/op\t   3.97 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "158152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8054,
            "unit": "ns/op",
            "extra": "158152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.97,
            "unit": "MB/s",
            "extra": "158152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "158152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "158152 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17222,
            "unit": "ns/op\t 237.84 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "71421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17222,
            "unit": "ns/op",
            "extra": "71421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 237.84,
            "unit": "MB/s",
            "extra": "71421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "71421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "71421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7595,
            "unit": "ns/op\t   8.43 MB/s\t   17043 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7595,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.43,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17043,
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
            "value": 11760,
            "unit": "ns/op\t 348.31 MB/s\t   33633 B/op\t      11 allocs/op",
            "extra": "354434 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11760,
            "unit": "ns/op",
            "extra": "354434 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 348.31,
            "unit": "MB/s",
            "extra": "354434 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33633,
            "unit": "B/op",
            "extra": "354434 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "354434 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121425,
            "unit": "ns/op\t 134.93 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121425,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.93,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56846,
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
            "value": 1496104,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1496104,
            "unit": "ns/op",
            "extra": "775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "775 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 584.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2097538 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 584.1,
            "unit": "ns/op",
            "extra": "2097538 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2097538 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2097538 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50017,
            "unit": "ns/op\t 163.78 MB/s\t   27521 B/op\t     454 allocs/op",
            "extra": "25687 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50017,
            "unit": "ns/op",
            "extra": "25687 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.78,
            "unit": "MB/s",
            "extra": "25687 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27521,
            "unit": "B/op",
            "extra": "25687 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25687 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6466375,
            "unit": "ns/op\t67523122 B/op\t    2579 allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6466375,
            "unit": "ns/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523122,
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
            "name": "BenchmarkARTInsert",
            "value": 598.3,
            "unit": "ns/op\t 106.98 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2077258 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 598.3,
            "unit": "ns/op",
            "extra": "2077258 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 106.98,
            "unit": "MB/s",
            "extra": "2077258 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2077258 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2077258 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9421963 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.3,
            "unit": "ns/op",
            "extra": "9421963 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9421963 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9421963 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1395,
            "unit": "ns/op\t  45.87 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1395,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.87,
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
            "value": 456.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2624341 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 456.7,
            "unit": "ns/op",
            "extra": "2624341 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2624341 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2624341 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26534,
            "unit": "ns/op\t 308.73 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74055 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26534,
            "unit": "ns/op",
            "extra": "74055 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.73,
            "unit": "MB/s",
            "extra": "74055 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74055 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74055 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 154.4,
            "unit": "ns/op\t1657.65 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7592293 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 154.4,
            "unit": "ns/op",
            "extra": "7592293 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1657.65,
            "unit": "MB/s",
            "extra": "7592293 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7592293 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7592293 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 681.2,
            "unit": "ns/op\t 375.80 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3488866 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 681.2,
            "unit": "ns/op",
            "extra": "3488866 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 375.8,
            "unit": "MB/s",
            "extra": "3488866 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3488866 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3488866 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2023700,
            "unit": "ns/op\t 3064044 B/op\t   40018 allocs/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2023700,
            "unit": "ns/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064044,
            "unit": "B/op",
            "extra": "582 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "582 times\n4 procs"
          }
        ]
      }
    ]
  }
}