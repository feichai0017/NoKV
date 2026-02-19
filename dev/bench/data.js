window.BENCHMARK_DATA = {
  "lastUpdate": 1771493322269,
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
          "id": "2aebb7c84bffc3373606504309c085d74f05c7b9",
          "message": "ci: simplify issue labels to status-only triage",
          "timestamp": "2026-02-19T17:25:04+08:00",
          "tree_id": "7851e8120945c8743c17f921397d241f299ba85c",
          "url": "https://github.com/feichai0017/NoKV/commit/2aebb7c84bffc3373606504309c085d74f05c7b9"
        },
        "date": 1771493321077,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7702,
            "unit": "ns/op\t   4.15 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "144087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7702,
            "unit": "ns/op",
            "extra": "144087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.15,
            "unit": "MB/s",
            "extra": "144087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "144087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "144087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19328,
            "unit": "ns/op\t 211.92 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "61627 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19328,
            "unit": "ns/op",
            "extra": "61627 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 211.92,
            "unit": "MB/s",
            "extra": "61627 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "61627 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "61627 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8216,
            "unit": "ns/op\t   7.79 MB/s\t   18625 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8216,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.79,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18625,
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
            "value": 12171,
            "unit": "ns/op\t 336.55 MB/s\t   33547 B/op\t      11 allocs/op",
            "extra": "326022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12171,
            "unit": "ns/op",
            "extra": "326022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 336.55,
            "unit": "MB/s",
            "extra": "326022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33547,
            "unit": "B/op",
            "extra": "326022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "326022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 129071,
            "unit": "ns/op\t 126.94 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 129071,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 126.94,
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
            "value": 1485582,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1485582,
            "unit": "ns/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "789 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 614.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1930263 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 614.5,
            "unit": "ns/op",
            "extra": "1930263 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1930263 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1930263 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49483,
            "unit": "ns/op\t 165.55 MB/s\t   27968 B/op\t     454 allocs/op",
            "extra": "24638 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49483,
            "unit": "ns/op",
            "extra": "24638 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.55,
            "unit": "MB/s",
            "extra": "24638 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27968,
            "unit": "B/op",
            "extra": "24638 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24638 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6698775,
            "unit": "ns/op\t67523272 B/op\t    2579 allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6698775,
            "unit": "ns/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523272,
            "unit": "B/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 621.7,
            "unit": "ns/op\t 102.95 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1950127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 621.7,
            "unit": "ns/op",
            "extra": "1950127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 102.95,
            "unit": "MB/s",
            "extra": "1950127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1950127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1950127 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9368919 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.4,
            "unit": "ns/op",
            "extra": "9368919 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9368919 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9368919 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1383,
            "unit": "ns/op\t  46.28 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1383,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.28,
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
            "value": 472.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2510182 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 472.9,
            "unit": "ns/op",
            "extra": "2510182 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2510182 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2510182 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26952,
            "unit": "ns/op\t 303.95 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73746 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26952,
            "unit": "ns/op",
            "extra": "73746 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.95,
            "unit": "MB/s",
            "extra": "73746 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73746 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73746 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 178.3,
            "unit": "ns/op\t1436.05 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7246092 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 178.3,
            "unit": "ns/op",
            "extra": "7246092 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1436.05,
            "unit": "MB/s",
            "extra": "7246092 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7246092 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7246092 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 704.5,
            "unit": "ns/op\t 363.39 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3377929 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 704.5,
            "unit": "ns/op",
            "extra": "3377929 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 363.39,
            "unit": "MB/s",
            "extra": "3377929 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3377929 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3377929 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2009001,
            "unit": "ns/op\t 3064036 B/op\t   40018 allocs/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2009001,
            "unit": "ns/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064036,
            "unit": "B/op",
            "extra": "595 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "595 times\n4 procs"
          }
        ]
      }
    ]
  }
}