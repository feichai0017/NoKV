window.BENCHMARK_DATA = {
  "lastUpdate": 1771132848751,
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
          "id": "9f8453d0910369954aa1470df80984f7595d9c20",
          "message": "fix: resolve linux-only lint errors in mmap tests",
          "timestamp": "2026-02-15T13:19:13+08:00",
          "tree_id": "f4377bea07093b9f0e9744414d5380b5542fe290",
          "url": "https://github.com/feichai0017/NoKV/commit/9f8453d0910369954aa1470df80984f7595d9c20"
        },
        "date": 1771132847728,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7416,
            "unit": "ns/op\t   4.32 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "168285 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7416,
            "unit": "ns/op",
            "extra": "168285 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.32,
            "unit": "MB/s",
            "extra": "168285 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "168285 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "168285 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19024,
            "unit": "ns/op\t 215.31 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19024,
            "unit": "ns/op",
            "extra": "70821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 215.31,
            "unit": "MB/s",
            "extra": "70821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70821 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8036,
            "unit": "ns/op\t   7.96 MB/s\t   18274 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8036,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.96,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18274,
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
            "value": 11558,
            "unit": "ns/op\t 354.40 MB/s\t   32029 B/op\t      11 allocs/op",
            "extra": "363379 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11558,
            "unit": "ns/op",
            "extra": "363379 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 354.4,
            "unit": "MB/s",
            "extra": "363379 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32029,
            "unit": "B/op",
            "extra": "363379 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "363379 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120759,
            "unit": "ns/op\t 135.68 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120759,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.68,
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
            "value": 1525766,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1525766,
            "unit": "ns/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 584.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2122988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 584.2,
            "unit": "ns/op",
            "extra": "2122988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2122988 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2122988 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50368,
            "unit": "ns/op\t 162.64 MB/s\t   27707 B/op\t     454 allocs/op",
            "extra": "25240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50368,
            "unit": "ns/op",
            "extra": "25240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.64,
            "unit": "MB/s",
            "extra": "25240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27707,
            "unit": "B/op",
            "extra": "25240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6363909,
            "unit": "ns/op\t67523069 B/op\t    2578 allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6363909,
            "unit": "ns/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523069,
            "unit": "B/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 605.2,
            "unit": "ns/op\t 105.75 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1984978 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 605.2,
            "unit": "ns/op",
            "extra": "1984978 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 105.75,
            "unit": "MB/s",
            "extra": "1984978 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1984978 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1984978 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9348406 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "9348406 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9348406 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9348406 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1411,
            "unit": "ns/op\t  45.37 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1411,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.37,
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
            "value": 495.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2709585 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 495.8,
            "unit": "ns/op",
            "extra": "2709585 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2709585 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2709585 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26777,
            "unit": "ns/op\t 305.93 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74268 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26777,
            "unit": "ns/op",
            "extra": "74268 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 305.93,
            "unit": "MB/s",
            "extra": "74268 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74268 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74268 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 161,
            "unit": "ns/op\t1590.06 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7361412 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 161,
            "unit": "ns/op",
            "extra": "7361412 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1590.06,
            "unit": "MB/s",
            "extra": "7361412 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7361412 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7361412 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 682,
            "unit": "ns/op\t 375.36 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3509886 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 682,
            "unit": "ns/op",
            "extra": "3509886 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 375.36,
            "unit": "MB/s",
            "extra": "3509886 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3509886 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3509886 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2091214,
            "unit": "ns/op\t 3064031 B/op\t   40017 allocs/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2091214,
            "unit": "ns/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064031,
            "unit": "B/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "560 times\n4 procs"
          }
        ]
      }
    ]
  }
}