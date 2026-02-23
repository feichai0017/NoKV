window.BENCHMARK_DATA = {
  "lastUpdate": 1771847560327,
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
          "id": "dacc115a50aad8dc7060645159adf0cb3a42049f",
          "message": "Merge pull request #72 from ByteByteUp/fix/skiplist-refcount-panic-on-invalid\n\nfix: add refcount lifecycle validation with panic on invalid state",
          "timestamp": "2026-02-23T19:51:26+08:00",
          "tree_id": "74ad0505bd707d6ebd724c318a9a98d9e79b4c9e",
          "url": "https://github.com/feichai0017/NoKV/commit/dacc115a50aad8dc7060645159adf0cb3a42049f"
        },
        "date": 1771847558618,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7095,
            "unit": "ns/op\t   4.51 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "148807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7095,
            "unit": "ns/op",
            "extra": "148807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.51,
            "unit": "MB/s",
            "extra": "148807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "148807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "148807 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16516,
            "unit": "ns/op\t 248.00 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "78433 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16516,
            "unit": "ns/op",
            "extra": "78433 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 248,
            "unit": "MB/s",
            "extra": "78433 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "78433 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "78433 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9136,
            "unit": "ns/op\t   7.01 MB/s\t   21170 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9136,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.01,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 21170,
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
            "value": 11811,
            "unit": "ns/op\t 346.79 MB/s\t   34007 B/op\t      11 allocs/op",
            "extra": "335595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11811,
            "unit": "ns/op",
            "extra": "335595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 346.79,
            "unit": "MB/s",
            "extra": "335595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34007,
            "unit": "B/op",
            "extra": "335595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "335595 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 126061,
            "unit": "ns/op\t 129.97 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 126061,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 129.97,
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
            "value": 1530149,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "798 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1530149,
            "unit": "ns/op",
            "extra": "798 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "798 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "798 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 599.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2030809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 599.1,
            "unit": "ns/op",
            "extra": "2030809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2030809 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2030809 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49902,
            "unit": "ns/op\t 164.16 MB/s\t   27909 B/op\t     454 allocs/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49902,
            "unit": "ns/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.16,
            "unit": "MB/s",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27909,
            "unit": "B/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6301549,
            "unit": "ns/op\t67523090 B/op\t    2579 allocs/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6301549,
            "unit": "ns/op",
            "extra": "189 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523090,
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
            "name": "BenchmarkARTInsert",
            "value": 494.7,
            "unit": "ns/op\t 129.36 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2054838 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 494.7,
            "unit": "ns/op",
            "extra": "2054838 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 129.36,
            "unit": "MB/s",
            "extra": "2054838 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2054838 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2054838 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9264518 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.6,
            "unit": "ns/op",
            "extra": "9264518 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9264518 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9264518 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1409,
            "unit": "ns/op\t  45.43 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1409,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.43,
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
            "value": 462.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2721201 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 462.3,
            "unit": "ns/op",
            "extra": "2721201 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2721201 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2721201 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25296,
            "unit": "ns/op\t 323.84 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77860 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25296,
            "unit": "ns/op",
            "extra": "77860 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 323.84,
            "unit": "MB/s",
            "extra": "77860 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77860 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77860 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 163.8,
            "unit": "ns/op\t1563.32 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7348212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 163.8,
            "unit": "ns/op",
            "extra": "7348212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1563.32,
            "unit": "MB/s",
            "extra": "7348212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7348212 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7348212 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 703.4,
            "unit": "ns/op\t 363.95 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3422172 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 703.4,
            "unit": "ns/op",
            "extra": "3422172 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 363.95,
            "unit": "MB/s",
            "extra": "3422172 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3422172 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3422172 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1959310,
            "unit": "ns/op\t 3064030 B/op\t   40017 allocs/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1959310,
            "unit": "ns/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064030,
            "unit": "B/op",
            "extra": "609 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "609 times\n4 procs"
          }
        ]
      }
    ]
  }
}