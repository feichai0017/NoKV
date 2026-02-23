window.BENCHMARK_DATA = {
  "lastUpdate": 1771847575363,
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
          "id": "e55917180740fcf90fe942733c9b7ea2d48d9be8",
          "message": "Merge pull request #74 from hacker4257/fix/request-decrref-underflow-guard-v2\n\nfix: add underflow guard to request.DecrRef",
          "timestamp": "2026-02-23T19:51:46+08:00",
          "tree_id": "da5038317a01bd550f02fb1bc9e366d905d8ecc9",
          "url": "https://github.com/feichai0017/NoKV/commit/e55917180740fcf90fe942733c9b7ea2d48d9be8"
        },
        "date": 1771847574332,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7649,
            "unit": "ns/op\t   4.18 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "169159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7649,
            "unit": "ns/op",
            "extra": "169159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.18,
            "unit": "MB/s",
            "extra": "169159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "169159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "169159 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16658,
            "unit": "ns/op\t 245.88 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "81902 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16658,
            "unit": "ns/op",
            "extra": "81902 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 245.88,
            "unit": "MB/s",
            "extra": "81902 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "81902 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "81902 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8744,
            "unit": "ns/op\t   7.32 MB/s\t   19923 B/op\t       8 allocs/op",
            "extra": "943766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8744,
            "unit": "ns/op",
            "extra": "943766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.32,
            "unit": "MB/s",
            "extra": "943766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19923,
            "unit": "B/op",
            "extra": "943766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "943766 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 11563,
            "unit": "ns/op\t 354.25 MB/s\t   32386 B/op\t      11 allocs/op",
            "extra": "363171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11563,
            "unit": "ns/op",
            "extra": "363171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 354.25,
            "unit": "MB/s",
            "extra": "363171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32386,
            "unit": "B/op",
            "extra": "363171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "363171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 127159,
            "unit": "ns/op\t 128.85 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 127159,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 128.85,
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
            "value": 1543012,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "700 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1543012,
            "unit": "ns/op",
            "extra": "700 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "700 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "700 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 605.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1882022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 605.8,
            "unit": "ns/op",
            "extra": "1882022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1882022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1882022 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48392,
            "unit": "ns/op\t 169.28 MB/s\t   27601 B/op\t     454 allocs/op",
            "extra": "25494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48392,
            "unit": "ns/op",
            "extra": "25494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.28,
            "unit": "MB/s",
            "extra": "25494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27601,
            "unit": "B/op",
            "extra": "25494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25494 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6686275,
            "unit": "ns/op\t67523188 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6686275,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523188,
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
            "name": "BenchmarkARTInsert",
            "value": 588.1,
            "unit": "ns/op\t 108.82 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1992952 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 588.1,
            "unit": "ns/op",
            "extra": "1992952 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 108.82,
            "unit": "MB/s",
            "extra": "1992952 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1992952 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1992952 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9354388 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.1,
            "unit": "ns/op",
            "extra": "9354388 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9354388 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9354388 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1421,
            "unit": "ns/op\t  45.04 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1421,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.04,
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
            "value": 443.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2635016 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 443.9,
            "unit": "ns/op",
            "extra": "2635016 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2635016 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2635016 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25950,
            "unit": "ns/op\t 315.68 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77053 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25950,
            "unit": "ns/op",
            "extra": "77053 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.68,
            "unit": "MB/s",
            "extra": "77053 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77053 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77053 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 162.3,
            "unit": "ns/op\t1577.22 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7235217 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 162.3,
            "unit": "ns/op",
            "extra": "7235217 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1577.22,
            "unit": "MB/s",
            "extra": "7235217 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7235217 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7235217 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 705.2,
            "unit": "ns/op\t 363.04 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3359943 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 705.2,
            "unit": "ns/op",
            "extra": "3359943 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 363.04,
            "unit": "MB/s",
            "extra": "3359943 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3359943 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3359943 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1993431,
            "unit": "ns/op\t 3064038 B/op\t   40018 allocs/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1993431,
            "unit": "ns/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064038,
            "unit": "B/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "583 times\n4 procs"
          }
        ]
      }
    ]
  }
}