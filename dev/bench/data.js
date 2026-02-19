window.BENCHMARK_DATA = {
  "lastUpdate": 1771484926166,
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
          "id": "38fbb94680d78e1648d2679ab101e012a70c5277",
          "message": "test: rename levels slow follower tests to levels_test",
          "timestamp": "2026-02-19T15:06:21+08:00",
          "tree_id": "7bd320a515d6245f736dc4c9c1decfebcbcb51fe",
          "url": "https://github.com/feichai0017/NoKV/commit/38fbb94680d78e1648d2679ab101e012a70c5277"
        },
        "date": 1771484924587,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6652,
            "unit": "ns/op\t   4.81 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "181052 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6652,
            "unit": "ns/op",
            "extra": "181052 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.81,
            "unit": "MB/s",
            "extra": "181052 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "181052 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "181052 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15139,
            "unit": "ns/op\t 270.57 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "93727 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15139,
            "unit": "ns/op",
            "extra": "93727 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 270.57,
            "unit": "MB/s",
            "extra": "93727 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "93727 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "93727 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7182,
            "unit": "ns/op\t   8.91 MB/s\t   16838 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7182,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.91,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16838,
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
            "value": 8600,
            "unit": "ns/op\t 476.31 MB/s\t   26027 B/op\t      11 allocs/op",
            "extra": "302140 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8600,
            "unit": "ns/op",
            "extra": "302140 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 476.31,
            "unit": "MB/s",
            "extra": "302140 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26027,
            "unit": "B/op",
            "extra": "302140 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "302140 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 124085,
            "unit": "ns/op\t 132.04 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 124085,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.04,
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
            "value": 1554472,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1554472,
            "unit": "ns/op",
            "extra": "770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 549.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2060166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 549.8,
            "unit": "ns/op",
            "extra": "2060166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2060166 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2060166 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 47966,
            "unit": "ns/op\t 170.79 MB/s\t   27477 B/op\t     454 allocs/op",
            "extra": "25797 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47966,
            "unit": "ns/op",
            "extra": "25797 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 170.79,
            "unit": "MB/s",
            "extra": "25797 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27477,
            "unit": "B/op",
            "extra": "25797 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25797 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8344594,
            "unit": "ns/op\t67523308 B/op\t    2580 allocs/op",
            "extra": "140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8344594,
            "unit": "ns/op",
            "extra": "140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523308,
            "unit": "B/op",
            "extra": "140 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2580,
            "unit": "allocs/op",
            "extra": "140 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 528.3,
            "unit": "ns/op\t 121.14 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1968740 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 528.3,
            "unit": "ns/op",
            "extra": "1968740 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 121.14,
            "unit": "MB/s",
            "extra": "1968740 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1968740 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1968740 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 113.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "10735888 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 113.6,
            "unit": "ns/op",
            "extra": "10735888 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "10735888 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "10735888 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1268,
            "unit": "ns/op\t  50.49 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1268,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 50.49,
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
            "value": 475.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2585569 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 475.8,
            "unit": "ns/op",
            "extra": "2585569 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2585569 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2585569 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 23237,
            "unit": "ns/op\t 352.55 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "102322 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 23237,
            "unit": "ns/op",
            "extra": "102322 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 352.55,
            "unit": "MB/s",
            "extra": "102322 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "102322 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "102322 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.8,
            "unit": "ns/op\t1643.20 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7660789 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.8,
            "unit": "ns/op",
            "extra": "7660789 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1643.2,
            "unit": "MB/s",
            "extra": "7660789 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7660789 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7660789 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 626.8,
            "unit": "ns/op\t 408.41 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "4247005 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 626.8,
            "unit": "ns/op",
            "extra": "4247005 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 408.41,
            "unit": "MB/s",
            "extra": "4247005 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "4247005 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4247005 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1955957,
            "unit": "ns/op\t 3064047 B/op\t   40018 allocs/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1955957,
            "unit": "ns/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064047,
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