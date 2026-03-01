window.BENCHMARK_DATA = {
  "lastUpdate": 1772374572106,
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
          "id": "eb9382e6b68721adfbc582caa27ddae35bef1a35",
          "message": "chore: remove unused runtime store mode helper",
          "timestamp": "2026-03-02T01:14:57+11:00",
          "tree_id": "737e1ddba41c8d6f5f9edf58292609365bbf8881",
          "url": "https://github.com/feichai0017/NoKV/commit/eb9382e6b68721adfbc582caa27ddae35bef1a35"
        },
        "date": 1772374571063,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9745,
            "unit": "ns/op\t   3.28 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "106250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9745,
            "unit": "ns/op",
            "extra": "106250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.28,
            "unit": "MB/s",
            "extra": "106250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "106250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "106250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17792,
            "unit": "ns/op\t 230.22 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "77869 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17792,
            "unit": "ns/op",
            "extra": "77869 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 230.22,
            "unit": "MB/s",
            "extra": "77869 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "77869 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "77869 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7814,
            "unit": "ns/op\t   8.19 MB/s\t   16573 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7814,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.19,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16573,
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
            "value": 12125,
            "unit": "ns/op\t 337.82 MB/s\t   34915 B/op\t      11 allocs/op",
            "extra": "331905 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12125,
            "unit": "ns/op",
            "extra": "331905 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.82,
            "unit": "MB/s",
            "extra": "331905 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34915,
            "unit": "B/op",
            "extra": "331905 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "331905 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122226,
            "unit": "ns/op\t 134.05 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122226,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.05,
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
            "value": 1540491,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1540491,
            "unit": "ns/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "768 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 623.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1986421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 623.5,
            "unit": "ns/op",
            "extra": "1986421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1986421 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1986421 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 24.42,
            "unit": "ns/op",
            "extra": "47177798 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.97,
            "unit": "ns/op",
            "extra": "21064503 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.99,
            "unit": "ns/op",
            "extra": "58770902 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.86,
            "unit": "ns/op",
            "extra": "72118374 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20411307,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.08,
            "unit": "ns/op",
            "extra": "15370590 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51337,
            "unit": "ns/op",
            "extra": "23726 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 51047,
            "unit": "ns/op\t 160.48 MB/s\t   27759 B/op\t     454 allocs/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 51047,
            "unit": "ns/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 160.48,
            "unit": "MB/s",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27759,
            "unit": "B/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6804445,
            "unit": "ns/op\t67523401 B/op\t    2579 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6804445,
            "unit": "ns/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523401,
            "unit": "B/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": null,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": null,
            "unit": "ns/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "0 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26380,
            "unit": "ns/op\t 310.54 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26380,
            "unit": "ns/op",
            "extra": "74166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 310.54,
            "unit": "MB/s",
            "extra": "74166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74166 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.9,
            "unit": "ns/op\t1631.65 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7326826 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.9,
            "unit": "ns/op",
            "extra": "7326826 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1631.65,
            "unit": "MB/s",
            "extra": "7326826 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7326826 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7326826 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 688.2,
            "unit": "ns/op\t 372.01 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3396622 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 688.2,
            "unit": "ns/op",
            "extra": "3396622 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 372.01,
            "unit": "MB/s",
            "extra": "3396622 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3396622 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3396622 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1979227,
            "unit": "ns/op\t 3064029 B/op\t   40017 allocs/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1979227,
            "unit": "ns/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064029,
            "unit": "B/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "603 times\n4 procs"
          }
        ]
      }
    ]
  }
}