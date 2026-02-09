window.BENCHMARK_DATA = {
  "lastUpdate": 1770642201254,
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
          "id": "fd9b02827f8d0680daa4abd1763d6d83f1e511d1",
          "message": "Merge pull request #36 from feichai0017/dependabot/go_modules/github.com/panjf2000/ants/v2-2.11.5\n\ndeps(deps): bump github.com/panjf2000/ants/v2 from 2.11.4 to 2.11.5",
          "timestamp": "2026-02-09T21:01:50+08:00",
          "tree_id": "730523efaf4af82ea65568154f0615d37f9c0c42",
          "url": "https://github.com/feichai0017/NoKV/commit/fd9b02827f8d0680daa4abd1763d6d83f1e511d1"
        },
        "date": 1770642199222,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9084,
            "unit": "ns/op\t   3.52 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "175737 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9084,
            "unit": "ns/op",
            "extra": "175737 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.52,
            "unit": "MB/s",
            "extra": "175737 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "175737 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "175737 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16605,
            "unit": "ns/op\t 246.68 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "75478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16605,
            "unit": "ns/op",
            "extra": "75478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 246.68,
            "unit": "MB/s",
            "extra": "75478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "75478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "75478 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8660,
            "unit": "ns/op\t   7.39 MB/s\t   16622 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8660,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.39,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16622,
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
            "value": 8972,
            "unit": "ns/op\t 456.53 MB/s\t   19429 B/op\t       7 allocs/op",
            "extra": "277357 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8972,
            "unit": "ns/op",
            "extra": "277357 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 456.53,
            "unit": "MB/s",
            "extra": "277357 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19429,
            "unit": "B/op",
            "extra": "277357 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "277357 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 129987,
            "unit": "ns/op\t 126.04 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 129987,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 126.04,
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
            "value": 1541582,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1541582,
            "unit": "ns/op",
            "extra": "694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "694 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 574.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2110698 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 574.4,
            "unit": "ns/op",
            "extra": "2110698 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2110698 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2110698 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 46276,
            "unit": "ns/op\t 177.03 MB/s\t   27642 B/op\t     454 allocs/op",
            "extra": "25395 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 46276,
            "unit": "ns/op",
            "extra": "25395 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 177.03,
            "unit": "MB/s",
            "extra": "25395 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27642,
            "unit": "B/op",
            "extra": "25395 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25395 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 5253848,
            "unit": "ns/op\t67523071 B/op\t    2585 allocs/op",
            "extra": "218 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 5253848,
            "unit": "ns/op",
            "extra": "218 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523071,
            "unit": "B/op",
            "extra": "218 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2585,
            "unit": "allocs/op",
            "extra": "218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 701.8,
            "unit": "ns/op\t  91.20 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2054758 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 701.8,
            "unit": "ns/op",
            "extra": "2054758 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 91.2,
            "unit": "MB/s",
            "extra": "2054758 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2054758 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2054758 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 132.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9041173 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 132.5,
            "unit": "ns/op",
            "extra": "9041173 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9041173 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9041173 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1362,
            "unit": "ns/op\t  47.00 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1362,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 47,
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
            "value": 444.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2601886 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 444.1,
            "unit": "ns/op",
            "extra": "2601886 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2601886 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2601886 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26551,
            "unit": "ns/op\t 308.53 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73386 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26551,
            "unit": "ns/op",
            "extra": "73386 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.53,
            "unit": "MB/s",
            "extra": "73386 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73386 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73386 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 142.8,
            "unit": "ns/op\t1792.37 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8372606 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 142.8,
            "unit": "ns/op",
            "extra": "8372606 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1792.37,
            "unit": "MB/s",
            "extra": "8372606 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8372606 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8372606 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 712.7,
            "unit": "ns/op\t 359.19 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3270697 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 712.7,
            "unit": "ns/op",
            "extra": "3270697 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 359.19,
            "unit": "MB/s",
            "extra": "3270697 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3270697 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3270697 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1852260,
            "unit": "ns/op\t 3064045 B/op\t   40019 allocs/op",
            "extra": "632 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1852260,
            "unit": "ns/op",
            "extra": "632 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064045,
            "unit": "B/op",
            "extra": "632 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "632 times\n4 procs"
          }
        ]
      }
    ]
  }
}