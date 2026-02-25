window.BENCHMARK_DATA = {
  "lastUpdate": 1772006505525,
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
          "id": "6311247eafa0d2da8dab2f2f0c24291beb05285f",
          "message": "fix(lsm): prevent double decref in compaction top path\n\n- remove unconditional decrRefs(cd.top) in runCompactDef\\n- route top table deletion by IngestMode to ensure single ownership release\\n- add regression tests for IngestNone/IngestDrain/IngestKeep refcount lifecycle",
          "timestamp": "2026-02-25T16:00:27+08:00",
          "tree_id": "c0236db508943826bf4d9bcb17c79bdf0816ab17",
          "url": "https://github.com/feichai0017/NoKV/commit/6311247eafa0d2da8dab2f2f0c24291beb05285f"
        },
        "date": 1772006504629,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8711,
            "unit": "ns/op\t   3.67 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "146587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8711,
            "unit": "ns/op",
            "extra": "146587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.67,
            "unit": "MB/s",
            "extra": "146587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "146587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "146587 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17046,
            "unit": "ns/op\t 240.29 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "72313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17046,
            "unit": "ns/op",
            "extra": "72313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 240.29,
            "unit": "MB/s",
            "extra": "72313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "72313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "72313 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7926,
            "unit": "ns/op\t   8.07 MB/s\t   17301 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7926,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.07,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17301,
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
            "value": 11959,
            "unit": "ns/op\t 342.51 MB/s\t   32987 B/op\t      11 allocs/op",
            "extra": "342150 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11959,
            "unit": "ns/op",
            "extra": "342150 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 342.51,
            "unit": "MB/s",
            "extra": "342150 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32987,
            "unit": "B/op",
            "extra": "342150 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "342150 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 129691,
            "unit": "ns/op\t 126.33 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 129691,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 126.33,
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
            "value": 1486294,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1486294,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 601.3,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2110458 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 601.3,
            "unit": "ns/op",
            "extra": "2110458 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2110458 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2110458 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49335,
            "unit": "ns/op\t 166.05 MB/s\t   28152 B/op\t     454 allocs/op",
            "extra": "24230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49335,
            "unit": "ns/op",
            "extra": "24230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.05,
            "unit": "MB/s",
            "extra": "24230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28152,
            "unit": "B/op",
            "extra": "24230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24230 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6501207,
            "unit": "ns/op\t67523154 B/op\t    2579 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6501207,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523154,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 595.2,
            "unit": "ns/op\t 107.52 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1987546 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 595.2,
            "unit": "ns/op",
            "extra": "1987546 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 107.52,
            "unit": "MB/s",
            "extra": "1987546 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1987546 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1987546 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9376917 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.6,
            "unit": "ns/op",
            "extra": "9376917 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9376917 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9376917 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1416,
            "unit": "ns/op\t  45.19 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1416,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.19,
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
            "value": 496.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2620060 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 496.4,
            "unit": "ns/op",
            "extra": "2620060 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2620060 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2620060 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26527,
            "unit": "ns/op\t 308.82 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73954 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26527,
            "unit": "ns/op",
            "extra": "73954 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 308.82,
            "unit": "MB/s",
            "extra": "73954 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73954 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73954 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 181.1,
            "unit": "ns/op\t1413.49 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7100428 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 181.1,
            "unit": "ns/op",
            "extra": "7100428 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1413.49,
            "unit": "MB/s",
            "extra": "7100428 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7100428 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7100428 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 696.2,
            "unit": "ns/op\t 367.69 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3389018 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 696.2,
            "unit": "ns/op",
            "extra": "3389018 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 367.69,
            "unit": "MB/s",
            "extra": "3389018 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3389018 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3389018 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1975766,
            "unit": "ns/op\t 3064026 B/op\t   40017 allocs/op",
            "extra": "608 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1975766,
            "unit": "ns/op",
            "extra": "608 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064026,
            "unit": "B/op",
            "extra": "608 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "608 times\n4 procs"
          }
        ]
      }
    ]
  }
}