window.BENCHMARK_DATA = {
  "lastUpdate": 1772468251115,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "committer": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "distinct": true,
          "id": "f3fd70c60706eb727b391c55e4504a27c4db7ffa",
          "message": "revert: rollback builder buffer pool optimization",
          "timestamp": "2026-03-03T03:16:14+11:00",
          "tree_id": "d9bbcce2099a8289bc98653f7e0b2728eaf6714a",
          "url": "https://github.com/feichai0017/NoKV/commit/f3fd70c60706eb727b391c55e4504a27c4db7ffa"
        },
        "date": 1772468249946,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7715,
            "unit": "ns/op\t   4.15 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "140265 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7715,
            "unit": "ns/op",
            "extra": "140265 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.15,
            "unit": "MB/s",
            "extra": "140265 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "140265 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "140265 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17061,
            "unit": "ns/op\t 240.07 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "63961 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17061,
            "unit": "ns/op",
            "extra": "63961 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 240.07,
            "unit": "MB/s",
            "extra": "63961 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "63961 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "63961 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8812,
            "unit": "ns/op\t   7.26 MB/s\t   20769 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8812,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.26,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20769,
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
            "value": 12235,
            "unit": "ns/op\t 334.77 MB/s\t   34176 B/op\t      11 allocs/op",
            "extra": "333584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12235,
            "unit": "ns/op",
            "extra": "333584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 334.77,
            "unit": "MB/s",
            "extra": "333584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34176,
            "unit": "B/op",
            "extra": "333584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "333584 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 117767,
            "unit": "ns/op\t 139.12 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 117767,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 139.12,
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
            "value": 1540645,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1540645,
            "unit": "ns/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "778 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 567.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2000011 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 567.4,
            "unit": "ns/op",
            "extra": "2000011 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2000011 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2000011 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.78,
            "unit": "ns/op",
            "extra": "50666413 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.31,
            "unit": "ns/op",
            "extra": "20883327 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.95,
            "unit": "ns/op",
            "extra": "60046872 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.01,
            "unit": "ns/op",
            "extra": "72512328 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21777571,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 76.15,
            "unit": "ns/op",
            "extra": "15869334 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51833,
            "unit": "ns/op",
            "extra": "22999 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 47817,
            "unit": "ns/op\t 171.32 MB/s\t   27818 B/op\t     454 allocs/op",
            "extra": "24981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 47817,
            "unit": "ns/op",
            "extra": "24981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 171.32,
            "unit": "MB/s",
            "extra": "24981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27818,
            "unit": "B/op",
            "extra": "24981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6475771,
            "unit": "ns/op\t67523331 B/op\t    2579 allocs/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6475771,
            "unit": "ns/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523331,
            "unit": "B/op",
            "extra": "184 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "184 times\n4 procs"
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
            "value": 26620,
            "unit": "ns/op\t 307.74 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73387 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26620,
            "unit": "ns/op",
            "extra": "73387 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.74,
            "unit": "MB/s",
            "extra": "73387 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73387 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73387 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 167.4,
            "unit": "ns/op\t1529.09 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7700247 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 167.4,
            "unit": "ns/op",
            "extra": "7700247 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1529.09,
            "unit": "MB/s",
            "extra": "7700247 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7700247 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7700247 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 691.6,
            "unit": "ns/op\t 370.16 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3472080 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 691.6,
            "unit": "ns/op",
            "extra": "3472080 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.16,
            "unit": "MB/s",
            "extra": "3472080 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3472080 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3472080 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1971502,
            "unit": "ns/op\t 3064032 B/op\t   40017 allocs/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1971502,
            "unit": "ns/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
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