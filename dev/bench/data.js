window.BENCHMARK_DATA = {
  "lastUpdate": 1772400903994,
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
          "id": "c6ee04230d2c9dff5c36bc7c9849676025ddf502",
          "message": "docs: add concise package readmes for pd raftstore lsm and cmd",
          "timestamp": "2026-03-02T08:33:31+11:00",
          "tree_id": "64da2559f64065b5de8442de8f1b953aede4e3e1",
          "url": "https://github.com/feichai0017/NoKV/commit/c6ee04230d2c9dff5c36bc7c9849676025ddf502"
        },
        "date": 1772400902907,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7721,
            "unit": "ns/op\t   4.14 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "179656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7721,
            "unit": "ns/op",
            "extra": "179656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.14,
            "unit": "MB/s",
            "extra": "179656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "179656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "179656 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17864,
            "unit": "ns/op\t 229.28 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "71827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17864,
            "unit": "ns/op",
            "extra": "71827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 229.28,
            "unit": "MB/s",
            "extra": "71827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "71827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "71827 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8242,
            "unit": "ns/op\t   7.77 MB/s\t   18700 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8242,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.77,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18700,
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
            "value": 12194,
            "unit": "ns/op\t 335.90 MB/s\t   35023 B/op\t      11 allocs/op",
            "extra": "338617 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12194,
            "unit": "ns/op",
            "extra": "338617 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 335.9,
            "unit": "MB/s",
            "extra": "338617 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35023,
            "unit": "B/op",
            "extra": "338617 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "338617 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123179,
            "unit": "ns/op\t 133.01 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123179,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.01,
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
            "value": 1565869,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1565869,
            "unit": "ns/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "771 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 573.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1969837 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 573.9,
            "unit": "ns/op",
            "extra": "1969837 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1969837 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1969837 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.69,
            "unit": "ns/op",
            "extra": "48988336 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 60.08,
            "unit": "ns/op",
            "extra": "20899176 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20,
            "unit": "ns/op",
            "extra": "59941242 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 17.61,
            "unit": "ns/op",
            "extra": "68620689 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 21295272,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.46,
            "unit": "ns/op",
            "extra": "15957663 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 52576,
            "unit": "ns/op",
            "extra": "23028 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50086,
            "unit": "ns/op\t 163.56 MB/s\t   27892 B/op\t     454 allocs/op",
            "extra": "24810 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50086,
            "unit": "ns/op",
            "extra": "24810 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.56,
            "unit": "MB/s",
            "extra": "24810 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27892,
            "unit": "B/op",
            "extra": "24810 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24810 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6305726,
            "unit": "ns/op\t67523220 B/op\t    2578 allocs/op",
            "extra": "193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6305726,
            "unit": "ns/op",
            "extra": "193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523220,
            "unit": "B/op",
            "extra": "193 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "193 times\n4 procs"
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
            "value": 26706,
            "unit": "ns/op\t 306.75 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73772 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26706,
            "unit": "ns/op",
            "extra": "73772 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 306.75,
            "unit": "MB/s",
            "extra": "73772 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73772 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73772 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.2,
            "unit": "ns/op\t1617.99 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7573599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.2,
            "unit": "ns/op",
            "extra": "7573599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1617.99,
            "unit": "MB/s",
            "extra": "7573599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7573599 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7573599 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 689.8,
            "unit": "ns/op\t 371.13 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3458709 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 689.8,
            "unit": "ns/op",
            "extra": "3458709 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 371.13,
            "unit": "MB/s",
            "extra": "3458709 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3458709 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3458709 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2000082,
            "unit": "ns/op\t 3064025 B/op\t   40017 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2000082,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064025,
            "unit": "B/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "592 times\n4 procs"
          }
        ]
      }
    ]
  }
}