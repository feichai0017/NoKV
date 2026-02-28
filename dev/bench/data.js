window.BENCHMARK_DATA = {
  "lastUpdate": 1772260386519,
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
          "id": "aafe6ba13e9f850a62bb585f22844d3364900be8",
          "message": "perf: binary-search level table lookup",
          "timestamp": "2026-02-28T17:31:26+11:00",
          "tree_id": "cea9ba7f1bfe75c214a5365cc89975b70389c5c1",
          "url": "https://github.com/feichai0017/NoKV/commit/aafe6ba13e9f850a62bb585f22844d3364900be8"
        },
        "date": 1772260384907,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8004,
            "unit": "ns/op\t   4.00 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "137187 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8004,
            "unit": "ns/op",
            "extra": "137187 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4,
            "unit": "MB/s",
            "extra": "137187 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "137187 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "137187 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19868,
            "unit": "ns/op\t 206.16 MB/s\t     537 B/op\t      23 allocs/op",
            "extra": "89932 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19868,
            "unit": "ns/op",
            "extra": "89932 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 206.16,
            "unit": "MB/s",
            "extra": "89932 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 537,
            "unit": "B/op",
            "extra": "89932 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "89932 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7752,
            "unit": "ns/op\t   8.26 MB/s\t   17042 B/op\t       7 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7752,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.26,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17042,
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
            "value": 12775,
            "unit": "ns/op\t 320.64 MB/s\t   35022 B/op\t      11 allocs/op",
            "extra": "314013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12775,
            "unit": "ns/op",
            "extra": "314013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 320.64,
            "unit": "MB/s",
            "extra": "314013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35022,
            "unit": "B/op",
            "extra": "314013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "314013 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 128009,
            "unit": "ns/op\t 127.99 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 128009,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 127.99,
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
            "value": 1471154,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1471154,
            "unit": "ns/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 595.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1997192 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 595.7,
            "unit": "ns/op",
            "extra": "1997192 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1997192 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1997192 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.73,
            "unit": "ns/op",
            "extra": "50586219 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 61.51,
            "unit": "ns/op",
            "extra": "20522416 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.06,
            "unit": "ns/op",
            "extra": "59783502 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.81,
            "unit": "ns/op",
            "extra": "71962053 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20635481,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.17,
            "unit": "ns/op",
            "extra": "15910932 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49603,
            "unit": "ns/op",
            "extra": "23655 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49898,
            "unit": "ns/op\t 164.18 MB/s\t   27680 B/op\t     454 allocs/op",
            "extra": "25303 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49898,
            "unit": "ns/op",
            "extra": "25303 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.18,
            "unit": "MB/s",
            "extra": "25303 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27680,
            "unit": "B/op",
            "extra": "25303 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25303 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6750074,
            "unit": "ns/op\t67523157 B/op\t    2578 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6750074,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523157,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27092,
            "unit": "ns/op\t 302.37 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "72890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27092,
            "unit": "ns/op",
            "extra": "72890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 302.37,
            "unit": "MB/s",
            "extra": "72890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "72890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "72890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.6,
            "unit": "ns/op\t1613.66 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7594868 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.6,
            "unit": "ns/op",
            "extra": "7594868 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1613.66,
            "unit": "MB/s",
            "extra": "7594868 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7594868 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7594868 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 700.4,
            "unit": "ns/op\t 365.52 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3401360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 700.4,
            "unit": "ns/op",
            "extra": "3401360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 365.52,
            "unit": "MB/s",
            "extra": "3401360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3401360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3401360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1988860,
            "unit": "ns/op\t 3064026 B/op\t   40017 allocs/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1988860,
            "unit": "ns/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064026,
            "unit": "B/op",
            "extra": "601 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "601 times\n4 procs"
          }
        ]
      }
    ]
  }
}