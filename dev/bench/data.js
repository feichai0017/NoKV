window.BENCHMARK_DATA = {
  "lastUpdate": 1772401646338,
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
          "id": "925d0cf6d2670e45cee7f02d159c34c81efd38dc",
          "message": "Remove unused toml",
          "timestamp": "2026-03-02T08:45:44+11:00",
          "tree_id": "8352bcc1eb86adef5ca5ac745b143a841abba999",
          "url": "https://github.com/feichai0017/NoKV/commit/925d0cf6d2670e45cee7f02d159c34c81efd38dc"
        },
        "date": 1772401644660,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8351,
            "unit": "ns/op\t   3.83 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "137662 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8351,
            "unit": "ns/op",
            "extra": "137662 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.83,
            "unit": "MB/s",
            "extra": "137662 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "137662 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "137662 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17680,
            "unit": "ns/op\t 231.67 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "81232 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17680,
            "unit": "ns/op",
            "extra": "81232 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 231.67,
            "unit": "MB/s",
            "extra": "81232 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "81232 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "81232 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7986,
            "unit": "ns/op\t   8.01 MB/s\t   18179 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7986,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.01,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18179,
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
            "value": 11774,
            "unit": "ns/op\t 347.89 MB/s\t   33526 B/op\t      11 allocs/op",
            "extra": "339920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11774,
            "unit": "ns/op",
            "extra": "339920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 347.89,
            "unit": "MB/s",
            "extra": "339920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33526,
            "unit": "B/op",
            "extra": "339920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "339920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125227,
            "unit": "ns/op\t 130.83 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125227,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.83,
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
            "value": 1533243,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1533243,
            "unit": "ns/op",
            "extra": "783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "783 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 600.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2012962 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 600.6,
            "unit": "ns/op",
            "extra": "2012962 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2012962 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2012962 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.97,
            "unit": "ns/op",
            "extra": "50348002 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.35,
            "unit": "ns/op",
            "extra": "20825071 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.98,
            "unit": "ns/op",
            "extra": "59970853 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.84,
            "unit": "ns/op",
            "extra": "72464581 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20845012,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.1,
            "unit": "ns/op",
            "extra": "15783460 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 50394,
            "unit": "ns/op",
            "extra": "23352 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48045,
            "unit": "ns/op\t 170.51 MB/s\t   27949 B/op\t     454 allocs/op",
            "extra": "24680 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48045,
            "unit": "ns/op",
            "extra": "24680 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 170.51,
            "unit": "MB/s",
            "extra": "24680 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27949,
            "unit": "B/op",
            "extra": "24680 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24680 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6414359,
            "unit": "ns/op\t67523189 B/op\t    2579 allocs/op",
            "extra": "163 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6414359,
            "unit": "ns/op",
            "extra": "163 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523189,
            "unit": "B/op",
            "extra": "163 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "163 times\n4 procs"
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
            "value": 27032,
            "unit": "ns/op\t 303.05 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27032,
            "unit": "ns/op",
            "extra": "73374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.05,
            "unit": "MB/s",
            "extra": "73374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73374 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 167.7,
            "unit": "ns/op\t1526.11 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7145876 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 167.7,
            "unit": "ns/op",
            "extra": "7145876 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1526.11,
            "unit": "MB/s",
            "extra": "7145876 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7145876 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7145876 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.9,
            "unit": "ns/op\t 367.85 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3435573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.9,
            "unit": "ns/op",
            "extra": "3435573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 367.85,
            "unit": "MB/s",
            "extra": "3435573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3435573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3435573 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1993815,
            "unit": "ns/op\t 3064040 B/op\t   40018 allocs/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1993815,
            "unit": "ns/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "589 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "589 times\n4 procs"
          }
        ]
      }
    ]
  }
}