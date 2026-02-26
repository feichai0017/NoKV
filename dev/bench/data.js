window.BENCHMARK_DATA = {
  "lastUpdate": 1772080917904,
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
          "id": "2cf35aba1b4f90b9c69572258bd4b5f21aec9268",
          "message": "fix: complete reverse iteration path for ART memtable\n\n- honor Options.IsAsc in ART iterator and add reverse Seek/Rewind/Next\n\n- remove redundant forward prefetch call in table seekToLast\n\n- add ART/DB reverse iterator regression tests\n\n- fix test errcheck issues and dedupe table test discard-stats helper",
          "timestamp": "2026-02-26T12:40:19+08:00",
          "tree_id": "a6bd5bc77b968542ba257a69b604d7eb16f0befd",
          "url": "https://github.com/feichai0017/NoKV/commit/2cf35aba1b4f90b9c69572258bd4b5f21aec9268"
        },
        "date": 1772080916182,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7149,
            "unit": "ns/op\t   4.48 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "162855 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7149,
            "unit": "ns/op",
            "extra": "162855 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.48,
            "unit": "MB/s",
            "extra": "162855 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "162855 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "162855 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17645,
            "unit": "ns/op\t 232.14 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "78762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17645,
            "unit": "ns/op",
            "extra": "78762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 232.14,
            "unit": "MB/s",
            "extra": "78762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "78762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "78762 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8112,
            "unit": "ns/op\t   7.89 MB/s\t   17963 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8112,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.89,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17963,
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
            "value": 12538,
            "unit": "ns/op\t 326.68 MB/s\t   34263 B/op\t      11 allocs/op",
            "extra": "325363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12538,
            "unit": "ns/op",
            "extra": "325363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 326.68,
            "unit": "MB/s",
            "extra": "325363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34263,
            "unit": "B/op",
            "extra": "325363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "325363 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121550,
            "unit": "ns/op\t 134.79 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121550,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.79,
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
            "value": 1491448,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1491448,
            "unit": "ns/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "806 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 589.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1918202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 589.9,
            "unit": "ns/op",
            "extra": "1918202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1918202 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1918202 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50692,
            "unit": "ns/op\t 161.60 MB/s\t   27651 B/op\t     454 allocs/op",
            "extra": "25374 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50692,
            "unit": "ns/op",
            "extra": "25374 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.6,
            "unit": "MB/s",
            "extra": "25374 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27651,
            "unit": "B/op",
            "extra": "25374 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25374 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6729918,
            "unit": "ns/op\t67523290 B/op\t    2579 allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6729918,
            "unit": "ns/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523290,
            "unit": "B/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "177 times\n4 procs"
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
            "value": 26392,
            "unit": "ns/op\t 310.40 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26392,
            "unit": "ns/op",
            "extra": "74224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 310.4,
            "unit": "MB/s",
            "extra": "74224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74224 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.6,
            "unit": "ns/op\t1645.21 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7612758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.6,
            "unit": "ns/op",
            "extra": "7612758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1645.21,
            "unit": "MB/s",
            "extra": "7612758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7612758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7612758 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 687,
            "unit": "ns/op\t 372.62 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3463968 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 687,
            "unit": "ns/op",
            "extra": "3463968 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 372.62,
            "unit": "MB/s",
            "extra": "3463968 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3463968 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3463968 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1999128,
            "unit": "ns/op\t 3064033 B/op\t   40017 allocs/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1999128,
            "unit": "ns/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064033,
            "unit": "B/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "598 times\n4 procs"
          }
        ]
      }
    ]
  }
}