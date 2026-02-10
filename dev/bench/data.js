window.BENCHMARK_DATA = {
  "lastUpdate": 1770706903321,
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
          "id": "62c0fabf8b96efa54c75264f56887efd945d1b0b",
          "message": "docs: add semantic docstrings for core DB and LSM exported APIs",
          "timestamp": "2026-02-10T15:00:30+08:00",
          "tree_id": "f79652cd273cafbf0d08585311595e8fe9597b32",
          "url": "https://github.com/feichai0017/NoKV/commit/62c0fabf8b96efa54c75264f56887efd945d1b0b"
        },
        "date": 1770706902538,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7980,
            "unit": "ns/op\t   4.01 MB/s\t     408 B/op\t      18 allocs/op",
            "extra": "162037 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7980,
            "unit": "ns/op",
            "extra": "162037 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.01,
            "unit": "MB/s",
            "extra": "162037 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 408,
            "unit": "B/op",
            "extra": "162037 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "162037 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18301,
            "unit": "ns/op\t 223.81 MB/s\t     642 B/op\t      29 allocs/op",
            "extra": "65452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18301,
            "unit": "ns/op",
            "extra": "65452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 223.81,
            "unit": "MB/s",
            "extra": "65452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 642,
            "unit": "B/op",
            "extra": "65452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "65452 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7659,
            "unit": "ns/op\t   8.36 MB/s\t   16926 B/op\t       4 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7659,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.36,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16926,
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
            "value": 10526,
            "unit": "ns/op\t 389.12 MB/s\t   26799 B/op\t       8 allocs/op",
            "extra": "409518 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10526,
            "unit": "ns/op",
            "extra": "409518 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 389.12,
            "unit": "MB/s",
            "extra": "409518 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26799,
            "unit": "B/op",
            "extra": "409518 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "409518 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 130211,
            "unit": "ns/op\t 125.83 MB/s\t   56857 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 130211,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 125.83,
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
            "value": 1544328,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1544328,
            "unit": "ns/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 606.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1924532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 606.4,
            "unit": "ns/op",
            "extra": "1924532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1924532 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1924532 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49714,
            "unit": "ns/op\t 164.78 MB/s\t   27975 B/op\t     454 allocs/op",
            "extra": "24624 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49714,
            "unit": "ns/op",
            "extra": "24624 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.78,
            "unit": "MB/s",
            "extra": "24624 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27975,
            "unit": "B/op",
            "extra": "24624 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24624 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6993707,
            "unit": "ns/op\t67523310 B/op\t    2586 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6993707,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523310,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 660.4,
            "unit": "ns/op\t  96.92 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1994010 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 660.4,
            "unit": "ns/op",
            "extra": "1994010 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 96.92,
            "unit": "MB/s",
            "extra": "1994010 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1994010 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1994010 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9212763 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.8,
            "unit": "ns/op",
            "extra": "9212763 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9212763 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9212763 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1464,
            "unit": "ns/op\t  43.70 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1464,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.7,
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
            "value": 469.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2576307 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 469.9,
            "unit": "ns/op",
            "extra": "2576307 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2576307 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2576307 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25927,
            "unit": "ns/op\t 315.96 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77200 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25927,
            "unit": "ns/op",
            "extra": "77200 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 315.96,
            "unit": "MB/s",
            "extra": "77200 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77200 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77200 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 150.9,
            "unit": "ns/op\t1696.62 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7882444 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 150.9,
            "unit": "ns/op",
            "extra": "7882444 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1696.62,
            "unit": "MB/s",
            "extra": "7882444 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7882444 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7882444 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 737,
            "unit": "ns/op\t 347.36 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3134457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 737,
            "unit": "ns/op",
            "extra": "3134457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 347.36,
            "unit": "MB/s",
            "extra": "3134457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3134457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3134457 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2100840,
            "unit": "ns/op\t 3064027 B/op\t   40019 allocs/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2100840,
            "unit": "ns/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064027,
            "unit": "B/op",
            "extra": "567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "567 times\n4 procs"
          }
        ]
      }
    ]
  }
}