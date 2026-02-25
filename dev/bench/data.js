window.BENCHMARK_DATA = {
  "lastUpdate": 1772011049236,
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
          "id": "4eb5e39a851da3958e2405f759b13b92730a3c42",
          "message": "refactor: align refcount lifecycle semantics and docs",
          "timestamp": "2026-02-25T17:15:36+08:00",
          "tree_id": "181fdef4260c2dd86628c0d64d95488d616465d6",
          "url": "https://github.com/feichai0017/NoKV/commit/4eb5e39a851da3958e2405f759b13b92730a3c42"
        },
        "date": 1772011048274,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6965,
            "unit": "ns/op\t   4.59 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "162081 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6965,
            "unit": "ns/op",
            "extra": "162081 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.59,
            "unit": "MB/s",
            "extra": "162081 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "162081 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "162081 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16060,
            "unit": "ns/op\t 255.04 MB/s\t     537 B/op\t      23 allocs/op",
            "extra": "87632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16060,
            "unit": "ns/op",
            "extra": "87632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 255.04,
            "unit": "MB/s",
            "extra": "87632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 537,
            "unit": "B/op",
            "extra": "87632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "87632 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9138,
            "unit": "ns/op\t   7.00 MB/s\t   21582 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9138,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 21582,
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
            "value": 11914,
            "unit": "ns/op\t 343.79 MB/s\t   35424 B/op\t      12 allocs/op",
            "extra": "326293 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11914,
            "unit": "ns/op",
            "extra": "326293 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 343.79,
            "unit": "MB/s",
            "extra": "326293 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35424,
            "unit": "B/op",
            "extra": "326293 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 12,
            "unit": "allocs/op",
            "extra": "326293 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121960,
            "unit": "ns/op\t 134.34 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121960,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.34,
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
            "value": 1470916,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1470916,
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
            "value": 608.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1966310 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 608.4,
            "unit": "ns/op",
            "extra": "1966310 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1966310 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1966310 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49346,
            "unit": "ns/op\t 166.01 MB/s\t   27909 B/op\t     454 allocs/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49346,
            "unit": "ns/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.01,
            "unit": "MB/s",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27909,
            "unit": "B/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24772 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6288478,
            "unit": "ns/op\t67523077 B/op\t    2578 allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6288478,
            "unit": "ns/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523077,
            "unit": "B/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "188 times\n4 procs"
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
            "value": 25807,
            "unit": "ns/op\t 317.43 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25807,
            "unit": "ns/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 317.43,
            "unit": "MB/s",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.9,
            "unit": "ns/op\t1611.04 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7520360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.9,
            "unit": "ns/op",
            "extra": "7520360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1611.04,
            "unit": "MB/s",
            "extra": "7520360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7520360 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7520360 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 690.7,
            "unit": "ns/op\t 370.63 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3411020 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 690.7,
            "unit": "ns/op",
            "extra": "3411020 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.63,
            "unit": "MB/s",
            "extra": "3411020 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3411020 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3411020 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1960068,
            "unit": "ns/op\t 3064018 B/op\t   40017 allocs/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1960068,
            "unit": "ns/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064018,
            "unit": "B/op",
            "extra": "604 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "604 times\n4 procs"
          }
        ]
      }
    ]
  }
}