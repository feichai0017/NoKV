window.BENCHMARK_DATA = {
  "lastUpdate": 1770402378604,
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
          "id": "47d9076364c53b1dd08275a4c9f1205c1fc9f29c",
          "message": "feat: split hotring read/write stats",
          "timestamp": "2026-02-07T02:24:52+08:00",
          "tree_id": "44b131fad6a0120fa12b12a770359633682ba7f8",
          "url": "https://github.com/feichai0017/NoKV/commit/47d9076364c53b1dd08275a4c9f1205c1fc9f29c"
        },
        "date": 1770402376881,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 11438,
            "unit": "ns/op\t   2.80 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "120396 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 11438,
            "unit": "ns/op",
            "extra": "120396 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.8,
            "unit": "MB/s",
            "extra": "120396 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "120396 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "120396 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16169,
            "unit": "ns/op\t 253.32 MB/s\t     658 B/op\t      29 allocs/op",
            "extra": "86448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16169,
            "unit": "ns/op",
            "extra": "86448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 253.32,
            "unit": "MB/s",
            "extra": "86448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 658,
            "unit": "B/op",
            "extra": "86448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "86448 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 10955,
            "unit": "ns/op\t   5.84 MB/s\t   18795 B/op\t       5 allocs/op",
            "extra": "748914 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 10955,
            "unit": "ns/op",
            "extra": "748914 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.84,
            "unit": "MB/s",
            "extra": "748914 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18795,
            "unit": "B/op",
            "extra": "748914 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "748914 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9853,
            "unit": "ns/op\t 415.72 MB/s\t   19235 B/op\t       7 allocs/op",
            "extra": "246127 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9853,
            "unit": "ns/op",
            "extra": "246127 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 415.72,
            "unit": "MB/s",
            "extra": "246127 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19235,
            "unit": "B/op",
            "extra": "246127 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "246127 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 169908,
            "unit": "ns/op\t  96.43 MB/s\t   62899 B/op\t     686 allocs/op",
            "extra": "8828 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 169908,
            "unit": "ns/op",
            "extra": "8828 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 96.43,
            "unit": "MB/s",
            "extra": "8828 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62899,
            "unit": "B/op",
            "extra": "8828 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 686,
            "unit": "allocs/op",
            "extra": "8828 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2241721,
            "unit": "ns/op\t       5 B/op\t       0 allocs/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2241721,
            "unit": "ns/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 5,
            "unit": "B/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "529 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1041,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1246742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1041,
            "unit": "ns/op",
            "extra": "1246742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1246742 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1246742 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49580,
            "unit": "ns/op\t 165.23 MB/s\t   25675 B/op\t     454 allocs/op",
            "extra": "23379 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49580,
            "unit": "ns/op",
            "extra": "23379 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.23,
            "unit": "MB/s",
            "extra": "23379 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25675,
            "unit": "B/op",
            "extra": "23379 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23379 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6743955,
            "unit": "ns/op\t67523275 B/op\t    2586 allocs/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6743955,
            "unit": "ns/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523275,
            "unit": "B/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "170 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 637.9,
            "unit": "ns/op\t 100.32 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2180419 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 637.9,
            "unit": "ns/op",
            "extra": "2180419 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 100.32,
            "unit": "MB/s",
            "extra": "2180419 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2180419 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2180419 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9302706 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.5,
            "unit": "ns/op",
            "extra": "9302706 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9302706 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9302706 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1391,
            "unit": "ns/op\t  46.03 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1391,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.03,
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
            "value": 503.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2473755 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 503.2,
            "unit": "ns/op",
            "extra": "2473755 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2473755 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2473755 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27259,
            "unit": "ns/op\t 300.53 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "77437 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27259,
            "unit": "ns/op",
            "extra": "77437 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 300.53,
            "unit": "MB/s",
            "extra": "77437 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "77437 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77437 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.4,
            "unit": "ns/op\t1760.84 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8160238 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.4,
            "unit": "ns/op",
            "extra": "8160238 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1760.84,
            "unit": "MB/s",
            "extra": "8160238 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8160238 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8160238 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 737.4,
            "unit": "ns/op\t 347.15 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3141602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 737.4,
            "unit": "ns/op",
            "extra": "3141602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 347.15,
            "unit": "MB/s",
            "extra": "3141602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3141602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3141602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2014573,
            "unit": "ns/op\t 3064045 B/op\t   40019 allocs/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2014573,
            "unit": "ns/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064045,
            "unit": "B/op",
            "extra": "586 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "586 times\n4 procs"
          }
        ]
      }
    ]
  }
}