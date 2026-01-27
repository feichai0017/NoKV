window.BENCHMARK_DATA = {
  "lastUpdate": 1769492734497,
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
          "id": "7ec44c1560d191d7cf9051ff23d1f93df0df9253",
          "message": "Update docs",
          "timestamp": "2026-01-27T13:42:45+08:00",
          "tree_id": "2bcbf1efb250dfff9d54c738c7b699255f266103",
          "url": "https://github.com/feichai0017/NoKV/commit/7ec44c1560d191d7cf9051ff23d1f93df0df9253"
        },
        "date": 1769492733603,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 9566,
            "unit": "ns/op\t   3.35 MB/s\t     619 B/op\t      24 allocs/op",
            "extra": "120927 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 9566,
            "unit": "ns/op",
            "extra": "120927 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.35,
            "unit": "MB/s",
            "extra": "120927 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 619,
            "unit": "B/op",
            "extra": "120927 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "120927 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 14131,
            "unit": "ns/op\t 289.86 MB/s\t     691 B/op\t      27 allocs/op",
            "extra": "93087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 14131,
            "unit": "ns/op",
            "extra": "93087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 289.86,
            "unit": "MB/s",
            "extra": "93087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 691,
            "unit": "B/op",
            "extra": "93087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "93087 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12625,
            "unit": "ns/op\t   5.07 MB/s\t   20700 B/op\t       5 allocs/op",
            "extra": "774964 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12625,
            "unit": "ns/op",
            "extra": "774964 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.07,
            "unit": "MB/s",
            "extra": "774964 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20700,
            "unit": "B/op",
            "extra": "774964 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "774964 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9602,
            "unit": "ns/op\t 426.56 MB/s\t   18322 B/op\t       7 allocs/op",
            "extra": "257792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9602,
            "unit": "ns/op",
            "extra": "257792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 426.56,
            "unit": "MB/s",
            "extra": "257792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18322,
            "unit": "B/op",
            "extra": "257792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "257792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 168297,
            "unit": "ns/op\t  97.35 MB/s\t   66322 B/op\t     693 allocs/op",
            "extra": "8736 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 168297,
            "unit": "ns/op",
            "extra": "8736 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 97.35,
            "unit": "MB/s",
            "extra": "8736 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 66322,
            "unit": "B/op",
            "extra": "8736 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 693,
            "unit": "allocs/op",
            "extra": "8736 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2167350,
            "unit": "ns/op\t    8257 B/op\t       0 allocs/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2167350,
            "unit": "ns/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 8257,
            "unit": "B/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "598 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 952.5,
            "unit": "ns/op\t      35 B/op\t       1 allocs/op",
            "extra": "1325055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 952.5,
            "unit": "ns/op",
            "extra": "1325055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 35,
            "unit": "B/op",
            "extra": "1325055 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1325055 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49573,
            "unit": "ns/op\t 165.25 MB/s\t   27868 B/op\t     454 allocs/op",
            "extra": "24867 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49573,
            "unit": "ns/op",
            "extra": "24867 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.25,
            "unit": "MB/s",
            "extra": "24867 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27868,
            "unit": "B/op",
            "extra": "24867 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24867 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6604170,
            "unit": "ns/op\t67523266 B/op\t    2586 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6604170,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523266,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 616.5,
            "unit": "ns/op\t 103.81 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1892949 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 616.5,
            "unit": "ns/op",
            "extra": "1892949 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.81,
            "unit": "MB/s",
            "extra": "1892949 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1892949 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1892949 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9309057 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129.8,
            "unit": "ns/op",
            "extra": "9309057 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9309057 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9309057 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1423,
            "unit": "ns/op\t  44.99 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1423,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.99,
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
            "value": 476.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2499909 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 476.3,
            "unit": "ns/op",
            "extra": "2499909 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2499909 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2499909 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25472,
            "unit": "ns/op\t 321.60 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "77610 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25472,
            "unit": "ns/op",
            "extra": "77610 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 321.6,
            "unit": "MB/s",
            "extra": "77610 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "77610 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77610 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148.7,
            "unit": "ns/op\t1721.74 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8017890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.7,
            "unit": "ns/op",
            "extra": "8017890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1721.74,
            "unit": "MB/s",
            "extra": "8017890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8017890 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8017890 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 749.9,
            "unit": "ns/op\t 341.39 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3140186 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 749.9,
            "unit": "ns/op",
            "extra": "3140186 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 341.39,
            "unit": "MB/s",
            "extra": "3140186 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3140186 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3140186 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2083686,
            "unit": "ns/op\t 3064042 B/op\t   40019 allocs/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2083686,
            "unit": "ns/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064042,
            "unit": "B/op",
            "extra": "594 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "594 times\n4 procs"
          }
        ]
      }
    ]
  }
}