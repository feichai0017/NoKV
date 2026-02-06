window.BENCHMARK_DATA = {
  "lastUpdate": 1770381493342,
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
          "id": "bf8e2427bc4ae3703d5c6c47335523c5d7e64428",
          "message": "chore: disable hotring decay by default",
          "timestamp": "2026-02-06T20:36:42+08:00",
          "tree_id": "58b33b6435c93bfb30630e672f245a778c5986e0",
          "url": "https://github.com/feichai0017/NoKV/commit/bf8e2427bc4ae3703d5c6c47335523c5d7e64428"
        },
        "date": 1770381492400,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 10497,
            "unit": "ns/op\t   3.05 MB/s\t     566 B/op\t      20 allocs/op",
            "extra": "112250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 10497,
            "unit": "ns/op",
            "extra": "112250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.05,
            "unit": "MB/s",
            "extra": "112250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 566,
            "unit": "B/op",
            "extra": "112250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "112250 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17599,
            "unit": "ns/op\t 232.74 MB/s\t     797 B/op\t      31 allocs/op",
            "extra": "79503 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17599,
            "unit": "ns/op",
            "extra": "79503 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 232.74,
            "unit": "MB/s",
            "extra": "79503 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 797,
            "unit": "B/op",
            "extra": "79503 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 31,
            "unit": "allocs/op",
            "extra": "79503 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12420,
            "unit": "ns/op\t   5.15 MB/s\t   20272 B/op\t       5 allocs/op",
            "extra": "810874 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12420,
            "unit": "ns/op",
            "extra": "810874 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.15,
            "unit": "MB/s",
            "extra": "810874 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20272,
            "unit": "B/op",
            "extra": "810874 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "810874 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10080,
            "unit": "ns/op\t 406.36 MB/s\t   18164 B/op\t       7 allocs/op",
            "extra": "237601 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10080,
            "unit": "ns/op",
            "extra": "237601 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 406.36,
            "unit": "MB/s",
            "extra": "237601 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18164,
            "unit": "B/op",
            "extra": "237601 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "237601 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 171136,
            "unit": "ns/op\t  95.74 MB/s\t   62752 B/op\t     686 allocs/op",
            "extra": "8853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 171136,
            "unit": "ns/op",
            "extra": "8853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.74,
            "unit": "MB/s",
            "extra": "8853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62752,
            "unit": "B/op",
            "extra": "8853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 686,
            "unit": "allocs/op",
            "extra": "8853 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2172920,
            "unit": "ns/op\t    7974 B/op\t       0 allocs/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2172920,
            "unit": "ns/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 7974,
            "unit": "B/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "560 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1007,
            "unit": "ns/op\t      36 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1007,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50283,
            "unit": "ns/op\t 162.92 MB/s\t   25635 B/op\t     454 allocs/op",
            "extra": "23487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50283,
            "unit": "ns/op",
            "extra": "23487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.92,
            "unit": "MB/s",
            "extra": "23487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25635,
            "unit": "B/op",
            "extra": "23487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23487 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6781589,
            "unit": "ns/op\t67523407 B/op\t    2586 allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6781589,
            "unit": "ns/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523407,
            "unit": "B/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "176 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 710.4,
            "unit": "ns/op\t  90.09 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1594592 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 710.4,
            "unit": "ns/op",
            "extra": "1594592 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 90.09,
            "unit": "MB/s",
            "extra": "1594592 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1594592 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1594592 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9302383 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.6,
            "unit": "ns/op",
            "extra": "9302383 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9302383 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9302383 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1399,
            "unit": "ns/op\t  45.76 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1399,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.76,
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
            "value": 474.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2597956 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 474.4,
            "unit": "ns/op",
            "extra": "2597956 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2597956 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2597956 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26601,
            "unit": "ns/op\t 307.96 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26601,
            "unit": "ns/op",
            "extra": "75638 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.96,
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
            "value": 148.2,
            "unit": "ns/op\t1727.52 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8079567 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.2,
            "unit": "ns/op",
            "extra": "8079567 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1727.52,
            "unit": "MB/s",
            "extra": "8079567 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8079567 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8079567 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 733,
            "unit": "ns/op\t 349.23 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3133356 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 733,
            "unit": "ns/op",
            "extra": "3133356 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 349.23,
            "unit": "MB/s",
            "extra": "3133356 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3133356 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3133356 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2021843,
            "unit": "ns/op\t 3064042 B/op\t   40019 allocs/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2021843,
            "unit": "ns/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064042,
            "unit": "B/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "588 times\n4 procs"
          }
        ]
      }
    ]
  }
}