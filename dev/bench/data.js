window.BENCHMARK_DATA = {
  "lastUpdate": 1770225067971,
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
          "id": "726a4c68a398c3dd2ceb8b355512d0a62ae658fc",
          "message": "Merge branch 'main' of github.com:feichai0017/NoKV",
          "timestamp": "2026-02-05T01:09:45+08:00",
          "tree_id": "8f04aa60f96dc02b0437daeb36818a68c1b14cdc",
          "url": "https://github.com/feichai0017/NoKV/commit/726a4c68a398c3dd2ceb8b355512d0a62ae658fc"
        },
        "date": 1770225067035,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 12617,
            "unit": "ns/op\t   2.54 MB/s\t     626 B/op\t      24 allocs/op",
            "extra": "125659 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 12617,
            "unit": "ns/op",
            "extra": "125659 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.54,
            "unit": "MB/s",
            "extra": "125659 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 626,
            "unit": "B/op",
            "extra": "125659 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "125659 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15576,
            "unit": "ns/op\t 262.98 MB/s\t     657 B/op\t      27 allocs/op",
            "extra": "91436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15576,
            "unit": "ns/op",
            "extra": "91436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 262.98,
            "unit": "MB/s",
            "extra": "91436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 657,
            "unit": "B/op",
            "extra": "91436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "91436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11267,
            "unit": "ns/op\t   5.68 MB/s\t   17986 B/op\t       5 allocs/op",
            "extra": "731137 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11267,
            "unit": "ns/op",
            "extra": "731137 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.68,
            "unit": "MB/s",
            "extra": "731137 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17986,
            "unit": "B/op",
            "extra": "731137 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "731137 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10799,
            "unit": "ns/op\t 379.28 MB/s\t   19247 B/op\t       8 allocs/op",
            "extra": "212058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10799,
            "unit": "ns/op",
            "extra": "212058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 379.28,
            "unit": "MB/s",
            "extra": "212058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 19247,
            "unit": "B/op",
            "extra": "212058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "212058 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 176032,
            "unit": "ns/op\t  93.07 MB/s\t   62647 B/op\t     676 allocs/op",
            "extra": "9399 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 176032,
            "unit": "ns/op",
            "extra": "9399 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 93.07,
            "unit": "MB/s",
            "extra": "9399 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62647,
            "unit": "B/op",
            "extra": "9399 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 676,
            "unit": "allocs/op",
            "extra": "9399 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2260038,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "475 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2260038,
            "unit": "ns/op",
            "extra": "475 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "475 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "475 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1031,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1031,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
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
            "value": 50860,
            "unit": "ns/op\t 161.07 MB/s\t   25773 B/op\t     454 allocs/op",
            "extra": "23116 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50860,
            "unit": "ns/op",
            "extra": "23116 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.07,
            "unit": "MB/s",
            "extra": "23116 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25773,
            "unit": "B/op",
            "extra": "23116 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23116 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6795744,
            "unit": "ns/op\t67523398 B/op\t    2586 allocs/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6795744,
            "unit": "ns/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523398,
            "unit": "B/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "171 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 711.6,
            "unit": "ns/op\t  89.94 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1901800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 711.6,
            "unit": "ns/op",
            "extra": "1901800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 89.94,
            "unit": "MB/s",
            "extra": "1901800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1901800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1901800 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9074014 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.8,
            "unit": "ns/op",
            "extra": "9074014 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9074014 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9074014 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1443,
            "unit": "ns/op\t  44.37 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1443,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.37,
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
            "value": 464.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2527191 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 464.4,
            "unit": "ns/op",
            "extra": "2527191 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2527191 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2527191 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25634,
            "unit": "ns/op\t 319.57 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "75574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25634,
            "unit": "ns/op",
            "extra": "75574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 319.57,
            "unit": "MB/s",
            "extra": "75574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "75574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75574 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 148.9,
            "unit": "ns/op\t1719.73 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8156900 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 148.9,
            "unit": "ns/op",
            "extra": "8156900 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1719.73,
            "unit": "MB/s",
            "extra": "8156900 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8156900 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8156900 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 741.7,
            "unit": "ns/op\t 345.15 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3184046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 741.7,
            "unit": "ns/op",
            "extra": "3184046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 345.15,
            "unit": "MB/s",
            "extra": "3184046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3184046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3184046 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2058386,
            "unit": "ns/op\t 3064048 B/op\t   40019 allocs/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2058386,
            "unit": "ns/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064048,
            "unit": "B/op",
            "extra": "578 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "578 times\n4 procs"
          }
        ]
      }
    ]
  }
}