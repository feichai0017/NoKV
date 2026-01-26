window.BENCHMARK_DATA = {
  "lastUpdate": 1769424845022,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "Guocheng Song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8837fed457a878d910f5b0d4ec8d99b6961a6c67",
          "message": "Merge pull request #31 from feichai0017/dependabot/go_modules/github.com/dgraph-io/ristretto/v2-2.4.0",
          "timestamp": "2026-01-26T18:52:33+08:00",
          "tree_id": "1252503c502638d71cdc631e05ecce21b1f6ed56",
          "url": "https://github.com/feichai0017/NoKV/commit/8837fed457a878d910f5b0d4ec8d99b6961a6c67"
        },
        "date": 1769424843461,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 13832,
            "unit": "ns/op\t   2.31 MB/s\t     626 B/op\t      24 allocs/op",
            "extra": "122839 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 13832,
            "unit": "ns/op",
            "extra": "122839 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.31,
            "unit": "MB/s",
            "extra": "122839 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 626,
            "unit": "B/op",
            "extra": "122839 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "122839 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15603,
            "unit": "ns/op\t 262.51 MB/s\t     651 B/op\t      27 allocs/op",
            "extra": "91918 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15603,
            "unit": "ns/op",
            "extra": "91918 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 262.51,
            "unit": "MB/s",
            "extra": "91918 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 651,
            "unit": "B/op",
            "extra": "91918 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "91918 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11562,
            "unit": "ns/op\t   5.54 MB/s\t   19502 B/op\t       5 allocs/op",
            "extra": "864856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11562,
            "unit": "ns/op",
            "extra": "864856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.54,
            "unit": "MB/s",
            "extra": "864856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19502,
            "unit": "B/op",
            "extra": "864856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "864856 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9643,
            "unit": "ns/op\t 424.77 MB/s\t   18403 B/op\t       7 allocs/op",
            "extra": "257814 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9643,
            "unit": "ns/op",
            "extra": "257814 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 424.77,
            "unit": "MB/s",
            "extra": "257814 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18403,
            "unit": "B/op",
            "extra": "257814 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "257814 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 171418,
            "unit": "ns/op\t  95.58 MB/s\t   63695 B/op\t     681 allocs/op",
            "extra": "9200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 171418,
            "unit": "ns/op",
            "extra": "9200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 95.58,
            "unit": "MB/s",
            "extra": "9200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 63695,
            "unit": "B/op",
            "extra": "9200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 681,
            "unit": "allocs/op",
            "extra": "9200 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2146211,
            "unit": "ns/op\t    8528 B/op\t       0 allocs/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2146211,
            "unit": "ns/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 8528,
            "unit": "B/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 947.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1263164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 947.9,
            "unit": "ns/op",
            "extra": "1263164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1263164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1263164 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49310,
            "unit": "ns/op\t 166.13 MB/s\t   28123 B/op\t     454 allocs/op",
            "extra": "24296 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49310,
            "unit": "ns/op",
            "extra": "24296 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.13,
            "unit": "MB/s",
            "extra": "24296 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28123,
            "unit": "B/op",
            "extra": "24296 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24296 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6721082,
            "unit": "ns/op\t67523347 B/op\t    2586 allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6721082,
            "unit": "ns/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523347,
            "unit": "B/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "174 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 651.2,
            "unit": "ns/op\t  98.29 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1949424 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 651.2,
            "unit": "ns/op",
            "extra": "1949424 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 98.29,
            "unit": "MB/s",
            "extra": "1949424 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1949424 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1949424 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 126.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9555480 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 126.5,
            "unit": "ns/op",
            "extra": "9555480 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9555480 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9555480 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1443,
            "unit": "ns/op\t  44.35 MB/s\t     162 B/op\t       1 allocs/op",
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
            "value": 44.35,
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
            "value": 484.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2558850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 484.3,
            "unit": "ns/op",
            "extra": "2558850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2558850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2558850 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 24559,
            "unit": "ns/op\t 333.57 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "77466 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 24559,
            "unit": "ns/op",
            "extra": "77466 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 333.57,
            "unit": "MB/s",
            "extra": "77466 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "77466 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "77466 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145.9,
            "unit": "ns/op\t1754.04 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8140682 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145.9,
            "unit": "ns/op",
            "extra": "8140682 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1754.04,
            "unit": "MB/s",
            "extra": "8140682 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8140682 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8140682 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 729.6,
            "unit": "ns/op\t 350.87 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3209499 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 729.6,
            "unit": "ns/op",
            "extra": "3209499 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.87,
            "unit": "MB/s",
            "extra": "3209499 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3209499 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3209499 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2006840,
            "unit": "ns/op\t 3064057 B/op\t   40019 allocs/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2006840,
            "unit": "ns/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064057,
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