window.BENCHMARK_DATA = {
  "lastUpdate": 1769240973710,
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
          "id": "1e1944d89b6ac284ee7ebd8ce1f59a8be3cb63aa",
          "message": "Update docs",
          "timestamp": "2026-01-24T15:48:20+08:00",
          "tree_id": "8206b358ef09c215504d0ad85de2786688d3cf7a",
          "url": "https://github.com/feichai0017/NoKV/commit/1e1944d89b6ac284ee7ebd8ce1f59a8be3cb63aa"
        },
        "date": 1769240972405,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 14404,
            "unit": "ns/op\t   2.22 MB/s\t     625 B/op\t      24 allocs/op",
            "extra": "136657 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 14404,
            "unit": "ns/op",
            "extra": "136657 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 2.22,
            "unit": "MB/s",
            "extra": "136657 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 625,
            "unit": "B/op",
            "extra": "136657 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 24,
            "unit": "allocs/op",
            "extra": "136657 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16949,
            "unit": "ns/op\t 241.66 MB/s\t     661 B/op\t      27 allocs/op",
            "extra": "85684 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16949,
            "unit": "ns/op",
            "extra": "85684 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 241.66,
            "unit": "MB/s",
            "extra": "85684 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 661,
            "unit": "B/op",
            "extra": "85684 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "85684 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 11324,
            "unit": "ns/op\t   5.65 MB/s\t   16900 B/op\t       5 allocs/op",
            "extra": "546784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 11324,
            "unit": "ns/op",
            "extra": "546784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.65,
            "unit": "MB/s",
            "extra": "546784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16900,
            "unit": "B/op",
            "extra": "546784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "546784 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 10345,
            "unit": "ns/op\t 395.95 MB/s\t   17620 B/op\t       7 allocs/op",
            "extra": "235447 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 10345,
            "unit": "ns/op",
            "extra": "235447 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 395.95,
            "unit": "MB/s",
            "extra": "235447 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 17620,
            "unit": "B/op",
            "extra": "235447 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "235447 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 178407,
            "unit": "ns/op\t  91.83 MB/s\t   62444 B/op\t     675 allocs/op",
            "extra": "9439 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 178407,
            "unit": "ns/op",
            "extra": "9439 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 91.83,
            "unit": "MB/s",
            "extra": "9439 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 62444,
            "unit": "B/op",
            "extra": "9439 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 675,
            "unit": "allocs/op",
            "extra": "9439 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2926810,
            "unit": "ns/op\t    9663 B/op\t       0 allocs/op",
            "extra": "511 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2926810,
            "unit": "ns/op",
            "extra": "511 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 9663,
            "unit": "B/op",
            "extra": "511 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "511 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 1095,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "941443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 1095,
            "unit": "ns/op",
            "extra": "941443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "941443 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "941443 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50469,
            "unit": "ns/op\t 162.32 MB/s\t   28126 B/op\t     454 allocs/op",
            "extra": "24288 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50469,
            "unit": "ns/op",
            "extra": "24288 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 162.32,
            "unit": "MB/s",
            "extra": "24288 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28126,
            "unit": "B/op",
            "extra": "24288 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24288 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 7232060,
            "unit": "ns/op\t67523170 B/op\t    2586 allocs/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 7232060,
            "unit": "ns/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523170,
            "unit": "B/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2586,
            "unit": "allocs/op",
            "extra": "165 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 688.5,
            "unit": "ns/op\t  92.96 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1780875 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 688.5,
            "unit": "ns/op",
            "extra": "1780875 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 92.96,
            "unit": "MB/s",
            "extra": "1780875 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1780875 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1780875 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9389070 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.9,
            "unit": "ns/op",
            "extra": "9389070 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9389070 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9389070 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1438,
            "unit": "ns/op\t  44.50 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1438,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.5,
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
            "value": 479.4,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2499394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 479.4,
            "unit": "ns/op",
            "extra": "2499394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2499394 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2499394 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25820,
            "unit": "ns/op\t 317.27 MB/s\t    1666 B/op\t      35 allocs/op",
            "extra": "75085 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25820,
            "unit": "ns/op",
            "extra": "75085 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 317.27,
            "unit": "MB/s",
            "extra": "75085 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1666,
            "unit": "B/op",
            "extra": "75085 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75085 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 149.4,
            "unit": "ns/op\t1713.23 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8085302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 149.4,
            "unit": "ns/op",
            "extra": "8085302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1713.23,
            "unit": "MB/s",
            "extra": "8085302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8085302 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8085302 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 739.2,
            "unit": "ns/op\t 346.30 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3123082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 739.2,
            "unit": "ns/op",
            "extra": "3123082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 346.3,
            "unit": "MB/s",
            "extra": "3123082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3123082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3123082 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2094446,
            "unit": "ns/op\t 3064047 B/op\t   40019 allocs/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2094446,
            "unit": "ns/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064047,
            "unit": "B/op",
            "extra": "562 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "562 times\n4 procs"
          }
        ]
      }
    ]
  }
}