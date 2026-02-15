window.BENCHMARK_DATA = {
  "lastUpdate": 1771130570988,
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
          "id": "cc8dccc24cb3e031f9fa939d4be1041cbde95bb8",
          "message": "Merge pull request #65 from ByteByteUp/fix/db-close-short-circuits-cleanup\n\nfix: attempt to close all resources in DB.Close() even on error",
          "timestamp": "2026-02-15T12:41:33+08:00",
          "tree_id": "c399fc7b9a8642b5e6d8fd1c02224f896caba9f4",
          "url": "https://github.com/feichai0017/NoKV/commit/cc8dccc24cb3e031f9fa939d4be1041cbde95bb8"
        },
        "date": 1771130569135,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7280,
            "unit": "ns/op\t   4.40 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "164686 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7280,
            "unit": "ns/op",
            "extra": "164686 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.4,
            "unit": "MB/s",
            "extra": "164686 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "164686 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "164686 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17134,
            "unit": "ns/op\t 239.06 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "67813 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17134,
            "unit": "ns/op",
            "extra": "67813 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 239.06,
            "unit": "MB/s",
            "extra": "67813 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "67813 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "67813 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12037,
            "unit": "ns/op\t   5.32 MB/s\t   29981 B/op\t       9 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12037,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.32,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 29981,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 12873,
            "unit": "ns/op\t 318.18 MB/s\t   35574 B/op\t      11 allocs/op",
            "extra": "308936 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12873,
            "unit": "ns/op",
            "extra": "308936 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 318.18,
            "unit": "MB/s",
            "extra": "308936 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35574,
            "unit": "B/op",
            "extra": "308936 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "308936 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123229,
            "unit": "ns/op\t 132.96 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123229,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.96,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56846,
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
            "value": 1525193,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1525193,
            "unit": "ns/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "792 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 598.7,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1939575 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 598.7,
            "unit": "ns/op",
            "extra": "1939575 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1939575 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1939575 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48476,
            "unit": "ns/op\t 168.99 MB/s\t   25610 B/op\t     454 allocs/op",
            "extra": "23553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48476,
            "unit": "ns/op",
            "extra": "23553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.99,
            "unit": "MB/s",
            "extra": "23553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25610,
            "unit": "B/op",
            "extra": "23553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23553 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6577392,
            "unit": "ns/op\t67523094 B/op\t    2579 allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6577392,
            "unit": "ns/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523094,
            "unit": "B/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "180 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 607,
            "unit": "ns/op\t 105.44 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1937404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 607,
            "unit": "ns/op",
            "extra": "1937404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 105.44,
            "unit": "MB/s",
            "extra": "1937404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1937404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1937404 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 129,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9417482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 129,
            "unit": "ns/op",
            "extra": "9417482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9417482 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9417482 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1402,
            "unit": "ns/op\t  45.63 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1402,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 45.63,
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
            "value": 480,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2441822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 480,
            "unit": "ns/op",
            "extra": "2441822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2441822 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2441822 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26991,
            "unit": "ns/op\t 303.51 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26991,
            "unit": "ns/op",
            "extra": "73758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 303.51,
            "unit": "MB/s",
            "extra": "73758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73758 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.8,
            "unit": "ns/op\t1632.30 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7629510 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.8,
            "unit": "ns/op",
            "extra": "7629510 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1632.3,
            "unit": "MB/s",
            "extra": "7629510 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7629510 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7629510 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 684.2,
            "unit": "ns/op\t 374.17 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3453592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 684.2,
            "unit": "ns/op",
            "extra": "3453592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.17,
            "unit": "MB/s",
            "extra": "3453592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3453592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3453592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2040323,
            "unit": "ns/op\t 3064041 B/op\t   40018 allocs/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2040323,
            "unit": "ns/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064041,
            "unit": "B/op",
            "extra": "583 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "583 times\n4 procs"
          }
        ]
      }
    ]
  }
}