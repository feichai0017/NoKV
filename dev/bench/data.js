window.BENCHMARK_DATA = {
  "lastUpdate": 1771132515324,
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
          "id": "a849919ef5ea6dc6ea50b141bbc6385f6b38c792",
          "message": "fix: harden DB close semantics and closer shutdown",
          "timestamp": "2026-02-15T13:13:53+08:00",
          "tree_id": "a0fbc0613091acbb24411e634780621a09127088",
          "url": "https://github.com/feichai0017/NoKV/commit/a849919ef5ea6dc6ea50b141bbc6385f6b38c792"
        },
        "date": 1771132513978,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8267,
            "unit": "ns/op\t   3.87 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "182383 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8267,
            "unit": "ns/op",
            "extra": "182383 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.87,
            "unit": "MB/s",
            "extra": "182383 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "182383 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "182383 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17346,
            "unit": "ns/op\t 236.14 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "68920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17346,
            "unit": "ns/op",
            "extra": "68920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 236.14,
            "unit": "MB/s",
            "extra": "68920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "68920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "68920 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 9070,
            "unit": "ns/op\t   7.06 MB/s\t   20881 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 9070,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.06,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20881,
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
            "value": 9122,
            "unit": "ns/op\t 449.02 MB/s\t   25911 B/op\t      11 allocs/op",
            "extra": "306477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9122,
            "unit": "ns/op",
            "extra": "306477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 449.02,
            "unit": "MB/s",
            "extra": "306477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 25911,
            "unit": "B/op",
            "extra": "306477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "306477 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122531,
            "unit": "ns/op\t 133.71 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122531,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.71,
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
            "value": 1531379,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1531379,
            "unit": "ns/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 585.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1862946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 585.9,
            "unit": "ns/op",
            "extra": "1862946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1862946 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1862946 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 52545,
            "unit": "ns/op\t 155.90 MB/s\t   27982 B/op\t     454 allocs/op",
            "extra": "24608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 52545,
            "unit": "ns/op",
            "extra": "24608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 155.9,
            "unit": "MB/s",
            "extra": "24608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27982,
            "unit": "B/op",
            "extra": "24608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24608 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6916164,
            "unit": "ns/op\t67523153 B/op\t    2580 allocs/op",
            "extra": "148 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6916164,
            "unit": "ns/op",
            "extra": "148 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523153,
            "unit": "B/op",
            "extra": "148 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2580,
            "unit": "allocs/op",
            "extra": "148 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 612.9,
            "unit": "ns/op\t 104.42 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1931116 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 612.9,
            "unit": "ns/op",
            "extra": "1931116 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 104.42,
            "unit": "MB/s",
            "extra": "1931116 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1931116 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1931116 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9393422 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.8,
            "unit": "ns/op",
            "extra": "9393422 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9393422 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9393422 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1456,
            "unit": "ns/op\t  43.96 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1456,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 43.96,
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
            "value": 490.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2599473 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 490.5,
            "unit": "ns/op",
            "extra": "2599473 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2599473 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2599473 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27264,
            "unit": "ns/op\t 300.47 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "72632 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27264,
            "unit": "ns/op",
            "extra": "72632 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 300.47,
            "unit": "MB/s",
            "extra": "72632 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "72632 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "72632 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 160.6,
            "unit": "ns/op\t1593.76 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7529745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 160.6,
            "unit": "ns/op",
            "extra": "7529745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1593.76,
            "unit": "MB/s",
            "extra": "7529745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7529745 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7529745 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 669.2,
            "unit": "ns/op\t 382.57 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3495680 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 669.2,
            "unit": "ns/op",
            "extra": "3495680 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 382.57,
            "unit": "MB/s",
            "extra": "3495680 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3495680 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3495680 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2125815,
            "unit": "ns/op\t 3064042 B/op\t   40018 allocs/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2125815,
            "unit": "ns/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064042,
            "unit": "B/op",
            "extra": "552 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "552 times\n4 procs"
          }
        ]
      }
    ]
  }
}