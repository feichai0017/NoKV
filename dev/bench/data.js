window.BENCHMARK_DATA = {
  "lastUpdate": 1772338377718,
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
          "id": "eaf3c122ee46e183da76e263aaf5a18e5f070f53",
          "message": "bump version to 1.26.0",
          "timestamp": "2026-03-01T15:11:39+11:00",
          "tree_id": "9e57d707120437906898b4a88b497d128a35d66e",
          "url": "https://github.com/feichai0017/NoKV/commit/eaf3c122ee46e183da76e263aaf5a18e5f070f53"
        },
        "date": 1772338376727,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6895,
            "unit": "ns/op\t   4.64 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "174056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6895,
            "unit": "ns/op",
            "extra": "174056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.64,
            "unit": "MB/s",
            "extra": "174056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "174056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "174056 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19942,
            "unit": "ns/op\t 205.40 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "70608 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19942,
            "unit": "ns/op",
            "extra": "70608 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 205.4,
            "unit": "MB/s",
            "extra": "70608 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "70608 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "70608 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7913,
            "unit": "ns/op\t   8.09 MB/s\t   17723 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7913,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.09,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17723,
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
            "value": 11909,
            "unit": "ns/op\t 343.96 MB/s\t   33279 B/op\t      11 allocs/op",
            "extra": "353688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11909,
            "unit": "ns/op",
            "extra": "353688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 343.96,
            "unit": "MB/s",
            "extra": "353688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33279,
            "unit": "B/op",
            "extra": "353688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "353688 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125675,
            "unit": "ns/op\t 130.37 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125675,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 130.37,
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
            "value": 1545199,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1545199,
            "unit": "ns/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
            "unit": "B/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "776 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 608.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2023507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 608.2,
            "unit": "ns/op",
            "extra": "2023507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2023507 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2023507 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.69,
            "unit": "ns/op",
            "extra": "49007883 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.62,
            "unit": "ns/op",
            "extra": "20180016 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 19.99,
            "unit": "ns/op",
            "extra": "58870018 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.84,
            "unit": "ns/op",
            "extra": "72441321 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20591920,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 76.04,
            "unit": "ns/op",
            "extra": "15661795 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 52406,
            "unit": "ns/op",
            "extra": "23073 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50656,
            "unit": "ns/op\t 161.72 MB/s\t   27850 B/op\t     454 allocs/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50656,
            "unit": "ns/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 161.72,
            "unit": "MB/s",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27850,
            "unit": "B/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24909 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6571454,
            "unit": "ns/op\t67523129 B/op\t    2578 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6571454,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523129,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "183 times\n4 procs"
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
            "value": 26675,
            "unit": "ns/op\t 307.10 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73365 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26675,
            "unit": "ns/op",
            "extra": "73365 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.1,
            "unit": "MB/s",
            "extra": "73365 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73365 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73365 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.8,
            "unit": "ns/op\t1611.85 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7469702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.8,
            "unit": "ns/op",
            "extra": "7469702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1611.85,
            "unit": "MB/s",
            "extra": "7469702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7469702 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7469702 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 696.1,
            "unit": "ns/op\t 367.77 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3319935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 696.1,
            "unit": "ns/op",
            "extra": "3319935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 367.77,
            "unit": "MB/s",
            "extra": "3319935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3319935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3319935 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2024326,
            "unit": "ns/op\t 3064035 B/op\t   40018 allocs/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2024326,
            "unit": "ns/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064035,
            "unit": "B/op",
            "extra": "579 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "579 times\n4 procs"
          }
        ]
      }
    ]
  }
}