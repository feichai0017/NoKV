window.BENCHMARK_DATA = {
  "lastUpdate": 1771131643660,
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
          "id": "6b5ffc97344111630b67a4955ce7add89934881d",
          "message": "chore: fix lint violations and pin golangci-lint toolchain",
          "timestamp": "2026-02-15T12:59:09+08:00",
          "tree_id": "4c25c622864feb8f6f5922af0434fa928f461d1f",
          "url": "https://github.com/feichai0017/NoKV/commit/6b5ffc97344111630b67a4955ce7add89934881d"
        },
        "date": 1771131642250,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7332,
            "unit": "ns/op\t   4.36 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "159654 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7332,
            "unit": "ns/op",
            "extra": "159654 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.36,
            "unit": "MB/s",
            "extra": "159654 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "159654 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "159654 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19714,
            "unit": "ns/op\t 207.78 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "73540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19714,
            "unit": "ns/op",
            "extra": "73540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 207.78,
            "unit": "MB/s",
            "extra": "73540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "73540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "73540 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8617,
            "unit": "ns/op\t   7.43 MB/s\t   19229 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8617,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.43,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19229,
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
            "value": 12149,
            "unit": "ns/op\t 337.15 MB/s\t   34408 B/op\t      11 allocs/op",
            "extra": "325852 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12149,
            "unit": "ns/op",
            "extra": "325852 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 337.15,
            "unit": "MB/s",
            "extra": "325852 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34408,
            "unit": "B/op",
            "extra": "325852 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "325852 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 123681,
            "unit": "ns/op\t 132.47 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 123681,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 132.47,
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
            "value": 1509705,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1509705,
            "unit": "ns/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "812 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 644,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1955038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 644,
            "unit": "ns/op",
            "extra": "1955038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1955038 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1955038 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48702,
            "unit": "ns/op\t 168.21 MB/s\t   28078 B/op\t     454 allocs/op",
            "extra": "24393 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48702,
            "unit": "ns/op",
            "extra": "24393 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.21,
            "unit": "MB/s",
            "extra": "24393 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 28078,
            "unit": "B/op",
            "extra": "24393 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24393 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6234561,
            "unit": "ns/op\t67523033 B/op\t    2578 allocs/op",
            "extra": "192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6234561,
            "unit": "ns/op",
            "extra": "192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523033,
            "unit": "B/op",
            "extra": "192 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "192 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 542.2,
            "unit": "ns/op\t 118.03 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2235505 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 542.2,
            "unit": "ns/op",
            "extra": "2235505 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 118.03,
            "unit": "MB/s",
            "extra": "2235505 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2235505 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2235505 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9427720 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.5,
            "unit": "ns/op",
            "extra": "9427720 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9427720 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9427720 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1365,
            "unit": "ns/op\t  46.87 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1365,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.87,
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
            "value": 452.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2412913 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 452.9,
            "unit": "ns/op",
            "extra": "2412913 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2412913 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2412913 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27058,
            "unit": "ns/op\t 302.76 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "73402 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27058,
            "unit": "ns/op",
            "extra": "73402 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 302.76,
            "unit": "MB/s",
            "extra": "73402 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "73402 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "73402 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 158.4,
            "unit": "ns/op\t1616.54 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7234464 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 158.4,
            "unit": "ns/op",
            "extra": "7234464 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1616.54,
            "unit": "MB/s",
            "extra": "7234464 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7234464 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7234464 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 690.7,
            "unit": "ns/op\t 370.63 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3492226 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 690.7,
            "unit": "ns/op",
            "extra": "3492226 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.63,
            "unit": "MB/s",
            "extra": "3492226 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3492226 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3492226 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2033577,
            "unit": "ns/op\t 3064033 B/op\t   40018 allocs/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2033577,
            "unit": "ns/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064033,
            "unit": "B/op",
            "extra": "588 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "588 times\n4 procs"
          }
        ]
      }
    ]
  }
}