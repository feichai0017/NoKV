window.BENCHMARK_DATA = {
  "lastUpdate": 1771249262683,
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
          "id": "218007c77bff415edc8cd7336431243e3670c08f",
          "message": "Merge pull request #68 from feichai0017/dependabot/docker/golang-1.26\n\ndocker: bump golang from 1.25 to 1.26",
          "timestamp": "2026-02-16T21:39:13+08:00",
          "tree_id": "96e20ef34803d4dcc79147d1e496afb172d0ae22",
          "url": "https://github.com/feichai0017/NoKV/commit/218007c77bff415edc8cd7336431243e3670c08f"
        },
        "date": 1771249222410,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7563,
            "unit": "ns/op\t   4.23 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "144171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7563,
            "unit": "ns/op",
            "extra": "144171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.23,
            "unit": "MB/s",
            "extra": "144171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "144171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "144171 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18218,
            "unit": "ns/op\t 224.83 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "72398 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18218,
            "unit": "ns/op",
            "extra": "72398 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 224.83,
            "unit": "MB/s",
            "extra": "72398 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "72398 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "72398 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8230,
            "unit": "ns/op\t   7.78 MB/s\t   18500 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8230,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.78,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18500,
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
            "value": 12461,
            "unit": "ns/op\t 328.70 MB/s\t   34271 B/op\t      11 allocs/op",
            "extra": "322220 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12461,
            "unit": "ns/op",
            "extra": "322220 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 328.7,
            "unit": "MB/s",
            "extra": "322220 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 34271,
            "unit": "B/op",
            "extra": "322220 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "322220 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122073,
            "unit": "ns/op\t 134.21 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122073,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.21,
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
            "value": 1524174,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1524174,
            "unit": "ns/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "811 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 579.4,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2008378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 579.4,
            "unit": "ns/op",
            "extra": "2008378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2008378 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2008378 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49545,
            "unit": "ns/op\t 165.35 MB/s\t   27841 B/op\t     454 allocs/op",
            "extra": "24928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49545,
            "unit": "ns/op",
            "extra": "24928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 165.35,
            "unit": "MB/s",
            "extra": "24928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27841,
            "unit": "B/op",
            "extra": "24928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24928 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6446792,
            "unit": "ns/op\t67523162 B/op\t    2579 allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6446792,
            "unit": "ns/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523162,
            "unit": "B/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "178 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 619,
            "unit": "ns/op\t 103.40 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "1936519 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 619,
            "unit": "ns/op",
            "extra": "1936519 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 103.4,
            "unit": "MB/s",
            "extra": "1936519 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "1936519 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1936519 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 127.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9457844 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 127.2,
            "unit": "ns/op",
            "extra": "9457844 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9457844 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9457844 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1454,
            "unit": "ns/op\t  44.03 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1454,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 44.03,
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
            "value": 456.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2675458 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 456.3,
            "unit": "ns/op",
            "extra": "2675458 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2675458 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2675458 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26098,
            "unit": "ns/op\t 313.90 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74942 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26098,
            "unit": "ns/op",
            "extra": "74942 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 313.9,
            "unit": "MB/s",
            "extra": "74942 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74942 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74942 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 151,
            "unit": "ns/op\t1695.66 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7649736 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 151,
            "unit": "ns/op",
            "extra": "7649736 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1695.66,
            "unit": "MB/s",
            "extra": "7649736 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7649736 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7649736 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.6,
            "unit": "ns/op\t 368.03 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3422155 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.6,
            "unit": "ns/op",
            "extra": "3422155 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.03,
            "unit": "MB/s",
            "extra": "3422155 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3422155 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3422155 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1998390,
            "unit": "ns/op\t 3064032 B/op\t   40018 allocs/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1998390,
            "unit": "ns/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "602 times\n4 procs"
          }
        ]
      },
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
          "id": "3b4a6daec518ee575d111a90a722625c517fe483",
          "message": "Merge pull request #69 from feichai0017/dependabot/go_modules/google.golang.org/grpc-1.79.1\n\ndeps(deps): bump google.golang.org/grpc from 1.78.0 to 1.79.1",
          "timestamp": "2026-02-16T21:39:32+08:00",
          "tree_id": "9a69288397f32d2d4dc88fe0ea9268d7d8e5e891",
          "url": "https://github.com/feichai0017/NoKV/commit/3b4a6daec518ee575d111a90a722625c517fe483"
        },
        "date": 1771249261562,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7284,
            "unit": "ns/op\t   4.39 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "153012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7284,
            "unit": "ns/op",
            "extra": "153012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.39,
            "unit": "MB/s",
            "extra": "153012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "153012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "153012 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 18765,
            "unit": "ns/op\t 218.28 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "67645 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 18765,
            "unit": "ns/op",
            "extra": "67645 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 218.28,
            "unit": "MB/s",
            "extra": "67645 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "67645 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "67645 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8438,
            "unit": "ns/op\t   7.58 MB/s\t   19688 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8438,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.58,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19688,
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
            "value": 11943,
            "unit": "ns/op\t 342.97 MB/s\t   33022 B/op\t      11 allocs/op",
            "extra": "348733 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11943,
            "unit": "ns/op",
            "extra": "348733 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 342.97,
            "unit": "MB/s",
            "extra": "348733 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33022,
            "unit": "B/op",
            "extra": "348733 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "348733 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121662,
            "unit": "ns/op\t 134.67 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121662,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.67,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56849,
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
            "value": 1504613,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1504613,
            "unit": "ns/op",
            "extra": "788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 3,
            "unit": "B/op",
            "extra": "788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 582.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1937629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 582.5,
            "unit": "ns/op",
            "extra": "1937629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1937629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1937629 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49692,
            "unit": "ns/op\t 164.86 MB/s\t   27390 B/op\t     454 allocs/op",
            "extra": "26013 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49692,
            "unit": "ns/op",
            "extra": "26013 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 164.86,
            "unit": "MB/s",
            "extra": "26013 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27390,
            "unit": "B/op",
            "extra": "26013 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "26013 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6311050,
            "unit": "ns/op\t67523054 B/op\t    2578 allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6311050,
            "unit": "ns/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523054,
            "unit": "B/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "187 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 614.5,
            "unit": "ns/op\t 104.16 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "1970374 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 614.5,
            "unit": "ns/op",
            "extra": "1970374 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 104.16,
            "unit": "MB/s",
            "extra": "1970374 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "1970374 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1970374 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 130.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9359188 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 130.2,
            "unit": "ns/op",
            "extra": "9359188 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9359188 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9359188 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1375,
            "unit": "ns/op\t  46.55 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1375,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.55,
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
            "value": 459.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2571321 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 459.9,
            "unit": "ns/op",
            "extra": "2571321 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2571321 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2571321 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 27255,
            "unit": "ns/op\t 300.57 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74684 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27255,
            "unit": "ns/op",
            "extra": "74684 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 300.57,
            "unit": "MB/s",
            "extra": "74684 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74684 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74684 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 168.2,
            "unit": "ns/op\t1522.02 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "6968590 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 168.2,
            "unit": "ns/op",
            "extra": "6968590 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1522.02,
            "unit": "MB/s",
            "extra": "6968590 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "6968590 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6968590 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 678.2,
            "unit": "ns/op\t 377.47 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3440733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 678.2,
            "unit": "ns/op",
            "extra": "3440733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 377.47,
            "unit": "MB/s",
            "extra": "3440733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3440733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3440733 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2010386,
            "unit": "ns/op\t 3064032 B/op\t   40018 allocs/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2010386,
            "unit": "ns/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064032,
            "unit": "B/op",
            "extra": "592 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "592 times\n4 procs"
          }
        ]
      }
    ]
  }
}