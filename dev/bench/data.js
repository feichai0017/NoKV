window.BENCHMARK_DATA = {
  "lastUpdate": 1772354726784,
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
          "id": "c3c8e81bc6e4c068d6fe9209c20ef01e75de47d1",
          "message": "docs(code): clarify scheduler and PD control-plane semantics",
          "timestamp": "2026-03-01T19:38:59+11:00",
          "tree_id": "0b42c591719814136973a96659d404c59741c015",
          "url": "https://github.com/feichai0017/NoKV/commit/c3c8e81bc6e4c068d6fe9209c20ef01e75de47d1"
        },
        "date": 1772354422319,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 8165,
            "unit": "ns/op\t   3.92 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "140442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 8165,
            "unit": "ns/op",
            "extra": "140442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.92,
            "unit": "MB/s",
            "extra": "140442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "140442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "140442 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16642,
            "unit": "ns/op\t 246.12 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "74354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16642,
            "unit": "ns/op",
            "extra": "74354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 246.12,
            "unit": "MB/s",
            "extra": "74354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "74354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "74354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8412,
            "unit": "ns/op\t   7.61 MB/s\t   19250 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8412,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.61,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19250,
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
            "value": 12541,
            "unit": "ns/op\t 326.61 MB/s\t   35875 B/op\t      11 allocs/op",
            "extra": "319104 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12541,
            "unit": "ns/op",
            "extra": "319104 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 326.61,
            "unit": "MB/s",
            "extra": "319104 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35875,
            "unit": "B/op",
            "extra": "319104 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "319104 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122771,
            "unit": "ns/op\t 133.45 MB/s\t   56846 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122771,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.45,
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
            "value": 1536958,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1536958,
            "unit": "ns/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "782 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 588.5,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2053436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 588.5,
            "unit": "ns/op",
            "extra": "2053436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2053436 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2053436 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.93,
            "unit": "ns/op",
            "extra": "41998358 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 57.26,
            "unit": "ns/op",
            "extra": "20931565 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.04,
            "unit": "ns/op",
            "extra": "60101013 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.85,
            "unit": "ns/op",
            "extra": "72756780 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20462346,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.46,
            "unit": "ns/op",
            "extra": "15799488 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51476,
            "unit": "ns/op",
            "extra": "22806 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49341,
            "unit": "ns/op\t 166.03 MB/s\t   27730 B/op\t     454 allocs/op",
            "extra": "25186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49341,
            "unit": "ns/op",
            "extra": "25186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.03,
            "unit": "MB/s",
            "extra": "25186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27730,
            "unit": "B/op",
            "extra": "25186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6354740,
            "unit": "ns/op\t67523222 B/op\t    2578 allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6354740,
            "unit": "ns/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523222,
            "unit": "B/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 26655,
            "unit": "ns/op\t 307.33 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74714 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26655,
            "unit": "ns/op",
            "extra": "74714 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 307.33,
            "unit": "MB/s",
            "extra": "74714 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74714 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74714 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156.2,
            "unit": "ns/op\t1639.41 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7578810 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156.2,
            "unit": "ns/op",
            "extra": "7578810 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1639.41,
            "unit": "MB/s",
            "extra": "7578810 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7578810 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7578810 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 695.6,
            "unit": "ns/op\t 368.04 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3410689 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 695.6,
            "unit": "ns/op",
            "extra": "3410689 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 368.04,
            "unit": "MB/s",
            "extra": "3410689 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3410689 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3410689 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2008547,
            "unit": "ns/op\t 3064037 B/op\t   40018 allocs/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2008547,
            "unit": "ns/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064037,
            "unit": "B/op",
            "extra": "584 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "584 times\n4 procs"
          }
        ]
      },
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
          "id": "a553a55faa06f941d6182082856324819c44e7ed",
          "message": "docs: align architecture docs with PD-first control plane",
          "timestamp": "2026-03-01T18:27:40+11:00",
          "tree_id": "b4e54b93080faafdf009c57d811ec58fb97d25dc",
          "url": "https://github.com/feichai0017/NoKV/commit/a553a55faa06f941d6182082856324819c44e7ed"
        },
        "date": 1772354725711,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7950,
            "unit": "ns/op\t   4.02 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7950,
            "unit": "ns/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.02,
            "unit": "MB/s",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "151131 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 13870,
            "unit": "ns/op\t 295.32 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "117770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 13870,
            "unit": "ns/op",
            "extra": "117770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 295.32,
            "unit": "MB/s",
            "extra": "117770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "117770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "117770 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 7186,
            "unit": "ns/op\t   8.91 MB/s\t   17239 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 7186,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 8.91,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 17239,
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
            "value": 8804,
            "unit": "ns/op\t 465.25 MB/s\t   26898 B/op\t      11 allocs/op",
            "extra": "290496 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 8804,
            "unit": "ns/op",
            "extra": "290496 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 465.25,
            "unit": "MB/s",
            "extra": "290496 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 26898,
            "unit": "B/op",
            "extra": "290496 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "290496 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120920,
            "unit": "ns/op\t 135.49 MB/s\t   56849 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120920,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.49,
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
            "value": 1635390,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1635390,
            "unit": "ns/op",
            "extra": "723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "723 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 640.6,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2121198 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 640.6,
            "unit": "ns/op",
            "extra": "2121198 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2121198 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2121198 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 30.78,
            "unit": "ns/op",
            "extra": "38805228 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 81.52,
            "unit": "ns/op",
            "extra": "14870188 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 23.82,
            "unit": "ns/op",
            "extra": "50292115 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 14.59,
            "unit": "ns/op",
            "extra": "82674361 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20221132,
            "unit": "ns/op",
            "extra": "58 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 82.67,
            "unit": "ns/op",
            "extra": "14430279 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 70812,
            "unit": "ns/op",
            "extra": "17907 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48532,
            "unit": "ns/op\t 168.80 MB/s\t   27672 B/op\t     454 allocs/op",
            "extra": "25323 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48532,
            "unit": "ns/op",
            "extra": "25323 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 168.8,
            "unit": "MB/s",
            "extra": "25323 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27672,
            "unit": "B/op",
            "extra": "25323 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25323 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 8682427,
            "unit": "ns/op\t67523585 B/op\t    2580 allocs/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 8682427,
            "unit": "ns/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523585,
            "unit": "B/op",
            "extra": "136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2580,
            "unit": "allocs/op",
            "extra": "136 times\n4 procs"
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
            "value": 23216,
            "unit": "ns/op\t 352.87 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "102028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 23216,
            "unit": "ns/op",
            "extra": "102028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 352.87,
            "unit": "MB/s",
            "extra": "102028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "102028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "102028 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 156,
            "unit": "ns/op\t1640.84 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7563690 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 156,
            "unit": "ns/op",
            "extra": "7563690 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1640.84,
            "unit": "MB/s",
            "extra": "7563690 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7563690 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7563690 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 635.3,
            "unit": "ns/op\t 402.97 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "4169829 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 635.3,
            "unit": "ns/op",
            "extra": "4169829 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 402.97,
            "unit": "MB/s",
            "extra": "4169829 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "4169829 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "4169829 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1968998,
            "unit": "ns/op\t 3064039 B/op\t   40018 allocs/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1968998,
            "unit": "ns/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064039,
            "unit": "B/op",
            "extra": "603 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "603 times\n4 procs"
          }
        ]
      }
    ]
  }
}