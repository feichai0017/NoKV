window.BENCHMARK_DATA = {
  "lastUpdate": 1772258477364,
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
          "id": "4454f5b85dbc4c110b93d25ce766a19deebe73bd",
          "message": "Merge pull request #95 from ByteByteUp/fix/levels-l0-fid-sort-preservation\n\nfix: preserve L0 fid ordering in replaceTables",
          "timestamp": "2026-02-28T14:00:06+08:00",
          "tree_id": "0adc38c62570229273b087cf3d5b788136b91c15",
          "url": "https://github.com/feichai0017/NoKV/commit/4454f5b85dbc4c110b93d25ce766a19deebe73bd"
        },
        "date": 1772258476598,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7401,
            "unit": "ns/op\t   4.32 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "191190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7401,
            "unit": "ns/op",
            "extra": "191190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.32,
            "unit": "MB/s",
            "extra": "191190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "191190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "191190 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 19126,
            "unit": "ns/op\t 214.16 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "57000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 19126,
            "unit": "ns/op",
            "extra": "57000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 214.16,
            "unit": "MB/s",
            "extra": "57000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "57000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "57000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8542,
            "unit": "ns/op\t   7.49 MB/s\t   19444 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8542,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.49,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19444,
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
            "value": 11769,
            "unit": "ns/op\t 348.04 MB/s\t   32741 B/op\t      11 allocs/op",
            "extra": "356588 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11769,
            "unit": "ns/op",
            "extra": "356588 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 348.04,
            "unit": "MB/s",
            "extra": "356588 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32741,
            "unit": "B/op",
            "extra": "356588 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "356588 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 122632,
            "unit": "ns/op\t 133.60 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 122632,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 133.6,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 56847,
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
            "value": 1480762,
            "unit": "ns/op\t       3 B/op\t       0 allocs/op",
            "extra": "788 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1480762,
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
            "value": 588.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1993234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 588.2,
            "unit": "ns/op",
            "extra": "1993234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1993234 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1993234 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch",
            "value": 23.66,
            "unit": "ns/op",
            "extra": "50814760 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 59.99,
            "unit": "ns/op",
            "extra": "19744464 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20.02,
            "unit": "ns/op",
            "extra": "59759386 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.81,
            "unit": "ns/op",
            "extra": "71844680 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 22096574,
            "unit": "ns/op",
            "extra": "57 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 75.39,
            "unit": "ns/op",
            "extra": "15654512 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 51103,
            "unit": "ns/op",
            "extra": "23437 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 48248,
            "unit": "ns/op\t 169.79 MB/s\t   27797 B/op\t     454 allocs/op",
            "extra": "25030 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 48248,
            "unit": "ns/op",
            "extra": "25030 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 169.79,
            "unit": "MB/s",
            "extra": "25030 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27797,
            "unit": "B/op",
            "extra": "25030 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "25030 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6319792,
            "unit": "ns/op\t67523202 B/op\t    2579 allocs/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6319792,
            "unit": "ns/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523202,
            "unit": "B/op",
            "extra": "186 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "186 times\n4 procs"
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
            "value": 26311,
            "unit": "ns/op\t 311.36 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74090 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 26311,
            "unit": "ns/op",
            "extra": "74090 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 311.36,
            "unit": "MB/s",
            "extra": "74090 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74090 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74090 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 159.9,
            "unit": "ns/op\t1600.80 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7324668 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 159.9,
            "unit": "ns/op",
            "extra": "7324668 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1600.8,
            "unit": "MB/s",
            "extra": "7324668 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7324668 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7324668 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 690.5,
            "unit": "ns/op\t 370.77 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3392812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 690.5,
            "unit": "ns/op",
            "extra": "3392812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 370.77,
            "unit": "MB/s",
            "extra": "3392812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3392812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3392812 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2055657,
            "unit": "ns/op\t 3064031 B/op\t   40018 allocs/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2055657,
            "unit": "ns/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064031,
            "unit": "B/op",
            "extra": "596 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "596 times\n4 procs"
          }
        ]
      }
    ]
  }
}