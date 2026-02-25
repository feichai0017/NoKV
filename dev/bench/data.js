window.BENCHMARK_DATA = {
  "lastUpdate": 1772035755243,
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
          "id": "4efeaa1c5ebcbae37184fb0776cd647c94048634",
          "message": "fix: harden vlog truncate state and sstable stat handling",
          "timestamp": "2026-02-26T00:06:30+08:00",
          "tree_id": "15681f7eb0dee943118bd786b8b36b90c88643aa",
          "url": "https://github.com/feichai0017/NoKV/commit/4efeaa1c5ebcbae37184fb0776cd647c94048634"
        },
        "date": 1772035754293,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6835,
            "unit": "ns/op\t   4.68 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "154076 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6835,
            "unit": "ns/op",
            "extra": "154076 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.68,
            "unit": "MB/s",
            "extra": "154076 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "154076 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "154076 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16502,
            "unit": "ns/op\t 248.21 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "84218 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16502,
            "unit": "ns/op",
            "extra": "84218 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 248.21,
            "unit": "MB/s",
            "extra": "84218 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "84218 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "84218 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8139,
            "unit": "ns/op\t   7.86 MB/s\t   18858 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8139,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.86,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 18858,
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
            "value": 12045,
            "unit": "ns/op\t 340.05 MB/s\t   35277 B/op\t      11 allocs/op",
            "extra": "330625 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12045,
            "unit": "ns/op",
            "extra": "330625 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 340.05,
            "unit": "MB/s",
            "extra": "330625 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 35277,
            "unit": "B/op",
            "extra": "330625 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "330625 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 120613,
            "unit": "ns/op\t 135.84 MB/s\t   56848 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 120613,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 135.84,
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
            "value": 1510608,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1510608,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 596.9,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1955628 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 596.9,
            "unit": "ns/op",
            "extra": "1955628 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1955628 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1955628 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49323,
            "unit": "ns/op\t 166.09 MB/s\t   27945 B/op\t     454 allocs/op",
            "extra": "24690 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49323,
            "unit": "ns/op",
            "extra": "24690 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.09,
            "unit": "MB/s",
            "extra": "24690 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27945,
            "unit": "B/op",
            "extra": "24690 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24690 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6325359,
            "unit": "ns/op\t67523080 B/op\t    2578 allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6325359,
            "unit": "ns/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523080,
            "unit": "B/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2578,
            "unit": "allocs/op",
            "extra": "190 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25911,
            "unit": "ns/op\t 316.16 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "78470 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25911,
            "unit": "ns/op",
            "extra": "78470 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 316.16,
            "unit": "MB/s",
            "extra": "78470 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "78470 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "78470 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 155.2,
            "unit": "ns/op\t1649.02 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7690608 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 155.2,
            "unit": "ns/op",
            "extra": "7690608 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1649.02,
            "unit": "MB/s",
            "extra": "7690608 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7690608 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7690608 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 707.8,
            "unit": "ns/op\t 361.66 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3408654 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 707.8,
            "unit": "ns/op",
            "extra": "3408654 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 361.66,
            "unit": "MB/s",
            "extra": "3408654 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3408654 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3408654 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2046109,
            "unit": "ns/op\t 3064026 B/op\t   40017 allocs/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2046109,
            "unit": "ns/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064026,
            "unit": "B/op",
            "extra": "600 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "600 times\n4 procs"
          }
        ]
      }
    ]
  }
}