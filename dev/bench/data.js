window.BENCHMARK_DATA = {
  "lastUpdate": 1772008616991,
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
          "id": "bfb146af06918ace96118c8018ba69c3afc6d45f",
          "message": "Merge pull request #78 from vasilytrofimchuk/fix/art-decref-underflow-guard\n\nfix: add refcount underflow guards to ART.DecrRef and table.DecrRef",
          "timestamp": "2026-02-25T16:35:54+08:00",
          "tree_id": "7ec9a12e6e3c7425d6fa8528095a347d8f124ce8",
          "url": "https://github.com/feichai0017/NoKV/commit/bfb146af06918ace96118c8018ba69c3afc6d45f"
        },
        "date": 1772008616055,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 6640,
            "unit": "ns/op\t   4.82 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "171495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 6640,
            "unit": "ns/op",
            "extra": "171495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.82,
            "unit": "MB/s",
            "extra": "171495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "171495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "171495 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 16901,
            "unit": "ns/op\t 242.35 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "62985 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 16901,
            "unit": "ns/op",
            "extra": "62985 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 242.35,
            "unit": "MB/s",
            "extra": "62985 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "62985 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "62985 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8727,
            "unit": "ns/op\t   7.33 MB/s\t   20196 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8727,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.33,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 20196,
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
            "value": 12335,
            "unit": "ns/op\t 332.06 MB/s\t   33905 B/op\t      11 allocs/op",
            "extra": "310764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 12335,
            "unit": "ns/op",
            "extra": "310764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 332.06,
            "unit": "MB/s",
            "extra": "310764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 33905,
            "unit": "B/op",
            "extra": "310764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "310764 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 125006,
            "unit": "ns/op\t 131.07 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 125006,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 131.07,
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
            "value": 1487574,
            "unit": "ns/op\t       4 B/op\t       0 allocs/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1487574,
            "unit": "ns/op",
            "extra": "802 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 4,
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
            "value": 608.2,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2019022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 608.2,
            "unit": "ns/op",
            "extra": "2019022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2019022 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2019022 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 50160,
            "unit": "ns/op\t 163.32 MB/s\t   27859 B/op\t     454 allocs/op",
            "extra": "24886 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 50160,
            "unit": "ns/op",
            "extra": "24886 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 163.32,
            "unit": "MB/s",
            "extra": "24886 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 27859,
            "unit": "B/op",
            "extra": "24886 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "24886 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6823346,
            "unit": "ns/op\t67523145 B/op\t    2579 allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6823346,
            "unit": "ns/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523145,
            "unit": "B/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
            "unit": "allocs/op",
            "extra": "181 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25766,
            "unit": "ns/op\t 317.93 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "76316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25766,
            "unit": "ns/op",
            "extra": "76316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 317.93,
            "unit": "MB/s",
            "extra": "76316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "76316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "76316 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 164.8,
            "unit": "ns/op\t1553.67 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7345749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 164.8,
            "unit": "ns/op",
            "extra": "7345749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1553.67,
            "unit": "MB/s",
            "extra": "7345749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7345749 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7345749 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 683.5,
            "unit": "ns/op\t 374.53 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3411337 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 683.5,
            "unit": "ns/op",
            "extra": "3411337 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.53,
            "unit": "MB/s",
            "extra": "3411337 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3411337 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3411337 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1994384,
            "unit": "ns/op\t 3064026 B/op\t   40017 allocs/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1994384,
            "unit": "ns/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064026,
            "unit": "B/op",
            "extra": "597 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "597 times\n4 procs"
          }
        ]
      }
    ]
  }
}