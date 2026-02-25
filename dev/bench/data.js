window.BENCHMARK_DATA = {
  "lastUpdate": 1772035183329,
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
          "id": "73599d3fc90fabae5e1eea2734743defc840415e",
          "message": "Merge pull request #82 from ByteByteUp/fix/vlog-donewriting-durability\n\nfix: enforce strong durability semantics in LogFile.DoneWriting",
          "timestamp": "2026-02-25T23:58:27+08:00",
          "tree_id": "32274c16d914f99169373af5e8148112bc14dedb",
          "url": "https://github.com/feichai0017/NoKV/commit/73599d3fc90fabae5e1eea2734743defc840415e"
        },
        "date": 1772035182323,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 7483,
            "unit": "ns/op\t   4.28 MB/s\t     344 B/op\t      15 allocs/op",
            "extra": "172107 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 7483,
            "unit": "ns/op",
            "extra": "172107 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 4.28,
            "unit": "MB/s",
            "extra": "172107 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 344,
            "unit": "B/op",
            "extra": "172107 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 15,
            "unit": "allocs/op",
            "extra": "172107 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 17350,
            "unit": "ns/op\t 236.08 MB/s\t     538 B/op\t      23 allocs/op",
            "extra": "77872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 17350,
            "unit": "ns/op",
            "extra": "77872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 236.08,
            "unit": "MB/s",
            "extra": "77872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 538,
            "unit": "B/op",
            "extra": "77872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 23,
            "unit": "allocs/op",
            "extra": "77872 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 8365,
            "unit": "ns/op\t   7.65 MB/s\t   19297 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 8365,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 7.65,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 19297,
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
            "value": 11576,
            "unit": "ns/op\t 353.84 MB/s\t   32270 B/op\t      11 allocs/op",
            "extra": "362528 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 11576,
            "unit": "ns/op",
            "extra": "362528 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 353.84,
            "unit": "MB/s",
            "extra": "362528 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 32270,
            "unit": "B/op",
            "extra": "362528 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "362528 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 121956,
            "unit": "ns/op\t 134.34 MB/s\t   56847 B/op\t     659 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 121956,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 134.34,
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
            "value": 1499171,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 1499171,
            "unit": "ns/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "816 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 613.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "2036629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 613.1,
            "unit": "ns/op",
            "extra": "2036629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "2036629 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "2036629 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 46641,
            "unit": "ns/op\t 175.64 MB/s\t   25453 B/op\t     454 allocs/op",
            "extra": "23992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 46641,
            "unit": "ns/op",
            "extra": "23992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 175.64,
            "unit": "MB/s",
            "extra": "23992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25453,
            "unit": "B/op",
            "extra": "23992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23992 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6555483,
            "unit": "ns/op\t67523234 B/op\t    2579 allocs/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6555483,
            "unit": "ns/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523234,
            "unit": "B/op",
            "extra": "183 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2579,
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
            "value": 27496,
            "unit": "ns/op\t 297.94 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "74706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 27496,
            "unit": "ns/op",
            "extra": "74706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 297.94,
            "unit": "MB/s",
            "extra": "74706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "74706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "74706 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 159.9,
            "unit": "ns/op\t1600.99 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "7460552 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 159.9,
            "unit": "ns/op",
            "extra": "7460552 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1600.99,
            "unit": "MB/s",
            "extra": "7460552 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "7460552 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "7460552 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 684,
            "unit": "ns/op\t 374.25 MB/s\t      36 B/op\t       5 allocs/op",
            "extra": "3459988 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 684,
            "unit": "ns/op",
            "extra": "3459988 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 374.25,
            "unit": "MB/s",
            "extra": "3459988 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 36,
            "unit": "B/op",
            "extra": "3459988 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "3459988 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 1990373,
            "unit": "ns/op\t 3064031 B/op\t   40017 allocs/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 1990373,
            "unit": "ns/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064031,
            "unit": "B/op",
            "extra": "602 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "602 times\n4 procs"
          }
        ]
      }
    ]
  }
}