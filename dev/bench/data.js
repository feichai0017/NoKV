window.BENCHMARK_DATA = {
  "lastUpdate": 1773301102733,
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
          "id": "8dcd99094bb16639c5d21863422a78e6d22e38ba",
          "message": "fix: make compact state deletion atomic and propagate cleanup errors",
          "timestamp": "2026-03-12T18:36:53+11:00",
          "tree_id": "6c3f2590e2eb8010873d4f2e56da3f26dd6bef52",
          "url": "https://github.com/feichai0017/NoKV/commit/8dcd99094bb16639c5d21863422a78e6d22e38ba"
        },
        "date": 1773301101658,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 8804,
            "unit": "ns/op\t   3.63 MB/s\t    2392 B/op\t      17 allocs/op",
            "extra": "147120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 8804,
            "unit": "ns/op",
            "extra": "147120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 3.63,
            "unit": "MB/s",
            "extra": "147120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 2392,
            "unit": "B/op",
            "extra": "147120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 17,
            "unit": "allocs/op",
            "extra": "147120 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 16726,
            "unit": "ns/op\t 244.88 MB/s\t    2444 B/op\t      27 allocs/op",
            "extra": "75482 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 16726,
            "unit": "ns/op",
            "extra": "75482 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 244.88,
            "unit": "MB/s",
            "extra": "75482 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 2444,
            "unit": "B/op",
            "extra": "75482 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 27,
            "unit": "allocs/op",
            "extra": "75482 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 8050,
            "unit": "ns/op\t   7.95 MB/s\t   18864 B/op\t       8 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 8050,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 7.95,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 18864,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 12173,
            "unit": "ns/op\t 336.48 MB/s\t   33318 B/op\t      11 allocs/op",
            "extra": "339757 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 12173,
            "unit": "ns/op",
            "extra": "339757 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 336.48,
            "unit": "MB/s",
            "extra": "339757 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 33318,
            "unit": "B/op",
            "extra": "339757 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "339757 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet (github.com/feichai0017/NoKV)",
            "value": 85430,
            "unit": "ns/op\t 191.78 MB/s\t  194184 B/op\t     167 allocs/op",
            "extra": "14354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet (github.com/feichai0017/NoKV) - ns/op",
            "value": 85430,
            "unit": "ns/op",
            "extra": "14354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet (github.com/feichai0017/NoKV) - MB/s",
            "value": 191.78,
            "unit": "MB/s",
            "extra": "14354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet (github.com/feichai0017/NoKV) - B/op",
            "value": 194184,
            "unit": "B/op",
            "extra": "14354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet (github.com/feichai0017/NoKV) - allocs/op",
            "value": 167,
            "unit": "allocs/op",
            "extra": "14354 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1644784,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1644784,
            "unit": "ns/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "739 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 248.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "4903371 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 248.1,
            "unit": "ns/op",
            "extra": "4903371 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "4903371 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "4903371 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.9,
            "unit": "ns/op",
            "extra": "48949047 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 59.33,
            "unit": "ns/op",
            "extra": "21025066 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20.68,
            "unit": "ns/op",
            "extra": "57893473 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 16.71,
            "unit": "ns/op",
            "extra": "73926075 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 21275165,
            "unit": "ns/op",
            "extra": "61 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 74.64,
            "unit": "ns/op",
            "extra": "16015250 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 54845,
            "unit": "ns/op",
            "extra": "22764 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm)",
            "value": 43474,
            "unit": "ns/op\t 188.44 MB/s\t   44027 B/op\t     210 allocs/op",
            "extra": "28731 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 43474,
            "unit": "ns/op",
            "extra": "28731 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 188.44,
            "unit": "MB/s",
            "extra": "28731 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 44027,
            "unit": "B/op",
            "extra": "28731 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "28731 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm)",
            "value": 6406609,
            "unit": "ns/op\t67661475 B/op\t     477 allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6406609,
            "unit": "ns/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 67661475,
            "unit": "B/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 477,
            "unit": "allocs/op",
            "extra": "188 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils)",
            "value": 467.2,
            "unit": "ns/op\t 136.99 MB/s\t    1543 B/op\t       0 allocs/op",
            "extra": "2572563 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 467.2,
            "unit": "ns/op",
            "extra": "2572563 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 136.99,
            "unit": "MB/s",
            "extra": "2572563 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 1543,
            "unit": "B/op",
            "extra": "2572563 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2572563 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils)",
            "value": 149.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "8051314 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 149.2,
            "unit": "ns/op",
            "extra": "8051314 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "8051314 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "8051314 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils)",
            "value": 123.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9835551 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 123.3,
            "unit": "ns/op",
            "extra": "9835551 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9835551 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9835551 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 54.56,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "21932464 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 54.56,
            "unit": "ns/op",
            "extra": "21932464 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "21932464 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "21932464 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils)",
            "value": 1394,
            "unit": "ns/op\t  45.90 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 1394,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 45.9,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils)",
            "value": 456.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2774883 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 456.8,
            "unit": "ns/op",
            "extra": "2774883 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2774883 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2774883 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils)",
            "value": 403,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "3009098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 403,
            "unit": "ns/op",
            "extra": "3009098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "3009098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "3009098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 44.98,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "26920653 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 44.98,
            "unit": "ns/op",
            "extra": "26920653 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "26920653 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "26920653 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils)",
            "value": 445.3,
            "unit": "ns/op\t 143.71 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "3021368 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 445.3,
            "unit": "ns/op",
            "extra": "3021368 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 143.71,
            "unit": "MB/s",
            "extra": "3021368 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "3021368 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3021368 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils)",
            "value": 1328,
            "unit": "ns/op\t  48.21 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 1328,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 48.21,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 26498,
            "unit": "ns/op\t 309.16 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "72976 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 26498,
            "unit": "ns/op",
            "extra": "72976 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 309.16,
            "unit": "MB/s",
            "extra": "72976 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "72976 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "72976 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 176.7,
            "unit": "ns/op\t1449.08 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "6702496 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 176.7,
            "unit": "ns/op",
            "extra": "6702496 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1449.08,
            "unit": "MB/s",
            "extra": "6702496 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "6702496 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "6702496 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 1080,
            "unit": "ns/op\t 236.98 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 1080,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 236.98,
            "unit": "MB/s",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2225832,
            "unit": "ns/op\t 3544159 B/op\t   40018 allocs/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2225832,
            "unit": "ns/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 3544159,
            "unit": "B/op",
            "extra": "542 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40018,
            "unit": "allocs/op",
            "extra": "542 times\n4 procs"
          }
        ]
      }
    ]
  }
}