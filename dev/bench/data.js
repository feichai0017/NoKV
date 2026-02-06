window.BENCHMARK_DATA = {
  "lastUpdate": 1770404908513,
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
          "id": "399b563936abca5bad2f5def51d6330c508c3a09",
          "message": "docs: add dbdb badge",
          "timestamp": "2026-02-07T03:05:49+08:00",
          "tree_id": "1f813d098c841809b7d2625a8621614dde2c307c",
          "url": "https://github.com/feichai0017/NoKV/commit/399b563936abca5bad2f5def51d6330c508c3a09"
        },
        "date": 1770404907425,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall",
            "value": 10218,
            "unit": "ns/op\t   3.13 MB/s\t     424 B/op\t      18 allocs/op",
            "extra": "110344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - ns/op",
            "value": 10218,
            "unit": "ns/op",
            "extra": "110344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - MB/s",
            "value": 3.13,
            "unit": "MB/s",
            "extra": "110344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "110344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "110344 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge",
            "value": 15403,
            "unit": "ns/op\t 265.92 MB/s\t     658 B/op\t      29 allocs/op",
            "extra": "84519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - ns/op",
            "value": 15403,
            "unit": "ns/op",
            "extra": "84519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - MB/s",
            "value": 265.92,
            "unit": "MB/s",
            "extra": "84519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - B/op",
            "value": 658,
            "unit": "B/op",
            "extra": "84519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "84519 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall",
            "value": 12046,
            "unit": "ns/op\t   5.31 MB/s\t   16421 B/op\t       4 allocs/op",
            "extra": "834609 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - ns/op",
            "value": 12046,
            "unit": "ns/op",
            "extra": "834609 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - MB/s",
            "value": 5.31,
            "unit": "MB/s",
            "extra": "834609 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - B/op",
            "value": 16421,
            "unit": "B/op",
            "extra": "834609 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "834609 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge",
            "value": 9448,
            "unit": "ns/op\t 433.52 MB/s\t   18636 B/op\t       8 allocs/op",
            "extra": "257794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - ns/op",
            "value": 9448,
            "unit": "ns/op",
            "extra": "257794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - MB/s",
            "value": 433.52,
            "unit": "MB/s",
            "extra": "257794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - B/op",
            "value": 18636,
            "unit": "B/op",
            "extra": "257794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge - allocs/op",
            "value": 8,
            "unit": "allocs/op",
            "extra": "257794 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet",
            "value": 164396,
            "unit": "ns/op\t  99.66 MB/s\t   57934 B/op\t     664 allocs/op",
            "extra": "9769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - ns/op",
            "value": 164396,
            "unit": "ns/op",
            "extra": "9769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - MB/s",
            "value": 99.66,
            "unit": "MB/s",
            "extra": "9769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - B/op",
            "value": 57934,
            "unit": "B/op",
            "extra": "9769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet - allocs/op",
            "value": 664,
            "unit": "allocs/op",
            "extra": "9769 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan",
            "value": 2208860,
            "unit": "ns/op\t       5 B/op\t       0 allocs/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - ns/op",
            "value": 2208860,
            "unit": "ns/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - B/op",
            "value": 5,
            "unit": "B/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "526 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek",
            "value": 943.8,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "1230908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - ns/op",
            "value": 943.8,
            "unit": "ns/op",
            "extra": "1230908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "1230908 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "1230908 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch",
            "value": 49319,
            "unit": "ns/op\t 166.10 MB/s\t   25527 B/op\t     454 allocs/op",
            "extra": "23785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - ns/op",
            "value": 49319,
            "unit": "ns/op",
            "extra": "23785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - MB/s",
            "value": 166.1,
            "unit": "MB/s",
            "extra": "23785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - B/op",
            "value": 25527,
            "unit": "B/op",
            "extra": "23785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch - allocs/op",
            "value": 454,
            "unit": "allocs/op",
            "extra": "23785 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush",
            "value": 6552176,
            "unit": "ns/op\t67523281 B/op\t    2585 allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - ns/op",
            "value": 6552176,
            "unit": "ns/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - B/op",
            "value": 67523281,
            "unit": "B/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush - allocs/op",
            "value": 2585,
            "unit": "allocs/op",
            "extra": "177 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert",
            "value": 624.2,
            "unit": "ns/op\t 102.54 MB/s\t    1544 B/op\t       0 allocs/op",
            "extra": "2096708 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - ns/op",
            "value": 624.2,
            "unit": "ns/op",
            "extra": "2096708 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - MB/s",
            "value": 102.54,
            "unit": "MB/s",
            "extra": "2096708 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "2096708 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2096708 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet",
            "value": 128.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "9387942 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - ns/op",
            "value": 128.3,
            "unit": "ns/op",
            "extra": "9387942 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "9387942 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "9387942 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert",
            "value": 1386,
            "unit": "ns/op\t  46.18 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - ns/op",
            "value": 1386,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert - MB/s",
            "value": 46.18,
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
            "value": 475,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2505326 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - ns/op",
            "value": 475,
            "unit": "ns/op",
            "extra": "2505326 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2505326 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2505326 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries",
            "value": 25673,
            "unit": "ns/op\t 319.09 MB/s\t    1794 B/op\t      35 allocs/op",
            "extra": "75604 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - ns/op",
            "value": 25673,
            "unit": "ns/op",
            "extra": "75604 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - MB/s",
            "value": 319.09,
            "unit": "MB/s",
            "extra": "75604 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - B/op",
            "value": 1794,
            "unit": "B/op",
            "extra": "75604 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "75604 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue",
            "value": 145,
            "unit": "ns/op\t1765.33 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "8288353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - ns/op",
            "value": 145,
            "unit": "ns/op",
            "extra": "8288353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - MB/s",
            "value": 1765.33,
            "unit": "MB/s",
            "extra": "8288353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "8288353 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "8288353 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend",
            "value": 729.5,
            "unit": "ns/op\t 350.90 MB/s\t      68 B/op\t       6 allocs/op",
            "extra": "3159786 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - ns/op",
            "value": 729.5,
            "unit": "ns/op",
            "extra": "3159786 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - MB/s",
            "value": 350.9,
            "unit": "MB/s",
            "extra": "3159786 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - B/op",
            "value": 68,
            "unit": "B/op",
            "extra": "3159786 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "3159786 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay",
            "value": 2017517,
            "unit": "ns/op\t 3064040 B/op\t   40019 allocs/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - ns/op",
            "value": 2017517,
            "unit": "ns/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - B/op",
            "value": 3064040,
            "unit": "B/op",
            "extra": "591 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay - allocs/op",
            "value": 40019,
            "unit": "allocs/op",
            "extra": "591 times\n4 procs"
          }
        ]
      }
    ]
  }
}