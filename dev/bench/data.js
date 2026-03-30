window.BENCHMARK_DATA = {
  "lastUpdate": 1774828804740,
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
          "id": "2478146990cdfccb450786c849726e1a6abff77b",
          "message": "docs: align architecture docs with current code",
          "timestamp": "2026-03-30T07:56:22+08:00",
          "tree_id": "481865795870718899f7dab2873be8130ee5c63c",
          "url": "https://github.com/feichai0017/NoKV/commit/2478146990cdfccb450786c849726e1a6abff77b"
        },
        "date": 1774828801370,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4442,
            "unit": "ns/op\t   7.20 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "24759 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4442,
            "unit": "ns/op",
            "extra": "24759 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 7.2,
            "unit": "MB/s",
            "extra": "24759 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "24759 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "24759 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 14686,
            "unit": "ns/op\t 278.91 MB/s\t     399 B/op\t      20 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 14686,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 278.91,
            "unit": "MB/s",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 399,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1546,
            "unit": "ns/op\t  41.40 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "76346 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1546,
            "unit": "ns/op",
            "extra": "76346 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 41.4,
            "unit": "MB/s",
            "extra": "76346 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "76346 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "76346 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 2297,
            "unit": "ns/op\t1783.57 MB/s\t    4312 B/op\t       7 allocs/op",
            "extra": "48061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 2297,
            "unit": "ns/op",
            "extra": "48061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 1783.57,
            "unit": "MB/s",
            "extra": "48061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 4312,
            "unit": "B/op",
            "extra": "48061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "48061 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 65368,
            "unit": "ns/op\t 250.64 MB/s\t  176040 B/op\t     147 allocs/op",
            "extra": "2071 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 65368,
            "unit": "ns/op",
            "extra": "2071 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 250.64,
            "unit": "MB/s",
            "extra": "2071 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 176040,
            "unit": "B/op",
            "extra": "2071 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "2071 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 846986,
            "unit": "ns/op\t  19.34 MB/s\t   46585 B/op\t     148 allocs/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 846986,
            "unit": "ns/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 19.34,
            "unit": "MB/s",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46585,
            "unit": "B/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "164 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 6429143,
            "unit": "ns/op\t   2.55 MB/s\t   47255 B/op\t     152 allocs/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 6429143,
            "unit": "ns/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 2.55,
            "unit": "MB/s",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 47255,
            "unit": "B/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 152,
            "unit": "allocs/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1605168,
            "unit": "ns/op\t       1 B/op\t       0 allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1605168,
            "unit": "ns/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 1,
            "unit": "B/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "68 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 262.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "462007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 262.1,
            "unit": "ns/op",
            "extra": "462007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "462007 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "462007 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/hotring)",
            "value": 23.96,
            "unit": "ns/op",
            "extra": "4963106 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/hotring)",
            "value": 59.05,
            "unit": "ns/op",
            "extra": "2142978 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/hotring)",
            "value": 20.76,
            "unit": "ns/op",
            "extra": "5668962 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/hotring)",
            "value": 16.45,
            "unit": "ns/op",
            "extra": "7334479 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/hotring)",
            "value": 19937894,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/hotring)",
            "value": 77.07,
            "unit": "ns/op",
            "extra": "1557423 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/hotring)",
            "value": 56092,
            "unit": "ns/op",
            "extra": "1981 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm)",
            "value": 33931,
            "unit": "ns/op\t 241.43 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "3174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 33931,
            "unit": "ns/op",
            "extra": "3174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - MB/s",
            "value": 241.43,
            "unit": "MB/s",
            "extra": "3174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "3174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "3174 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm)",
            "value": 6216452,
            "unit": "ns/op\t71593458 B/op\t     474 allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6216452,
            "unit": "ns/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 71593458,
            "unit": "B/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 474,
            "unit": "allocs/op",
            "extra": "33 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 35024,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3309 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 35024,
            "unit": "ns/op",
            "extra": "3309 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3309 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3309 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 89.63,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1336779 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 89.63,
            "unit": "ns/op",
            "extra": "1336779 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1336779 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1336779 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17475,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6400 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17475,
            "unit": "ns/op",
            "extra": "6400 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6400 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6400 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 500.7,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "219860 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 500.7,
            "unit": "ns/op",
            "extra": "219860 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "219860 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "219860 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 17086,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6890 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 17086,
            "unit": "ns/op",
            "extra": "6890 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6890 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6890 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 468,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "251311 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 468,
            "unit": "ns/op",
            "extra": "251311 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "251311 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "251311 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 56716,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "2000 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 56716,
            "unit": "ns/op",
            "extra": "2000 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "2000 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "2000 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 919,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "128953 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 919,
            "unit": "ns/op",
            "extra": "128953 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "128953 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "128953 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 59873,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1876 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 59873,
            "unit": "ns/op",
            "extra": "1876 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1876 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1876 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 4676,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "25026 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 4676,
            "unit": "ns/op",
            "extra": "25026 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "25026 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "25026 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 90840,
            "unit": "ns/op\t   45808 B/op\t     390 allocs/op",
            "extra": "1312 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 90840,
            "unit": "ns/op",
            "extra": "1312 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 45808,
            "unit": "B/op",
            "extra": "1312 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1312 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 34891,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3224 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 34891,
            "unit": "ns/op",
            "extra": "3224 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3224 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3224 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 1873,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "62316 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1873,
            "unit": "ns/op",
            "extra": "62316 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "62316 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "62316 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 1777,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "64705 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 1777,
            "unit": "ns/op",
            "extra": "64705 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "64705 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "64705 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 9969,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "11576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 9969,
            "unit": "ns/op",
            "extra": "11576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "11576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "11576 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 10781,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 10781,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 6752,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm)",
            "value": 36734,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3120 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 36734,
            "unit": "ns/op",
            "extra": "3120 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3120 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3120 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm)",
            "value": 40020,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2646 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 40020,
            "unit": "ns/op",
            "extra": "2646 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2646 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2646 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 45072,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2498 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 45072,
            "unit": "ns/op",
            "extra": "2498 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2498 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2498 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 899.5,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "126402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 899.5,
            "unit": "ns/op",
            "extra": "126402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "126402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "126402 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 53269,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2200 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 53269,
            "unit": "ns/op",
            "extra": "2200 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2200 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2200 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 479.1,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "243591 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 479.1,
            "unit": "ns/op",
            "extra": "243591 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "243591 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "243591 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 21046,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 21046,
            "unit": "ns/op",
            "extra": "5378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5378 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 20851,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5412 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20851,
            "unit": "ns/op",
            "extra": "5412 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5412 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5412 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 20348,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5415 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 20348,
            "unit": "ns/op",
            "extra": "5415 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5415 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5415 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 19851,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "6188 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 19851,
            "unit": "ns/op",
            "extra": "6188 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "6188 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "6188 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 31021,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 31021,
            "unit": "ns/op",
            "extra": "3240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3240 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 746.6,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "148734 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 746.6,
            "unit": "ns/op",
            "extra": "148734 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "148734 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "148734 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 29736,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3692 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 29736,
            "unit": "ns/op",
            "extra": "3692 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3692 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3692 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 804.5,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "139738 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 804.5,
            "unit": "ns/op",
            "extra": "139738 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "139738 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "139738 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 86652,
            "unit": "ns/op\t   30336 B/op\t      30 allocs/op",
            "extra": "1305 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 86652,
            "unit": "ns/op",
            "extra": "1305 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 30336,
            "unit": "B/op",
            "extra": "1305 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1305 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 2156,
            "unit": "ns/op\t    1928 B/op\t      28 allocs/op",
            "extra": "54741 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 2156,
            "unit": "ns/op",
            "extra": "54741 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 1928,
            "unit": "B/op",
            "extra": "54741 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "54741 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 91528,
            "unit": "ns/op\t   33368 B/op\t      73 allocs/op",
            "extra": "1243 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 91528,
            "unit": "ns/op",
            "extra": "1243 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 33368,
            "unit": "B/op",
            "extra": "1243 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1243 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 6177,
            "unit": "ns/op\t    5016 B/op\t      71 allocs/op",
            "extra": "18940 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 6177,
            "unit": "ns/op",
            "extra": "18940 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 5016,
            "unit": "B/op",
            "extra": "18940 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "18940 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm)",
            "value": 125715,
            "unit": "ns/op\t   57112 B/op\t     409 allocs/op",
            "extra": "919 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 125715,
            "unit": "ns/op",
            "extra": "919 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 57112,
            "unit": "B/op",
            "extra": "919 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "919 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm)",
            "value": 38222,
            "unit": "ns/op\t   29208 B/op\t     407 allocs/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - ns/op",
            "value": 38222,
            "unit": "ns/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - B/op",
            "value": 29208,
            "unit": "B/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "2830 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils)",
            "value": 471.6,
            "unit": "ns/op\t 135.71 MB/s\t    1540 B/op\t       0 allocs/op",
            "extra": "263499 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 471.6,
            "unit": "ns/op",
            "extra": "263499 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 135.71,
            "unit": "MB/s",
            "extra": "263499 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 1540,
            "unit": "B/op",
            "extra": "263499 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "263499 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils)",
            "value": 153.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "762608 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 153.5,
            "unit": "ns/op",
            "extra": "762608 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "762608 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "762608 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils)",
            "value": 127.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "925605 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 127.7,
            "unit": "ns/op",
            "extra": "925605 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "925605 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "925605 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 57.44,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2093845 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 57.44,
            "unit": "ns/op",
            "extra": "2093845 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2093845 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2093845 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 53.5,
            "unit": "ns/op",
            "extra": "2360406 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 154.4,
            "unit": "ns/op",
            "extra": "823743 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 164.3,
            "unit": "ns/op",
            "extra": "825136 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 188.1,
            "unit": "ns/op",
            "extra": "740684 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 41.93,
            "unit": "ns/op",
            "extra": "2926705 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 121.1,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 101.2,
            "unit": "ns/op",
            "extra": "1068261 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 131.9,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 57.5,
            "unit": "ns/op",
            "extra": "1831782 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 167.8,
            "unit": "ns/op",
            "extra": "775072 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 180.3,
            "unit": "ns/op",
            "extra": "709071 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 199.8,
            "unit": "ns/op",
            "extra": "705506 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.07,
            "unit": "ns/op",
            "extra": "4599729 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 276.6,
            "unit": "ns/op",
            "extra": "395986 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4663,
            "unit": "ns/op",
            "extra": "25504 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 7988,
            "unit": "ns/op",
            "extra": "14391 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 47.94,
            "unit": "ns/op",
            "extra": "2295062 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 71.18,
            "unit": "ns/op",
            "extra": "1684635 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 70.07,
            "unit": "ns/op",
            "extra": "1713014 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 69.62,
            "unit": "ns/op",
            "extra": "1732702 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 9.985,
            "unit": "ns/op",
            "extra": "12065930 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils)",
            "value": 1019,
            "unit": "ns/op\t  62.78 MB/s\t     157 B/op\t       1 allocs/op",
            "extra": "174927 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 1019,
            "unit": "ns/op",
            "extra": "174927 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 62.78,
            "unit": "MB/s",
            "extra": "174927 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 157,
            "unit": "B/op",
            "extra": "174927 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "174927 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils)",
            "value": 427.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "289098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 427.2,
            "unit": "ns/op",
            "extra": "289098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "289098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "289098 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils)",
            "value": 410.5,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "300770 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 410.5,
            "unit": "ns/op",
            "extra": "300770 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "300770 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "300770 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils)",
            "value": 44.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2686850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 44.9,
            "unit": "ns/op",
            "extra": "2686850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2686850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2686850 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils)",
            "value": 412.4,
            "unit": "ns/op\t 155.18 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "337131 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 412.4,
            "unit": "ns/op",
            "extra": "337131 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 155.18,
            "unit": "MB/s",
            "extra": "337131 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "337131 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "337131 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils)",
            "value": 976.1,
            "unit": "ns/op\t  65.57 MB/s\t     160 B/op\t       1 allocs/op",
            "extra": "374161 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - ns/op",
            "value": 976.1,
            "unit": "ns/op",
            "extra": "374161 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - MB/s",
            "value": 65.57,
            "unit": "MB/s",
            "extra": "374161 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - B/op",
            "value": 160,
            "unit": "B/op",
            "extra": "374161 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/utils) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "374161 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog)",
            "value": 9799,
            "unit": "ns/op\t 835.97 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11766 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 9799,
            "unit": "ns/op",
            "extra": "11766 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 835.97,
            "unit": "MB/s",
            "extra": "11766 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11766 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11766 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog)",
            "value": 177.9,
            "unit": "ns/op\t1439.05 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "648903 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - ns/op",
            "value": 177.9,
            "unit": "ns/op",
            "extra": "648903 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - MB/s",
            "value": 1439.05,
            "unit": "MB/s",
            "extra": "648903 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "648903 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "648903 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal)",
            "value": 564.2,
            "unit": "ns/op\t 453.71 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "215505 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 564.2,
            "unit": "ns/op",
            "extra": "215505 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - MB/s",
            "value": 453.71,
            "unit": "MB/s",
            "extra": "215505 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "215505 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "215505 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal)",
            "value": 2458565,
            "unit": "ns/op\t 7476084 B/op\t   40017 allocs/op",
            "extra": "49 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - ns/op",
            "value": 2458565,
            "unit": "ns/op",
            "extra": "49 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - B/op",
            "value": 7476084,
            "unit": "B/op",
            "extra": "49 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/wal) - allocs/op",
            "value": 40017,
            "unit": "allocs/op",
            "extra": "49 times\n4 procs"
          }
        ]
      }
    ]
  }
}