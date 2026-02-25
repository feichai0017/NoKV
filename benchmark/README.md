# Benchmarks

This document captures the most recent results from running the default
benchmark script (`scripts/run_benchmarks.sh`).

## YCSB Framework Overview

The benchmark harness uses the YCSB workloads (A/B/C/D/E/F/G) to exercise NoKV,
Badger, and Pebble by default (RocksDB is optional via build tags) with a fixed total operation count and report both
throughput and latency percentiles. For memtable comparisons, the NoKV engine
can be split into `nokv-skiplist` and `nokv-art` variants. The default script
runs a load phase to seed data, then executes each workload and collects:
- Ops/s, average latency, and latency percentiles (P50/P95/P99)
- Operation mix counts (reads, updates, inserts, scans, read-modify-write)
- Value size stats and total data size

## Test Environment

- Machine: MacBook Pro (Apple M3 Pro)
- Memory: 36 GB

## YCSB Architecture

The YCSB harness is organized as a Go test entrypoint plus a small engine
abstraction so every storage engine is driven by the same workload generator,
key distribution, and metrics pipeline.

Flow:

```
scripts/run_benchmarks.sh
  -> go test ./benchmark -run TestBenchmarkYCSB -args <flags>
     -> TestBenchmarkYCSB (benchmark/ycsb_test.go)
        -> runYCSBBenchmarks (benchmark/ycsb_runner.go)
           -> engine.Open(clean)
           -> ycsbLoad (parallel preload)
           -> [optional warm-up]
           -> ycsbRunWorkload (parallel workload run)
           -> engine.Close
```

Key components:

- Engine interface: `benchmark/ycsb_engine.go` defines `Read/Insert/Update/Scan`
  and per-engine implementations live in `benchmark/ycsb_engine_*` (including
  `nokv-skiplist` / `nokv-art` for memtable-only comparisons).
- Workload model: `benchmark/ycsb_runner.go` defines YCSB A/B/C/D/E/F/G mixes,
  request ratios, and key distributions (zipfian/uniform/latest).
- Value generator: fixed/uniform/normal/percentile sizing with a shared buffer
  pool to reduce allocations (`valuePool`).
- Concurrency model: each workload runs with `ycsb_conc` goroutines; each op
  records latency samples and operation counts; optional global throttling is
  available via `ycsb_target_ops`.
- Workload isolation: each workload reopens and reloads the engine to avoid
  cross-workload state pollution (compaction debt/history carry-over).
- Results pipeline: summaries are printed to stdout, written as CSV under
  `benchmark_data/ycsb/results`, and a text report is saved under
  `benchmark_results/benchmark_results_*.txt`.

## Full Results

```
=== Benchmark Results ===
Generated at: 2026-02-23 19:59:51

Summary:
ENGINE   OPERATION  MODE                          OPS/S    AVG LATENCY  P50       P95        P99        TOTAL OPS  READS     UPDATES  INSERTS  SCANS  SCAN ITEMS  RMW      VAL AVG  VAL P95  DATA (MB)  DURATION

NoKV    YCSB-A     50/50 read/update             847660   1.179µs      16.542µs  36.208µs   54.75µs    1000000    500756   499244   0        0       0           0       255      256      243.16     1.179717958s
NoKV    YCSB-B     95/5 read/update              1742820  573ns        3.167µs   41.042µs   68.875µs   1000000    950170   49830    0        0       0           0       253      256      241.24     573.78275ms
NoKV    YCSB-C     100% read                     2070856  482ns        4.25µs    9.25µs     20.75µs    1000000    1000000  0        0        0       0           0       250      256      237.97     482.892042ms
NoKV    YCSB-D     95% read, 5% insert (latest)  1754955  569ns        2.583µs   41.166µs   71.459µs   1000000    950080   0        49920    0       0           0       248      256      236.79     569.815042ms
NoKV    YCSB-E     95% scan, 5% insert           205489   4.866µs      36.75µs   179.541µs  841.083µs  1000000    0        0        50085    949915  94991132    0       256      256      23203.42   4.866442833s
NoKV    YCSB-F     read-modify-write             715946   1.396µs      18.5µs    51.042µs   72.625µs   1000000    500756   0        0        0       0           499244  255      256      364.07     1.39675425s
NoKV    YCSB-G     100% insert                   413521   2.418µs      23.334µs  77µs       398.875µs  1000000    0        0        1000000  0       0           0       256      256      244.14     2.418256208s
Badger  YCSB-A     50/50 read/update             396314   2.523µs      35.875µs  80.166µs   112.833µs  1000000    500756   499244   0        0       0           0       256      256      244.14     2.52325475s
Badger  YCSB-B     95/5 read/update              716151   1.396µs      5.791µs   81.5µs     137.167µs  1000000    950170   49830    0        0       0           0       256      256      244.14     1.396353959s
Badger  YCSB-C     100% read                     826766   1.209µs      3.541µs   113.583µs  324.042µs  1000000    1000000  0        0        0       0           0       256      256      244.14     1.209532458s
Badger  YCSB-D     95% read, 5% insert (latest)  842637   1.186µs      3.958µs   86.584µs   155.5µs    1000000    950080   0        49920    0       0           0       242      256      230.38     1.186751333s
Badger  YCSB-E     95% scan, 5% insert           41508    24.091µs     317.25µs  921.458µs  1.514ms    1000000    0        0        50085    949915  94991433    0       256      256      23203.50   24.0916655s
Badger  YCSB-F     read-modify-write             326343   3.064µs      42.042µs  110.791µs  145.584µs  1000000    500756   0        0        0       0           499244  256      256      366.03     3.06425625s
Badger  YCSB-G     100% insert                   399405   2.503µs      38.875µs  62.75µs    74.458µs   1000000    0        0        1000000  0       0           0       256      256      244.14     2.503722625s
Pebble  YCSB-A     50/50 read/update             1282218  779ns        2.625µs   50.583µs   103.083µs  1000000    500756   499244   0        0       0           0       256      256      244.14     779.898709ms
Pebble  YCSB-B     95/5 read/update              1941330  515ns        2.417µs   17.042µs   70.084µs   1000000    950170   49830    0        0       0           0       256      256      244.14     515.110666ms
Pebble  YCSB-C     100% read                     847764   1.179µs      10.458µs  20.583µs   142.75µs   1000000    1000000  0        0        0       0           0       256      256      244.14     1.179573042s
Pebble  YCSB-D     95% read, 5% insert (latest)  2509809  398ns        2.084µs   14.5µs     54.75µs    1000000    950080   0        49920    0       0           0       251      256      239.28     398.436667ms
Pebble  YCSB-E     95% scan, 5% insert           554557   1.803µs      15.292µs  38.625µs   132.792µs  1000000    0        0        50091    949909  94990696    0       256      256      23203.32   1.803239875s
Pebble  YCSB-F     read-modify-write             1123473  890ns        3.542µs   62.125µs   135.25µs   1000000    500756   0        0        0       0           499244  256      256      366.03     890.096667ms
Pebble  YCSB-G     100% insert                   583584   1.713µs      2µs       51.833µs   88.333µs   1000000    0        0        1000000  0       0           0       256      256      244.14     1.713548s
```
