# Benchmarks

This document captures the most recent results from running the default
benchmark script (`scripts/run_benchmarks.sh`).

## YCSB Framework Overview

The benchmark harness uses the YCSB workloads (A/B/C/D/E/F) to exercise NoKV,
Badger, Pebble, and RocksDB with a fixed total operation count and report both
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
- Workload model: `benchmark/ycsb_runner.go` defines YCSB A/B/C/D/E/F mixes,
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
Generated at: 2026-01-05 13:30:38

Summary:
ENGINE   OPERATION  MODE                          OPS/S    AVG LATENCY  P50       P95        P99        TOTAL OPS  READS     UPDATES  INSERTS  SCANS  SCAN ITEMS  RMW      VAL AVG  VAL P95  DATA (MB)  DURATION

NoKV    YCSB-A     50/50 read/update             830602   1.203µs      16.834µs   38.084µs   58.25µs    1000000    500756   499244   0        0       0           0       255      256      243.19     1.203945709s
NoKV    YCSB-B     95/5 read/update              1666600  600ns        3.292µs    42.708µs   72.25µs    1000000    950170   49830    0        0       0           0       253      256      241.23     600.023959ms
NoKV    YCSB-C     100% read                     1931369  517ns        4.292µs    9.959µs    49.667µs   1000000    1000000  0        0        0       0           0       250      256      238.00     517.767417ms
NoKV    YCSB-D     95% read, 5% insert (latest)  1845861  541ns        2.458µs    39.375µs   67.25µs    1000000    950080   0        49920    0       0           0       248      256      236.71     541.752583ms
NoKV    YCSB-E     95% scan, 5% insert           185123   5.401µs      37.708µs   200.083µs  911.625µs  1000000    0        0        50085    949915  94991307    0       256      256      23203.46   5.401817s
NoKV    YCSB-F     read-modify-write             674619   1.482µs      19µs       55.625µs   81.75µs    1000000    500756   0        0        0       0           499244  255      256      364.07     1.482318208s
Badger  YCSB-A     50/50 read/update             456435   2.19µs       32.208µs   65.875µs   83.958µs   1000000    500756   499244   0        0       0           0       256      256      244.14     2.190894792s
Badger  YCSB-B     95/5 read/update              688155   1.453µs      5.875µs    84.792µs   144.875µs  1000000    950170   49830    0        0       0           0       256      256      244.14     1.453160584s
Badger  YCSB-C     100% read                     873820   1.144µs      3.75µs     98.667µs   276.125µs  1000000    1000000  0        0        0       0           0       256      256      244.14     1.144400583s
Badger  YCSB-D     95% read, 5% insert (latest)  777686   1.285µs      4.916µs    74.916µs   129.5µs    1000000    950080   0        49920    0       0           0       244      256      233.09     1.285866583s
Badger  YCSB-E     95% scan, 5% insert           42527    23.514µs     308.333µs  904.541µs  1.486ms    1000000    0        0        50085    949915  94991276    0       256      256      23203.46   23.514482084s
Badger  YCSB-F     read-modify-write             344726   2.9µs        40.875µs   97.75µs    126.042µs  1000000    500756   0        0        0       0           499244  256      256      366.03     2.900853667s
Pebble  YCSB-A     50/50 read/update             1269815  787ns        2.625µs    51.708µs   107.083µs  1000000    500756   499244   0        0       0           0       256      256      244.14     787.516541ms
Pebble  YCSB-B     95/5 read/update              1943445  514ns        2.458µs    17.167µs   66.542µs   1000000    950170   49830    0        0       0           0       256      256      244.14     514.550166ms
Pebble  YCSB-C     100% read                     889292   1.124µs      10.208µs   20.958µs   142.666µs  1000000    1000000  0        0        0       0           0       256      256      244.14     1.124489708s
Pebble  YCSB-D     95% read, 5% insert (latest)  2530967  395ns        2.042µs    14.333µs   55.083µs   1000000    950080   0        49920    0       0           0       251      256      239.25     395.105917ms
Pebble  YCSB-E     95% scan, 5% insert           565647   1.767µs      15.167µs   37.917µs   127.125µs  1000000    0        0        50108    949892  94988983    0       256      256      23202.90   1.767886792s
Pebble  YCSB-F     read-modify-write             1128722  885ns        3.5µs      62.042µs   135.25µs   1000000    500756   0        0        0       0           499244  256      256      366.03     885.957542ms
```
