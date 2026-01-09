# Benchmarks

This document captures the most recent results from running the default
benchmark script (`scripts/run_benchmarks.sh`).

## YCSB Framework Overview

The benchmark harness uses the YCSB workloads (A/B/C/D/F) to exercise NoKV,
Badger, and RocksDB with a fixed total operation count and report both
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
- Workload model: `benchmark/ycsb_runner.go` defines YCSB A/B/C/D/F mixes,
  request ratios, and key distributions (zipfian/uniform/latest).
- Value generator: fixed/uniform/normal/percentile sizing with a shared buffer
  pool to reduce allocations (`valuePool`).
- Concurrency model: each workload runs with `ycsb_conc` goroutines; each op
  records latency samples and operation counts; optional global throttling is
  available via `ycsb_target_ops`.
- Results pipeline: summaries are printed to stdout, written as CSV under
  `benchmark_data/ycsb/results`, and a text report is saved under
  `benchmark_results/benchmark_results_*.txt`.

## Full Results

```
=== Benchmark Results ===
Generated at: 2026-01-05 13:30:38

Summary:
ENGINE   OPERATION  MODE                          OPS/S    AVG LATENCY  P50       P95        P99        TOTAL OPS  READS     UPDATES  INSERTS  SCANS  SCAN ITEMS  RMW      VAL AVG  VAL P95  DATA (MB)  DURATION
NoKV     YCSB-A     50/50 read/update             447220   2.236µs      23.875µs  73.084µs   158.459µs  10000000   5001099   4998901  0        0      0           0        249      256      2373.56    22.360348458s
NoKV     YCSB-B     95/5 read/update              960899   1.04µs       5.458µs   66.042µs   133.209µs  10000000   9499771   500229   0        0      0           0        246      256      2346.79    10.406915958s
NoKV     YCSB-C     100% read                     1606419  622ns        3.5µs     21.625µs   115.916µs  10000000   10000000  0        0        0      0           0        246      256      2345.57    6.22502675s
NoKV     YCSB-D     95% read, 5% insert (latest)  1264526  790ns        3.375µs   53.125µs   100.459µs  10000000   9500230   0        499770   0      0           0        236      256      2251.33    7.90809875s
NoKV     YCSB-F     read-modify-write             516298   1.936µs      23.208µs  75.208µs   123.667µs  10000000   5002011   0        0        0      0           4997989  251      256      3593.88    19.3686775s
Badger   YCSB-A     50/50 read/update             291703   3.428µs      47.5µs    115.25µs   166.041µs  10000000   5001099   4998901  0        0      0           0        256      256      2441.41    34.281447708s
Badger   YCSB-B     95/5 read/update              327478   3.053µs      19.458µs  181.958µs  320.167µs  10000000   9499771   500229   0        0      0           0        256      256      2441.41    30.536414125s
Badger   YCSB-C     100% read                     533953   1.872µs      5.958µs   164.875µs  411.666µs  10000000   10000000  0        0        0      0           0        256      256      2441.41    18.728238125s
Badger   YCSB-D     95% read, 5% insert (latest)  749205   1.334µs      4.625µs   95.666µs   168.458µs  10000000   9500230   0        499770   0      0           0        244      256      2322.35    13.347484917s
Badger   YCSB-F     read-modify-write             187017   5.347µs      70.333µs  198.709µs  281.541µs  10000000   5002011   0        0        0      0           4997989  256      256      3661.62    53.471144541s
RocksDB  YCSB-A     50/50 read/update             151815   6.586µs      9.042µs   105.875µs  346.5µs    10000000   5001099   4998901  0        0      0           0        256      256      2441.41    1m5.869592875s
RocksDB  YCSB-B     95/5 read/update              1060978  942ns        3.5µs     36.667µs   184.459µs  10000000   9499771   500229   0        0      0           0        256      256      2441.41    9.4252665s
RocksDB  YCSB-C     100% read                     1519805  657ns        3.167µs   20.542µs   53.292µs   10000000   10000000  0        0        0      0           0        256      256      2441.41    6.579790708s
RocksDB  YCSB-D     95% read, 5% insert (latest)  1555050  643ns        2.5µs     29.042µs   130.916µs  10000000   9500230   0        499770   0      0           0        232      256      2212.56    6.4306615s
RocksDB  YCSB-F     read-modify-write             274121   3.648µs      39.25µs   131.375µs  177.083µs  10000000   5002011   0        0        0      0           4997989  256      256      3661.62    36.48025975s
```
