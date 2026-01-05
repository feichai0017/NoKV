# Benchmarks

This document captures the most recent results from running the default
benchmark script (`scripts/run_benchmarks.sh`).

## YCSB Framework Overview

The benchmark harness uses the YCSB workloads (A/B/C/D/F) to exercise NoKV,
Badger, and RocksDB with a fixed total operation count and report both
throughput and latency percentiles. The default script runs a load phase to
seed data, then executes each workload and collects:
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
  and per-engine implementations live in `benchmark/ycsb_engine_*`.
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
NoKV     YCSB-A     50/50 read/update             307827   3.248us      24.541us  84.209us   161.917us  10000000   5001099   4998901  0        0      0           0        249      256      2375.66    32.485813458s
NoKV     YCSB-B     95/5 read/update              904900   1.105us      6.125us   64.958us   127.583us  10000000   9499771   500229   0        0      0           0        245      256      2334.66    11.0509435s
NoKV     YCSB-C     100% read                     1323274  755ns        3.834us   16.75us    145.125us  10000000   10000000  0        0        0      0           0        245      256      2332.53    7.557016125s
NoKV     YCSB-D     95% read, 5% insert (latest)  1028166  972ns        4.166us   56.75us    121.583us  10000000   9500230   0        499770   0      0           0        237      256      2259.51    9.726053375s
NoKV     YCSB-F     read-modify-write             377600   2.648us      29.167us  105.833us  183.167us  10000000   5002011   0        0        0      0           4997989  251      256      3583.11    26.48308125s
Badger   YCSB-A     50/50 read/update             229118   4.364us      59.167us  131.833us  180.333us  10000000   5001099   4998901  0        0      0           0        256      256      2441.41    43.645710541s
Badger   YCSB-B     95/5 read/update              327207   3.056us      21.75us   169.417us  282.417us  10000000   9499771   500229   0        0      0           0        256      256      2441.41    30.561696834s
Badger   YCSB-C     100% read                     469097   2.131us      6.708us   187.834us  458.375us  10000000   10000000  0        0        0      0           0        256      256      2441.41    21.317565208s
Badger   YCSB-D     95% read, 5% insert (latest)  666866   1.499us      5.333us   92.166us   160.25us   10000000   9500230   0        499770   0      0           0        245      256      2338.52    14.995520375s
Badger   YCSB-F     read-modify-write             187611   5.33us       73.958us  181.959us  244.167us  10000000   5002011   0        0        0      0           4997989  256      256      3661.62    53.301762833s
RocksDB  YCSB-A     50/50 read/update             155600   6.426us      7.792us   107us      308.459us  10000000   5001099   4998901  0        0      0           0        256      256      2441.41    1m4.267376375s
RocksDB  YCSB-B     95/5 read/update              1285043  778ns        3.458us   32.25us    147.667us  10000000   9499771   500229   0        0      0           0        256      256      2441.41    7.781842959s
RocksDB  YCSB-C     100% read                     1789003  558ns        3.083us   18.334us   45.5us     10000000   10000000  0        0        0      0           0        256      256      2441.41    5.589705208s
RocksDB  YCSB-D     95% read, 5% insert (latest)  1595406  626ns        2.583us   29.417us   132.042us  10000000   9500230   0        499770   0      0           0        233      256      2223.08    6.267996334s
RocksDB  YCSB-F     read-modify-write             255982   3.906us      26.083us  143.5us    200.292us  10000000   5002011   0        0        0      0           4997989  256      256      3661.62    39.065297417s
```
