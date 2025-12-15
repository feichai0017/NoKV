# NoKV Benchmarks

This document describes the benchmarking tools and procedures for NoKV.

## Overview

NoKV includes comprehensive benchmarking tools to measure performance across different workloads and compare against other storage engines like Badger and RocksDB.

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks using the script
./scripts/run_benchmarks.sh

# Or using Make
make bench
```

### Manual Benchmark Runs

```bash
# Run YCSB-style benchmarks
cd benchmark
go test -bench=. -benchtime=10s -benchmem

# Run with specific workload
go test -bench=BenchmarkYCSB -benchtime=30s -benchmem
```

## Benchmark Types

### 1. YCSB Workloads

The benchmark suite implements Yahoo! Cloud Serving Benchmark (YCSB) workloads:

- **Workload A**: 50% reads, 50% writes (mixed)
- **Workload B**: 95% reads, 5% writes (read-heavy)
- **Workload C**: 100% reads (read-only)
- **Workload D**: 95% reads, 5% inserts (read-latest)
- **Workload E**: 5% reads, 95% scans (scan-heavy)
- **Workload F**: 50% reads, 50% read-modify-write

### 2. Engine Comparisons

Benchmarks support multiple storage engines:

- **NoKV**: Native implementation
- **Badger**: Comparison with dgraph-io/badger
- **RocksDB**: Comparison with RocksDB (requires CGO)

### 3. Custom Benchmarks

Location: `benchmark/`

Key benchmark files:
- `ycsb_runner.go`: Main benchmark runner
- `ycsb_engine_nokv.go`: NoKV implementation
- `ycsb_engine_badger.go`: Badger implementation
- `ycsb_engine_rocksdb.go`: RocksDB implementation

## Benchmark Configuration

### Flags

```bash
# Customize benchmark parameters
go test -bench=. \
  -keys=1000000 \        # Number of keys
  -valueSize=1024 \      # Value size in bytes
  -duration=60s \        # Test duration
  -threads=16 \          # Number of concurrent threads
  -workload=A            # YCSB workload type
```

Available flags (defined in `ycsb_flags.go`):
- `-keys`: Total number of keys to use
- `-valueSize`: Size of values in bytes
- `-duration`: Duration to run the benchmark
- `-threads`: Number of concurrent worker threads
- `-workload`: YCSB workload type (A-F)
- `-engine`: Storage engine to benchmark (nokv, badger, rocksdb)

## Analyzing Results

### Output Format

Benchmark results are saved to `benchmark/benchmark_results/`:

```
benchmark_results/
├── nokv_workloadA_20240315_143022.json
├── badger_workloadA_20240315_143122.json
└── summary.txt
```

### Result Structure

```json
{
  "engine": "nokv",
  "workload": "A",
  "operations": 1000000,
  "duration_seconds": 60.5,
  "throughput_ops_sec": 16528,
  "latency_p50_us": 580,
  "latency_p95_us": 1240,
  "latency_p99_us": 2180,
  "read_ops": 500000,
  "write_ops": 500000
}
```

### Comparing Results

Use the benchmark results tool:

```bash
go run ./benchmark/benchmark_results.go \
  --compare \
  --baseline=nokv_workloadA_20240315_143022.json \
  --candidate=nokv_workloadA_20240315_153022.json
```

## Performance Metrics

### Key Metrics Tracked

1. **Throughput**: Operations per second
2. **Latency**: P50, P95, P99, P999 percentiles
3. **Memory Usage**: Heap allocations, GC pressure
4. **Disk I/O**: Read/write amplification
5. **Cache Hit Rate**: Block cache effectiveness

### Expected Performance

Typical results on modern hardware (indicative):

| Workload   | Throughput (ops/sec) | P99 Latency (μs) |
|------------|----------------------|------------------|
| A (Mixed)  | ~50K                 | ~2000            |
| B (Read)   | ~100K                | ~800             |
| C (Read)   | ~120K                | ~600             |
| D (Latest) | ~80K                 | ~1200            |
| E (Scan)   | ~10K                 | ~15000           |
| F (RMW)    | ~45K                 | ~2500            |

*Note: Actual performance varies based on hardware, configuration, and data characteristics.*

## Profiling

### CPU Profiling

```bash
go test -bench=BenchmarkYCSB \
  -cpuprofile=cpu.pprof \
  -benchtime=30s

go tool pprof cpu.pprof
```

### Memory Profiling

```bash
go test -bench=BenchmarkYCSB \
  -memprofile=mem.pprof \
  -benchtime=30s

go tool pprof mem.pprof
```

### Using pprof Analysis Script

```bash
./scripts/analyze_pprof.sh cpu.pprof
./scripts/analyze_pprof.sh mem.pprof
```

## Continuous Benchmarking

### CI Integration

Benchmarks run automatically on:
- Pull requests (regression detection)
- Nightly builds (trend tracking)
- Release candidates (validation)

### Regression Detection

Set up benchmark comparison in CI:

```yaml
- name: Run benchmarks
  run: make bench

- name: Compare with baseline
  run: |
    go test -bench=. -benchmem -run=^$ \
      | tee new_bench.txt
    benchstat baseline_bench.txt new_bench.txt
```

## Best Practices

### 1. Consistent Environment

- Use dedicated hardware for benchmarks
- Disable CPU frequency scaling
- Close background applications
- Use the same Go version

### 2. Sufficient Duration

- Run benchmarks for at least 30 seconds
- Use `-benchtime=30s` or longer
- Multiple iterations for statistical significance

### 3. Warm-up Phase

- Discard first few runs
- Let OS page cache warm up
- Allow compaction to stabilize

### 4. Monitoring

- Monitor system resources (CPU, memory, I/O)
- Check for throttling or resource contention
- Verify no background compaction during runs

## Troubleshooting

### Low Throughput

- Check CPU utilization
- Verify disk I/O is not saturated
- Review compaction overhead
- Examine value log GC frequency

### High Latency

- Check P99/P999 latencies specifically
- Review write amplification
- Verify memtable size configuration
- Check for lock contention

### OOM Errors

- Reduce concurrent threads
- Decrease memtable size
- Limit cache sizes
- Use smaller value sizes

## Contributing Benchmarks

When adding new benchmarks:

1. Follow existing patterns in `benchmark/`
2. Add documentation for new workloads
3. Include baseline results
4. Test on multiple hardware configurations
5. Add to CI if appropriate

## References

- [YCSB Paper](https://research.cs.yale.edu/haystackfs/ycsb.pdf)
- [Go Benchmarking Guide](https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
- [benchstat Tool](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)

---

Last updated: December 2024
