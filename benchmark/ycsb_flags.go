package benchmark

import (
	"flag"
	"time"
)

var (
	fBenchDir   = flag.String("benchdir", "benchmark_data", "benchmark working directory")
	fSeed       = flag.Int64("seed", 42, "random seed for data generation")
	fSyncWrites = flag.Bool("sync", false, "force fsync on every write")

	fValueThreshold = flag.Int("value_threshold", 32, "value size threshold (bytes) before spilling to the value log")

	fBadgerBlockMB     = flag.Int("badger_block_cache_mb", -1, "Badger block cache size (MB); <=0 falls back to ycsb_block_cache_mb")
	fBadgerIndexMB     = flag.Int("badger_index_cache_mb", -1, "Badger index cache size (MB); <=0 falls back to block cache size")
	fBadgerCompression = flag.String("badger_compression", "none", "Badger compression codec: none|snappy|zstd")

	ycsbWorkloads        = flag.String("ycsb_workloads", "A,B,C,D,F", "comma-separated YCSB workloads (A-F)")
	ycsbEngines          = flag.String("ycsb_engines", "nokv,badger", "comma-separated engines to benchmark (nokv,badger,rocksdb)")
	ycsbRecords          = flag.Int("ycsb_records", 1000000, "number of records to preload during YCSB load phase")
	ycsbOperations       = flag.Int("ycsb_ops", 1000000, "number of transactional operations per workload")
	ycsbConcurrency      = flag.Int("ycsb_conc", 16, "worker goroutine count for YCSB transactional phase")
	ycsbScanLength       = flag.Int("ycsb_scan_len", 100, "scan length (items) used by YCSB workload E")
	ycsbValueSize        = flag.Int("ycsb_value_size", 256, "value size (bytes) for YCSB records")
	ycsbWarmOperations   = flag.Int("ycsb_warm_ops", 100000, "warm-up operations executed per workload before measuring")
	ycsbTargetOps        = flag.Int("ycsb_target_ops", 0, "optional target ops/sec (overall) during run; 0 disables throttling")
	ycsbStatusInterval   = flag.Duration("ycsb_status_interval", 10*time.Second, "interval for progress/status reporting; 0 disables")
	ycsbRocksCompression = flag.String("ycsb_rocks_compression", "none", "RocksDB compression codec: none|snappy|zstd")
	ycsbBlockCacheMB     = flag.Int("ycsb_block_cache_mb", 256, "Block cache size (MB) applied to RocksDB/NoKV tables")
)

const benchmarkEnvKey = "NOKV_RUN_BENCHMARKS"
