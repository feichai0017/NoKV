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

	ycsbWorkloads         = flag.String("ycsb_workloads", "A,B,C,D,F", "comma-separated YCSB workloads (A-F)")
	ycsbEngines           = flag.String("ycsb_engines", "nokv,badger", "comma-separated engines to benchmark (nokv,nokv-skiplist,nokv-art,badger,pebble,rocksdb)")
	ycsbRecords           = flag.Int("ycsb_records", 1000000, "number of records to preload during YCSB load phase")
	ycsbOperations        = flag.Int("ycsb_ops", 1000000, "number of transactional operations per workload")
	ycsbConcurrency       = flag.Int("ycsb_conc", 16, "worker goroutine count for YCSB transactional phase")
	ycsbScanLength        = flag.Int("ycsb_scan_len", 100, "scan length (items) used by YCSB workload E")
	ycsbValueSize         = flag.Int("ycsb_value_size", 256, "value size (bytes) for YCSB records")
	ycsbWarmOperations    = flag.Int("ycsb_warm_ops", 100000, "warm-up operations executed per workload before measuring")
	ycsbTargetOps         = flag.Int("ycsb_target_ops", 0, "optional target ops/sec (overall) during run; 0 disables throttling")
	ycsbStatusInterval    = flag.Duration("ycsb_status_interval", 10*time.Second, "interval for progress/status reporting; 0 disables")
	ycsbPebbleCompression = flag.String("ycsb_pebble_compression", "none", "Pebble compression codec: none|snappy|zstd")
	ycsbRocksCompression  = flag.String("ycsb_rocks_compression", "none", "RocksDB compression codec: none|snappy|zstd")
	ycsbBlockCacheMB      = flag.Int("ycsb_block_cache_mb", 256, "Block cache size (MB) applied to RocksDB/NoKV tables")
	ycsbMemtableMB        = flag.Int("ycsb_memtable_mb", 64, "Memtable size (MB) for LSM engines (NoKV/RocksDB where applicable)")
	ycsbSSTableMB         = flag.Int("ycsb_sstable_mb", 512, "Target SST size (MB) for LSM engines (NoKV/RocksDB where applicable)")
	ycsbVlogFileMB        = flag.Int("ycsb_vlog_mb", 512, "Value log file size (MB) for engines that separate values (NoKV)")
	ycsbValueDist         = flag.String("ycsb_value_dist", "fixed", "Value size distribution: fixed|uniform|normal|percentile")
	ycsbValueMin          = flag.Int("ycsb_value_min", 0, "Min value size for uniform/normal distributions (bytes); 0 defaults to value_size")
	ycsbValueMax          = flag.Int("ycsb_value_max", 0, "Max value size for uniform/normal distributions (bytes); 0 defaults to value_size")
	ycsbValueStd          = flag.Int("ycsb_value_std", 0, "Stddev for normal distribution (bytes); 0 defaults to value_size/4")
	ycsbValuePercentiles  = flag.String("ycsb_value_percentiles", "", "Percentile map for percentile dist, e.g. \"50:256,90:512,99:1024,100:2048\"")
	ycsbBatchInsert       = flag.Bool("ycsb_batch_insert", false, "enable batch insert for Badger")
	ycsbBatchSize         = flag.Int("ycsb_batch_size", 1000000, "batch size for batch insert")
)

const benchmarkEnvKey = "NOKV_RUN_BENCHMARKS"
