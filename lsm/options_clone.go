package lsm

import "runtime"

const (
	// DefaultBlockCacheBytes is the default budget for cached L0/L1 blocks.
	DefaultBlockCacheBytes int64 = 16 << 20
	// DefaultIndexCacheBytes is the default budget for decoded SSTable indexes.
	DefaultIndexCacheBytes int64 = 32 << 20
	// DefaultNumLevelZeroTables is the default L0 table budget before throttling starts.
	DefaultNumLevelZeroTables = 32
	// DefaultCompactionSlowdownTrigger starts write pacing once max score reaches this value.
	DefaultCompactionSlowdownTrigger = 4.0
	// DefaultCompactionStopTrigger blocks writes once max score reaches this value.
	DefaultCompactionStopTrigger = 12.0
	// DefaultCompactionResumeTrigger clears throttling once max score falls below this value.
	DefaultCompactionResumeTrigger = 2.0
	// DefaultIngestCompactBatchSize is the default number of ingest tables compacted per cycle.
	DefaultIngestCompactBatchSize = 4
	// DefaultIngestBacklogMergeScore triggers ingest merge when backlog crosses this value.
	DefaultIngestBacklogMergeScore = 2.0
	// DefaultCompactionValueWeight biases picker priorities toward value-pointer-heavy levels.
	DefaultCompactionValueWeight = 0.35
	// DefaultCompactionValueAlertThreshold raises value-density alerts above this ratio.
	DefaultCompactionValueAlertThreshold = 0.6
	// DefaultWriteThrottleMinRate is the slowest write rate used during slowdown mode.
	DefaultWriteThrottleMinRate int64 = 128 << 20
	// DefaultWriteThrottleMaxRate is the initial write rate used when slowdown first activates.
	DefaultWriteThrottleMaxRate int64 = 1 << 30
)

// Clone returns a shallow copy of the LSM options. It is used when background
// workers (e.g. compaction) need an immutable view of the configuration while
// the user may continue tweaking the top-level DB options.
func (opt *Options) Clone() *Options {
	if opt == nil {
		return nil
	}
	clone := *opt
	return &clone
}

func (opt *Options) normalized() *Options {
	if opt == nil {
		return nil
	}
	clone := *opt
	clone.normalizeInPlace()
	return &clone
}

func (opt *Options) normalizeInPlace() {
	if opt == nil {
		return
	}
	if opt.NumCompactors <= 0 {
		opt.NumCompactors = DefaultNumCompactors()
	}
	if opt.NumLevelZeroTables <= 0 {
		opt.NumLevelZeroTables = DefaultNumLevelZeroTables
	}
	if opt.L0SlowdownWritesTrigger <= 0 {
		opt.L0SlowdownWritesTrigger = opt.NumLevelZeroTables
	}
	if opt.L0StopWritesTrigger <= 0 {
		opt.L0StopWritesTrigger = opt.NumLevelZeroTables * 3
	}
	if opt.L0StopWritesTrigger <= opt.L0SlowdownWritesTrigger {
		opt.L0StopWritesTrigger = opt.L0SlowdownWritesTrigger + 1
	}
	if opt.L0ResumeWritesTrigger <= 0 {
		opt.L0ResumeWritesTrigger = max(1, int(float64(opt.L0SlowdownWritesTrigger)*0.75))
	}
	if opt.L0ResumeWritesTrigger >= opt.L0SlowdownWritesTrigger {
		opt.L0ResumeWritesTrigger = max(1, opt.L0SlowdownWritesTrigger-1)
	}
	if opt.CompactionSlowdownTrigger <= 0 {
		opt.CompactionSlowdownTrigger = DefaultCompactionSlowdownTrigger
	}
	if opt.CompactionStopTrigger <= 0 {
		opt.CompactionStopTrigger = DefaultCompactionStopTrigger
	}
	if opt.CompactionStopTrigger < opt.CompactionSlowdownTrigger {
		opt.CompactionStopTrigger = opt.CompactionSlowdownTrigger
	}
	if opt.CompactionResumeTrigger <= 0 {
		opt.CompactionResumeTrigger = DefaultCompactionResumeTrigger
	}
	if opt.CompactionResumeTrigger > opt.CompactionSlowdownTrigger {
		opt.CompactionResumeTrigger = opt.CompactionSlowdownTrigger
	}
	if opt.IngestCompactBatchSize <= 0 {
		opt.IngestCompactBatchSize = DefaultIngestCompactBatchSize
	}
	if opt.IngestBacklogMergeScore <= 0 {
		opt.IngestBacklogMergeScore = DefaultIngestBacklogMergeScore
	}
	if opt.IngestShardParallelism <= 0 {
		opt.IngestShardParallelism = max(opt.NumCompactors/2, 2)
	}
	if opt.CompactionValueWeight < 0 {
		opt.CompactionValueWeight = 0
	}
	if opt.CompactionValueWeight == 0 {
		opt.CompactionValueWeight = DefaultCompactionValueWeight
	}
	if opt.CompactionValueAlertThreshold <= 0 {
		opt.CompactionValueAlertThreshold = DefaultCompactionValueAlertThreshold
	}
	if opt.WriteThrottleMinRate <= 0 {
		opt.WriteThrottleMinRate = DefaultWriteThrottleMinRate
	}
	if opt.WriteThrottleMaxRate <= 0 {
		opt.WriteThrottleMaxRate = DefaultWriteThrottleMaxRate
	}
	if opt.WriteThrottleMaxRate < opt.WriteThrottleMinRate {
		opt.WriteThrottleMaxRate = opt.WriteThrottleMinRate
	}
	if opt.BlockCacheBytes < 0 {
		opt.BlockCacheBytes = 0
	}
	if opt.IndexCacheBytes < 0 {
		opt.IndexCacheBytes = 0
	}
}

// DefaultNumCompactors derives the default compaction worker count from host CPU count.
func DefaultNumCompactors() int {
	cpu := runtime.NumCPU()
	if cpu <= 1 {
		return 1
	}
	return min(max(cpu/2, 2), 8)
}
