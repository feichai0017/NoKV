package lsm

import (
	"log/slog"
	"runtime"
	"time"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
)

// PrefixExtractor maps a user key to the prefix used for SST-level prefix
// bloom filtering. Returning nil or an empty prefix disables prefix filtering
// for that key.
type PrefixExtractor func(userKey []byte) []byte

// BlockCompression selects the SST data-block compression codec.
type BlockCompression uint32

const (
	BlockCompressionNone BlockCompression = iota
	BlockCompressionSnappy
)

const (
	// DefaultBlockSize is the default SST block size in bytes.
	DefaultBlockSize = 8 << 10
	// DefaultBloomFalsePositive is the default bloom filter false positive rate.
	DefaultBloomFalsePositive = 0.01
	// DefaultBlockCompression is the default data-block compression codec.
	DefaultBlockCompression = BlockCompressionSnappy
	// DefaultLevelSizeMultiplier is the default leveled target size ratio between levels.
	DefaultLevelSizeMultiplier = 8
	// DefaultTableSizeMultiplier is the default target size ratio between adjacent tables.
	DefaultTableSizeMultiplier = 2
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
	// DefaultCompactionTombstoneWeight biases picker priorities toward levels
	// with dense range tombstones.
	DefaultCompactionTombstoneWeight = 1.0
	// DefaultWriteThrottleMinRate is the slowest write rate used during slowdown mode.
	DefaultWriteThrottleMinRate int64 = 128 << 20
	// DefaultWriteThrottleMaxRate is the initial write rate used when slowdown first activates.
	DefaultWriteThrottleMaxRate int64 = 1 << 30
)

// Options captures LSM-local runtime configuration derived from the top-level
// DB options before the storage engine starts.
type Options struct {
	// FS provides the filesystem implementation for manifest operations.
	FS vfs.FS
	// Logger handles background/storage logs for the LSM subsystem.
	Logger *slog.Logger

	WorkDir        string
	MemTableSize   int64
	MemTableEngine string
	SSTableMaxSz   int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64
	// PrefixExtractor enables SST prefix bloom filters for prefix-local
	// workloads. It receives user keys, not internal CF/timestamp keys.
	PrefixExtractor PrefixExtractor
	// BlockCompression controls SST data-block compression.
	BlockCompression BlockCompression

	// Cache budgets. Zero disables the corresponding user-space cache.
	BlockCacheBytes int64
	IndexCacheBytes int64

	// compact
	NumCompactors       int
	CompactionPolicy    string
	BaseLevelSize       int64
	LevelSizeMultiplier int // Target size ratio between levels.
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
	// L0SlowdownWritesTrigger starts write pacing when L0 table count reaches
	// this threshold. Values <= 0 disable L0-based slowdown.
	L0SlowdownWritesTrigger int
	// L0StopWritesTrigger blocks writes when L0 table count reaches this
	// threshold. Values <= 0 disable L0-based hard stop.
	L0StopWritesTrigger int
	// L0ResumeWritesTrigger clears slowdown/stop only when L0 table count drops
	// to this threshold or lower, providing hysteresis and reducing oscillation.
	L0ResumeWritesTrigger int
	// CompactionSlowdownTrigger starts write pacing when max compaction score
	// reaches this value. Values <= 0 disable score-based slowdown.
	CompactionSlowdownTrigger float64
	// CompactionStopTrigger blocks writes when max compaction score reaches this
	// value. Values <= 0 disable score-based hard stop.
	CompactionStopTrigger float64
	// CompactionResumeTrigger clears throttling only when max compaction score
	// drops to this value or lower, providing hysteresis.
	CompactionResumeTrigger float64
	// WriteThrottleMinRate is the target write admission rate in bytes/sec when
	// slowdown pressure approaches the stop threshold.
	WriteThrottleMinRate int64
	// WriteThrottleMaxRate is the target write admission rate in bytes/sec when
	// slowdown first becomes active.
	WriteThrottleMaxRate int64

	IngestCompactBatchSize  int
	IngestBacklogMergeScore float64
	IngestShardParallelism  int

	// CompactionValueWeight increases the priority of levels containing a high
	// proportion of ValueLog-backed payloads. Must be non-negative.
	CompactionValueWeight float64
	// CompactionTombstoneWeight increases the priority of levels containing a
	// high proportion of range tombstones. Must be non-negative.
	CompactionTombstoneWeight float64
	// TTLCompactionMinAge forces stale-data cleanup for max-level tables older
	// than this age. Zero disables age-triggered cleanup; size-triggered stale
	// cleanup remains active.
	TTLCompactionMinAge time.Duration

	// CompactionWriteBytesPerSec paces compaction output writes. Zero disables
	// pacing. Flush writes are never paced.
	CompactionWriteBytesPerSec int64
	// CompactionPacingBypassL0 bypasses output pacing when L0 table count
	// reaches this threshold. Zero derives a conservative threshold when
	// compaction pacing is enabled; negative values are clamped to zero.
	CompactionPacingBypassL0 int

	// CompactionValueAlertThreshold triggers stats alerts when value density
	// exceeds this ratio.
	CompactionValueAlertThreshold float64

	DiscardStatsCh *chan map[manifest.ValueLogID]int64

	// ManifestSync controls whether manifest edits are fsynced immediately.
	ManifestSync bool
	// ManifestRewriteThreshold triggers a manifest rewrite when the manifest
	// grows beyond this size (bytes). Values <= 0 disable rewrites.
	ManifestRewriteThreshold int64

	// ThrottleCallback receives write admission changes after the LSM updates its
	// internal throttle state.
	ThrottleCallback func(WriteThrottleState)

	// NegativeCachePersistent enables snapshot-on-Close + restore-on-Open for
	// the in-memory negative cache, backed by an engine/slab segment under
	// WorkDir. Defaults to false. When enabled, a process restart skips the
	// cold-start re-warm phase for previously-known not-found keys (HDFS path
	// probes, S3 GetObject 404 patterns, fsmeta Lookup misses). The slab is
	// best-effort (Derived consistency class — see
	// docs/notes/2026-04-27-slab-substrate.md §6.1): a corrupt or missing
	// snapshot forces a re-warm but does not affect read correctness.
	NegativeCachePersistent bool
	// NegativeCacheSlabMaxSize bounds the on-disk snapshot size. Snapshots
	// stop appending once the limit is hit; remaining keys re-warm normally.
	// Zero falls back to a 64 MiB default.
	NegativeCacheSlabMaxSize int64
}

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

// NormalizeInPlace resolves constructor defaults and clamps invalid
// compaction/throttle settings in place.
func (opt *Options) NormalizeInPlace() {
	if opt == nil {
		return
	}
	if opt.NumCompactors <= 0 {
		opt.NumCompactors = DefaultNumCompactors()
	}
	opt.normalizeCompactionOptions()
	if opt.IngestShardParallelism <= 0 {
		opt.IngestShardParallelism = max(opt.NumCompactors/2, 2)
	}
	if opt.CompactionValueWeight < 0 {
		opt.CompactionValueWeight = 0
	}
	if opt.CompactionValueWeight == 0 {
		opt.CompactionValueWeight = DefaultCompactionValueWeight
	}
	if opt.CompactionTombstoneWeight < 0 {
		opt.CompactionTombstoneWeight = 0
	}
	if opt.CompactionTombstoneWeight == 0 {
		opt.CompactionTombstoneWeight = DefaultCompactionTombstoneWeight
	}
	if opt.TTLCompactionMinAge < 0 {
		opt.TTLCompactionMinAge = 0
	}
	if opt.CompactionValueAlertThreshold <= 0 {
		opt.CompactionValueAlertThreshold = DefaultCompactionValueAlertThreshold
	}
	if opt.CompactionWriteBytesPerSec < 0 {
		opt.CompactionWriteBytesPerSec = 0
	}
	if opt.CompactionPacingBypassL0 < 0 {
		opt.CompactionPacingBypassL0 = 0
	}
	if opt.CompactionWriteBytesPerSec > 0 && opt.CompactionPacingBypassL0 == 0 {
		opt.CompactionPacingBypassL0 = max(1, opt.L0SlowdownWritesTrigger/2)
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

func (opt *Options) normalizeCompactionOptions() {
	if opt.NumLevelZeroTables <= 0 {
		opt.NumLevelZeroTables = DefaultNumLevelZeroTables
	}
	if opt.L0SlowdownWritesTrigger <= 0 {
		opt.L0SlowdownWritesTrigger = opt.NumLevelZeroTables
	}
	if opt.L0StopWritesTrigger <= 0 {
		opt.L0StopWritesTrigger = opt.NumLevelZeroTables * 3
	}
	if opt.L0ResumeWritesTrigger <= 0 {
		opt.L0ResumeWritesTrigger = max(1, int(float64(opt.L0SlowdownWritesTrigger)*0.75))
	}
	if opt.CompactionSlowdownTrigger <= 0 {
		opt.CompactionSlowdownTrigger = DefaultCompactionSlowdownTrigger
	}
	if opt.CompactionStopTrigger <= 0 {
		opt.CompactionStopTrigger = DefaultCompactionStopTrigger
	}
	if opt.CompactionResumeTrigger <= 0 {
		opt.CompactionResumeTrigger = DefaultCompactionResumeTrigger
	}
	if opt.IngestCompactBatchSize <= 0 {
		opt.IngestCompactBatchSize = DefaultIngestCompactBatchSize
	}
	if opt.IngestBacklogMergeScore <= 0 {
		opt.IngestBacklogMergeScore = DefaultIngestBacklogMergeScore
	}
	if opt.L0StopWritesTrigger <= opt.L0SlowdownWritesTrigger {
		opt.L0StopWritesTrigger = opt.L0SlowdownWritesTrigger + 1
	}
	if opt.L0ResumeWritesTrigger >= opt.L0SlowdownWritesTrigger {
		opt.L0ResumeWritesTrigger = max(1, opt.L0SlowdownWritesTrigger-1)
	}
	if opt.CompactionStopTrigger < opt.CompactionSlowdownTrigger {
		opt.CompactionStopTrigger = opt.CompactionSlowdownTrigger
	}
	if opt.CompactionResumeTrigger > opt.CompactionSlowdownTrigger {
		opt.CompactionResumeTrigger = opt.CompactionSlowdownTrigger
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
