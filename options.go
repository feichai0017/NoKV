package NoKV

import (
	"time"

	"github.com/feichai0017/NoKV/kv"
	lsmpkg "github.com/feichai0017/NoKV/lsm"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
)

const (
	defaultWriteBatchMaxCount             = 64
	defaultWriteBatchMaxSize        int64 = 1 << 20
	defaultHotRingTopK                    = 16
	defaultHotReadPrefetchThreshold int32 = 16
	defaultHotReadPrefetchCooldown        = 15 * time.Second
	defaultWALBufferSize                  = 4 << 20 // 4 MiB
)

// Options holds the top-level database configuration.
type Options struct {
	// FS provides the filesystem implementation used by DB runtime components.
	// Nil defaults to vfs.OSFS.
	FS vfs.FS

	ValueThreshold     int64
	WorkDir            string
	MemTableSize       int64
	MemTableEngine     MemTableEngine
	SSTableMaxSz       int64
	MaxBatchCount      int64
	MaxBatchSize       int64 // max batch size in bytes
	ValueLogFileSize   int
	ValueLogMaxEntries uint32
	// ValueLogBucketCount controls how many hash buckets the value log uses.
	// Values <= 1 disable bucketization.
	ValueLogBucketCount int
	// ValueLogHotBucketCount reserves this many buckets for hot keys when
	// HotRing-based routing is enabled. Values <= 0 disable hot/cold splitting.
	ValueLogHotBucketCount int
	// ValueLogHotKeyThreshold marks a key as hot once its HotRing counter reaches
	// this value. Values <= 0 disable HotRing-based routing.
	ValueLogHotKeyThreshold int32
	ValueSeparationPolicies []*kv.ValueSeparationPolicy

	// ValueLogGCInterval specifies how frequently to trigger a check for value
	// log garbage collection. Zero or negative values disable automatic GC.
	ValueLogGCInterval time.Duration
	// ValueLogGCDiscardRatio is the discard ratio for a value log file to be
	// considered for garbage collection. It must be in the range (0.0, 1.0).
	ValueLogGCDiscardRatio float64
	// ValueLogGCParallelism controls how many value-log GC tasks can run in
	// parallel. Values <= 0 auto-tune based on compaction workers.
	ValueLogGCParallelism int
	// ValueLogGCReduceScore lowers GC parallelism when compaction max score meets
	// or exceeds this threshold. Values <= 0 use defaults.
	ValueLogGCReduceScore float64
	// ValueLogGCSkipScore skips GC when compaction max score meets or exceeds this
	// threshold. Values <= 0 use defaults.
	ValueLogGCSkipScore float64
	// ValueLogGCReduceBacklog lowers GC parallelism when compaction backlog meets
	// or exceeds this threshold. Values <= 0 use defaults.
	ValueLogGCReduceBacklog int
	// ValueLogGCSkipBacklog skips GC when compaction backlog meets or exceeds this
	// threshold. Values <= 0 use defaults.
	ValueLogGCSkipBacklog int

	// Value log GC sampling parameters. Ratios <= 0 fall back to defaults.
	ValueLogGCSampleSizeRatio  float64
	ValueLogGCSampleCountRatio float64
	ValueLogGCSampleFromHead   bool

	// ValueLogVerbose enables verbose logging across value-log operations.
	ValueLogVerbose bool

	WriteBatchMaxCount int
	WriteBatchMaxSize  int64

	DetectConflicts bool
	HotRingEnabled  bool
	HotRingBits     uint8
	HotRingTopK     int
	// HotReadPrefetchThreshold enqueues async read prefetch once a key reaches
	// this hot-read count. Values <= 0 fall back to the default threshold.
	HotReadPrefetchThreshold int32
	// HotReadPrefetchCooldown suppresses repeated prefetch for the same hot key
	// during this interval. Values <= 0 fall back to the default cooldown.
	HotReadPrefetchCooldown time.Duration
	// HotRingRotationInterval enables dual-ring rotation for hotness tracking.
	// Zero disables rotation.
	HotRingRotationInterval time.Duration
	// HotRingNodeCap caps the number of tracked keys per ring. Zero disables the cap.
	HotRingNodeCap uint64
	// HotRingNodeSampleBits controls stable sampling once the cap is reached.
	// A value of 0 enforces a strict cap; larger values sample 1/2^N keys.
	HotRingNodeSampleBits uint8
	// HotRingDecayInterval controls how often HotRing halves its global counters.
	// Zero disables periodic decay.
	HotRingDecayInterval time.Duration
	// HotRingDecayShift determines how aggressively counters decay (count >>= shift).
	HotRingDecayShift uint32
	// HotRingWindowSlots controls the number of sliding-window buckets tracked per key.
	// Zero disables the sliding window.
	HotRingWindowSlots int
	// HotRingWindowSlotDuration sets the duration of each sliding-window bucket.
	HotRingWindowSlotDuration time.Duration
	// ValueLogHotRingOverride uses the dedicated ValueLogHotRing* settings instead
	// of the global HotRing configuration when routing hot value-log keys.
	ValueLogHotRingOverride bool
	// ValueLogHotRingBits controls the hash bucket count for the value-log ring.
	// Zero uses the default HotRing bucket count.
	ValueLogHotRingBits uint8
	// ValueLogHotRingRotationInterval enables rotation for the value-log ring.
	// Zero disables rotation.
	ValueLogHotRingRotationInterval time.Duration
	// ValueLogHotRingNodeCap caps the number of tracked keys per value-log ring.
	ValueLogHotRingNodeCap uint64
	// ValueLogHotRingNodeSampleBits controls stable sampling for value-log keys.
	// A value of 0 enforces a strict cap; larger values sample 1/2^N keys.
	ValueLogHotRingNodeSampleBits uint8
	// ValueLogHotRingDecayInterval controls how often the value-log ring decays counters.
	ValueLogHotRingDecayInterval time.Duration
	// ValueLogHotRingDecayShift determines decay aggressiveness for the value-log ring.
	ValueLogHotRingDecayShift uint32
	// ValueLogHotRingWindowSlots controls the number of sliding-window buckets for the value-log ring.
	ValueLogHotRingWindowSlots int
	// ValueLogHotRingWindowSlotDuration sets the duration of each value-log window bucket.
	ValueLogHotRingWindowSlotDuration time.Duration

	SyncWrites bool
	// SyncPipeline enables a dedicated sync worker goroutine that decouples
	// WAL fsync from the commit pipeline. When false (the default), the commit
	// worker performs fsync inline. Only effective when SyncWrites is true.
	SyncPipeline bool
	ManifestSync bool
	// ManifestRewriteThreshold triggers a manifest rewrite when the active
	// MANIFEST file grows beyond this size (bytes). Values <= 0 disable rewrites.
	ManifestRewriteThreshold int64
	// WriteHotKeyLimit caps how many consecutive writes a single key can issue
	// before the DB returns utils.ErrHotKeyWriteThrottle. Zero disables write-path
	// throttling.
	WriteHotKeyLimit int32
	// HotWriteBurstThreshold marks a key as "hot" for batching when its write
	// frequency exceeds this count; zero disables hot write batching.
	HotWriteBurstThreshold int32
	// HotWriteBatchMultiplier scales write batch limits when a hot key is
	// detected, allowing short-term coalescing of repeated writes.
	HotWriteBatchMultiplier int
	// WriteBatchWait adds an optional coalescing delay when the commit queue is
	// momentarily empty, letting small bursts share one WAL fsync/apply pass.
	// Zero disables the delay.
	WriteBatchWait time.Duration
	// WriteThrottleMinRate is the target write admission rate in bytes/sec when
	// slowdown pressure approaches the stop threshold.
	WriteThrottleMinRate int64
	// WriteThrottleMaxRate is the target write admission rate in bytes/sec when
	// slowdown first becomes active.
	WriteThrottleMaxRate int64

	// BlockCacheBytes bounds the in-memory budget for cached L0/L1 data blocks.
	// Deeper levels continue to rely on the OS page cache.
	BlockCacheBytes int64
	// IndexCacheBytes bounds the in-memory budget for decoded SSTable indexes.
	IndexCacheBytes int64
	// BloomCacheBytes bounds the in-memory budget for cached bloom filters.
	BloomCacheBytes int64

	// RaftLagWarnSegments determines how many WAL segments a follower can lag
	// behind the active segment before stats surfaces a warning. Zero disables
	// the alert.
	RaftLagWarnSegments int64

	// EnableWALWatchdog enables the background WAL backlog watchdog which
	// surfaces typed-record warnings and optionally runs automated segment GC.
	EnableWALWatchdog bool
	// WALBufferSize controls the size of the in-memory write buffer used by
	// the WAL manager. Larger buffers reduce syscall frequency at the cost of
	// memory. Values <= 0 fall back to the default (4 MiB).
	WALBufferSize int
	// WALAutoGCInterval controls how frequently the watchdog evaluates WAL
	// backlog for automated garbage collection.
	WALAutoGCInterval time.Duration
	// WALAutoGCMinRemovable is the minimum number of removable WAL segments
	// required before an automated GC pass will run.
	WALAutoGCMinRemovable int
	// WALAutoGCMaxBatch bounds how many WAL segments are removed during a single
	// automated GC pass.
	WALAutoGCMaxBatch int
	// WALTypedRecordWarnRatio triggers a typed-record warning when raft records
	// constitute at least this fraction of WAL writes. Zero disables ratio-based
	// warnings.
	WALTypedRecordWarnRatio float64
	// WALTypedRecordWarnSegments triggers a typed-record warning when the number
	// of WAL segments containing raft records exceeds this threshold. Zero
	// disables segment-count warnings.
	WALTypedRecordWarnSegments int64
	// RaftPointerSnapshot returns store-local raft WAL checkpoints used by WAL
	// watchdogs, GC policy, and diagnostics. It must return a detached snapshot.
	// Nil disables raft-specific backlog accounting.
	RaftPointerSnapshot func() map[uint64]raftmeta.RaftLogPointer

	// DiscardStatsFlushThreshold controls how many discard-stat updates must be
	// accumulated before they are flushed back into the LSM. Zero keeps the
	// default threshold.
	DiscardStatsFlushThreshold int

	// NumCompactors controls how many background compaction workers are spawned.
	// Zero uses an auto value derived from the host CPU count.
	NumCompactors int
	// CompactionPolicy selects how compaction priorities are arranged.
	// Supported values: leveled, tiered, hybrid.
	CompactionPolicy CompactionPolicy
	// NumLevelZeroTables controls when write throttling kicks in and feeds into
	// the compaction priority calculation. Zero falls back to the legacy default.
	NumLevelZeroTables int
	// L0SlowdownWritesTrigger starts write pacing when L0 table count reaches
	// this threshold. Zero falls back to NumLevelZeroTables.
	L0SlowdownWritesTrigger int
	// L0StopWritesTrigger blocks writes when L0 table count reaches this
	// threshold. Zero falls back to 3*NumLevelZeroTables.
	L0StopWritesTrigger int
	// L0ResumeWritesTrigger clears throttling only when L0 table count drops to
	// this threshold or lower. Zero falls back to ~75% of slowdown threshold.
	L0ResumeWritesTrigger int
	// CompactionSlowdownTrigger starts write pacing when max compaction score
	// reaches this value. Zero falls back to 4.0.
	CompactionSlowdownTrigger float64
	// CompactionStopTrigger blocks writes when max compaction score reaches this
	// value. Zero falls back to 12.0.
	CompactionStopTrigger float64
	// CompactionResumeTrigger clears throttling only when max compaction score
	// drops to this value or lower. Zero falls back to 2.0.
	CompactionResumeTrigger float64
	// IngestCompactBatchSize decides how many L0 tables to promote into the
	// ingest buffer per compaction cycle. Zero falls back to the legacy default.
	IngestCompactBatchSize int
	// IngestBacklogMergeScore triggers an ingest-merge task when the ingest
	// backlog score exceeds this threshold. Zero keeps the default (2.0).
	IngestBacklogMergeScore float64

	// CompactionValueWeight adjusts how aggressively the scheduler prioritises
	// levels whose entries reference large value log payloads. Higher values
	// make the compaction picker favour levels with high ValuePtr density.
	CompactionValueWeight float64

	// CompactionValueAlertThreshold triggers stats alerts when a level's
	// value-density (value bytes / total bytes) exceeds this ratio.
	CompactionValueAlertThreshold float64

	// IngestShardParallelism caps how many ingest shards can be compacted in a
	// single ingest-only pass. A value <= 0 falls back to 1 (sequential).
	IngestShardParallelism int
}

// CompactionPolicy defines compaction priority-arrangement strategy.
type CompactionPolicy string

const (
	CompactionPolicyLeveled CompactionPolicy = "leveled"
	CompactionPolicyTiered  CompactionPolicy = "tiered"
	CompactionPolicyHybrid  CompactionPolicy = "hybrid"
)

// MemTableEngine selects the in-memory index implementation used by memtables.
type MemTableEngine string

const (
	MemTableEngineSkiplist MemTableEngine = "skiplist"
	MemTableEngineART      MemTableEngine = "art"
)

// NewDefaultOptions returns the default option set.
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:                           "./work_test",
		MemTableSize:                      64 << 20,
		MemTableEngine:                    MemTableEngineART,
		SSTableMaxSz:                      256 << 20,
		HotRingEnabled:                    true,
		HotRingBits:                       12,
		HotRingTopK:                       defaultHotRingTopK,
		HotReadPrefetchThreshold:          defaultHotReadPrefetchThreshold,
		HotReadPrefetchCooldown:           defaultHotReadPrefetchCooldown,
		HotRingRotationInterval:           30 * time.Minute,
		HotRingNodeCap:                    250_000,
		HotRingNodeSampleBits:             0,
		ValueLogHotRingOverride:           true,
		ValueLogHotRingBits:               12,
		ValueLogHotRingRotationInterval:   10 * time.Minute,
		ValueLogHotRingNodeCap:            200_000,
		ValueLogHotRingNodeSampleBits:     0,
		ValueLogHotRingDecayInterval:      0,
		ValueLogHotRingDecayShift:         0,
		ValueLogHotRingWindowSlots:        6,
		ValueLogHotRingWindowSlotDuration: 100 * time.Millisecond,
		HotRingDecayInterval:              0,
		HotRingDecayShift:                 0,
		HotRingWindowSlots:                8,
		HotRingWindowSlotDuration:         250 * time.Millisecond,
		// Conservative defaults to avoid long batch-induced pauses.
		WriteBatchMaxCount:            64,
		WriteBatchMaxSize:             1 << 20,
		BlockCacheBytes:               lsmpkg.DefaultBlockCacheBytes,
		IndexCacheBytes:               lsmpkg.DefaultIndexCacheBytes,
		BloomCacheBytes:               lsmpkg.DefaultBloomCacheBytes,
		SyncWrites:                    false,
		ManifestSync:                  false,
		ManifestRewriteThreshold:      64 << 20,
		WriteHotKeyLimit:              128,
		HotWriteBurstThreshold:        8,
		HotWriteBatchMultiplier:       2,
		WriteBatchWait:                200 * time.Microsecond,
		WriteThrottleMinRate:          lsmpkg.DefaultWriteThrottleMinRate,
		WriteThrottleMaxRate:          lsmpkg.DefaultWriteThrottleMaxRate,
		RaftLagWarnSegments:           8,
		EnableWALWatchdog:             true,
		WALBufferSize:                 defaultWALBufferSize,
		WALAutoGCInterval:             15 * time.Second,
		WALAutoGCMinRemovable:         1,
		WALAutoGCMaxBatch:             4,
		WALTypedRecordWarnRatio:       0.35,
		WALTypedRecordWarnSegments:    6,
		CompactionValueWeight:         lsmpkg.DefaultCompactionValueWeight,
		CompactionValueAlertThreshold: lsmpkg.DefaultCompactionValueAlertThreshold,
		ValueLogGCInterval:            10 * time.Minute,
		ValueLogGCDiscardRatio:        0.5,
		ValueLogGCParallelism:         0,
		ValueLogGCReduceScore:         2.0,
		ValueLogGCSkipScore:           4.0,
		ValueLogGCReduceBacklog:       0,
		ValueLogGCSkipBacklog:         0,
		ValueLogGCSampleSizeRatio:     0.10,
		ValueLogGCSampleCountRatio:    0.01,
		ValueLogBucketCount:           16,
		ValueLogHotBucketCount:        4,
		ValueLogHotKeyThreshold:       8,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold

	// Relax L0 throttling defaults and increase compaction parallelism a bit to
	// reduce write-path sleeps under load.
	opt.NumLevelZeroTables = lsmpkg.DefaultNumLevelZeroTables
	opt.L0SlowdownWritesTrigger = lsmpkg.DefaultNumLevelZeroTables
	opt.L0StopWritesTrigger = lsmpkg.DefaultNumLevelZeroTables * 3
	opt.L0ResumeWritesTrigger = 24
	opt.CompactionSlowdownTrigger = lsmpkg.DefaultCompactionSlowdownTrigger
	opt.CompactionStopTrigger = lsmpkg.DefaultCompactionStopTrigger
	opt.CompactionResumeTrigger = lsmpkg.DefaultCompactionResumeTrigger
	opt.IngestCompactBatchSize = lsmpkg.DefaultIngestCompactBatchSize
	opt.IngestBacklogMergeScore = lsmpkg.DefaultIngestBacklogMergeScore
	opt.NumCompactors = 4
	opt.CompactionPolicy = CompactionPolicyLeveled
	opt.IngestShardParallelism = 2
	return opt
}

// normalized returns a shallow copy with runtime defaults resolved once at the
// DB boundary. Zero remains meaningful for settings that explicitly use zero to
// disable a feature; only legacy "fallback" fields are filled here.
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
	if opt.MemTableEngine == "" {
		opt.MemTableEngine = MemTableEngineART
	}
	if opt.CompactionPolicy == "" {
		opt.CompactionPolicy = CompactionPolicyLeveled
	}
	if opt.HotRingTopK <= 0 {
		opt.HotRingTopK = defaultHotRingTopK
	}
	if opt.HotReadPrefetchThreshold <= 0 {
		opt.HotReadPrefetchThreshold = defaultHotReadPrefetchThreshold
	}
	if opt.HotReadPrefetchCooldown <= 0 {
		opt.HotReadPrefetchCooldown = defaultHotReadPrefetchCooldown
	}
	if opt.WriteBatchMaxCount <= 0 {
		opt.WriteBatchMaxCount = defaultWriteBatchMaxCount
	}
	if opt.WriteBatchMaxSize <= 0 {
		opt.WriteBatchMaxSize = defaultWriteBatchMaxSize
	}
	if opt.MaxBatchCount <= 0 {
		opt.MaxBatchCount = int64(opt.WriteBatchMaxCount)
	}
	if opt.MaxBatchSize <= 0 {
		opt.MaxBatchSize = opt.WriteBatchMaxSize
	}
	if opt.WriteBatchWait < 0 {
		opt.WriteBatchWait = 0
	}
	if opt.WriteThrottleMinRate <= 0 {
		opt.WriteThrottleMinRate = lsmpkg.DefaultWriteThrottleMinRate
	}
	if opt.WriteThrottleMaxRate <= 0 {
		opt.WriteThrottleMaxRate = lsmpkg.DefaultWriteThrottleMaxRate
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
	if opt.BloomCacheBytes < 0 {
		opt.BloomCacheBytes = 0
	}
	if opt.NumCompactors <= 0 {
		opt.NumCompactors = lsmpkg.DefaultNumCompactors()
	}
	if opt.NumLevelZeroTables <= 0 {
		opt.NumLevelZeroTables = lsmpkg.DefaultNumLevelZeroTables
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
		opt.CompactionSlowdownTrigger = lsmpkg.DefaultCompactionSlowdownTrigger
	}
	if opt.CompactionStopTrigger <= 0 {
		opt.CompactionStopTrigger = lsmpkg.DefaultCompactionStopTrigger
	}
	if opt.CompactionStopTrigger < opt.CompactionSlowdownTrigger {
		opt.CompactionStopTrigger = opt.CompactionSlowdownTrigger
	}
	if opt.CompactionResumeTrigger <= 0 {
		opt.CompactionResumeTrigger = lsmpkg.DefaultCompactionResumeTrigger
	}
	if opt.CompactionResumeTrigger > opt.CompactionSlowdownTrigger {
		opt.CompactionResumeTrigger = opt.CompactionSlowdownTrigger
	}
	if opt.IngestCompactBatchSize <= 0 {
		opt.IngestCompactBatchSize = lsmpkg.DefaultIngestCompactBatchSize
	}
	if opt.IngestBacklogMergeScore <= 0 {
		opt.IngestBacklogMergeScore = lsmpkg.DefaultIngestBacklogMergeScore
	}
	if opt.IngestShardParallelism <= 0 {
		opt.IngestShardParallelism = max(opt.NumCompactors/2, 2)
	}
	if opt.CompactionValueWeight < 0 {
		opt.CompactionValueWeight = 0
	}
	if opt.CompactionValueWeight == 0 {
		opt.CompactionValueWeight = lsmpkg.DefaultCompactionValueWeight
	}
	if opt.CompactionValueAlertThreshold <= 0 {
		opt.CompactionValueAlertThreshold = lsmpkg.DefaultCompactionValueAlertThreshold
	}
	if opt.WALBufferSize <= 0 {
		opt.WALBufferSize = defaultWALBufferSize
	}
}
