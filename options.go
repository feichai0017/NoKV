package NoKV

import (
	"time"

	"github.com/feichai0017/NoKV/utils"
)

// Options holds the top-level database configuration.
type Options struct {
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

	SyncWrites   bool
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

	// Block cache configuration for read path optimization. Cached blocks
	// target L0/L1; colder data relies on the OS page cache.
	BlockCacheSize int
	BloomCacheSize int

	// RaftLagWarnSegments determines how many WAL segments a follower can lag
	// behind the active segment before stats surfaces a warning. Zero disables
	// the alert.
	RaftLagWarnSegments int64

	// EnableWALWatchdog enables the background WAL backlog watchdog which
	// surfaces typed-record warnings and optionally runs automated segment GC.
	EnableWALWatchdog bool
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

	// DiscardStatsFlushThreshold controls how many discard-stat updates must be
	// accumulated before they are flushed back into the LSM. Zero keeps the
	// default threshold.
	DiscardStatsFlushThreshold int

	// NumCompactors controls how many background compaction workers are spawned.
	// Zero uses an auto value derived from the host CPU count.
	NumCompactors int
	// NumLevelZeroTables controls when write throttling kicks in and feeds into
	// the compaction priority calculation. Zero falls back to the legacy default.
	NumLevelZeroTables int
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

// MemTableEngine selects the in-memory index implementation used by memtables.
type MemTableEngine string

const (
	MemTableEngineSkiplist MemTableEngine = "skiplist"
	MemTableEngineART      MemTableEngine = "art"
)

// NewDefaultOptions returns the default option set.
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:                   "./work_test",
		MemTableSize:              64 << 20,
		MemTableEngine:            MemTableEngineSkiplist,
		SSTableMaxSz:              256 << 20,
		HotRingEnabled:            true,
		HotRingBits:               12,
		HotRingTopK:               16,
		HotRingRotationInterval:   0,
		HotRingNodeCap:            0,
		HotRingNodeSampleBits:     0,
		HotRingDecayInterval:      time.Second,
		HotRingDecayShift:         1,
		HotRingWindowSlots:        8,
		HotRingWindowSlotDuration: 250 * time.Millisecond,
		// Conservative defaults to avoid long batch-induced pauses.
		WriteBatchMaxCount:            64,
		WriteBatchMaxSize:             1 << 20,
		BlockCacheSize:                4096,
		BloomCacheSize:                1024,
		SyncWrites:                    false,
		ManifestSync:                  false,
		ManifestRewriteThreshold:      64 << 20,
		WriteHotKeyLimit:              128,
		HotWriteBurstThreshold:        8,
		HotWriteBatchMultiplier:       2,
		WriteBatchWait:                200 * time.Microsecond,
		RaftLagWarnSegments:           8,
		EnableWALWatchdog:             true,
		WALAutoGCInterval:             15 * time.Second,
		WALAutoGCMinRemovable:         1,
		WALAutoGCMaxBatch:             4,
		WALTypedRecordWarnRatio:       0.35,
		WALTypedRecordWarnSegments:    6,
		CompactionValueWeight:         0.35,
		CompactionValueAlertThreshold: 0.6,
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
	opt.NumLevelZeroTables = 16
	opt.IngestCompactBatchSize = 4
	opt.IngestBacklogMergeScore = 2.0
	opt.NumCompactors = 4
	opt.IngestShardParallelism = 2
	return opt
}
