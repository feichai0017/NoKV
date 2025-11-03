package NoKV

import (
	"time"

	"github.com/feichai0017/NoKV/utils"
)

// Options NoKV 总的配置文件
type Options struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableMaxSz        int64
	MaxBatchCount       int64
	MaxBatchSize        int64 // max batch size in bytes
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64

	WriteBatchMaxCount int
	WriteBatchMaxSize  int64
	WriteBatchDelay    time.Duration

	DetectConflicts bool
	HotRingEnabled  bool
	HotRingBits     uint8
	HotRingTopK     int

	SyncWrites bool

	// Block cache configuration for read path optimization. The cache keeps a
	// two-tier layout (hot LRU + cold CLOCK) and currently targets L0/L1.
	BlockCacheSize        int
	BlockCacheHotFraction float64
	BloomCacheSize        int

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

	// CompactionValueWeight adjusts how aggressively the scheduler prioritises
	// levels whose entries reference large value log payloads. Higher values
	// make the compaction picker favour levels with high ValuePtr density.
	CompactionValueWeight float64

	// CompactionValueAlertThreshold triggers stats alerts when a level's
	// value-density (value bytes / total bytes) exceeds this ratio.
	CompactionValueAlertThreshold float64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:                       "./work_test",
		MemTableSize:                  64 << 20,
		SSTableMaxSz:                  256 << 20,
		HotRingEnabled:                true,
		HotRingBits:                   12,
		HotRingTopK:                   16,
		WriteBatchMaxCount:            64,
		WriteBatchMaxSize:             1 << 20,
		WriteBatchDelay:               2 * time.Millisecond,
		BlockCacheSize:                4096,
		BlockCacheHotFraction:         0.25,
		BloomCacheSize:                1024,
		SyncWrites:                    false,
		RaftLagWarnSegments:           8,
		EnableWALWatchdog:             true,
		WALAutoGCInterval:             15 * time.Second,
		WALAutoGCMinRemovable:         1,
		WALAutoGCMaxBatch:             4,
		WALTypedRecordWarnRatio:       0.35,
		WALTypedRecordWarnSegments:    6,
		CompactionValueWeight:         0.35,
		CompactionValueAlertThreshold: 0.6,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
