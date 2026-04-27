package NoKV

import (
	"encoding/binary"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	lsmpkg "github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultWriteBatchMaxCount       = 64
	defaultWriteBatchMaxSize  int64 = 1 << 20
	defaultThermosTopK              = 16
	// defaultLSMShardCount is the number of WAL Manager + memtable pairs
	// that back the LSM data plane. Each shard runs on its own fd, fsync
	// worker, and bufio.Writer so writes do not contend on a single
	// Manager.mu. The commit pipeline launches one processor per shard,
	// so write concurrency is determined entirely by LSMShardCount —
	// there is no separate CommitWorkers knob. Must be a power of two.
	defaultLSMShardCount = 4
)

// Options holds the top-level database configuration.
type Options struct {
	// FS provides the filesystem implementation used by DB runtime components.
	// Nil defaults to vfs.OSFS.
	FS vfs.FS

	// AllowedModes limits which migration workdir modes Open accepts. An empty
	// allow-list means standalone-only. Cluster runtime and offline diagnostics
	// must opt into seeded/cluster directories explicitly.
	AllowedModes []raftmode.Mode

	// EnableValueLog opts into the engine/vlog Authoritative consumer. When
	// false (the default) NoKV behaves as a pure metadata-first KV: every
	// value is inlined into the LSM regardless of size, no vlog directory
	// is created, no vlog manager is opened, no value-log GC goroutine
	// runs, and the commit pipeline never enters vlog code paths.
	//
	// Set to true for workloads that benefit from value separation: large
	// values (typically > a few KB), embedded blob storage, or any
	// workload where the inlined-value write amplification dominates.
	// When enabled, ValueThreshold / ValueLogFileSize / ValueLogBucketCount
	// / ValueLogGC* control the vlog behavior as before.
	//
	// This is a breaking change vs releases prior to the slab-substrate
	// redesign — older versions defaulted vlog ON. Existing deployments
	// that depend on value separation MUST set EnableValueLog=true on
	// upgrade. Reopening a DB with EnableValueLog=false against a
	// directory that contains previously-written vlog data is allowed
	// (the data is left in place, untouched) but any LSM SST entries
	// holding a ValuePtr will fail to read until the user re-enables
	// vlog or runs a future migration tool.
	EnableValueLog bool

	ValueThreshold int64
	WorkDir        string
	MemTableSize   int64
	MemTableEngine MemTableEngine
	SSTableMaxSz   int64
	// MaxBatchCount bounds the number of entries grouped into one internal
	// write batch. NewDefaultOptions exposes a concrete default; zero is only
	// interpreted Open resolves the constructor default when left zero.
	MaxBatchCount int64
	// MaxBatchSize bounds the size in bytes of one internal write batch.
	// NewDefaultOptions exposes a concrete default; zero is only interpreted as
	// a Open resolves the constructor default when left zero.
	MaxBatchSize       int64
	ValueLogFileSize   int
	ValueLogMaxEntries uint32
	// ValueLogBucketCount controls how many hash buckets the value log uses.
	// Values <= 1 disable bucketization.
	ValueLogBucketCount     int
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

	// WriteBatchMaxCount bounds how many requests the commit worker coalesces in
	// one pass. NewDefaultOptions exposes a concrete default; zero is only
	// interpreted Open resolves the constructor default when left zero.
	WriteBatchMaxCount int
	// WriteBatchMaxSize bounds the byte size the commit worker coalesces in one
	// pass. NewDefaultOptions exposes a concrete default; zero is only
	// interpreted Open resolves the constructor default when left zero.
	WriteBatchMaxSize int64

	DetectConflicts bool
	ThermosEnabled  bool
	ThermosBits     uint8
	ThermosTopK     int
	// ThermosRotationInterval enables dual-ring rotation for hotness tracking.
	// Zero disables rotation.
	ThermosRotationInterval time.Duration
	// ThermosNodeCap caps the number of tracked keys per ring. Zero disables the cap.
	ThermosNodeCap uint64
	// ThermosNodeSampleBits controls stable sampling once the cap is reached.
	// A value of 0 enforces a strict cap; larger values sample 1/2^N keys.
	ThermosNodeSampleBits uint8
	// ThermosDecayInterval controls how often Thermos halves its global counters.
	// Zero disables periodic decay.
	ThermosDecayInterval time.Duration
	// ThermosDecayShift determines how aggressively counters decay (count >>= shift).
	ThermosDecayShift uint32
	// ThermosWindowSlots controls the number of sliding-window buckets tracked per key.
	// Zero disables the sliding window.
	ThermosWindowSlots int
	// ThermosWindowSlotDuration sets the duration of each sliding-window bucket.
	ThermosWindowSlotDuration time.Duration
	SyncWrites                bool
	// SyncPipeline enables a dedicated sync worker goroutine that decouples
	// WAL fsync from the commit pipeline. When false (the default), the commit
	// worker performs fsync inline. Only effective when SyncWrites is true.
	SyncPipeline bool
	// LSMShardCount is the number of WAL Manager + memtable pairs that back
	// the LSM data plane and equivalently the number of commit-pipeline
	// processors (one per shard, pinned end-to-end so SetBatch atomicity
	// is preserved). The shard router uses `& (N-1)` for placement so the
	// value must be a power of two. Zero falls back to the constructor
	// default; non-power-of-two values are rounded DOWN to the nearest
	// power of two during Open (e.g. 6 → 4, 12 → 8). See
	// docs/notes/2026-04-27-sharded-wal-memtable.md.
	LSMShardCount int
	ManifestSync  bool
	// ManifestRewriteThreshold triggers a manifest rewrite when the active
	// MANIFEST file grows beyond this size (bytes). Values <= 0 disable rewrites.
	ManifestRewriteThreshold int64
	// WriteHotKeyLimit caps how many consecutive writes a single key can issue
	// before the DB returns utils.ErrHotKeyWriteThrottle. Zero disables write-path
	// throttling.
	WriteHotKeyLimit int32
	// WriteBatchWait adds an optional coalescing delay when the commit queue is
	// momentarily empty, letting small bursts share one WAL fsync/apply pass.
	// Zero disables the delay.
	WriteBatchWait time.Duration
	// WriteThrottleMinRate is the target write admission rate in bytes/sec when
	// slowdown pressure approaches the stop threshold. NewDefaultOptions
	// exposes a concrete default; zero lets Open resolve the constructor
	// default.
	WriteThrottleMinRate int64
	// WriteThrottleMaxRate is the target write admission rate in bytes/sec when
	// slowdown first becomes active. NewDefaultOptions exposes a concrete
	// default; zero lets Open resolve the constructor default.
	WriteThrottleMaxRate int64

	// BlockCacheBytes bounds the in-memory budget for cached L0/L1 data blocks.
	// Deeper levels continue to rely on the OS page cache.
	BlockCacheBytes int64
	// IndexCacheBytes bounds the in-memory budget for decoded SSTable indexes.
	IndexCacheBytes int64
	// PrefixExtractor enables SST prefix bloom filters. Nil disables prefix
	// bloom creation. The default extractor recognizes NoKV native metadata
	// keys and returns nil for unrelated user keys.
	PrefixExtractor lsmpkg.PrefixExtractor
	// BlockCompression controls SST data-block compression.
	BlockCompression lsmpkg.BlockCompression

	// RaftLagWarnSegments determines how many WAL segments a follower can lag
	// behind the active segment before stats surfaces a warning. Zero disables
	// the alert.
	RaftLagWarnSegments int64

	// EnableWALWatchdog enables the background WAL backlog watchdog which
	// surfaces typed-record warnings and optionally runs automated segment GC.
	EnableWALWatchdog bool
	// WALBufferSize controls the size of the in-memory write buffer used by
	// the WAL manager. Larger buffers reduce syscall frequency at the cost of
	// memory. NewDefaultOptions exposes a concrete default; zero is only
	// interpreted Open resolves the constructor default when left zero.
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
	RaftPointerSnapshot func() map[uint64]localmeta.RaftLogPointer

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
	// the compaction priority calculation. NewDefaultOptions populates a concrete
	// default; Open resolves the constructor default when left zero.
	NumLevelZeroTables int
	// L0SlowdownWritesTrigger starts write pacing when L0 table count reaches
	// this threshold. Defaults are populated up front; zero is only interpreted
	// Open resolves the constructor default when left zero.
	L0SlowdownWritesTrigger int
	// L0StopWritesTrigger blocks writes when L0 table count reaches this
	// threshold. Defaults are populated up front; zero is only interpreted as a
	// Open resolves the constructor default when left zero.
	L0StopWritesTrigger int
	// L0ResumeWritesTrigger clears throttling only when L0 table count drops to
	// this threshold or lower. Defaults are populated up front; zero is only
	// interpreted Open resolves the constructor default when left zero.
	L0ResumeWritesTrigger int
	// CompactionSlowdownTrigger starts write pacing when max compaction score
	// reaches this value. Defaults are populated up front; zero is only
	// interpreted Open resolves the constructor default when left zero.
	CompactionSlowdownTrigger float64
	// CompactionStopTrigger blocks writes when max compaction score reaches this
	// value. Defaults are populated up front; zero is only interpreted as a
	// Open resolves the constructor default when left zero.
	CompactionStopTrigger float64
	// CompactionResumeTrigger clears throttling only when max compaction score
	// drops to this value or lower. Defaults are populated up front; zero is only
	// interpreted Open resolves the constructor default when left zero.
	CompactionResumeTrigger float64
	// StagingCompactBatchSize decides how many L0 tables to promote into the
	// staging buffer per compaction cycle. NewDefaultOptions populates a concrete
	// default; Open resolves the constructor default when left zero.
	StagingCompactBatchSize int
	// StagingBacklogMergeScore triggers a staging-merge task when the staging
	// backlog score exceeds this threshold. Defaults are populated up front; zero
	// is only interpreted Open resolves the constructor default when left zero.
	StagingBacklogMergeScore float64

	// CompactionValueWeight adjusts how aggressively the scheduler prioritises
	// levels whose entries reference large value log payloads. Higher values
	// make the compaction picker favour levels with high ValuePtr density.
	CompactionValueWeight float64
	// CompactionTombstoneWeight adjusts how aggressively the scheduler
	// prioritizes levels with high range tombstone density.
	CompactionTombstoneWeight float64
	// TTLCompactionMinAge forces stale-data cleanup for max-level SSTables
	// older than this age. Zero disables age-triggered cleanup.
	TTLCompactionMinAge time.Duration

	// CompactionWriteBytesPerSec paces compaction output writes. Zero disables
	// pacing. Flush writes are never paced.
	CompactionWriteBytesPerSec int64
	// CompactionPacingBypassL0 bypasses output pacing when L0 table count
	// reaches this threshold. Zero lets Open derive a conservative threshold
	// when compaction pacing is enabled.
	CompactionPacingBypassL0 int

	// CompactionValueAlertThreshold triggers stats alerts when a level's
	// value-density (value bytes / total bytes) exceeds this ratio.
	CompactionValueAlertThreshold float64

	// StagingShardParallelism caps how many staging shards can be compacted in a
	// single staging-only pass. A value <= 0 falls back to 1 (sequential).
	StagingShardParallelism int

	// NegativeCachePersistent enables snapshot-on-Close + restore-on-Open for
	// the in-memory negative cache, backed by an engine/slab segment under
	// WorkDir/negative-slab/. Default false. When enabled, a process restart
	// skips the cold-start re-warm phase for previously-known not-found keys
	// (fsmeta Lookup misses, S3 GetObject 404, HDFS path probes). The slab
	// is best-effort (Derived consistency class — see
	// docs/notes/2026-04-27-slab-substrate.md §6.1): a corrupt or missing
	// snapshot forces a re-warm but does not affect read correctness.
	NegativeCachePersistent bool
	// NegativeCacheSlabMaxSize bounds the on-disk snapshot size in bytes.
	// Snapshots stop appending once the limit is hit; remaining keys re-warm
	// normally. Zero falls back to a 64 MiB default. Ignored unless
	// NegativeCachePersistent is true.
	NegativeCacheSlabMaxSize int64
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
		WorkDir:                   "./work_test",
		MemTableSize:              64 << 20,
		MemTableEngine:            MemTableEngineART,
		SSTableMaxSz:              256 << 20,
		ThermosEnabled:            false,
		ThermosBits:               12,
		ThermosTopK:               defaultThermosTopK,
		ThermosRotationInterval:   30 * time.Minute,
		ThermosNodeCap:            250_000,
		ThermosNodeSampleBits:     0,
		ThermosDecayInterval:      0,
		ThermosDecayShift:         0,
		ThermosWindowSlots:        8,
		ThermosWindowSlotDuration: 250 * time.Millisecond,
		// Conservative defaults to avoid long batch-induced pauses.
		WriteBatchMaxCount:            defaultWriteBatchMaxCount,
		WriteBatchMaxSize:             defaultWriteBatchMaxSize,
		LSMShardCount:                 defaultLSMShardCount,
		MaxBatchCount:                 defaultWriteBatchMaxCount,
		MaxBatchSize:                  defaultWriteBatchMaxSize,
		BlockCacheBytes:               lsmpkg.DefaultBlockCacheBytes,
		IndexCacheBytes:               lsmpkg.DefaultIndexCacheBytes,
		PrefixExtractor:               nativeMetadataPrefix,
		BlockCompression:              lsmpkg.DefaultBlockCompression,
		SyncWrites:                    false,
		ManifestSync:                  false,
		ManifestRewriteThreshold:      64 << 20,
		WriteHotKeyLimit:              128,
		WriteBatchWait:                200 * time.Microsecond,
		WriteThrottleMinRate:          lsmpkg.DefaultWriteThrottleMinRate,
		WriteThrottleMaxRate:          lsmpkg.DefaultWriteThrottleMaxRate,
		RaftLagWarnSegments:           8,
		EnableWALWatchdog:             true,
		WALBufferSize:                 wal.DefaultBufferSize,
		WALAutoGCInterval:             15 * time.Second,
		WALAutoGCMinRemovable:         1,
		WALAutoGCMaxBatch:             4,
		WALTypedRecordWarnRatio:       0.35,
		WALTypedRecordWarnSegments:    6,
		CompactionValueWeight:         lsmpkg.DefaultCompactionValueWeight,
		CompactionTombstoneWeight:     lsmpkg.DefaultCompactionTombstoneWeight,
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
	opt.StagingCompactBatchSize = lsmpkg.DefaultStagingCompactBatchSize
	opt.StagingBacklogMergeScore = lsmpkg.DefaultStagingBacklogMergeScore
	opt.NumCompactors = 4
	opt.CompactionPolicy = CompactionPolicyLeveled
	opt.StagingShardParallelism = 2
	return opt
}

// resolveOpenDefaults resolves constructor-owned defaults once at the DB
// boundary. Zero remains meaningful for settings that explicitly use zero to
// disable a feature.
func (opt *Options) resolveOpenDefaults() {
	if opt == nil {
		return
	}
	if opt.MemTableEngine == "" {
		opt.MemTableEngine = MemTableEngineART
	}
	if opt.CompactionPolicy == "" {
		opt.CompactionPolicy = CompactionPolicyLeveled
	}
	if opt.ThermosTopK <= 0 {
		opt.ThermosTopK = defaultThermosTopK
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
	opt.normalizeLSMSharedOptions()
	if opt.WALBufferSize <= 0 {
		opt.WALBufferSize = wal.DefaultBufferSize
	}
	if opt.LSMShardCount <= 0 {
		opt.LSMShardCount = defaultLSMShardCount
	}
	// Power-of-two so the eventual hash routing can use & (N-1).
	if opt.LSMShardCount&(opt.LSMShardCount-1) != 0 {
		// Round down to nearest power of two; never zero.
		n := 1
		for n*2 <= opt.LSMShardCount {
			n *= 2
		}
		opt.LSMShardCount = n
	}
}

func (opt *Options) normalizeLSMSharedOptions() {
	cfg := &lsmpkg.Options{}
	opt.applyLSMSharedOptions(cfg)
	cfg.NormalizeInPlace()
	opt.copyNormalizedLSMOptions(cfg)
}

func (opt *Options) applyLSMSharedOptions(dst *lsmpkg.Options) {
	if opt == nil || dst == nil {
		return
	}
	dst.NumCompactors = opt.NumCompactors
	dst.NumLevelZeroTables = opt.NumLevelZeroTables
	dst.L0SlowdownWritesTrigger = opt.L0SlowdownWritesTrigger
	dst.L0StopWritesTrigger = opt.L0StopWritesTrigger
	dst.L0ResumeWritesTrigger = opt.L0ResumeWritesTrigger
	dst.CompactionSlowdownTrigger = opt.CompactionSlowdownTrigger
	dst.CompactionStopTrigger = opt.CompactionStopTrigger
	dst.CompactionResumeTrigger = opt.CompactionResumeTrigger
	dst.WriteThrottleMinRate = opt.WriteThrottleMinRate
	dst.WriteThrottleMaxRate = opt.WriteThrottleMaxRate
	dst.StagingCompactBatchSize = opt.StagingCompactBatchSize
	dst.StagingBacklogMergeScore = opt.StagingBacklogMergeScore
	dst.StagingShardParallelism = opt.StagingShardParallelism
	dst.CompactionValueWeight = opt.CompactionValueWeight
	dst.CompactionTombstoneWeight = opt.CompactionTombstoneWeight
	dst.TTLCompactionMinAge = opt.TTLCompactionMinAge
	dst.CompactionWriteBytesPerSec = opt.CompactionWriteBytesPerSec
	dst.CompactionPacingBypassL0 = opt.CompactionPacingBypassL0
	dst.CompactionValueAlertThreshold = opt.CompactionValueAlertThreshold
	dst.BlockCacheBytes = opt.BlockCacheBytes
	dst.IndexCacheBytes = opt.IndexCacheBytes
	dst.PrefixExtractor = opt.PrefixExtractor
	dst.BlockCompression = opt.BlockCompression
	dst.NegativeCachePersistent = opt.NegativeCachePersistent
	dst.NegativeCacheSlabMaxSize = opt.NegativeCacheSlabMaxSize
}

func (opt *Options) copyNormalizedLSMOptions(src *lsmpkg.Options) {
	if opt == nil || src == nil {
		return
	}
	opt.NumCompactors = src.NumCompactors
	opt.NumLevelZeroTables = src.NumLevelZeroTables
	opt.L0SlowdownWritesTrigger = src.L0SlowdownWritesTrigger
	opt.L0StopWritesTrigger = src.L0StopWritesTrigger
	opt.L0ResumeWritesTrigger = src.L0ResumeWritesTrigger
	opt.CompactionSlowdownTrigger = src.CompactionSlowdownTrigger
	opt.CompactionStopTrigger = src.CompactionStopTrigger
	opt.CompactionResumeTrigger = src.CompactionResumeTrigger
	opt.WriteThrottleMinRate = src.WriteThrottleMinRate
	opt.WriteThrottleMaxRate = src.WriteThrottleMaxRate
	opt.StagingCompactBatchSize = src.StagingCompactBatchSize
	opt.StagingBacklogMergeScore = src.StagingBacklogMergeScore
	opt.StagingShardParallelism = src.StagingShardParallelism
	opt.CompactionValueWeight = src.CompactionValueWeight
	opt.CompactionTombstoneWeight = src.CompactionTombstoneWeight
	opt.TTLCompactionMinAge = src.TTLCompactionMinAge
	opt.CompactionWriteBytesPerSec = src.CompactionWriteBytesPerSec
	opt.CompactionPacingBypassL0 = src.CompactionPacingBypassL0
	opt.CompactionValueAlertThreshold = src.CompactionValueAlertThreshold
	opt.BlockCacheBytes = src.BlockCacheBytes
	opt.IndexCacheBytes = src.IndexCacheBytes
	opt.PrefixExtractor = src.PrefixExtractor
	opt.BlockCompression = src.BlockCompression
	opt.NegativeCachePersistent = src.NegativeCachePersistent
	opt.NegativeCacheSlabMaxSize = src.NegativeCacheSlabMaxSize
}

func nativeMetadataPrefix(key []byte) []byte {
	const (
		magicLen = 4
		version  = byte(1)
	)
	if len(key) < magicLen+3 {
		return nil
	}
	if key[0] != 'f' || key[1] != 's' || key[2] != 'm' || key[3] != 0 || key[4] != version {
		return nil
	}
	pos := magicLen + 1
	mountLen, n := binary.Uvarint(key[pos:])
	if n <= 0 {
		return nil
	}
	pos += n
	if mountLen == 0 || uint64(len(key)-pos) < mountLen+1 {
		return nil
	}
	pos += int(mountLen)
	kindPos := pos
	pos++
	switch key[kindPos] {
	case 'd', 'c':
		if len(key) < pos+8 {
			return nil
		}
		return key[:pos+8]
	case 'i', 'm', 's', 'u':
		return key[:pos]
	default:
		return nil
	}
}
