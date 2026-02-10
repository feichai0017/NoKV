package NoKV

import (
	"expvar"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/hotring"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	transportpkg "github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
)

type Stats struct {
	db       *DB
	closer   *utils.Closer
	once     sync.Once
	interval time.Duration

	EntryNum int64 // Mirrors Entries for backwards compatibility.

	entries                *expvar.Int
	flushPending           *expvar.Int
	flushQueueLen          *expvar.Int
	flushActive            *expvar.Int
	flushWaitMs            *expvar.Float
	flushWaitLastMs        *expvar.Float
	flushWaitMaxMs         *expvar.Float
	flushBuildMs           *expvar.Float
	flushBuildLastMs       *expvar.Float
	flushBuildMaxMs        *expvar.Float
	flushReleaseMs         *expvar.Float
	flushReleaseLastMs     *expvar.Float
	flushReleaseMaxMs      *expvar.Float
	flushCompleted         *expvar.Int
	compactionBacklog      *expvar.Int
	compactionMaxScore     *expvar.Float
	compactionLastMs       *expvar.Float
	compactionMaxMs        *expvar.Float
	compactionRuns         *expvar.Int
	compactionIngestRuns   *expvar.Int
	compactionMergeRuns    *expvar.Int
	compactionIngestMs     *expvar.Float
	compactionMergeMs      *expvar.Float
	compactionIngestTables *expvar.Int
	compactionMergeTables  *expvar.Int
	valueLogSegments       *expvar.Int
	valueLogPendingDel     *expvar.Int
	valueLogDiscardQueue   *expvar.Int
	walActiveSegment       *expvar.Int
	walSegmentCount        *expvar.Int
	walActiveSize          *expvar.Int
	walSegmentsRemoved     *expvar.Int
	raftGroupCount         *expvar.Int
	raftLaggingGroups      *expvar.Int
	raftMaxLagSegments     *expvar.Int
	raftMinSegment         *expvar.Int
	raftMaxSegment         *expvar.Int
	raftLagWarning         *expvar.Int
	writeQueueDepth        *expvar.Int
	writeQueueEntries      *expvar.Int
	writeQueueBytes        *expvar.Int
	writeBatchAvgEntries   *expvar.Float
	writeBatchAvgBytes     *expvar.Float
	writeRequestWaitMs     *expvar.Float
	writeValueLogMs        *expvar.Float
	writeApplyMs           *expvar.Float
	writeBatchesTotal      *expvar.Int
	writeThrottle          *expvar.Int
	writeHotKeyLimited     *expvar.Int
	txnActive              *expvar.Int
	txnStarted             *expvar.Int
	txnCommitted           *expvar.Int
	txnConflicts           *expvar.Int
	blockL0HitRate         *expvar.Float
	blockL1HitRate         *expvar.Float
	bloomHitRate           *expvar.Float
	indexHitRate           *expvar.Float
	iteratorReuses         *expvar.Int
	cfMap                  *expvar.Map
	walRecordCounts        *expvar.Map
	walSegmentsWithRaft    *expvar.Int
	walSegmentsRemovable   *expvar.Int
	walTypedRatio          *expvar.Float
	walTypedWarning        *expvar.Int
	walTypedReason         *expvar.String
	walAutoRuns            *expvar.Int
	walAutoRemoved         *expvar.Int
	walAutoLastUnix        *expvar.Int
	lsmValueBytes          *expvar.Int
	compactionValueWeight  *expvar.Float
	lsmValueDensityMax     *expvar.Float
	lsmValueDensityAlert   *expvar.Int
	regionMetrics          atomic.Pointer[storepkg.RegionMetrics]
	regionTotal            *expvar.Int
	regionNew              *expvar.Int
	regionRunning          *expvar.Int
	regionRemoving         *expvar.Int
	regionTombstone        *expvar.Int
	regionOther            *expvar.Int
}

type HotKeyStat struct {
	Key   string `json:"key"`
	Count int32  `json:"count"`
}

type ColumnFamilySnapshot struct {
	Writes uint64 `json:"writes"`
	Reads  uint64 `json:"reads"`
}

// LSMLevelStats captures aggregated metrics per LSM level.
type LSMLevelStats struct {
	Level              int     `json:"level"`
	TableCount         int     `json:"tables"`
	SizeBytes          int64   `json:"size_bytes"`
	ValueBytes         int64   `json:"value_bytes"`
	StaleBytes         int64   `json:"stale_bytes"`
	IngestTables       int     `json:"ingest_tables"`
	IngestSizeBytes    int64   `json:"ingest_size_bytes"`
	IngestValueBytes   int64   `json:"ingest_value_bytes"`
	ValueDensity       float64 `json:"value_density"`
	IngestValueDensity float64 `json:"ingest_value_density"`
	IngestRuns         int64   `json:"ingest_runs"`
	IngestMs           float64 `json:"ingest_ms"`
	IngestTablesCount  int64   `json:"ingest_tables_compacted"`
	MergeRuns          int64   `json:"ingest_merge_runs"`
	MergeMs            float64 `json:"ingest_merge_ms"`
	MergeTables        int64   `json:"ingest_merge_tables"`
}

func levelMetricsToStats(lvl lsm.LevelMetrics) LSMLevelStats {
	return LSMLevelStats{
		Level:              lvl.Level,
		TableCount:         lvl.TableCount,
		SizeBytes:          lvl.SizeBytes,
		ValueBytes:         lvl.ValueBytes,
		StaleBytes:         lvl.StaleBytes,
		IngestTables:       lvl.IngestTableCount,
		IngestSizeBytes:    lvl.IngestSizeBytes,
		IngestValueBytes:   lvl.IngestValueBytes,
		ValueDensity:       lvl.ValueDensity,
		IngestValueDensity: lvl.IngestValueDensity,
		IngestRuns:         lvl.IngestRuns,
		IngestMs:           lvl.IngestMs,
		IngestTablesCount:  lvl.IngestTablesCompacted,
		MergeRuns:          lvl.IngestMergeRuns,
		MergeMs:            lvl.IngestMergeMs,
		MergeTables:        lvl.IngestMergeTables,
	}
}

// StatsSnapshot captures a point-in-time view of internal backlog metrics.
type StatsSnapshot struct {
	Entries    int64                             `json:"entries"`
	Flush      FlushStatsSnapshot                `json:"flush"`
	Compaction CompactionStatsSnapshot           `json:"compaction"`
	ValueLog   ValueLogStatsSnapshot             `json:"value_log"`
	WAL        WALStatsSnapshot                  `json:"wal"`
	Raft       RaftStatsSnapshot                 `json:"raft"`
	Write      WriteStatsSnapshot                `json:"write"`
	Txn        TxnStatsSnapshot                  `json:"txn"`
	Region     RegionStatsSnapshot               `json:"region"`
	Hot        HotStatsSnapshot                  `json:"hot"`
	Cache      CacheStatsSnapshot                `json:"cache"`
	LSM        LSMStatsSnapshot                  `json:"lsm"`
	Transport  transportpkg.GRPCTransportMetrics `json:"transport"`
	Redis      metrics.RedisSnapshot             `json:"redis"`
}

type FlushStatsSnapshot struct {
	Pending       int64   `json:"pending"`
	QueueLength   int64   `json:"queue_length"`
	Active        int64   `json:"active"`
	WaitMs        float64 `json:"wait_ms"`
	LastWaitMs    float64 `json:"last_wait_ms"`
	MaxWaitMs     float64 `json:"max_wait_ms"`
	BuildMs       float64 `json:"build_ms"`
	LastBuildMs   float64 `json:"last_build_ms"`
	MaxBuildMs    float64 `json:"max_build_ms"`
	ReleaseMs     float64 `json:"release_ms"`
	LastReleaseMs float64 `json:"last_release_ms"`
	MaxReleaseMs  float64 `json:"max_release_ms"`
	Completed     int64   `json:"completed"`
}

type CompactionStatsSnapshot struct {
	Backlog              int64   `json:"backlog"`
	MaxScore             float64 `json:"max_score"`
	LastDurationMs       float64 `json:"last_duration_ms"`
	MaxDurationMs        float64 `json:"max_duration_ms"`
	Runs                 uint64  `json:"runs"`
	IngestRuns           int64   `json:"ingest_runs"`
	MergeRuns            int64   `json:"ingest_merge_runs"`
	IngestMs             float64 `json:"ingest_ms"`
	MergeMs              float64 `json:"ingest_merge_ms"`
	IngestTables         int64   `json:"ingest_tables"`
	MergeTables          int64   `json:"ingest_merge_tables"`
	ValueWeight          float64 `json:"value_weight"`
	ValueWeightSuggested float64 `json:"value_weight_suggested,omitempty"`
}

type ValueLogStatsSnapshot struct {
	Segments       int                        `json:"segments"`
	PendingDeletes int                        `json:"pending_deletes"`
	DiscardQueue   int                        `json:"discard_queue"`
	Heads          map[uint32]kv.ValuePtr     `json:"heads,omitempty"`
	GC             metrics.ValueLogGCSnapshot `json:"gc"`
}

type WALStatsSnapshot struct {
	ActiveSegment           int64             `json:"active_segment"`
	SegmentCount            int64             `json:"segment_count"`
	ActiveSize              int64             `json:"active_size"`
	SegmentsRemoved         uint64            `json:"segments_removed"`
	RecordCounts            wal.RecordMetrics `json:"record_counts"`
	SegmentsWithRaftRecords int               `json:"segments_with_raft_records"`
	RemovableRaftSegments   int               `json:"removable_raft_segments"`
	TypedRecordRatio        float64           `json:"typed_record_ratio"`
	TypedRecordWarning      bool              `json:"typed_record_warning"`
	TypedRecordReason       string            `json:"typed_record_reason,omitempty"`
	AutoGCRuns              uint64            `json:"auto_gc_runs"`
	AutoGCRemoved           uint64            `json:"auto_gc_removed"`
	AutoGCLastUnix          int64             `json:"auto_gc_last_unix"`
}

type RaftStatsSnapshot struct {
	GroupCount       int    `json:"group_count"`
	LaggingGroups    int    `json:"lagging_groups"`
	MinLogSegment    uint32 `json:"min_log_segment"`
	MaxLogSegment    uint32 `json:"max_log_segment"`
	MaxLagSegments   int64  `json:"max_lag_segments"`
	LagWarnThreshold int64  `json:"lag_warn_threshold"`
	LagWarning       bool   `json:"lag_warning"`
}

type WriteStatsSnapshot struct {
	QueueDepth       int64   `json:"queue_depth"`
	QueueEntries     int64   `json:"queue_entries"`
	QueueBytes       int64   `json:"queue_bytes"`
	AvgBatchEntries  float64 `json:"avg_batch_entries"`
	AvgBatchBytes    float64 `json:"avg_batch_bytes"`
	AvgRequestWaitMs float64 `json:"avg_request_wait_ms"`
	AvgValueLogMs    float64 `json:"avg_vlog_ms"`
	AvgApplyMs       float64 `json:"avg_apply_ms"`
	BatchesTotal     int64   `json:"batches_total"`
	ThrottleActive   bool    `json:"throttle_active"`
	HotKeyLimited    uint64  `json:"hot_key_limited"`
}

type TxnStatsSnapshot struct {
	Active    int64  `json:"active"`
	Started   uint64 `json:"started"`
	Committed uint64 `json:"committed"`
	Conflicts uint64 `json:"conflicts"`
}

type RegionStatsSnapshot struct {
	Total     int64 `json:"total"`
	New       int64 `json:"new"`
	Running   int64 `json:"running"`
	Removing  int64 `json:"removing"`
	Tombstone int64 `json:"tombstone"`
	Other     int64 `json:"other"`
}

type HotStatsSnapshot struct {
	ReadKeys  []HotKeyStat   `json:"read_keys,omitempty"`
	ReadRing  *hotring.Stats `json:"read_ring,omitempty"`
	WriteKeys []HotKeyStat   `json:"write_keys,omitempty"`
	WriteRing *hotring.Stats `json:"write_ring,omitempty"`
}

type CacheStatsSnapshot struct {
	BlockL0HitRate float64 `json:"block_l0_hit_rate"`
	BlockL1HitRate float64 `json:"block_l1_hit_rate"`
	BloomHitRate   float64 `json:"bloom_hit_rate"`
	IndexHitRate   float64 `json:"index_hit_rate"`
	IteratorReused uint64  `json:"iterator_reused"`
}

type LSMStatsSnapshot struct {
	Levels            []LSMLevelStats                 `json:"levels,omitempty"`
	ValueBytesTotal   int64                           `json:"value_bytes_total"`
	ValueDensityMax   float64                         `json:"value_density_max"`
	ValueDensityAlert bool                            `json:"value_density_alert"`
	ColumnFamilies    map[string]ColumnFamilySnapshot `json:"column_families,omitempty"`
}

func newStats(db *DB) *Stats {
	s := &Stats{
		db:                     db,
		closer:                 utils.NewCloser(),
		interval:               5 * time.Second,
		EntryNum:               0,
		entries:                reuseInt("NoKV.Stats.Entries"),
		flushPending:           reuseInt("NoKV.Stats.Flush.Pending"),
		flushQueueLen:          reuseInt("NoKV.Stats.Flush.QueueLength"),
		flushActive:            reuseInt("NoKV.Stats.Flush.Active"),
		flushWaitMs:            reuseFloat("NoKV.Stats.Flush.WaitMs"),
		flushWaitLastMs:        reuseFloat("NoKV.Stats.Flush.WaitLastMs"),
		flushWaitMaxMs:         reuseFloat("NoKV.Stats.Flush.WaitMaxMs"),
		flushBuildMs:           reuseFloat("NoKV.Stats.Flush.BuildMs"),
		flushBuildLastMs:       reuseFloat("NoKV.Stats.Flush.BuildLastMs"),
		flushBuildMaxMs:        reuseFloat("NoKV.Stats.Flush.BuildMaxMs"),
		flushReleaseMs:         reuseFloat("NoKV.Stats.Flush.ReleaseMs"),
		flushReleaseLastMs:     reuseFloat("NoKV.Stats.Flush.ReleaseLastMs"),
		flushReleaseMaxMs:      reuseFloat("NoKV.Stats.Flush.ReleaseMaxMs"),
		flushCompleted:         reuseInt("NoKV.Stats.Flush.Completed"),
		compactionBacklog:      reuseInt("NoKV.Stats.Compaction.Backlog"),
		compactionMaxScore:     reuseFloat("NoKV.Stats.Compaction.MaxScore"),
		compactionLastMs:       reuseFloat("NoKV.Stats.Compaction.LastDurationMs"),
		compactionMaxMs:        reuseFloat("NoKV.Stats.Compaction.MaxDurationMs"),
		compactionRuns:         reuseInt("NoKV.Stats.Compaction.RunsTotal"),
		compactionIngestRuns:   reuseInt("NoKV.Stats.Compaction.IngestRuns"),
		compactionMergeRuns:    reuseInt("NoKV.Stats.Compaction.IngestMergeRuns"),
		compactionIngestMs:     reuseFloat("NoKV.Stats.Compaction.IngestDurationMs"),
		compactionMergeMs:      reuseFloat("NoKV.Stats.Compaction.IngestMergeDurationMs"),
		compactionIngestTables: reuseInt("NoKV.Stats.Compaction.IngestTables"),
		compactionMergeTables:  reuseInt("NoKV.Stats.Compaction.IngestMergeTables"),
		valueLogSegments:       reuseInt("NoKV.Stats.ValueLog.Segments"),
		valueLogPendingDel:     reuseInt("NoKV.Stats.ValueLog.PendingDeletes"),
		valueLogDiscardQueue:   reuseInt("NoKV.Stats.ValueLog.DiscardQueue"),
		walActiveSegment:       reuseInt("NoKV.Stats.WAL.ActiveSegment"),
		walActiveSize:          reuseInt("NoKV.Stats.WAL.ActiveSize"),
		walSegmentCount:        reuseInt("NoKV.Stats.WAL.Segments"),
		walSegmentsRemoved:     reuseInt("NoKV.Stats.WAL.Removed"),
		raftGroupCount:         reuseInt("NoKV.Stats.Raft.Groups"),
		raftLaggingGroups:      reuseInt("NoKV.Stats.Raft.LaggingGroups"),
		raftMaxLagSegments:     reuseInt("NoKV.Stats.Raft.MaxLagSegments"),
		raftMinSegment:         reuseInt("NoKV.Stats.Raft.MinSegment"),
		raftMaxSegment:         reuseInt("NoKV.Stats.Raft.MaxSegment"),
		raftLagWarning:         reuseInt("NoKV.Stats.Raft.LagWarning"),
		writeQueueDepth:        reuseInt("NoKV.Stats.Write.QueueDepth"),
		writeQueueEntries:      reuseInt("NoKV.Stats.Write.QueueEntries"),
		writeQueueBytes:        reuseInt("NoKV.Stats.Write.QueueBytes"),
		writeBatchAvgEntries:   reuseFloat("NoKV.Stats.Write.BatchAvgEntries"),
		writeBatchAvgBytes:     reuseFloat("NoKV.Stats.Write.BatchAvgBytes"),
		writeRequestWaitMs:     reuseFloat("NoKV.Stats.Write.RequestWaitMs"),
		writeValueLogMs:        reuseFloat("NoKV.Stats.Write.ValueLogMs"),
		writeApplyMs:           reuseFloat("NoKV.Stats.Write.ApplyMs"),
		writeBatchesTotal:      reuseInt("NoKV.Stats.Write.Batches"),
		writeThrottle:          reuseInt("NoKV.Stats.Write.Throttle"),
		writeHotKeyLimited:     reuseInt("NoKV.Stats.Write.HotKeyLimited"),
		txnActive:              reuseInt("NoKV.Txns.Active"),
		txnStarted:             reuseInt("NoKV.Txns.Started"),
		txnCommitted:           reuseInt("NoKV.Txns.Committed"),
		txnConflicts:           reuseInt("NoKV.Txns.Conflicts"),
		blockL0HitRate:         reuseFloat("NoKV.Stats.Cache.L0HitRate"),
		blockL1HitRate:         reuseFloat("NoKV.Stats.Cache.L1HitRate"),
		bloomHitRate:           reuseFloat("NoKV.Stats.Cache.BloomHitRate"),
		indexHitRate:           reuseFloat("NoKV.Stats.Cache.IndexHitRate"),
		iteratorReuses:         reuseInt("NoKV.Stats.Iterator.Reused"),
		walSegmentsWithRaft:    reuseInt("NoKV.Stats.WAL.RaftSegments"),
		walSegmentsRemovable:   reuseInt("NoKV.Stats.WAL.RaftSegmentsRemovable"),
		walTypedRatio:          reuseFloat("NoKV.Stats.WAL.TypedRatio"),
		walTypedWarning:        reuseInt("NoKV.Stats.WAL.TypedWarning"),
		walTypedReason:         reuseString("NoKV.Stats.WAL.TypedReason"),
		walAutoRuns:            reuseInt("NoKV.Stats.WAL.AutoRuns"),
		walAutoRemoved:         reuseInt("NoKV.Stats.WAL.AutoRemoved"),
		walAutoLastUnix:        reuseInt("NoKV.Stats.WAL.AutoLastUnix"),
		lsmValueBytes:          reuseInt("NoKV.Stats.LSM.ValueBytes"),
		compactionValueWeight:  reuseFloat("NoKV.Stats.Compaction.ValueWeight"),
		lsmValueDensityMax:     reuseFloat("NoKV.Stats.LSM.ValueDensityMax"),
		lsmValueDensityAlert:   reuseInt("NoKV.Stats.LSM.ValueDensityAlert"),
		regionTotal:            reuseInt("NoKV.Stats.Region.Total"),
		regionNew:              reuseInt("NoKV.Stats.Region.New"),
		regionRunning:          reuseInt("NoKV.Stats.Region.Running"),
		regionRemoving:         reuseInt("NoKV.Stats.Region.Removing"),
		regionTombstone:        reuseInt("NoKV.Stats.Region.Tombstone"),
		regionOther:            reuseInt("NoKV.Stats.Region.Other"),
	}
	if v := expvar.Get("NoKV.Stats.ColumnFamilies"); v != nil {
		if m, ok := v.(*expvar.Map); ok {
			s.cfMap = m
		}
	}
	if s.cfMap == nil {
		s.cfMap = expvar.NewMap("NoKV.Stats.ColumnFamilies")
	}
	s.walRecordCounts = reuseMap("NoKV.Stats.WAL.RecordCounts")
	if expvar.Get("NoKV.Stats.LSM.Levels") == nil {
		expvar.Publish("NoKV.Stats.LSM.Levels", expvar.Func(func() any {
			if s == nil || s.db == nil || s.db.lsm == nil {
				return []map[string]any{}
			}
			levels := s.db.lsm.LevelMetrics()
			out := make([]map[string]any, 0, len(levels))
			for _, lvl := range levels {
				out = append(out, map[string]any{
					"level":              lvl.Level,
					"tables":             lvl.TableCount,
					"size_bytes":         lvl.SizeBytes,
					"value_bytes":        lvl.ValueBytes,
					"stale_bytes":        lvl.StaleBytes,
					"ingest_tables":      lvl.IngestTableCount,
					"ingest_size_bytes":  lvl.IngestSizeBytes,
					"ingest_value_bytes": lvl.IngestValueBytes,
				})
			}
			return out
		}))
	}
	if expvar.Get("NoKV.Stats.HotKeys") == nil {
		expvar.Publish("NoKV.Stats.HotKeys", expvar.Func(func() any {
			if db == nil || db.hotRead == nil {
				return []map[string]any{}
			}
			topK := db.opt.HotRingTopK
			if topK <= 0 {
				topK = 16
			}
			items := db.hotRead.TopN(topK)
			out := make([]map[string]any, 0, len(items))
			for _, item := range items {
				out = append(out, map[string]any{
					"key":   item.Key,
					"count": item.Count,
				})
			}
			return out
		}))
	}
	if expvar.Get("NoKV.Stats.HotWriteKeys") == nil {
		expvar.Publish("NoKV.Stats.HotWriteKeys", expvar.Func(func() any {
			if db == nil || db.hotWrite == nil {
				return []map[string]any{}
			}
			topK := db.opt.HotRingTopK
			if topK <= 0 {
				topK = 16
			}
			items := db.hotWrite.TopN(topK)
			out := make([]map[string]any, 0, len(items))
			for _, item := range items {
				out = append(out, map[string]any{
					"key":   item.Key,
					"count": item.Count,
				})
			}
			return out
		}))
	}
	if expvar.Get("NoKV.Stats.HotRing") == nil {
		expvar.Publish("NoKV.Stats.HotRing", expvar.Func(func() any {
			if db == nil || db.hotRead == nil {
				return map[string]any{}
			}
			return db.hotRead.Stats()
		}))
	}
	if expvar.Get("NoKV.Stats.HotWriteRing") == nil {
		expvar.Publish("NoKV.Stats.HotWriteRing", expvar.Func(func() any {
			if db == nil || db.hotWrite == nil {
				return map[string]any{}
			}
			return db.hotWrite.Stats()
		}))
	}
	if expvar.Get("NoKV.Stats.ValueLogGC") == nil {
		expvar.Publish("NoKV.Stats.ValueLogGC", expvar.Func(func() any {
			return metrics.ValueLogGCMetricsSnapshot()
		}))
	}
	if expvar.Get("NoKV.Stats.Transport") == nil {
		expvar.Publish("NoKV.Stats.Transport", expvar.Func(func() any {
			return transportpkg.GRPCMetricsSnapshot()
		}))
	}
	if expvar.Get("NoKV.Redis") == nil {
		expvar.Publish("NoKV.Redis", expvar.Func(func() any {
			return metrics.DefaultRedisSnapshot().ExpvarMap()
		}))
	}
	return s
}

func reuseInt(name string) *expvar.Int {
	if v := expvar.Get(name); v != nil {
		if iv, ok := v.(*expvar.Int); ok {
			return iv
		}
	}
	return expvar.NewInt(name)
}

func reuseFloat(name string) *expvar.Float {
	if v := expvar.Get(name); v != nil {
		if fv, ok := v.(*expvar.Float); ok {
			return fv
		}
	}
	return expvar.NewFloat(name)
}

func reuseString(name string) *expvar.String {
	if v := expvar.Get(name); v != nil {
		if sv, ok := v.(*expvar.String); ok {
			return sv
		}
	}
	return expvar.NewString(name)
}

func reuseMap(name string) *expvar.Map {
	if v := expvar.Get(name); v != nil {
		if mv, ok := v.(*expvar.Map); ok {
			return mv
		}
	}
	return expvar.NewMap(name)
}

func newIntVar(val int64) *expvar.Int {
	v := new(expvar.Int)
	v.Set(val)
	return v
}

// StartStats runs periodic collection of internal backlog metrics.
func (s *Stats) StartStats() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.closer.Add(1)
		go s.run()
	})
}

// SetRegionMetrics attaches region metrics recorder used in snapshots.
func (s *Stats) SetRegionMetrics(rm *storepkg.RegionMetrics) {
	if s == nil {
		return
	}
	s.regionMetrics.Store(rm)
}

func (s *Stats) run() {
	defer s.closer.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Collect once at startup so expvar has values immediately.
	s.collect()

	for {
		select {
		case <-ticker.C:
			s.collect()
		case <-s.closer.CloseSignal:
			return
		}
	}
}

// collect snapshots background queues and propagates them to expvar.
func (s *Stats) collect() {
	snap := s.Snapshot()
	s.entries.Set(snap.Entries)
	s.flushPending.Set(snap.Flush.Pending)
	s.flushQueueLen.Set(snap.Flush.QueueLength)
	s.flushActive.Set(snap.Flush.Active)
	s.flushWaitMs.Set(snap.Flush.WaitMs)
	s.flushWaitLastMs.Set(snap.Flush.LastWaitMs)
	s.flushWaitMaxMs.Set(snap.Flush.MaxWaitMs)
	s.flushBuildMs.Set(snap.Flush.BuildMs)
	s.flushBuildLastMs.Set(snap.Flush.LastBuildMs)
	s.flushBuildMaxMs.Set(snap.Flush.MaxBuildMs)
	s.flushReleaseMs.Set(snap.Flush.ReleaseMs)
	s.flushReleaseLastMs.Set(snap.Flush.LastReleaseMs)
	s.flushReleaseMaxMs.Set(snap.Flush.MaxReleaseMs)
	s.flushCompleted.Set(snap.Flush.Completed)
	s.compactionBacklog.Set(snap.Compaction.Backlog)
	s.compactionMaxScore.Set(snap.Compaction.MaxScore)
	s.compactionLastMs.Set(snap.Compaction.LastDurationMs)
	s.compactionMaxMs.Set(snap.Compaction.MaxDurationMs)
	s.compactionRuns.Set(int64(snap.Compaction.Runs))
	s.compactionIngestRuns.Set(snap.Compaction.IngestRuns)
	s.compactionIngestMs.Set(snap.Compaction.IngestMs)
	s.compactionIngestTables.Set(snap.Compaction.IngestTables)
	s.compactionMergeRuns.Set(snap.Compaction.MergeRuns)
	s.compactionMergeMs.Set(snap.Compaction.MergeMs)
	s.compactionMergeTables.Set(snap.Compaction.MergeTables)
	s.valueLogSegments.Set(int64(snap.ValueLog.Segments))
	s.valueLogPendingDel.Set(int64(snap.ValueLog.PendingDeletes))
	s.valueLogDiscardQueue.Set(int64(snap.ValueLog.DiscardQueue))
	s.walActiveSegment.Set(snap.WAL.ActiveSegment)
	s.walSegmentCount.Set(snap.WAL.SegmentCount)
	s.walActiveSize.Set(snap.WAL.ActiveSize)
	s.walSegmentsRemoved.Set(int64(snap.WAL.SegmentsRemoved))
	s.walSegmentsWithRaft.Set(int64(snap.WAL.SegmentsWithRaftRecords))
	s.walSegmentsRemovable.Set(int64(snap.WAL.RemovableRaftSegments))
	s.walTypedRatio.Set(snap.WAL.TypedRecordRatio)
	if snap.WAL.TypedRecordWarning {
		s.walTypedWarning.Set(1)
	} else {
		s.walTypedWarning.Set(0)
	}
	s.walTypedReason.Set(snap.WAL.TypedRecordReason)
	s.walAutoRuns.Set(int64(snap.WAL.AutoGCRuns))
	s.walAutoRemoved.Set(int64(snap.WAL.AutoGCRemoved))
	s.walAutoLastUnix.Set(snap.WAL.AutoGCLastUnix)
	s.writeQueueDepth.Set(snap.Write.QueueDepth)
	s.writeQueueEntries.Set(snap.Write.QueueEntries)
	s.writeQueueBytes.Set(snap.Write.QueueBytes)
	s.writeBatchAvgEntries.Set(snap.Write.AvgBatchEntries)
	s.writeBatchAvgBytes.Set(snap.Write.AvgBatchBytes)
	s.writeRequestWaitMs.Set(snap.Write.AvgRequestWaitMs)
	s.writeValueLogMs.Set(snap.Write.AvgValueLogMs)
	s.writeApplyMs.Set(snap.Write.AvgApplyMs)
	s.writeBatchesTotal.Set(snap.Write.BatchesTotal)
	if s.writeHotKeyLimited != nil {
		s.writeHotKeyLimited.Set(int64(snap.Write.HotKeyLimited))
	}
	s.raftGroupCount.Set(int64(snap.Raft.GroupCount))
	s.raftLaggingGroups.Set(int64(snap.Raft.LaggingGroups))
	s.raftMaxLagSegments.Set(snap.Raft.MaxLagSegments)
	s.raftMinSegment.Set(int64(snap.Raft.MinLogSegment))
	s.raftMaxSegment.Set(int64(snap.Raft.MaxLogSegment))
	if snap.Raft.LagWarning {
		s.raftLagWarning.Set(1)
	} else {
		s.raftLagWarning.Set(0)
	}
	if snap.Write.ThrottleActive {
		s.writeThrottle.Set(1)
	} else {
		s.writeThrottle.Set(0)
	}
	s.txnActive.Set(snap.Txn.Active)
	s.txnStarted.Set(int64(snap.Txn.Started))
	s.txnCommitted.Set(int64(snap.Txn.Committed))
	s.txnConflicts.Set(int64(snap.Txn.Conflicts))
	s.blockL0HitRate.Set(snap.Cache.BlockL0HitRate)
	s.blockL1HitRate.Set(snap.Cache.BlockL1HitRate)
	s.bloomHitRate.Set(snap.Cache.BloomHitRate)
	s.indexHitRate.Set(snap.Cache.IndexHitRate)
	s.iteratorReuses.Set(int64(snap.Cache.IteratorReused))
	if s.cfMap != nil {
		s.cfMap.Init()
		for cfName, cf := range snap.LSM.ColumnFamilies {
			sub := &expvar.Map{}
			sub.Init()
			sub.Set("writes", newIntVar(int64(cf.Writes)))
			sub.Set("reads", newIntVar(int64(cf.Reads)))
			s.cfMap.Set(cfName, sub)
		}
	}
	if s.walRecordCounts != nil {
		s.walRecordCounts.Init()
		s.walRecordCounts.Set("entries", newIntVar(int64(snap.WAL.RecordCounts.Entries)))
		s.walRecordCounts.Set("raft_entries", newIntVar(int64(snap.WAL.RecordCounts.RaftEntries)))
		s.walRecordCounts.Set("raft_states", newIntVar(int64(snap.WAL.RecordCounts.RaftStates)))
		s.walRecordCounts.Set("raft_snapshots", newIntVar(int64(snap.WAL.RecordCounts.RaftSnapshots)))
		s.walRecordCounts.Set("total", newIntVar(int64(snap.WAL.RecordCounts.Total())))
	}
	if s.regionTotal != nil {
		s.regionTotal.Set(snap.Region.Total)
		s.regionNew.Set(snap.Region.New)
		s.regionRunning.Set(snap.Region.Running)
		s.regionRemoving.Set(snap.Region.Removing)
		s.regionTombstone.Set(snap.Region.Tombstone)
		s.regionOther.Set(snap.Region.Other)
	}
	s.lsmValueBytes.Set(snap.LSM.ValueBytesTotal)
	s.lsmValueDensityMax.Set(snap.LSM.ValueDensityMax)
	if snap.LSM.ValueDensityAlert {
		s.lsmValueDensityAlert.Set(1)
	} else {
		s.lsmValueDensityAlert.Set(0)
	}
	s.compactionValueWeight.Set(snap.Compaction.ValueWeight)
	atomic.StoreInt64(&s.EntryNum, snap.Entries)
}

// Snapshot returns a point-in-time metrics snapshot without mutating state.
func (s *Stats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	if s == nil || s.db == nil {
		return snap
	}

	if s.db.opt != nil {
		if thresh := s.db.opt.RaftLagWarnSegments; thresh > 0 {
			snap.Raft.LagWarnThreshold = thresh
		}
	}

	// Flush backlog (pending flush tasks).
	if s.db.lsm != nil {
		snap.Compaction.ValueWeight = s.db.lsm.CompactionValueWeight()
		alertThreshold := s.db.lsm.CompactionValueAlertThreshold()
		fstats := s.db.lsm.FlushMetrics()
		snap.Flush.Pending = fstats.Pending
		snap.Flush.QueueLength = fstats.Queue
		snap.Flush.Active = fstats.Active
		if fstats.WaitCount > 0 {
			snap.Flush.WaitMs = float64(fstats.WaitNs) / float64(fstats.WaitCount) / 1e6
		}
		if fstats.WaitLastNs > 0 {
			snap.Flush.LastWaitMs = float64(fstats.WaitLastNs) / 1e6
		}
		if fstats.WaitMaxNs > 0 {
			snap.Flush.MaxWaitMs = float64(fstats.WaitMaxNs) / 1e6
		}
		if fstats.BuildCount > 0 {
			snap.Flush.BuildMs = float64(fstats.BuildNs) / float64(fstats.BuildCount) / 1e6
		}
		if fstats.BuildLastNs > 0 {
			snap.Flush.LastBuildMs = float64(fstats.BuildLastNs) / 1e6
		}
		if fstats.BuildMaxNs > 0 {
			snap.Flush.MaxBuildMs = float64(fstats.BuildMaxNs) / 1e6
		}
		if fstats.ReleaseCount > 0 {
			snap.Flush.ReleaseMs = float64(fstats.ReleaseNs) / float64(fstats.ReleaseCount) / 1e6
		}
		if fstats.ReleaseLastNs > 0 {
			snap.Flush.LastReleaseMs = float64(fstats.ReleaseLastNs) / 1e6
		}
		if fstats.ReleaseMaxNs > 0 {
			snap.Flush.MaxReleaseMs = float64(fstats.ReleaseMaxNs) / 1e6
		}
		snap.Flush.Completed = fstats.Completed
		snap.Compaction.Backlog, snap.Compaction.MaxScore = s.db.lsm.CompactionStats()
		if levels := s.db.lsm.LevelMetrics(); len(levels) > 0 {
			snap.LSM.Levels = make([]LSMLevelStats, 0, len(levels))
			var maxDensity float64
			var ingestRuns, ingestMergeRuns int64
			var ingestMs, ingestMergeMs float64
			var ingestTables, ingestMergeTables int64
			for _, lvl := range levels {
				statsLvl := levelMetricsToStats(lvl)
				snap.LSM.Levels = append(snap.LSM.Levels, statsLvl)
				if statsLvl.ValueDensity > maxDensity {
					maxDensity = statsLvl.ValueDensity
				}
				if statsLvl.IngestValueDensity > maxDensity {
					maxDensity = statsLvl.IngestValueDensity
				}
				ingestRuns += statsLvl.IngestRuns
				ingestMergeRuns += statsLvl.MergeRuns
				ingestMs += statsLvl.IngestMs
				ingestMergeMs += statsLvl.MergeMs
				ingestTables += statsLvl.IngestTablesCount
				ingestMergeTables += statsLvl.MergeTables
			}
			snap.Compaction.IngestRuns = ingestRuns
			snap.Compaction.MergeRuns = ingestMergeRuns
			snap.Compaction.IngestMs = ingestMs
			snap.Compaction.MergeMs = ingestMergeMs
			snap.Compaction.IngestTables = ingestTables
			snap.Compaction.MergeTables = ingestMergeTables
			snap.LSM.ValueDensityMax = maxDensity
			if alertThreshold > 0 && maxDensity >= alertThreshold {
				snap.LSM.ValueDensityAlert = true
				delta := maxDensity - alertThreshold
				recommend := snap.Compaction.ValueWeight + delta
				if recommend < snap.Compaction.ValueWeight {
					recommend = snap.Compaction.ValueWeight
				}
				if recommend > 4.0 {
					recommend = 4.0
				}
				snap.Compaction.ValueWeightSuggested = math.Round(recommend*100) / 100
			}
		}
	}
	if len(snap.LSM.Levels) > 0 {
		var totalValue int64
		for _, lvl := range snap.LSM.Levels {
			totalValue += lvl.ValueBytes + lvl.IngestValueBytes
		}
		snap.LSM.ValueBytesTotal = totalValue
	}

	if s.db.writeMetrics != nil {
		wsnap := s.db.writeMetrics.Snapshot()
		snap.Write.QueueDepth = wsnap.QueueLen
		snap.Write.QueueEntries = wsnap.QueueEntries
		snap.Write.QueueBytes = wsnap.QueueBytes
		snap.Write.AvgBatchEntries = wsnap.AvgBatchEntries
		snap.Write.AvgBatchBytes = wsnap.AvgBatchBytes
		snap.Write.AvgRequestWaitMs = wsnap.AvgRequestWaitMs
		snap.Write.AvgValueLogMs = wsnap.AvgValueLogMs
		snap.Write.AvgApplyMs = wsnap.AvgApplyMs
		snap.Write.BatchesTotal = wsnap.Batches
	}
	snap.Write.ThrottleActive = atomic.LoadInt32(&s.db.blockWrites) == 1
	snap.Write.HotKeyLimited = atomic.LoadUint64(&s.db.hotWriteLimited)

	if rm := s.regionMetrics.Load(); rm != nil {
		rms := rm.Snapshot()
		snap.Region.Total = int64(rms.Total)
		snap.Region.New = int64(rms.New)
		snap.Region.Running = int64(rms.Running)
		snap.Region.Removing = int64(rms.Removing)
		snap.Region.Tombstone = int64(rms.Tombstone)
		snap.Region.Other = int64(rms.Other)
	}

	var (
		wstats         *wal.Metrics
		segmentMetrics map[uint32]wal.RecordMetrics
		ptrs           map[uint64]manifest.RaftLogPointer
	)
	if s.db.wal != nil {
		wstats = s.db.wal.Metrics()
		if wstats != nil {
			snap.WAL.ActiveSegment = int64(wstats.ActiveSegment)
			snap.WAL.ActiveSize = wstats.ActiveSize
			snap.WAL.SegmentCount = int64(wstats.SegmentCount)
			snap.WAL.SegmentsRemoved = wstats.RemovedSegments
		}
		segmentMetrics = s.db.wal.SegmentMetrics()
	}
	if man := s.db.Manifest(); man != nil {
		ptrs = man.RaftPointerSnapshot()
		snap.Raft.GroupCount = len(ptrs)
	}

	analysis := metrics.AnalyzeWALBacklog(wstats, segmentMetrics, ptrs)
	snap.WAL.RecordCounts = analysis.RecordCounts
	snap.WAL.SegmentsWithRaftRecords = analysis.SegmentsWithRaft
	snap.WAL.RemovableRaftSegments = len(analysis.RemovableSegments)
	snap.WAL.TypedRecordRatio = analysis.TypedRecordRatio

	if len(ptrs) > 0 {
		var minSeg uint32
		var maxSeg uint32
		var maxLag int64
		lagging := 0
		effectiveActive := snap.WAL.ActiveSegment
		if snap.WAL.ActiveSize == 0 && effectiveActive > 0 {
			effectiveActive--
		}
		for _, ptr := range ptrs {
			if ptr.Segment == 0 {
				lagging++
				if effectiveActive > maxLag {
					maxLag = effectiveActive
				}
				continue
			}
			if minSeg == 0 || ptr.Segment < minSeg {
				minSeg = ptr.Segment
			}
			if ptr.Segment > maxSeg {
				maxSeg = ptr.Segment
			}
			if effectiveActive > 0 {
				lag := effectiveActive - int64(ptr.Segment)
				if lag < 0 {
					lag = 0
				}
				if lag > 0 {
					lagging++
				}
				if lag > maxLag {
					maxLag = lag
				}
			}
		}
		snap.Raft.MinLogSegment = minSeg
		snap.Raft.MaxLogSegment = maxSeg
		snap.Raft.MaxLagSegments = maxLag
		snap.Raft.LaggingGroups = lagging
	}
	threshold := s.db.opt.RaftLagWarnSegments
	if threshold < 0 {
		threshold = 0
	}
	snap.Raft.LagWarnThreshold = threshold
	if threshold > 0 && snap.Raft.MaxLagSegments >= threshold && snap.Raft.LaggingGroups > 0 {
		snap.Raft.LagWarning = true
	}

	warning, reason := metrics.WALTypedWarning(snap.WAL.TypedRecordRatio, analysis.SegmentsWithRaft, s.db.opt.WALTypedRecordWarnRatio, s.db.opt.WALTypedRecordWarnSegments)
	if watchdog := s.db.walWatchdog; watchdog != nil {
		wsnap := watchdog.Snapshot()
		snap.WAL.AutoGCRuns = wsnap.AutoRuns
		snap.WAL.AutoGCRemoved = wsnap.SegmentsRemoved
		snap.WAL.AutoGCLastUnix = wsnap.LastAutoUnix
		if wsnap.Warning {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = wsnap.WarningReason
		} else if warning {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = reason
		}
	} else if warning {
		snap.WAL.TypedRecordWarning = true
		snap.WAL.TypedRecordReason = reason
	}

	// Value log backlog.
	if s.db.vlog != nil {
		stats := s.db.vlog.metrics()
		snap.ValueLog.Segments = stats.Segments
		snap.ValueLog.PendingDeletes = stats.PendingDeletes
		snap.ValueLog.DiscardQueue = stats.DiscardQueue
		snap.ValueLog.Heads = stats.Heads
	}
	if s.db.orc != nil {
		tm := s.db.orc.txnMetricsSnapshot()
		snap.Txn.Active = tm.Active
		snap.Txn.Started = tm.Started
		snap.Txn.Committed = tm.Committed
		snap.Txn.Conflicts = tm.Conflicts
	}
	if s.db != nil && s.db.hotRead != nil {
		topK := s.db.opt.HotRingTopK
		if topK <= 0 {
			topK = 16
		}
		for _, item := range s.db.hotRead.TopN(topK) {
			snap.Hot.ReadKeys = append(snap.Hot.ReadKeys, HotKeyStat{Key: item.Key, Count: item.Count})
		}
		hotStats := s.db.hotRead.Stats()
		snap.Hot.ReadRing = &hotStats
	}
	if s.db != nil && s.db.hotWrite != nil {
		topK := s.db.opt.HotRingTopK
		if topK <= 0 {
			topK = 16
		}
		for _, item := range s.db.hotWrite.TopN(topK) {
			snap.Hot.WriteKeys = append(snap.Hot.WriteKeys, HotKeyStat{Key: item.Key, Count: item.Count})
		}
		hotStats := s.db.hotWrite.Stats()
		snap.Hot.WriteRing = &hotStats
	}
	if s.db != nil && s.db.lsm != nil {
		cm := s.db.lsm.CacheMetrics()
		if total := cm.L0Hits + cm.L0Misses; total > 0 {
			snap.Cache.BlockL0HitRate = float64(cm.L0Hits) / float64(total)
		}
		if total := cm.L1Hits + cm.L1Misses; total > 0 {
			snap.Cache.BlockL1HitRate = float64(cm.L1Hits) / float64(total)
		}
		if total := cm.BloomHits + cm.BloomMisses; total > 0 {
			snap.Cache.BloomHitRate = float64(cm.BloomHits) / float64(total)
		}
		if total := cm.IndexHits + cm.IndexMisses; total > 0 {
			snap.Cache.IndexHitRate = float64(cm.IndexHits) / float64(total)
		}
	}
	if s.db != nil && s.db.iterPool != nil {
		snap.Cache.IteratorReused = s.db.iterPool.reused()
	}
	if s.db != nil && s.db.lsm != nil {
		snap.Entries = s.db.lsm.EntryCount()
		lastMs, maxMs, runs := s.db.lsm.CompactionDurations()
		snap.Compaction.LastDurationMs = lastMs
		snap.Compaction.MaxDurationMs = maxMs
		snap.Compaction.Runs = runs
	}
	if s.db != nil {
		snap.LSM.ColumnFamilies = s.db.columnFamilyStats()
	}
	snap.ValueLog.GC = metrics.ValueLogGCMetricsSnapshot()
	snap.Transport = transportpkg.GRPCMetricsSnapshot()
	snap.Redis = metrics.DefaultRedisSnapshot()
	return snap
}

// close stops the stats loop.
func (s *Stats) close() error {
	if s == nil {
		return nil
	}
	s.closer.Close()
	return nil
}
