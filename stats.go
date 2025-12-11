package NoKV

import (
	"expvar"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/internal/metrics"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
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
	regionMetrics          *storepkg.RegionMetrics
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
	Entries                        int64                           `json:"entries"`
	FlushPending                   int64                           `json:"flush_pending"`
	FlushQueueLength               int64                           `json:"flush_queue_length"`
	FlushActive                    int64                           `json:"flush_active"`
	FlushWaitMs                    float64                         `json:"flush_wait_ms"`
	FlushLastWaitMs                float64                         `json:"flush_last_wait_ms"`
	FlushMaxWaitMs                 float64                         `json:"flush_max_wait_ms"`
	FlushBuildMs                   float64                         `json:"flush_build_ms"`
	FlushLastBuildMs               float64                         `json:"flush_last_build_ms"`
	FlushMaxBuildMs                float64                         `json:"flush_max_build_ms"`
	FlushReleaseMs                 float64                         `json:"flush_release_ms"`
	FlushLastReleaseMs             float64                         `json:"flush_last_release_ms"`
	FlushMaxReleaseMs              float64                         `json:"flush_max_release_ms"`
	FlushCompleted                 int64                           `json:"flush_completed"`
	CompactionBacklog              int64                           `json:"compaction_backlog"`
	CompactionMaxScore             float64                         `json:"compaction_max_score"`
	CompactionLastDurationMs       float64                         `json:"compaction_last_duration_ms"`
	CompactionMaxDurationMs        float64                         `json:"compaction_max_duration_ms"`
	CompactionRuns                 uint64                          `json:"compaction_runs"`
	CompactionIngestRuns           int64                           `json:"compaction_ingest_runs"`
	CompactionMergeRuns            int64                           `json:"compaction_ingest_merge_runs"`
	CompactionIngestMs             float64                         `json:"compaction_ingest_ms"`
	CompactionMergeMs              float64                         `json:"compaction_ingest_merge_ms"`
	CompactionIngestTables         int64                           `json:"compaction_ingest_tables"`
	CompactionMergeTables          int64                           `json:"compaction_ingest_merge_tables"`
	CompactionValueWeight          float64                         `json:"compaction_value_weight"`
	CompactionValueWeightSuggested float64                         `json:"compaction_value_weight_suggested,omitempty"`
	ValueLogSegments               int                             `json:"vlog_segments"`
	ValueLogPendingDel             int                             `json:"vlog_pending_deletes"`
	ValueLogDiscardQueue           int                             `json:"vlog_discard_queue"`
	ValueLogHead                   kv.ValuePtr                     `json:"vlog_head"`
	WALActiveSegment               int64                           `json:"wal_active_segment"`
	WALSegmentCount                int64                           `json:"wal_segment_count"`
	WALActiveSize                  int64                           `json:"wal_active_size"`
	WALSegmentsRemoved             uint64                          `json:"wal_segments_removed"`
	WALRecordCounts                wal.RecordMetrics               `json:"wal_record_counts"`
	WALSegmentsWithRaftRecords     int                             `json:"wal_segments_with_raft_records"`
	WALRemovableRaftSegments       int                             `json:"wal_removable_raft_segments"`
	WALTypedRecordRatio            float64                         `json:"wal_typed_record_ratio"`
	WALTypedRecordWarning          bool                            `json:"wal_typed_record_warning"`
	WALTypedRecordReason           string                          `json:"wal_typed_record_reason,omitempty"`
	WALAutoGCRuns                  uint64                          `json:"wal_auto_gc_runs"`
	WALAutoGCRemoved               uint64                          `json:"wal_auto_gc_removed"`
	WALAutoGCLastUnix              int64                           `json:"wal_auto_gc_last_unix"`
	RaftGroupCount                 int                             `json:"raft_group_count"`
	RaftLaggingGroups              int                             `json:"raft_lagging_groups"`
	RaftMinLogSegment              uint32                          `json:"raft_min_log_segment"`
	RaftMaxLogSegment              uint32                          `json:"raft_max_log_segment"`
	RaftMaxLagSegments             int64                           `json:"raft_max_lag_segments"`
	RaftLagWarnThreshold           int64                           `json:"raft_lag_warn_threshold"`
	RaftLagWarning                 bool                            `json:"raft_lag_warning"`
	WriteQueueDepth                int64                           `json:"write_queue_depth"`
	WriteQueueEntries              int64                           `json:"write_queue_entries"`
	WriteQueueBytes                int64                           `json:"write_queue_bytes"`
	WriteAvgBatchEntries           float64                         `json:"write_avg_batch_entries"`
	WriteAvgBatchBytes             float64                         `json:"write_avg_batch_bytes"`
	WriteAvgRequestWaitMs          float64                         `json:"write_avg_request_wait_ms"`
	WriteAvgValueLogMs             float64                         `json:"write_avg_vlog_ms"`
	WriteAvgApplyMs                float64                         `json:"write_avg_apply_ms"`
	WriteBatchesTotal              int64                           `json:"write_batches_total"`
	WriteThrottleActive            bool                            `json:"write_throttle_active"`
	TxnsActive                     int64                           `json:"txns_active"`
	TxnsStarted                    uint64                          `json:"txns_started"`
	TxnsCommitted                  uint64                          `json:"txns_committed"`
	TxnsConflicts                  uint64                          `json:"txns_conflicts"`
	RegionTotal                    int64                           `json:"region_total"`
	RegionNew                      int64                           `json:"region_new"`
	RegionRunning                  int64                           `json:"region_running"`
	RegionRemoving                 int64                           `json:"region_removing"`
	RegionTombstone                int64                           `json:"region_tombstone"`
	RegionOther                    int64                           `json:"region_other"`
	HotKeys                        []HotKeyStat                    `json:"hot_keys,omitempty"`
	HotWriteLimited                uint64                          `json:"hot_write_limited"`
	BlockL0HitRate                 float64                         `json:"block_l0_hit_rate"`
	BlockL1HitRate                 float64                         `json:"block_l1_hit_rate"`
	BloomHitRate                   float64                         `json:"bloom_hit_rate"`
	IndexHitRate                   float64                         `json:"index_hit_rate"`
	IteratorReused                 uint64                          `json:"iterator_reused"`
	ColumnFamilies                 map[string]ColumnFamilySnapshot `json:"column_families,omitempty"`
	LSMLevels                      []LSMLevelStats                 `json:"lsm_levels,omitempty"`
	LSMValueBytesTotal             int64                           `json:"lsm_value_bytes_total"`
	LSMValueDensityMax             float64                         `json:"lsm_value_density_max"`
	LSMValueDensityAlert           bool                            `json:"lsm_value_density_alert"`
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
			if db == nil || db.hot == nil {
				return []map[string]any{}
			}
			topK := db.opt.HotRingTopK
			if topK <= 0 {
				topK = 16
			}
			items := db.hot.TopN(topK)
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

func newFloatVar(val float64) *expvar.Float {
	v := new(expvar.Float)
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
	s.regionMetrics = rm
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
	s.flushPending.Set(snap.FlushPending)
	s.flushQueueLen.Set(snap.FlushQueueLength)
	s.flushActive.Set(snap.FlushActive)
	s.flushWaitMs.Set(snap.FlushWaitMs)
	s.flushWaitLastMs.Set(snap.FlushLastWaitMs)
	s.flushWaitMaxMs.Set(snap.FlushMaxWaitMs)
	s.flushBuildMs.Set(snap.FlushBuildMs)
	s.flushBuildLastMs.Set(snap.FlushLastBuildMs)
	s.flushBuildMaxMs.Set(snap.FlushMaxBuildMs)
	s.flushReleaseMs.Set(snap.FlushReleaseMs)
	s.flushReleaseLastMs.Set(snap.FlushLastReleaseMs)
	s.flushReleaseMaxMs.Set(snap.FlushMaxReleaseMs)
	s.flushCompleted.Set(snap.FlushCompleted)
	s.compactionBacklog.Set(snap.CompactionBacklog)
	s.compactionMaxScore.Set(snap.CompactionMaxScore)
	s.compactionLastMs.Set(snap.CompactionLastDurationMs)
	s.compactionMaxMs.Set(snap.CompactionMaxDurationMs)
	s.compactionRuns.Set(int64(snap.CompactionRuns))
	s.compactionIngestRuns.Set(snap.CompactionIngestRuns)
	s.compactionIngestMs.Set(snap.CompactionIngestMs)
	s.compactionIngestTables.Set(snap.CompactionIngestTables)
	s.compactionMergeRuns.Set(snap.CompactionMergeRuns)
	s.compactionMergeMs.Set(snap.CompactionMergeMs)
	s.compactionMergeTables.Set(snap.CompactionMergeTables)
	s.compactionIngestRuns.Set(snap.CompactionIngestRuns)
	s.compactionIngestMs.Set(snap.CompactionIngestMs)
	s.compactionIngestTables.Set(snap.CompactionIngestTables)
	s.compactionMergeRuns.Set(snap.CompactionMergeRuns)
	s.compactionMergeMs.Set(snap.CompactionMergeMs)
	s.compactionMergeTables.Set(snap.CompactionMergeTables)
	s.valueLogSegments.Set(int64(snap.ValueLogSegments))
	s.valueLogPendingDel.Set(int64(snap.ValueLogPendingDel))
	s.valueLogDiscardQueue.Set(int64(snap.ValueLogDiscardQueue))
	s.walActiveSegment.Set(snap.WALActiveSegment)
	s.walSegmentCount.Set(snap.WALSegmentCount)
	s.walActiveSize.Set(snap.WALActiveSize)
	s.walSegmentsRemoved.Set(int64(snap.WALSegmentsRemoved))
	s.walSegmentsWithRaft.Set(int64(snap.WALSegmentsWithRaftRecords))
	s.walSegmentsRemovable.Set(int64(snap.WALRemovableRaftSegments))
	s.walTypedRatio.Set(snap.WALTypedRecordRatio)
	if snap.WALTypedRecordWarning {
		s.walTypedWarning.Set(1)
	} else {
		s.walTypedWarning.Set(0)
	}
	s.walTypedReason.Set(snap.WALTypedRecordReason)
	s.walAutoRuns.Set(int64(snap.WALAutoGCRuns))
	s.walAutoRemoved.Set(int64(snap.WALAutoGCRemoved))
	s.walAutoLastUnix.Set(snap.WALAutoGCLastUnix)
	s.writeQueueDepth.Set(snap.WriteQueueDepth)
	s.writeQueueEntries.Set(snap.WriteQueueEntries)
	s.writeQueueBytes.Set(snap.WriteQueueBytes)
	s.writeBatchAvgEntries.Set(snap.WriteAvgBatchEntries)
	s.writeBatchAvgBytes.Set(snap.WriteAvgBatchBytes)
	s.writeRequestWaitMs.Set(snap.WriteAvgRequestWaitMs)
	s.writeValueLogMs.Set(snap.WriteAvgValueLogMs)
	s.writeApplyMs.Set(snap.WriteAvgApplyMs)
	s.writeBatchesTotal.Set(snap.WriteBatchesTotal)
	if s.writeHotKeyLimited != nil {
		s.writeHotKeyLimited.Set(int64(snap.HotWriteLimited))
	}
	s.raftGroupCount.Set(int64(snap.RaftGroupCount))
	s.raftLaggingGroups.Set(int64(snap.RaftLaggingGroups))
	s.raftMaxLagSegments.Set(snap.RaftMaxLagSegments)
	s.raftMinSegment.Set(int64(snap.RaftMinLogSegment))
	s.raftMaxSegment.Set(int64(snap.RaftMaxLogSegment))
	if snap.RaftLagWarning {
		s.raftLagWarning.Set(1)
	} else {
		s.raftLagWarning.Set(0)
	}
	if snap.WriteThrottleActive {
		s.writeThrottle.Set(1)
	} else {
		s.writeThrottle.Set(0)
	}
	s.txnActive.Set(snap.TxnsActive)
	s.txnStarted.Set(int64(snap.TxnsStarted))
	s.txnCommitted.Set(int64(snap.TxnsCommitted))
	s.txnConflicts.Set(int64(snap.TxnsConflicts))
	s.blockL0HitRate.Set(snap.BlockL0HitRate)
	s.blockL1HitRate.Set(snap.BlockL1HitRate)
	s.bloomHitRate.Set(snap.BloomHitRate)
	s.indexHitRate.Set(snap.IndexHitRate)
	s.iteratorReuses.Set(int64(snap.IteratorReused))
	if s.cfMap != nil {
		s.cfMap.Init()
		for cfName, cf := range snap.ColumnFamilies {
			sub := &expvar.Map{}
			sub.Init()
			sub.Set("writes", newIntVar(int64(cf.Writes)))
			sub.Set("reads", newIntVar(int64(cf.Reads)))
			s.cfMap.Set(cfName, sub)
		}
	}
	if s.walRecordCounts != nil {
		s.walRecordCounts.Init()
		s.walRecordCounts.Set("entries", newIntVar(int64(snap.WALRecordCounts.Entries)))
		s.walRecordCounts.Set("raft_entries", newIntVar(int64(snap.WALRecordCounts.RaftEntries)))
		s.walRecordCounts.Set("raft_states", newIntVar(int64(snap.WALRecordCounts.RaftStates)))
		s.walRecordCounts.Set("raft_snapshots", newIntVar(int64(snap.WALRecordCounts.RaftSnapshots)))
		s.walRecordCounts.Set("total", newIntVar(int64(snap.WALRecordCounts.Total())))
	}
	if s.regionTotal != nil {
		s.regionTotal.Set(snap.RegionTotal)
		s.regionNew.Set(snap.RegionNew)
		s.regionRunning.Set(snap.RegionRunning)
		s.regionRemoving.Set(snap.RegionRemoving)
		s.regionTombstone.Set(snap.RegionTombstone)
		s.regionOther.Set(snap.RegionOther)
	}
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
			snap.RaftLagWarnThreshold = thresh
		}
	}

	// Flush backlog (pending flush tasks).
	if s.db.lsm != nil {
		snap.CompactionValueWeight = s.db.lsm.CompactionValueWeight()
		alertThreshold := s.db.lsm.CompactionValueAlertThreshold()
		fstats := s.db.lsm.FlushMetrics()
		snap.FlushPending = fstats.Pending
		snap.FlushQueueLength = fstats.Queue
		snap.FlushActive = fstats.Active
		if fstats.WaitCount > 0 {
			snap.FlushWaitMs = float64(fstats.WaitNs) / float64(fstats.WaitCount) / 1e6
		}
		if fstats.WaitLastNs > 0 {
			snap.FlushLastWaitMs = float64(fstats.WaitLastNs) / 1e6
		}
		if fstats.WaitMaxNs > 0 {
			snap.FlushMaxWaitMs = float64(fstats.WaitMaxNs) / 1e6
		}
		if fstats.BuildCount > 0 {
			snap.FlushBuildMs = float64(fstats.BuildNs) / float64(fstats.BuildCount) / 1e6
		}
		if fstats.BuildLastNs > 0 {
			snap.FlushLastBuildMs = float64(fstats.BuildLastNs) / 1e6
		}
		if fstats.BuildMaxNs > 0 {
			snap.FlushMaxBuildMs = float64(fstats.BuildMaxNs) / 1e6
		}
		if fstats.ReleaseCount > 0 {
			snap.FlushReleaseMs = float64(fstats.ReleaseNs) / float64(fstats.ReleaseCount) / 1e6
		}
		if fstats.ReleaseLastNs > 0 {
			snap.FlushLastReleaseMs = float64(fstats.ReleaseLastNs) / 1e6
		}
		if fstats.ReleaseMaxNs > 0 {
			snap.FlushMaxReleaseMs = float64(fstats.ReleaseMaxNs) / 1e6
		}
		snap.FlushCompleted = fstats.Completed
		snap.CompactionBacklog, snap.CompactionMaxScore = s.db.lsm.CompactionStats()
		if levels := s.db.lsm.LevelMetrics(); len(levels) > 0 {
			snap.LSMLevels = make([]LSMLevelStats, 0, len(levels))
			var maxDensity float64
			var ingestRuns, ingestMergeRuns int64
			var ingestMs, ingestMergeMs float64
			var ingestTables, ingestMergeTables int64
			for _, lvl := range levels {
				statsLvl := levelMetricsToStats(lvl)
				snap.LSMLevels = append(snap.LSMLevels, statsLvl)
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
			snap.CompactionIngestRuns = ingestRuns
			snap.CompactionMergeRuns = ingestMergeRuns
			snap.CompactionIngestMs = ingestMs
			snap.CompactionMergeMs = ingestMergeMs
			snap.CompactionIngestTables = ingestTables
			snap.CompactionMergeTables = ingestMergeTables
			snap.LSMValueDensityMax = maxDensity
			if alertThreshold > 0 && maxDensity >= alertThreshold {
				snap.LSMValueDensityAlert = true
				delta := maxDensity - alertThreshold
				recommend := snap.CompactionValueWeight + delta
				if recommend < snap.CompactionValueWeight {
					recommend = snap.CompactionValueWeight
				}
				if recommend > 4.0 {
					recommend = 4.0
				}
				snap.CompactionValueWeightSuggested = math.Round(recommend*100) / 100
			}
		}
	}
	if len(snap.LSMLevels) > 0 {
		var totalValue int64
		for _, lvl := range snap.LSMLevels {
			totalValue += lvl.ValueBytes + lvl.IngestValueBytes
		}
		snap.LSMValueBytesTotal = totalValue
		s.lsmValueBytes.Set(totalValue)
		s.lsmValueDensityMax.Set(snap.LSMValueDensityMax)
		if snap.LSMValueDensityAlert {
			s.lsmValueDensityAlert.Set(1)
		} else {
			s.lsmValueDensityAlert.Set(0)
		}
	} else {
		s.lsmValueBytes.Set(0)
		s.lsmValueDensityMax.Set(0)
		s.lsmValueDensityAlert.Set(0)
	}
	s.compactionValueWeight.Set(snap.CompactionValueWeight)

	if s.db.writeMetrics != nil {
		wsnap := s.db.writeMetrics.Snapshot()
		snap.WriteQueueDepth = wsnap.QueueLen
		snap.WriteQueueEntries = wsnap.QueueEntries
		snap.WriteQueueBytes = wsnap.QueueBytes
		snap.WriteAvgBatchEntries = wsnap.AvgBatchEntries
		snap.WriteAvgBatchBytes = wsnap.AvgBatchBytes
		snap.WriteAvgRequestWaitMs = wsnap.AvgRequestWaitMs
		snap.WriteAvgValueLogMs = wsnap.AvgValueLogMs
		snap.WriteAvgApplyMs = wsnap.AvgApplyMs
		snap.WriteBatchesTotal = wsnap.Batches
	}
	snap.WriteThrottleActive = atomic.LoadInt32(&s.db.blockWrites) == 1
	snap.HotWriteLimited = atomic.LoadUint64(&s.db.hotWriteLimited)

	if rm := s.regionMetrics; rm != nil {
		rms := rm.Snapshot()
		snap.RegionTotal = int64(rms.Total)
		snap.RegionNew = int64(rms.New)
		snap.RegionRunning = int64(rms.Running)
		snap.RegionRemoving = int64(rms.Removing)
		snap.RegionTombstone = int64(rms.Tombstone)
		snap.RegionOther = int64(rms.Other)
	}

	var (
		wstats         *wal.Metrics
		segmentMetrics map[uint32]wal.RecordMetrics
		ptrs           map[uint64]manifest.RaftLogPointer
	)
	if s.db.wal != nil {
		wstats = s.db.wal.Metrics()
		if wstats != nil {
			snap.WALActiveSegment = int64(wstats.ActiveSegment)
			snap.WALActiveSize = wstats.ActiveSize
			snap.WALSegmentCount = int64(wstats.SegmentCount)
			snap.WALSegmentsRemoved = wstats.RemovedSegments
		}
		segmentMetrics = s.db.wal.SegmentMetrics()
	}
	if man := s.db.Manifest(); man != nil {
		ptrs = man.RaftPointerSnapshot()
		snap.RaftGroupCount = len(ptrs)
	}

	analysis := metrics.AnalyzeWALBacklog(wstats, segmentMetrics, ptrs)
	snap.WALRecordCounts = analysis.RecordCounts
	snap.WALSegmentsWithRaftRecords = analysis.SegmentsWithRaft
	snap.WALRemovableRaftSegments = len(analysis.RemovableSegments)
	snap.WALTypedRecordRatio = analysis.TypedRecordRatio

	if len(ptrs) > 0 {
		var minSeg uint32
		var maxSeg uint32
		var maxLag int64
		lagging := 0
		effectiveActive := snap.WALActiveSegment
		if snap.WALActiveSize == 0 && effectiveActive > 0 {
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
		snap.RaftMinLogSegment = minSeg
		snap.RaftMaxLogSegment = maxSeg
		snap.RaftMaxLagSegments = maxLag
		snap.RaftLaggingGroups = lagging
	}
	threshold := s.db.opt.RaftLagWarnSegments
	if threshold < 0 {
		threshold = 0
	}
	snap.RaftLagWarnThreshold = threshold
	if threshold > 0 && snap.RaftMaxLagSegments >= threshold && snap.RaftLaggingGroups > 0 {
		snap.RaftLagWarning = true
	}

	warning, reason := metrics.WALTypedWarning(snap.WALTypedRecordRatio, analysis.SegmentsWithRaft, s.db.opt.WALTypedRecordWarnRatio, s.db.opt.WALTypedRecordWarnSegments)
	if watchdog := s.db.walWatchdog; watchdog != nil {
		wsnap := watchdog.snapshot()
		snap.WALAutoGCRuns = wsnap.AutoRuns
		snap.WALAutoGCRemoved = wsnap.SegmentsRemoved
		snap.WALAutoGCLastUnix = wsnap.LastAutoUnix
		if wsnap.Warning {
			snap.WALTypedRecordWarning = true
			snap.WALTypedRecordReason = wsnap.WarningReason
		} else if warning {
			snap.WALTypedRecordWarning = true
			snap.WALTypedRecordReason = reason
		}
	} else if warning {
		snap.WALTypedRecordWarning = true
		snap.WALTypedRecordReason = reason
	}

	// Value log backlog.
	if s.db.vlog != nil {
		stats := s.db.vlog.metrics()
		snap.ValueLogSegments = stats.Segments
		snap.ValueLogPendingDel = stats.PendingDeletes
		snap.ValueLogDiscardQueue = stats.DiscardQueue
		snap.ValueLogHead = stats.Head
	}
	if s.db.orc != nil {
		tm := s.db.orc.txnMetricsSnapshot()
		snap.TxnsActive = tm.Active
		snap.TxnsStarted = tm.Started
		snap.TxnsCommitted = tm.Committed
		snap.TxnsConflicts = tm.Conflicts
	}
	if s.db != nil && s.db.hot != nil {
		topK := s.db.opt.HotRingTopK
		if topK <= 0 {
			topK = 16
		}
		for _, item := range s.db.hot.TopN(topK) {
			snap.HotKeys = append(snap.HotKeys, HotKeyStat{Key: item.Key, Count: item.Count})
		}
	}
	if s.db != nil && s.db.lsm != nil {
		cm := s.db.lsm.CacheMetrics()
		if total := cm.L0Hits + cm.L0Misses; total > 0 {
			snap.BlockL0HitRate = float64(cm.L0Hits) / float64(total)
		}
		if total := cm.L1Hits + cm.L1Misses; total > 0 {
			snap.BlockL1HitRate = float64(cm.L1Hits) / float64(total)
		}
		if total := cm.BloomHits + cm.BloomMisses; total > 0 {
			snap.BloomHitRate = float64(cm.BloomHits) / float64(total)
		}
		if total := cm.IndexHits + cm.IndexMisses; total > 0 {
			snap.IndexHitRate = float64(cm.IndexHits) / float64(total)
		}
	}
	if s.db != nil && s.db.iterPool != nil {
		snap.IteratorReused = s.db.iterPool.reused()
	}
	if s.db != nil && s.db.lsm != nil {
		snap.Entries = s.db.lsm.EntryCount()
		lastMs, maxMs, runs := s.db.lsm.CompactionDurations()
		snap.CompactionLastDurationMs = lastMs
		snap.CompactionMaxDurationMs = maxMs
		snap.CompactionRuns = runs
	}
	if s.db != nil {
		snap.ColumnFamilies = s.db.columnFamilyStats()
	}
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
