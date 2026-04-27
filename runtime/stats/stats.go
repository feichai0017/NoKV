// Package stats owns periodic runtime metric collection and snapshot
// publication for a NoKV.DB. The Stats type runs a small ticker that
// builds a StatsSnapshot from its Host and republishes it through expvar
// under "NoKV.Stats". The root NoKV package provides Stats with a Host
// implementation; tests construct mock Hosts directly.
package stats

import (
	"expvar"
	"maps"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	transportpkg "github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/feichai0017/NoKV/utils"
)

// LSMSource is the narrow LSM surface stats reads from. Implemented by
// engine/lsm.LSM.
type LSMSource interface {
	Diagnostics() lsm.Diagnostics
	ThrottlePressurePermille() uint32
	ThrottleRateBytesPerSec() uint64
}

// VlogSource is the narrow value-log surface stats reads from.
// Implemented by engine/vlog.Consumer.
type VlogSource interface {
	Metrics() metrics.ValueLogMetrics
}

// Host wires the Stats subsystem back into its DB host. Every accessor
// is read-only; Stats never mutates host state.
type Host interface {
	// Storage subsystems.
	LSM() LSMSource
	Vlog() VlogSource
	LSMWALs() []*wal.Manager
	// RaftWALsLocked invokes fn while holding the host's raft-WAL mutex.
	// Stats only iterates the slice while the lock is held.
	RaftWALsLocked(fn func(wals []*wal.Manager))
	BackgroundWatchdogs() []*wal.Watchdog
	HotWrite() *thermos.RotatingThermos
	IteratorReused() uint64
	WriteMetrics() *metrics.WriteMetrics

	// Atomic indicators of write throttling state.
	BlockWritesActive() bool
	SlowWritesActive() bool
	HotWriteLimited() uint64

	// ValueLogDisabledOrphans returns the number of value-log segments
	// the manifest still references when EnableValueLog=false. Zero
	// when vlog is enabled, or when there is nothing to migrate. The
	// stats subsystem surfaces this only when Vlog() returns nil.
	ValueLogDisabledOrphans() int

	// Options-snapshot accessors.
	RaftLagWarnSegments() int64
	WALTypedRecordWarnRatio() float64
	WALTypedRecordWarnSegments() int64
	ThermosTopK() int
	RaftPointerSnapshot() func() map[uint64]localmeta.RaftLogPointer
}

// Stats owns periodic runtime metric collection and snapshot publication.
type Stats struct {
	host     Host
	closer   *utils.Closer
	once     sync.Once
	interval time.Duration

	regionMetrics atomic.Pointer[metrics.RegionMetrics]
}

var (
	statsExpvarOnce       sync.Once
	exportedStatsSnapshot atomic.Pointer[StatsSnapshot]
)

// HotKeyStat represents one hot key and its observed touch count.
type HotKeyStat struct {
	Key   string `json:"key"`
	Count int32  `json:"count"`
}

// LSMLevelStats captures aggregated metrics per LSM level.
type LSMLevelStats struct {
	Level                       int     `json:"level"`
	TableCount                  int     `json:"tables"`
	SizeBytes                   int64   `json:"size_bytes"`
	ValueBytes                  int64   `json:"value_bytes"`
	StaleBytes                  int64   `json:"stale_bytes"`
	StagingTables               int     `json:"staging_tables"`
	StagingSizeBytes            int64   `json:"staging_size_bytes"`
	StagingValueBytes           int64   `json:"staging_value_bytes"`
	ValueDensity                float64 `json:"value_density"`
	StagingValueDensity         float64 `json:"staging_value_density"`
	StagingRuns                 int64   `json:"staging_runs"`
	StagingMs                   float64 `json:"staging_ms"`
	StagingTablesCompactedCount int64   `json:"staging_tables_compacted"`
	MergeRuns                   int64   `json:"staging_merge_runs"`
	MergeMs                     float64 `json:"staging_merge_ms"`
	MergeTables                 int64   `json:"staging_merge_tables"`
}

func levelMetricsToStats(lvl metrics.LevelMetrics) LSMLevelStats {
	return LSMLevelStats{
		Level:                       lvl.Level,
		TableCount:                  lvl.TableCount,
		SizeBytes:                   lvl.SizeBytes,
		ValueBytes:                  lvl.ValueBytes,
		StaleBytes:                  lvl.StaleBytes,
		StagingTables:               lvl.StagingTableCount,
		StagingSizeBytes:            lvl.StagingSizeBytes,
		StagingValueBytes:           lvl.StagingValueBytes,
		ValueDensity:                lvl.ValueDensity,
		StagingValueDensity:         lvl.StagingValueDensity,
		StagingRuns:                 lvl.StagingRuns,
		StagingMs:                   lvl.StagingMs,
		StagingTablesCompactedCount: lvl.StagingTablesCompacted,
		MergeRuns:                   lvl.StagingMergeRuns,
		MergeMs:                     lvl.StagingMergeMs,
		MergeTables:                 lvl.StagingMergeTables,
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
	Region     RegionStatsSnapshot               `json:"region"`
	Hot        HotStatsSnapshot                  `json:"hot"`
	Cache      CacheStatsSnapshot                `json:"cache"`
	LSM        LSMStatsSnapshot                  `json:"lsm"`
	Transport  transportpkg.GRPCTransportMetrics `json:"transport"`
	Redis      metrics.RedisSnapshot             `json:"redis"`
}

// FlushStatsSnapshot summarizes flush queue depth and stage timing.
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

// CompactionStatsSnapshot summarizes compaction backlog, runtime, and staging behavior.
type CompactionStatsSnapshot struct {
	Backlog              int64   `json:"backlog"`
	MaxScore             float64 `json:"max_score"`
	LastDurationMs       float64 `json:"last_duration_ms"`
	MaxDurationMs        float64 `json:"max_duration_ms"`
	Runs                 uint64  `json:"runs"`
	StagingRuns          int64   `json:"staging_runs"`
	MergeRuns            int64   `json:"staging_merge_runs"`
	StagingMs            float64 `json:"staging_ms"`
	MergeMs              float64 `json:"staging_merge_ms"`
	StagingTables        int64   `json:"staging_tables"`
	MergeTables          int64   `json:"staging_merge_tables"`
	ValueWeight          float64 `json:"value_weight"`
	ValueWeightSuggested float64 `json:"value_weight_suggested,omitempty"`
}

// ValueLogStatsSnapshot reports value-log segment status and GC counters.
type ValueLogStatsSnapshot struct {
	Segments       int                        `json:"segments"`
	PendingDeletes int                        `json:"pending_deletes"`
	DiscardQueue   int                        `json:"discard_queue"`
	Heads          map[uint32]kv.ValuePtr     `json:"heads,omitempty"`
	GC             metrics.ValueLogGCSnapshot `json:"gc"`
	// DisabledOrphans is non-zero when EnableValueLog=false but the
	// manifest still references that many value-log segments — every
	// Get/iterator hit on a value pointer will fail until the operator
	// either re-enables EnableValueLog or migrates values out of vlog.
	DisabledOrphans int `json:"disabled_orphans,omitempty"`
}

// WALStatsSnapshot captures WAL head position, record mix, and watchdog status.
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

// RaftStatsSnapshot summarizes raft log lag across tracked groups.
type RaftStatsSnapshot struct {
	GroupCount       int    `json:"group_count"`
	LaggingGroups    int    `json:"lagging_groups"`
	MinLogSegment    uint32 `json:"min_log_segment"`
	MaxLogSegment    uint32 `json:"max_log_segment"`
	MaxLagSegments   int64  `json:"max_lag_segments"`
	LagWarnThreshold int64  `json:"lag_warn_threshold"`
	LagWarning       bool   `json:"lag_warning"`
}

// WriteStatsSnapshot tracks write-path queue pressure, latency, and throttling.
type WriteStatsSnapshot struct {
	QueueDepth       int64   `json:"queue_depth"`
	QueueEntries     int64   `json:"queue_entries"`
	QueueBytes       int64   `json:"queue_bytes"`
	AvgBatchEntries  float64 `json:"avg_batch_entries"`
	AvgBatchBytes    float64 `json:"avg_batch_bytes"`
	AvgRequestWaitMs float64 `json:"avg_request_wait_ms"`
	AvgValueLogMs    float64 `json:"avg_vlog_ms"`
	AvgApplyMs       float64 `json:"avg_apply_ms"`
	AvgSyncMs        float64 `json:"avg_sync_ms"`
	AvgSyncBatch     float64 `json:"avg_sync_batch"`
	SyncCount        int64   `json:"sync_count"`
	BatchesTotal     int64   `json:"batches_total"`
	ThrottleActive   bool    `json:"throttle_active"`
	SlowdownActive   bool    `json:"slowdown_active"`
	ThrottleMode     string  `json:"throttle_mode"`
	ThrottlePressure uint32  `json:"throttle_pressure_permille"`
	ThrottleRate     uint64  `json:"throttle_rate_bytes_per_sec"`
	HotKeyLimited    uint64  `json:"hot_key_limited"`
}

// RegionStatsSnapshot reports region counts grouped by region state.
type RegionStatsSnapshot struct {
	Total     int64 `json:"total"`
	New       int64 `json:"new"`
	Running   int64 `json:"running"`
	Removing  int64 `json:"removing"`
	Tombstone int64 `json:"tombstone"`
	Other     int64 `json:"other"`
}

// HotStatsSnapshot contains write-hot keys and optional ring internals.
type HotStatsSnapshot struct {
	WriteKeys []HotKeyStat   `json:"write_keys,omitempty"`
	WriteRing *thermos.Stats `json:"write_ring,omitempty"`
}

// CacheStatsSnapshot captures block/index/bloom hit-rate indicators.
type CacheStatsSnapshot struct {
	BlockL0HitRate float64 `json:"block_l0_hit_rate"`
	BlockL1HitRate float64 `json:"block_l1_hit_rate"`
	IndexHitRate   float64 `json:"index_hit_rate"`
	IteratorReused uint64  `json:"iterator_reused"`
}

// LSMStatsSnapshot summarizes per-level storage shape and value-density signals.
type LSMStatsSnapshot struct {
	Levels            []LSMLevelStats          `json:"levels,omitempty"`
	ValueBytesTotal   int64                    `json:"value_bytes_total"`
	ValueDensityMax   float64                  `json:"value_density_max"`
	ValueDensityAlert bool                     `json:"value_density_alert"`
	RangeFilter       RangeFilterStatsSnapshot `json:"range_filter"`
}

// RangeFilterStatsSnapshot summarizes range-filter pruning activity on read paths.
type RangeFilterStatsSnapshot struct {
	PointCandidates   uint64 `json:"point_candidates"`
	PointPruned       uint64 `json:"point_pruned"`
	BoundedCandidates uint64 `json:"bounded_candidates"`
	BoundedPruned     uint64 `json:"bounded_pruned"`
	Fallbacks         uint64 `json:"fallbacks"`
}

// New constructs a Stats wired to host. interval defaults to 5s when 0.
func New(host Host, interval time.Duration) *Stats {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	s := &Stats{
		host:     host,
		closer:   utils.NewCloser(),
		interval: interval,
	}
	statsExpvarOnce.Do(func() {
		expvar.Publish("NoKV.Stats", expvar.Func(func() any {
			if ptr := exportedStatsSnapshot.Load(); ptr != nil {
				return *ptr
			}
			return StatsSnapshot{}
		}))
	})
	return s
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

// Close stops the stats loop.
func (s *Stats) Close() error {
	if s == nil {
		return nil
	}
	s.closer.Close()
	return nil
}

// SetRegionMetrics attaches region metrics recorder used in snapshots.
func (s *Stats) SetRegionMetrics(rm *metrics.RegionMetrics) {
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
	s.Collect()

	for {
		select {
		case <-ticker.C:
			s.Collect()
		case <-s.closer.Closed():
			return
		}
	}
}

// Collect snapshots background queues and propagates them to expvar.
func (s *Stats) Collect() {
	if s == nil {
		return
	}
	snap := s.Snapshot()
	exportedStatsSnapshot.Store(&snap)
}

// Snapshot returns a point-in-time metrics snapshot without mutating state.
func (s *Stats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	if s == nil || s.host == nil {
		return snap
	}

	if thresh := s.host.RaftLagWarnSegments(); thresh > 0 {
		snap.Raft.LagWarnThreshold = thresh
	}

	// Flush backlog and LSM diagnostics.
	if lsmSrc := s.host.LSM(); lsmSrc != nil {
		diag := lsmSrc.Diagnostics()
		snap.Compaction.ValueWeight = diag.Compaction.ValueWeight
		alertThreshold := diag.Compaction.AlertThreshold
		fstats := diag.Flush
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
		snap.Compaction.Backlog = diag.Compaction.Backlog
		snap.Compaction.MaxScore = diag.Compaction.MaxScore
		if levels := diag.Levels; len(levels) > 0 {
			snap.LSM.Levels = make([]LSMLevelStats, 0, len(levels))
			var maxDensity float64
			var stagingRuns, stagingMergeRuns int64
			var stagingMs, stagingMergeMs float64
			var stagingTables, stagingMergeTables int64
			for _, lvl := range levels {
				statsLvl := levelMetricsToStats(lvl)
				snap.LSM.Levels = append(snap.LSM.Levels, statsLvl)
				if statsLvl.ValueDensity > maxDensity {
					maxDensity = statsLvl.ValueDensity
				}
				if statsLvl.StagingValueDensity > maxDensity {
					maxDensity = statsLvl.StagingValueDensity
				}
				stagingRuns += statsLvl.StagingRuns
				stagingMergeRuns += statsLvl.MergeRuns
				stagingMs += statsLvl.StagingMs
				stagingMergeMs += statsLvl.MergeMs
				stagingTables += statsLvl.StagingTablesCompactedCount
				stagingMergeTables += statsLvl.MergeTables
			}
			snap.Compaction.StagingRuns = stagingRuns
			snap.Compaction.MergeRuns = stagingMergeRuns
			snap.Compaction.StagingMs = stagingMs
			snap.Compaction.MergeMs = stagingMergeMs
			snap.Compaction.StagingTables = stagingTables
			snap.Compaction.MergeTables = stagingMergeTables
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
		if len(snap.LSM.Levels) > 0 {
			var totalValue int64
			for _, lvl := range snap.LSM.Levels {
				totalValue += lvl.ValueBytes + lvl.StagingValueBytes
			}
			snap.LSM.ValueBytesTotal = totalValue
		}
		snap.LSM.RangeFilter = RangeFilterStatsSnapshot{
			PointCandidates:   diag.RangeFilter.PointCandidates,
			PointPruned:       diag.RangeFilter.PointPruned,
			BoundedCandidates: diag.RangeFilter.BoundedCandidates,
			BoundedPruned:     diag.RangeFilter.BoundedPruned,
			Fallbacks:         diag.RangeFilter.Fallbacks,
		}
		snap.Entries = diag.Entries
		snap.Compaction.LastDurationMs = diag.Compaction.LastDurationMs
		snap.Compaction.MaxDurationMs = diag.Compaction.MaxDurationMs
		snap.Compaction.Runs = diag.Compaction.Runs
		cm := diag.Cache
		if total := cm.L0Hits + cm.L0Misses; total > 0 {
			snap.Cache.BlockL0HitRate = float64(cm.L0Hits) / float64(total)
		}
		if total := cm.L1Hits + cm.L1Misses; total > 0 {
			snap.Cache.BlockL1HitRate = float64(cm.L1Hits) / float64(total)
		}
		if total := cm.IndexHits + cm.IndexMisses; total > 0 {
			snap.Cache.IndexHitRate = float64(cm.IndexHits) / float64(total)
		}
		snap.Write.ThrottlePressure = lsmSrc.ThrottlePressurePermille()
		snap.Write.ThrottleRate = lsmSrc.ThrottleRateBytesPerSec()
	}

	if wm := s.host.WriteMetrics(); wm != nil {
		wsnap := wm.Snapshot()
		snap.Write.QueueDepth = wsnap.QueueLen
		snap.Write.QueueEntries = wsnap.QueueEntries
		snap.Write.QueueBytes = wsnap.QueueBytes
		snap.Write.AvgBatchEntries = wsnap.AvgBatchEntries
		snap.Write.AvgBatchBytes = wsnap.AvgBatchBytes
		snap.Write.AvgRequestWaitMs = wsnap.AvgRequestWaitMs
		snap.Write.AvgValueLogMs = wsnap.AvgValueLogMs
		snap.Write.AvgApplyMs = wsnap.AvgApplyMs
		snap.Write.AvgSyncMs = wsnap.AvgSyncMs
		snap.Write.AvgSyncBatch = wsnap.AvgSyncBatch
		snap.Write.SyncCount = wsnap.SyncSamples
		snap.Write.BatchesTotal = wsnap.Batches
	}
	stopActive := s.host.BlockWritesActive()
	slowActive := s.host.SlowWritesActive()
	snap.Write.ThrottleActive = stopActive || slowActive
	snap.Write.SlowdownActive = slowActive
	switch {
	case stopActive:
		snap.Write.ThrottleMode = "stop"
	case slowActive:
		snap.Write.ThrottleMode = "slowdown"
	default:
		snap.Write.ThrottleMode = "none"
	}
	if stopActive && snap.Write.ThrottlePressure == 0 {
		snap.Write.ThrottlePressure = 1000
	} else if slowActive && snap.Write.ThrottlePressure == 0 {
		snap.Write.ThrottlePressure = 1
	}
	snap.Write.HotKeyLimited = s.host.HotWriteLimited()

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
		ptrs           map[uint64]localmeta.RaftLogPointer
	)
	// Aggregate metrics across every LSM data-plane WAL shard. Each
	// shard owns its own fd / fsync worker, so we sum SegmentCount /
	// RecordCounts and take the highest ActiveSegment as a coarse
	// health signal. Per-segment metrics are unioned (segment IDs are
	// allocated from a single global counter so they never collide
	// across shards).
	var aggregated wal.Metrics
	var anyShardStats bool
	for _, mgr := range s.host.LSMWALs() {
		if mgr == nil {
			continue
		}
		shardStats := mgr.Metrics()
		if shardStats != nil {
			anyShardStats = true
			if shardStats.ActiveSegment > aggregated.ActiveSegment {
				aggregated.ActiveSegment = shardStats.ActiveSegment
			}
			aggregated.ActiveSize += shardStats.ActiveSize
			aggregated.SegmentCount += shardStats.SegmentCount
			aggregated.RemovedSegments += shardStats.RemovedSegments
			aggregated.SegmentsWithRaftRecords += shardStats.SegmentsWithRaftRecords
			aggregated.RecordCounts.Entries += shardStats.RecordCounts.Entries
			aggregated.RecordCounts.RaftEntries += shardStats.RecordCounts.RaftEntries
			aggregated.RecordCounts.RaftStates += shardStats.RecordCounts.RaftStates
			aggregated.RecordCounts.RaftSnapshots += shardStats.RecordCounts.RaftSnapshots
			aggregated.RecordCounts.Other += shardStats.RecordCounts.Other
		}
		shardSegments := mgr.SegmentMetrics()
		if segmentMetrics == nil {
			segmentMetrics = shardSegments
		} else {
			maps.Copy(segmentMetrics, shardSegments)
		}
	}
	if anyShardStats {
		wstats = &aggregated
		snap.WAL.ActiveSegment = int64(aggregated.ActiveSegment)
		snap.WAL.ActiveSize = aggregated.ActiveSize
		snap.WAL.SegmentCount = int64(aggregated.SegmentCount)
		snap.WAL.SegmentsRemoved = aggregated.RemovedSegments
	}
	if ptrFn := s.host.RaftPointerSnapshot(); ptrFn != nil {
		ptrs = ptrFn()
		snap.Raft.GroupCount = len(ptrs)
	}

	analysis := metrics.AnalyzeWALBacklog(wstats, segmentMetrics)
	snap.WAL.RecordCounts = analysis.RecordCounts
	snap.WAL.SegmentsWithRaftRecords = analysis.SegmentsWithRaft
	removableRaftSegments := 0
	for _, id := range analysis.RemovableSegments {
		if segmentMetrics[id].RaftRecords() > 0 && wstats != nil && id < wstats.ActiveSegment {
			removableRaftSegments++
		}
	}
	s.host.RaftWALsLocked(func(wals []*wal.Manager) {
		for _, mgr := range wals {
			if mgr == nil {
				continue
			}
			shardStats := mgr.Metrics()
			shardSegments := mgr.SegmentMetrics()
			shardAnalysis := metrics.AnalyzeWALBacklog(shardStats, shardSegments)
			snap.WAL.RecordCounts.Entries += shardAnalysis.RecordCounts.Entries
			snap.WAL.RecordCounts.RaftEntries += shardAnalysis.RecordCounts.RaftEntries
			snap.WAL.RecordCounts.RaftStates += shardAnalysis.RecordCounts.RaftStates
			snap.WAL.RecordCounts.RaftSnapshots += shardAnalysis.RecordCounts.RaftSnapshots
			snap.WAL.RecordCounts.Other += shardAnalysis.RecordCounts.Other
			snap.WAL.SegmentsWithRaftRecords += shardAnalysis.SegmentsWithRaft
			if shardStats != nil {
				snap.WAL.SegmentCount += int64(shardStats.SegmentCount)
				snap.WAL.SegmentsRemoved += shardStats.RemovedSegments
			}
			for _, id := range shardAnalysis.RemovableSegments {
				if shardSegments[id].RaftRecords() > 0 && shardStats != nil && id < shardStats.ActiveSegment {
					removableRaftSegments++
				}
			}
		}
	})
	snap.WAL.RemovableRaftSegments = removableRaftSegments
	if total := snap.WAL.RecordCounts.Total(); total > 0 {
		raftRecords := snap.WAL.RecordCounts.RaftRecords()
		snap.WAL.TypedRecordRatio = float64(raftRecords) / float64(total)
	}

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
				lag := max(effectiveActive-int64(ptr.Segment), 0)
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
	threshold := max(s.host.RaftLagWarnSegments(), 0)
	snap.Raft.LagWarnThreshold = threshold
	if threshold > 0 && snap.Raft.MaxLagSegments >= threshold && snap.Raft.LaggingGroups > 0 {
		snap.Raft.LagWarning = true
	}

	warning, reason := metrics.WALTypedWarning(snap.WAL.TypedRecordRatio, snap.WAL.SegmentsWithRaftRecords, s.host.WALTypedRecordWarnRatio(), s.host.WALTypedRecordWarnSegments())
	watchdogs := s.host.BackgroundWatchdogs()
	if len(watchdogs) > 0 {
		var anyWarn bool
		var warnReason string
		for _, watchdog := range watchdogs {
			if watchdog == nil {
				continue
			}
			wsnap := watchdog.Snapshot()
			snap.WAL.AutoGCRuns += wsnap.AutoRuns
			snap.WAL.AutoGCRemoved += wsnap.SegmentsRemoved
			if wsnap.LastAutoUnix > snap.WAL.AutoGCLastUnix {
				snap.WAL.AutoGCLastUnix = wsnap.LastAutoUnix
			}
			if wsnap.Warning && !anyWarn {
				anyWarn = true
				warnReason = wsnap.WarningReason
			}
		}
		if anyWarn {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = warnReason
		} else if warning {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = reason
		}
	} else if warning {
		snap.WAL.TypedRecordWarning = true
		snap.WAL.TypedRecordReason = reason
	}

	// Value log backlog.
	if vlogSrc := s.host.Vlog(); vlogSrc != nil {
		stats := vlogSrc.Metrics()
		snap.ValueLog.Segments = stats.Segments
		snap.ValueLog.PendingDeletes = stats.PendingDeletes
		snap.ValueLog.DiscardQueue = stats.DiscardQueue
		snap.ValueLog.Heads = stats.Heads
	} else {
		// EnableValueLog=false. If the manifest still references
		// value-log segments, surface the orphan count so operators
		// see the mismatch in stats output without waiting for a
		// failing read to surface it.
		snap.ValueLog.DisabledOrphans = s.host.ValueLogDisabledOrphans()
	}
	if hot := s.host.HotWrite(); hot != nil {
		topK := s.host.ThermosTopK()
		for _, item := range hot.TopN(topK) {
			snap.Hot.WriteKeys = append(snap.Hot.WriteKeys, HotKeyStat{Key: item.Key, Count: item.Count})
		}
		hotStats := hot.Stats()
		snap.Hot.WriteRing = &hotStats
	}
	snap.Cache.IteratorReused = s.host.IteratorReused()
	snap.ValueLog.GC = metrics.DefaultValueLogGCCollector().Snapshot()
	snap.Transport = transportpkg.GRPCMetricsSnapshot()
	snap.Redis = metrics.DefaultRedisSnapshot()
	return snap
}
