package NoKV

import (
	"expvar"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

type Stats struct {
	db       *DB
	closer   *utils.Closer
	once     sync.Once
	interval time.Duration

	EntryNum int64 // Mirrors Entries for backwards compatibility.
	
	entries              *expvar.Int
	flushPending         *expvar.Int
	flushQueueLen        *expvar.Int
	flushActive          *expvar.Int
	flushWaitMs          *expvar.Float
	flushBuildMs         *expvar.Float
	flushReleaseMs       *expvar.Float
	flushCompleted       *expvar.Int
	compactionBacklog    *expvar.Int
	compactionMaxScore   *expvar.Float
	valueLogSegments     *expvar.Int
	valueLogPendingDel   *expvar.Int
	valueLogDiscardQueue *expvar.Int
	walActiveSegment     *expvar.Int
	walSegmentCount      *expvar.Int
	walSegmentsRemoved   *expvar.Int
	writeQueueDepth      *expvar.Int
	writeQueueEntries    *expvar.Int
	writeQueueBytes      *expvar.Int
	writeBatchAvgEntries *expvar.Float
	writeBatchAvgBytes   *expvar.Float
	writeRequestWaitMs   *expvar.Float
	writeValueLogMs      *expvar.Float
	writeApplyMs         *expvar.Float
	writeBatchesTotal    *expvar.Int
	writeThrottle        *expvar.Int
	txnActive            *expvar.Int
	txnStarted           *expvar.Int
	txnCommitted         *expvar.Int
	txnConflicts         *expvar.Int
	blockL0HitRate       *expvar.Float
	blockL1HitRate       *expvar.Float
	bloomHitRate         *expvar.Float
	iteratorReuses       *expvar.Int
}

type HotKeyStat struct {
	Key   string `json:"key"`
	Count int32  `json:"count"`
}

// StatsSnapshot captures a point-in-time view of internal backlog metrics.
type StatsSnapshot struct {
	Entries               int64          `json:"entries"`
	FlushPending          int64          `json:"flush_pending"`
	FlushQueueLength      int64          `json:"flush_queue_length"`
	FlushActive           int64          `json:"flush_active"`
	FlushWaitMs           float64        `json:"flush_wait_ms"`
	FlushBuildMs          float64        `json:"flush_build_ms"`
	FlushReleaseMs        float64        `json:"flush_release_ms"`
	FlushCompleted        int64          `json:"flush_completed"`
	CompactionBacklog     int64          `json:"compaction_backlog"`
	CompactionMaxScore    float64        `json:"compaction_max_score"`
	ValueLogSegments      int            `json:"vlog_segments"`
	ValueLogPendingDel    int            `json:"vlog_pending_deletes"`
	ValueLogDiscardQueue  int            `json:"vlog_discard_queue"`
	ValueLogHead          utils.ValuePtr `json:"vlog_head"`
	WALActiveSegment      int64          `json:"wal_active_segment"`
	WALSegmentCount       int64          `json:"wal_segment_count"`
	WALSegmentsRemoved    uint64         `json:"wal_segments_removed"`
	WriteQueueDepth       int64          `json:"write_queue_depth"`
	WriteQueueEntries     int64          `json:"write_queue_entries"`
	WriteQueueBytes       int64          `json:"write_queue_bytes"`
	WriteAvgBatchEntries  float64        `json:"write_avg_batch_entries"`
	WriteAvgBatchBytes    float64        `json:"write_avg_batch_bytes"`
	WriteAvgRequestWaitMs float64        `json:"write_avg_request_wait_ms"`
	WriteAvgValueLogMs    float64        `json:"write_avg_vlog_ms"`
	WriteAvgApplyMs       float64        `json:"write_avg_apply_ms"`
	WriteBatchesTotal     int64          `json:"write_batches_total"`
	WriteThrottleActive   bool           `json:"write_throttle_active"`
	TxnsActive            int64          `json:"txns_active"`
	TxnsStarted           uint64         `json:"txns_started"`
	TxnsCommitted         uint64         `json:"txns_committed"`
	TxnsConflicts         uint64         `json:"txns_conflicts"`
	HotKeys               []HotKeyStat   `json:"hot_keys,omitempty"`
	BlockL0HitRate        float64        `json:"block_l0_hit_rate"`
	BlockL1HitRate        float64        `json:"block_l1_hit_rate"`
	BloomHitRate          float64        `json:"bloom_hit_rate"`
	IteratorReused        uint64         `json:"iterator_reused"`
}

func newStats(db *DB) *Stats {
	s := &Stats{
		db:                   db,
		closer:               utils.NewCloser(),
		interval:             5 * time.Second,
		EntryNum:             0,
		entries:              reuseInt("NoKV.Stats.Entries"),
		flushPending:         reuseInt("NoKV.Stats.Flush.Pending"),
		flushQueueLen:        reuseInt("NoKV.Stats.Flush.QueueLength"),
		flushActive:          reuseInt("NoKV.Stats.Flush.Active"),
		flushWaitMs:          reuseFloat("NoKV.Stats.Flush.WaitMs"),
		flushBuildMs:         reuseFloat("NoKV.Stats.Flush.BuildMs"),
		flushReleaseMs:       reuseFloat("NoKV.Stats.Flush.ReleaseMs"),
		flushCompleted:       reuseInt("NoKV.Stats.Flush.Completed"),
		compactionBacklog:    reuseInt("NoKV.Stats.Compaction.Backlog"),
		compactionMaxScore:   reuseFloat("NoKV.Stats.Compaction.MaxScore"),
		valueLogSegments:     reuseInt("NoKV.Stats.ValueLog.Segments"),
		valueLogPendingDel:   reuseInt("NoKV.Stats.ValueLog.PendingDeletes"),
		valueLogDiscardQueue: reuseInt("NoKV.Stats.ValueLog.DiscardQueue"),
		walActiveSegment:     reuseInt("NoKV.Stats.WAL.ActiveSegment"),
		walSegmentCount:      reuseInt("NoKV.Stats.WAL.Segments"),
		walSegmentsRemoved:   reuseInt("NoKV.Stats.WAL.Removed"),
		writeQueueDepth:      reuseInt("NoKV.Stats.Write.QueueDepth"),
		writeQueueEntries:    reuseInt("NoKV.Stats.Write.QueueEntries"),
		writeQueueBytes:      reuseInt("NoKV.Stats.Write.QueueBytes"),
		writeBatchAvgEntries: reuseFloat("NoKV.Stats.Write.BatchAvgEntries"),
		writeBatchAvgBytes:   reuseFloat("NoKV.Stats.Write.BatchAvgBytes"),
		writeRequestWaitMs:   reuseFloat("NoKV.Stats.Write.RequestWaitMs"),
		writeValueLogMs:      reuseFloat("NoKV.Stats.Write.ValueLogMs"),
		writeApplyMs:         reuseFloat("NoKV.Stats.Write.ApplyMs"),
		writeBatchesTotal:    reuseInt("NoKV.Stats.Write.Batches"),
		writeThrottle:        reuseInt("NoKV.Stats.Write.Throttle"),
		txnActive:            reuseInt("NoKV.Txns.Active"),
		txnStarted:           reuseInt("NoKV.Txns.Started"),
		txnCommitted:         reuseInt("NoKV.Txns.Committed"),
		txnConflicts:         reuseInt("NoKV.Txns.Conflicts"),
		blockL0HitRate:       reuseFloat("NoKV.Stats.Cache.L0HitRate"),
		blockL1HitRate:       reuseFloat("NoKV.Stats.Cache.L1HitRate"),
		bloomHitRate:         reuseFloat("NoKV.Stats.Cache.BloomHitRate"),
		iteratorReuses:       reuseInt("NoKV.Stats.Iterator.Reused"),
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
	s.flushBuildMs.Set(snap.FlushBuildMs)
	s.flushReleaseMs.Set(snap.FlushReleaseMs)
	s.flushCompleted.Set(snap.FlushCompleted)
	s.compactionBacklog.Set(snap.CompactionBacklog)
	s.compactionMaxScore.Set(snap.CompactionMaxScore)
	s.valueLogSegments.Set(int64(snap.ValueLogSegments))
	s.valueLogPendingDel.Set(int64(snap.ValueLogPendingDel))
	s.valueLogDiscardQueue.Set(int64(snap.ValueLogDiscardQueue))
	s.walActiveSegment.Set(snap.WALActiveSegment)
	s.walSegmentCount.Set(snap.WALSegmentCount)
	s.walSegmentsRemoved.Set(int64(snap.WALSegmentsRemoved))
	s.writeQueueDepth.Set(snap.WriteQueueDepth)
	s.writeQueueEntries.Set(snap.WriteQueueEntries)
	s.writeQueueBytes.Set(snap.WriteQueueBytes)
	s.writeBatchAvgEntries.Set(snap.WriteAvgBatchEntries)
	s.writeBatchAvgBytes.Set(snap.WriteAvgBatchBytes)
	s.writeRequestWaitMs.Set(snap.WriteAvgRequestWaitMs)
	s.writeValueLogMs.Set(snap.WriteAvgValueLogMs)
	s.writeApplyMs.Set(snap.WriteAvgApplyMs)
	s.writeBatchesTotal.Set(snap.WriteBatchesTotal)
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
	s.iteratorReuses.Set(int64(snap.IteratorReused))
	atomic.StoreInt64(&s.EntryNum, snap.Entries)
}

// Snapshot returns a point-in-time metrics snapshot without mutating state.
func (s *Stats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	if s == nil || s.db == nil {
		return snap
	}

	// Flush backlog (pending flush tasks).
	if s.db.lsm != nil {
		fstats := s.db.lsm.FlushMetrics()
		snap.FlushPending = fstats.Pending
		snap.FlushQueueLength = fstats.Queue
		snap.FlushActive = fstats.Active
		if fstats.WaitCount > 0 {
			snap.FlushWaitMs = float64(fstats.WaitNs) / float64(fstats.WaitCount) / 1e6
		}
		if fstats.BuildCount > 0 {
			snap.FlushBuildMs = float64(fstats.BuildNs) / float64(fstats.BuildCount) / 1e6
		}
		if fstats.ReleaseCount > 0 {
			snap.FlushReleaseMs = float64(fstats.ReleaseNs) / float64(fstats.ReleaseCount) / 1e6
		}
		snap.FlushCompleted = fstats.Completed
		snap.CompactionBacklog, snap.CompactionMaxScore = s.db.lsm.CompactionStats()
	}

	if s.db.writeMetrics != nil {
		wsnap := s.db.writeMetrics.snapshot()
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

	if s.db.wal != nil {
		if wstats := s.db.wal.Metrics(); wstats != nil {
			snap.WALActiveSegment = int64(wstats.ActiveSegment)
			snap.WALSegmentCount = int64(wstats.SegmentCount)
			snap.WALSegmentsRemoved = wstats.RemovedSegments
		}
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
	}
	if s.db != nil && s.db.iterPool != nil {
		snap.IteratorReused = s.db.iterPool.reused()
	}
	if s.db != nil && s.db.lsm != nil {
		snap.Entries = s.db.lsm.EntryCount()
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
