package NoKV

import (
	"expvar"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

type Stats struct {
	db       *DB
	closer   *utils.Closer
	once     sync.Once
	interval time.Duration

	EntryNum int64 // TODO: wire real value stats.

	flushPending         *expvar.Int
	compactionBacklog    *expvar.Int
	compactionMaxScore   *expvar.Float
	valueLogSegments     *expvar.Int
	valueLogPendingDel   *expvar.Int
	valueLogDiscardQueue *expvar.Int
}

// StatsSnapshot captures a point-in-time view of internal backlog metrics.
type StatsSnapshot struct {
	FlushPending         int64          `json:"flush_pending"`
	CompactionBacklog    int64          `json:"compaction_backlog"`
	CompactionMaxScore   float64        `json:"compaction_max_score"`
	ValueLogSegments     int            `json:"vlog_segments"`
	ValueLogPendingDel   int            `json:"vlog_pending_deletes"`
	ValueLogDiscardQueue int            `json:"vlog_discard_queue"`
	ValueLogHead         utils.ValuePtr `json:"vlog_head"`
}

func newStats(db *DB) *Stats {
	return &Stats{
		db:                   db,
		closer:               utils.NewCloser(),
		interval:             5 * time.Second,
		EntryNum:             0,
		flushPending:         expvar.NewInt("NoKV.Stats.Flush.Pending"),
		compactionBacklog:    expvar.NewInt("NoKV.Stats.Compaction.Backlog"),
		compactionMaxScore:   expvar.NewFloat("NoKV.Stats.Compaction.MaxScore"),
		valueLogSegments:     expvar.NewInt("NoKV.Stats.ValueLog.Segments"),
		valueLogPendingDel:   expvar.NewInt("NoKV.Stats.ValueLog.PendingDeletes"),
		valueLogDiscardQueue: expvar.NewInt("NoKV.Stats.ValueLog.DiscardQueue"),
	}
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
	s.flushPending.Set(snap.FlushPending)
	s.compactionBacklog.Set(snap.CompactionBacklog)
	s.compactionMaxScore.Set(snap.CompactionMaxScore)
	s.valueLogSegments.Set(int64(snap.ValueLogSegments))
	s.valueLogPendingDel.Set(int64(snap.ValueLogPendingDel))
	s.valueLogDiscardQueue.Set(int64(snap.ValueLogDiscardQueue))
}

// Snapshot returns a point-in-time metrics snapshot without mutating state.
func (s *Stats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	if s == nil || s.db == nil {
		return snap
	}

	// Flush backlog (pending flush tasks).
	if s.db.lsm != nil {
		snap.FlushPending = s.db.lsm.FlushPending()
		snap.CompactionBacklog, snap.CompactionMaxScore = s.db.lsm.CompactionStats()
	}

	// Value log backlog.
	if s.db.vlog != nil {
		stats := s.db.vlog.metrics()
		snap.ValueLogSegments = stats.Segments
		snap.ValueLogPendingDel = stats.PendingDeletes
		snap.ValueLogDiscardQueue = stats.DiscardQueue
		snap.ValueLogHead = stats.Head
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
