package metrics

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
)

// ValueLogMetrics captures backlog counters for the value log.
type ValueLogMetrics struct {
	Segments       int
	PendingDeletes int
	DiscardQueue   int
	Heads          map[uint32]kv.ValuePtr
}

// ValueLogGCSnapshot captures point-in-time value-log GC counters.
type ValueLogGCSnapshot struct {
	GCRuns          uint64 `json:"gc_runs"`
	SegmentsRemoved uint64 `json:"segments_removed"`
	HeadUpdates     uint64 `json:"head_updates"`
	GCActive        int64  `json:"gc_active"`
	GCScheduled     uint64 `json:"gc_scheduled"`
	GCThrottled     uint64 `json:"gc_throttled"`
	GCSkipped       uint64 `json:"gc_skipped"`
	GCRejected      uint64 `json:"gc_rejected"`
	GCParallelism   int64  `json:"gc_parallelism"`
}

// ValueLogGCCollector records value-log GC counters.
type ValueLogGCCollector struct {
	gcRuns          atomic.Uint64
	segmentsRemoved atomic.Uint64
	headUpdates     atomic.Uint64
	gcActive        atomic.Int64
	gcScheduled     atomic.Uint64
	gcThrottled     atomic.Uint64
	gcSkipped       atomic.Uint64
	gcRejected      atomic.Uint64
	gcParallelism   atomic.Int64
}

// NewValueLogGCCollector creates a new value for the API.
func NewValueLogGCCollector() *ValueLogGCCollector {
	return &ValueLogGCCollector{}
}

// Snapshot is part of the exported receiver API.
func (c *ValueLogGCCollector) Snapshot() ValueLogGCSnapshot {
	if c == nil {
		return ValueLogGCSnapshot{}
	}
	return ValueLogGCSnapshot{
		GCRuns:          c.gcRuns.Load(),
		SegmentsRemoved: c.segmentsRemoved.Load(),
		HeadUpdates:     c.headUpdates.Load(),
		GCActive:        c.gcActive.Load(),
		GCScheduled:     c.gcScheduled.Load(),
		GCThrottled:     c.gcThrottled.Load(),
		GCSkipped:       c.gcSkipped.Load(),
		GCRejected:      c.gcRejected.Load(),
		GCParallelism:   c.gcParallelism.Load(),
	}
}

// Reset is part of the exported receiver API.
func (c *ValueLogGCCollector) Reset() {
	if c == nil {
		return
	}
	c.gcRuns.Store(0)
	c.segmentsRemoved.Store(0)
	c.headUpdates.Store(0)
	c.gcActive.Store(0)
	c.gcScheduled.Store(0)
	c.gcThrottled.Store(0)
	c.gcSkipped.Store(0)
	c.gcRejected.Store(0)
	c.gcParallelism.Store(0)
}

// IncRuns is part of the exported receiver API.
func (c *ValueLogGCCollector) IncRuns() {
	if c != nil {
		c.gcRuns.Add(1)
	}
}

// IncSegmentsRemoved is part of the exported receiver API.
func (c *ValueLogGCCollector) IncSegmentsRemoved() {
	if c != nil {
		c.segmentsRemoved.Add(1)
	}
}

// IncHeadUpdates is part of the exported receiver API.
func (c *ValueLogGCCollector) IncHeadUpdates() {
	if c != nil {
		c.headUpdates.Add(1)
	}
}

// IncScheduled is part of the exported receiver API.
func (c *ValueLogGCCollector) IncScheduled() {
	if c != nil {
		c.gcScheduled.Add(1)
	}
}

// IncThrottled is part of the exported receiver API.
func (c *ValueLogGCCollector) IncThrottled() {
	if c != nil {
		c.gcThrottled.Add(1)
	}
}

// IncSkipped is part of the exported receiver API.
func (c *ValueLogGCCollector) IncSkipped() {
	if c != nil {
		c.gcSkipped.Add(1)
	}
}

// IncRejected is part of the exported receiver API.
func (c *ValueLogGCCollector) IncRejected() {
	if c != nil {
		c.gcRejected.Add(1)
	}
}

// IncActive is part of the exported receiver API.
func (c *ValueLogGCCollector) IncActive() {
	if c != nil {
		c.gcActive.Add(1)
	}
}

// DecActive is part of the exported receiver API.
func (c *ValueLogGCCollector) DecActive() {
	if c != nil {
		c.gcActive.Add(-1)
	}
}

// SetParallelism is part of the exported receiver API.
func (c *ValueLogGCCollector) SetParallelism(v int) {
	if c != nil {
		c.gcParallelism.Store(int64(v))
	}
}

var defaultValueLogGCCollector = NewValueLogGCCollector()

// DefaultValueLogGCCollector is part of the exported package API.
func DefaultValueLogGCCollector() *ValueLogGCCollector {
	return defaultValueLogGCCollector
}

// ResetValueLogGCMetricsForTesting is part of the exported package API.
func ResetValueLogGCMetricsForTesting() {
	defaultValueLogGCCollector.Reset()
}
