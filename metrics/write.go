package metrics

import (
	"sync/atomic"
	"time"
)

// WriteMetrics aggregates queue and latency counters for the write pipeline.
type WriteMetrics struct {
	queueLen     atomic.Int64
	queueEntries atomic.Int64
	queueBytes   atomic.Int64

	batchCount   atomic.Int64
	batchEntries atomic.Int64
	batchBytes   atomic.Int64

	waitSumNs   atomic.Int64
	waitSamples atomic.Int64

	vlogSumNs   atomic.Int64
	vlogSamples atomic.Int64

	applySumNs   atomic.Int64
	applySamples atomic.Int64
}

// WriteMetricsSnapshot is a read-only view of WriteMetrics counters.
type WriteMetricsSnapshot struct {
	QueueLen         int64
	QueueEntries     int64
	QueueBytes       int64
	Batches          int64
	AvgBatchEntries  float64
	AvgBatchBytes    float64
	AvgRequestWaitMs float64
	AvgValueLogMs    float64
	AvgApplyMs       float64
	RequestSamples   int64
	ValueLogSamples  int64
	ApplySamples     int64
}

// NewWriteMetrics creates a new value for the API.
func NewWriteMetrics() *WriteMetrics {
	return &WriteMetrics{}
}

// UpdateQueue is part of the exported receiver API.
func (m *WriteMetrics) UpdateQueue(len int, entries int, bytes int64) {
	if m == nil {
		return
	}
	m.queueLen.Store(int64(len))
	m.queueEntries.Store(int64(entries))
	m.queueBytes.Store(bytes)
}

// RecordBatch is part of the exported receiver API.
func (m *WriteMetrics) RecordBatch(reqs int, entries int, size int64, waitSumNs int64) {
	if m == nil {
		return
	}
	m.batchCount.Add(int64(reqs))
	m.batchEntries.Add(int64(entries))
	m.batchBytes.Add(size)
	m.waitSumNs.Add(waitSumNs)
	m.waitSamples.Add(int64(reqs))
}

// RecordValueLog is part of the exported receiver API.
func (m *WriteMetrics) RecordValueLog(d time.Duration) {
	if m == nil {
		return
	}
	m.vlogSumNs.Add(d.Nanoseconds())
	m.vlogSamples.Add(1)
}

// RecordApply is part of the exported receiver API.
func (m *WriteMetrics) RecordApply(d time.Duration) {
	if m == nil {
		return
	}
	m.applySumNs.Add(d.Nanoseconds())
	m.applySamples.Add(1)
}

// Snapshot is part of the exported receiver API.
func (m *WriteMetrics) Snapshot() WriteMetricsSnapshot {
	if m == nil {
		return WriteMetricsSnapshot{}
	}
	queueLen := m.queueLen.Load()
	queueEntries := m.queueEntries.Load()
	queueBytes := m.queueBytes.Load()
	batchCount := m.batchCount.Load()
	batchEntries := m.batchEntries.Load()
	batchBytes := m.batchBytes.Load()
	waitSumNs := m.waitSumNs.Load()
	waitSamples := m.waitSamples.Load()
	vlogSumNs := m.vlogSumNs.Load()
	vlogSamples := m.vlogSamples.Load()
	applySumNs := m.applySumNs.Load()
	applySamples := m.applySamples.Load()
	snap := WriteMetricsSnapshot{
		QueueLen:        queueLen,
		QueueEntries:    queueEntries,
		QueueBytes:      queueBytes,
		Batches:         batchCount,
		RequestSamples:  waitSamples,
		ValueLogSamples: vlogSamples,
		ApplySamples:    applySamples,
	}
	if batchCount > 0 {
		snap.AvgBatchEntries = float64(batchEntries) / float64(batchCount)
		snap.AvgBatchBytes = float64(batchBytes) / float64(batchCount)
	}
	if waitSamples > 0 {
		snap.AvgRequestWaitMs = float64(waitSumNs) / float64(waitSamples) / 1e6
	}
	if vlogSamples > 0 {
		snap.AvgValueLogMs = float64(vlogSumNs) / float64(vlogSamples) / 1e6
	}
	if applySamples > 0 {
		snap.AvgApplyMs = float64(applySumNs) / float64(applySamples) / 1e6
	}
	return snap
}
