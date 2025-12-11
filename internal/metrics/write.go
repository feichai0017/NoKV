package metrics

import "time"

// WriteMetrics aggregates queue and latency counters for the write pipeline.
type WriteMetrics struct {
	queueLen     int64
	queueEntries int64
	queueBytes   int64

	batchCount   int64
	batchEntries int64
	batchBytes   int64

	waitSumNs   int64
	waitSamples int64

	vlogSumNs   int64
	vlogSamples int64

	applySumNs   int64
	applySamples int64
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

func NewWriteMetrics() *WriteMetrics {
	return &WriteMetrics{}
}

func (m *WriteMetrics) UpdateQueue(len int, entries int, bytes int64) {
	if m == nil {
		return
	}
	m.queueLen = int64(len)
	m.queueEntries = int64(entries)
	m.queueBytes = bytes
}

func (m *WriteMetrics) RecordBatch(reqs int, entries int, size int64, waitSumNs int64) {
	if m == nil {
		return
	}
	m.batchCount += int64(reqs)
	m.batchEntries += int64(entries)
	m.batchBytes += size
	m.waitSumNs += waitSumNs
	m.waitSamples += int64(reqs)
}

func (m *WriteMetrics) RecordValueLog(d time.Duration) {
	if m == nil {
		return
	}
	m.vlogSumNs += d.Nanoseconds()
	m.vlogSamples++
}

func (m *WriteMetrics) RecordApply(d time.Duration) {
	if m == nil {
		return
	}
	m.applySumNs += d.Nanoseconds()
	m.applySamples++
}

func (m *WriteMetrics) Snapshot() WriteMetricsSnapshot {
	if m == nil {
		return WriteMetricsSnapshot{}
	}
	snap := WriteMetricsSnapshot{
		QueueLen:        m.queueLen,
		QueueEntries:    m.queueEntries,
		QueueBytes:      m.queueBytes,
		Batches:         m.batchCount,
		RequestSamples:  m.waitSamples,
		ValueLogSamples: m.vlogSamples,
		ApplySamples:    m.applySamples,
	}
	if m.batchCount > 0 {
		snap.AvgBatchEntries = float64(m.batchEntries) / float64(m.batchCount)
		snap.AvgBatchBytes = float64(m.batchBytes) / float64(m.batchCount)
	}
	if m.waitSamples > 0 {
		snap.AvgRequestWaitMs = float64(m.waitSumNs) / float64(m.waitSamples) / 1e6
	}
	if m.vlogSamples > 0 {
		snap.AvgValueLogMs = float64(m.vlogSumNs) / float64(m.vlogSamples) / 1e6
	}
	if m.applySamples > 0 {
		snap.AvgApplyMs = float64(m.applySumNs) / float64(m.applySamples) / 1e6
	}
	return snap
}
