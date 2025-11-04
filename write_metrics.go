package NoKV

import (
	"sync/atomic"
	"time"
)

type writeMetrics struct {
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

func newWriteMetrics() *writeMetrics {
	return &writeMetrics{}
}

func (m *writeMetrics) updateQueue(len int, entries int, bytes int64) {
	atomic.StoreInt64(&m.queueLen, int64(len))
	atomic.StoreInt64(&m.queueEntries, int64(entries))
	atomic.StoreInt64(&m.queueBytes, bytes)
}

func (m *writeMetrics) recordBatch(reqs int, entries int, size int64, waitSumNs int64) {
	atomic.AddInt64(&m.batchCount, 1)
	atomic.AddInt64(&m.batchEntries, int64(entries))
	atomic.AddInt64(&m.batchBytes, size)
	if reqs > 0 {
		atomic.AddInt64(&m.waitSumNs, waitSumNs)
		atomic.AddInt64(&m.waitSamples, int64(reqs))
	}
}

func (m *writeMetrics) recordValueLog(d time.Duration) {
	atomic.AddInt64(&m.vlogSumNs, d.Nanoseconds())
	atomic.AddInt64(&m.vlogSamples, 1)
}

func (m *writeMetrics) recordApply(d time.Duration) {
	atomic.AddInt64(&m.applySumNs, d.Nanoseconds())
	atomic.AddInt64(&m.applySamples, 1)
}

func (m *writeMetrics) snapshot() WriteMetricsSnapshot {
	batches := atomic.LoadInt64(&m.batchCount)
	batchEntries := atomic.LoadInt64(&m.batchEntries)
	batchBytes := atomic.LoadInt64(&m.batchBytes)
	waitSamples := atomic.LoadInt64(&m.waitSamples)
	waitSum := atomic.LoadInt64(&m.waitSumNs)
	vlogSamples := atomic.LoadInt64(&m.vlogSamples)
	vlogSum := atomic.LoadInt64(&m.vlogSumNs)
	applySamples := atomic.LoadInt64(&m.applySamples)
	applySum := atomic.LoadInt64(&m.applySumNs)

	snap := WriteMetricsSnapshot{
		QueueLen:        atomic.LoadInt64(&m.queueLen),
		QueueEntries:    atomic.LoadInt64(&m.queueEntries),
		QueueBytes:      atomic.LoadInt64(&m.queueBytes),
		Batches:         batches,
		RequestSamples:  waitSamples,
		ValueLogSamples: vlogSamples,
		ApplySamples:    applySamples,
	}

	if batches > 0 {
		snap.AvgBatchEntries = float64(batchEntries) / float64(batches)
		snap.AvgBatchBytes = float64(batchBytes) / float64(batches)
	}
	if waitSamples > 0 {
		snap.AvgRequestWaitMs = float64(waitSum) / float64(waitSamples) / 1e6
	}
	if vlogSamples > 0 {
		snap.AvgValueLogMs = float64(vlogSum) / float64(vlogSamples) / 1e6
	}
	if applySamples > 0 {
		snap.AvgApplyMs = float64(applySum) / float64(applySamples) / 1e6
	}
	return snap
}
