package metrics

import (
	"testing"
	"time"
)

func TestWriteMetricsSnapshot(t *testing.T) {
	m := NewWriteMetrics()
	m.UpdateQueue(5, 10, 1024)
	m.RecordBatch(2, 20, 2048, int64(3*time.Millisecond))
	m.RecordValueLog(5 * time.Millisecond)
	m.RecordApply(7 * time.Millisecond)

	snap := m.Snapshot()
	if snap.QueueLen != 5 || snap.QueueEntries != 10 || snap.QueueBytes != 1024 {
		t.Fatalf("queue snapshot mismatch: %+v", snap)
	}
	if snap.Batches != 2 || snap.AvgBatchEntries <= 0 || snap.AvgBatchBytes <= 0 {
		t.Fatalf("batch snapshot mismatch: %+v", snap)
	}
	if snap.AvgRequestWaitMs <= 0 || snap.AvgValueLogMs <= 0 || snap.AvgApplyMs <= 0 {
		t.Fatalf("timing averages missing: %+v", snap)
	}
}
