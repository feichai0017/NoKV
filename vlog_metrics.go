package NoKV

import (
	"expvar"

	"github.com/feichai0017/NoKV/kv"
)

var (
	valueLogGCRuns          = expvar.NewInt("NoKV.ValueLog.GcRuns")
	valueLogSegmentsRemoved = expvar.NewInt("NoKV.ValueLog.SegmentsRemoved")
	valueLogHeadUpdates     = expvar.NewInt("NoKV.ValueLog.HeadUpdates")
)

type valueLogMetrics struct {
	Segments       int
	PendingDeletes int
	DiscardQueue   int
	Head           kv.ValuePtr
}

func (vlog *valueLog) metrics() valueLogMetrics {
	if vlog == nil || vlog.manager == nil {
		return valueLogMetrics{}
	}
	stats := valueLogMetrics{
		Segments: len(vlog.manager.ListFIDs()),
		Head:     vlog.manager.Head(),
	}

	if vlog.lfDiscardStats != nil {
		stats.DiscardQueue = len(vlog.lfDiscardStats.flushChan)
	}

	vlog.filesToDeleteLock.Lock()
	stats.PendingDeletes = len(vlog.filesToBeDeleted)
	vlog.filesToDeleteLock.Unlock()

	return stats
}
