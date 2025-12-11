package NoKV

import (
	"github.com/feichai0017/NoKV/internal/metrics"
)

func (vlog *valueLog) metrics() metrics.ValueLogMetrics {
	if vlog == nil || vlog.manager == nil {
		return metrics.ValueLogMetrics{}
	}
	stats := metrics.ValueLogMetrics{
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
