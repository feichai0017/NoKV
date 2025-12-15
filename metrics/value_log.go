package metrics

import (
	"expvar"

	"github.com/feichai0017/NoKV/kv"
)

var (
	valueLogGCRuns          = expvar.NewInt("NoKV.ValueLog.GcRuns")
	valueLogSegmentsRemoved = expvar.NewInt("NoKV.ValueLog.SegmentsRemoved")
	valueLogHeadUpdates     = expvar.NewInt("NoKV.ValueLog.HeadUpdates")
)

// ValueLogMetrics captures backlog counters for the value log.
type ValueLogMetrics struct {
	Segments       int
	PendingDeletes int
	DiscardQueue   int
	Head           kv.ValuePtr
}

func IncValueLogGCRuns()          { valueLogGCRuns.Add(1) }
func IncValueLogSegmentsRemoved() { valueLogSegmentsRemoved.Add(1) }
func IncValueLogHeadUpdates()     { valueLogHeadUpdates.Add(1) }
