package metrics

import (
	"expvar"

	"github.com/feichai0017/NoKV/kv"
)

var (
	valueLogGCRuns          = expvar.NewInt("NoKV.ValueLog.GcRuns")
	valueLogSegmentsRemoved = expvar.NewInt("NoKV.ValueLog.SegmentsRemoved")
	valueLogHeadUpdates     = expvar.NewInt("NoKV.ValueLog.HeadUpdates")
	valueLogGCActive        = expvar.NewInt("NoKV.ValueLog.GcActive")
	valueLogGCScheduled     = expvar.NewInt("NoKV.ValueLog.GcScheduled")
	valueLogGCThrottled     = expvar.NewInt("NoKV.ValueLog.GcThrottled")
	valueLogGCSkipped       = expvar.NewInt("NoKV.ValueLog.GcSkipped")
	valueLogGCRejected      = expvar.NewInt("NoKV.ValueLog.GcRejected")
	valueLogGCParallelism   = expvar.NewInt("NoKV.ValueLog.GcParallelism")
)

// ValueLogMetrics captures backlog counters for the value log.
type ValueLogMetrics struct {
	Segments       int
	PendingDeletes int
	DiscardQueue   int
	Heads          map[uint32]kv.ValuePtr
}

func IncValueLogGCRuns()          { valueLogGCRuns.Add(1) }
func IncValueLogSegmentsRemoved() { valueLogSegmentsRemoved.Add(1) }
func IncValueLogHeadUpdates()     { valueLogHeadUpdates.Add(1) }
func IncValueLogGCScheduled()     { valueLogGCScheduled.Add(1) }
func IncValueLogGCThrottled()     { valueLogGCThrottled.Add(1) }
func IncValueLogGCSkipped()       { valueLogGCSkipped.Add(1) }
func IncValueLogGCRejected()      { valueLogGCRejected.Add(1) }
func IncValueLogGCActive()        { valueLogGCActive.Add(1) }
func DecValueLogGCActive()        { valueLogGCActive.Add(-1) }
func SetValueLogGCParallelism(v int) {
	valueLogGCParallelism.Set(int64(v))
}
