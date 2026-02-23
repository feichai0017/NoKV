package metrics

// TxnMetrics captures MVCC transaction activity counters.
type TxnMetrics struct {
	Started   uint64
	Committed uint64
	Conflicts uint64
	Active    int64
}
