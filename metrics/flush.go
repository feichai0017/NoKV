package metrics

// FlushMetrics captures queue and timing stats for flush tasks.
type FlushMetrics struct {
	Pending       int64
	Queue         int64
	Active        int64
	WaitNs        int64
	WaitCount     int64
	WaitLastNs    int64
	WaitMaxNs     int64
	BuildNs       int64
	BuildCount    int64
	BuildLastNs   int64
	BuildMaxNs    int64
	ReleaseNs     int64
	ReleaseCount  int64
	ReleaseLastNs int64
	ReleaseMaxNs  int64
	Completed     int64
}
