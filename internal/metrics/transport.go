package metrics

// GRPCTransportMetrics captures gRPC transport watchdog counters exposed via expvar.
type GRPCTransportMetrics struct {
	DialsTotal               int64
	DialFailures             int64
	SendAttempts             int64
	SendSuccesses            int64
	SendFailures             int64
	Retries                  int64
	RetryExhausted           int64
	BlockedPeers             int64
	LastFailureUnix          int64
	WatchdogActive           bool
	WatchdogConsecutiveFails int64
	WatchdogThreshold        int64
	WatchdogReason           string
}
