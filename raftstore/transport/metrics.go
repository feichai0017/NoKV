package transport

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/metrics"
)

const (
	defaultGRPCWatchdogThreshold = int64(3)
)

// GRPCTransportMetrics captures gRPC transport watchdog counters.
type GRPCTransportMetrics = metrics.GRPCTransportMetrics

type grpcMetricsRegistry struct {
	dials          atomic.Int64
	dialFailures   atomic.Int64
	sendAttempts   atomic.Int64
	sendSuccesses  atomic.Int64
	sendFailures   atomic.Int64
	retries        atomic.Int64
	retryExhausted atomic.Int64
	blockedPeers   atomic.Int64
	lastFailureTs  atomic.Int64
	watchdogActive atomic.Int64

	consecutiveFailures atomic.Int64
	threshold           atomic.Int64
	watchdogReason      atomic.Value // string
}

var (
	metricsOnce sync.Once
	metricsReg  *grpcMetricsRegistry
)

func grpcMetrics() *grpcMetricsRegistry {
	metricsOnce.Do(func() {
		metricsReg = &grpcMetricsRegistry{}
		metricsReg.threshold.Store(defaultGRPCWatchdogThreshold)
		metricsReg.watchdogReason.Store("")
	})
	return metricsReg
}

// GRPCMetricsSnapshot returns the current global gRPC transport metrics snapshot.
func GRPCMetricsSnapshot() GRPCTransportMetrics {
	reg := grpcMetrics()
	reason := ""
	if v, ok := reg.watchdogReason.Load().(string); ok {
		reason = v
	}
	return GRPCTransportMetrics{
		DialsTotal:               reg.dials.Load(),
		DialFailures:             reg.dialFailures.Load(),
		SendAttempts:             reg.sendAttempts.Load(),
		SendSuccesses:            reg.sendSuccesses.Load(),
		SendFailures:             reg.sendFailures.Load(),
		Retries:                  reg.retries.Load(),
		RetryExhausted:           reg.retryExhausted.Load(),
		BlockedPeers:             reg.blockedPeers.Load(),
		LastFailureUnix:          reg.lastFailureTs.Load(),
		WatchdogActive:           reg.watchdogActive.Load() > 0,
		WatchdogConsecutiveFails: reg.consecutiveFailures.Load(),
		WatchdogThreshold:        reg.threshold.Load(),
		WatchdogReason:           reason,
	}
}

// ResetGRPCMetricsForTesting clears the global transport metrics. Intended for tests.
func ResetGRPCMetricsForTesting() {
	reg := grpcMetrics()
	reg.dials.Store(0)
	reg.dialFailures.Store(0)
	reg.sendAttempts.Store(0)
	reg.sendSuccesses.Store(0)
	reg.sendFailures.Store(0)
	reg.retries.Store(0)
	reg.retryExhausted.Store(0)
	reg.blockedPeers.Store(0)
	reg.lastFailureTs.Store(0)
	reg.watchdogActive.Store(0)
	reg.consecutiveFailures.Store(0)
	reg.threshold.Store(defaultGRPCWatchdogThreshold)
	reg.watchdogReason.Store("")
}

// recordDialAttempt increments the dial attempt counter.
func (m *grpcMetricsRegistry) recordDialAttempt() {
	m.dials.Add(1)
}

// recordDialFailure adds a failed dial attempt and feeds the watchdog.
func (m *grpcMetricsRegistry) recordDialFailure(err error) {
	m.dialFailures.Add(1)
	m.recordFailure("dial", err)
}

// recordDialSuccess resets the watchdog after a successful dial.
func (m *grpcMetricsRegistry) recordDialSuccess() {
	m.recordSuccess()
}

// recordSendAttempt tracks send attempts and retries.
func (m *grpcMetricsRegistry) recordSendAttempt(isRetry bool) {
	m.sendAttempts.Add(1)
	if isRetry {
		m.retries.Add(1)
	}
}

// recordSendSuccess increments success counters and clears the watchdog.
func (m *grpcMetricsRegistry) recordSendSuccess() {
	m.sendSuccesses.Add(1)
	m.recordSuccess()
}

// recordSendFailure tracks send failures and optionally marks retry exhaustion.
func (m *grpcMetricsRegistry) recordSendFailure(err error, exhausted bool) {
	m.sendFailures.Add(1)
	if exhausted {
		m.retryExhausted.Add(1)
	}
	m.recordFailure("send", err)
}

// recordBlocked adjusts the blocked peer gauge by delta.
func (m *grpcMetricsRegistry) recordBlocked(delta int64) {
	if delta == 0 {
		return
	}
	m.blockedPeers.Add(delta)
}

func (m *grpcMetricsRegistry) recordFailure(kind string, err error) {
	now := time.Now().Unix()
	consecutive := m.consecutiveFailures.Add(1)
	message := fmt.Sprintf("%s failure #%d", kind, consecutive)
	if err != nil {
		message = fmt.Sprintf("%s failure #%d: %v", kind, consecutive, err)
	}
	m.lastFailureTs.Store(now)
	m.watchdogReason.Store(message)

	if threshold := m.threshold.Load(); threshold > 0 && consecutive >= threshold {
		m.watchdogActive.Store(1)
	}
}

func (m *grpcMetricsRegistry) recordSuccess() {
	if m.consecutiveFailures.Load() == 0 {
		return
	}
	m.consecutiveFailures.Store(0)
	m.watchdogActive.Store(0)
	m.watchdogReason.Store("")
}
