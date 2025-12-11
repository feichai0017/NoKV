package transport

import (
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/internal/metrics"
)

const (
	defaultGRPCWatchdogThreshold = int64(3)
)

// GRPCTransportMetrics captures gRPC transport watchdog counters exposed via expvar.
type GRPCTransportMetrics = metrics.GRPCTransportMetrics

type grpcMetricsRegistry struct {
	dials          *expvar.Int
	dialFailures   *expvar.Int
	sendAttempts   *expvar.Int
	sendSuccesses  *expvar.Int
	sendFailures   *expvar.Int
	retries        *expvar.Int
	retryExhausted *expvar.Int
	blockedPeers   *expvar.Int
	lastFailureTs  *expvar.Int
	watchdogActive *expvar.Int
	watchdogReason *expvar.String

	consecutiveFailures atomic.Int64
	threshold           atomic.Int64
	reason              atomic.Value
}

var (
	metricsOnce sync.Once
	metricsReg  *grpcMetricsRegistry
)

func grpcMetrics() *grpcMetricsRegistry {
	metricsOnce.Do(func() {
		metricsReg = &grpcMetricsRegistry{
			dials:          reuseInt("NoKV.Transport.GRPC.DialsTotal"),
			dialFailures:   reuseInt("NoKV.Transport.GRPC.DialsFailed"),
			sendAttempts:   reuseInt("NoKV.Transport.GRPC.SendsTotal"),
			sendSuccesses:  reuseInt("NoKV.Transport.GRPC.SendsSucceeded"),
			sendFailures:   reuseInt("NoKV.Transport.GRPC.SendsFailed"),
			retries:        reuseInt("NoKV.Transport.GRPC.RetriesTotal"),
			retryExhausted: reuseInt("NoKV.Transport.GRPC.RetriesExhausted"),
			blockedPeers:   reuseInt("NoKV.Transport.GRPC.BlockedPeers"),
			lastFailureTs:  reuseInt("NoKV.Transport.GRPC.LastFailureUnix"),
			watchdogActive: reuseInt("NoKV.Transport.GRPC.WatchdogActive"),
			watchdogReason: reuseString("NoKV.Transport.GRPC.WatchdogReason"),
		}
		metricsReg.threshold.Store(defaultGRPCWatchdogThreshold)
		metricsReg.reason.Store("")
	})
	return metricsReg
}

// GRPCMetricsSnapshot returns the current global gRPC transport metrics snapshot.
func GRPCMetricsSnapshot() GRPCTransportMetrics {
	reg := grpcMetrics()
	reason, _ := reg.reason.Load().(string)
	return GRPCTransportMetrics{
		DialsTotal:               reg.dials.Value(),
		DialFailures:             reg.dialFailures.Value(),
		SendAttempts:             reg.sendAttempts.Value(),
		SendSuccesses:            reg.sendSuccesses.Value(),
		SendFailures:             reg.sendFailures.Value(),
		Retries:                  reg.retries.Value(),
		RetryExhausted:           reg.retryExhausted.Value(),
		BlockedPeers:             reg.blockedPeers.Value(),
		LastFailureUnix:          reg.lastFailureTs.Value(),
		WatchdogActive:           reg.watchdogActive.Value() > 0,
		WatchdogConsecutiveFails: reg.consecutiveFailures.Load(),
		WatchdogThreshold:        reg.threshold.Load(),
		WatchdogReason:           reason,
	}
}

// ResetGRPCMetricsForTesting clears the global transport metrics. Intended for tests.
func ResetGRPCMetricsForTesting() {
	reg := grpcMetrics()
	reg.dials.Set(0)
	reg.dialFailures.Set(0)
	reg.sendAttempts.Set(0)
	reg.sendSuccesses.Set(0)
	reg.sendFailures.Set(0)
	reg.retries.Set(0)
	reg.retryExhausted.Set(0)
	reg.blockedPeers.Set(0)
	reg.lastFailureTs.Set(0)
	reg.watchdogActive.Set(0)
	reg.watchdogReason.Set("")
	reg.consecutiveFailures.Store(0)
	reg.threshold.Store(defaultGRPCWatchdogThreshold)
	reg.reason.Store("")
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
	m.lastFailureTs.Set(now)
	m.watchdogReason.Set(message)
	m.reason.Store(message)

	if threshold := m.threshold.Load(); threshold > 0 && consecutive >= threshold {
		m.watchdogActive.Set(1)
	}
}

func (m *grpcMetricsRegistry) recordSuccess() {
	if m.consecutiveFailures.Load() == 0 {
		return
	}
	m.consecutiveFailures.Store(0)
	m.watchdogActive.Set(0)
	m.watchdogReason.Set("")
	m.reason.Store("")
}

func reuseInt(name string) *expvar.Int {
	if v := expvar.Get(name); v != nil {
		if iv, ok := v.(*expvar.Int); ok {
			return iv
		}
	}
	return expvar.NewInt(name)
}

func reuseString(name string) *expvar.String {
	if v := expvar.Get(name); v != nil {
		if sv, ok := v.(*expvar.String); ok {
			return sv
		}
	}
	return expvar.NewString(name)
}
