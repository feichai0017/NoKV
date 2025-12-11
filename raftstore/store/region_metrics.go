package store

import "github.com/feichai0017/NoKV/internal/metrics"

// RegionMetrics records region counts grouped by lifecycle state. It is safe
// for concurrent use.
type RegionMetrics struct {
	inner *metrics.RegionMetrics
}

// RegionMetricsSnapshot captures point-in-time counts per region state.
type RegionMetricsSnapshot = metrics.RegionMetricsSnapshot

// NewRegionMetrics creates an empty recorder.
func NewRegionMetrics() *RegionMetrics {
	return &RegionMetrics{inner: metrics.NewRegionMetrics()}
}

// Hooks adapts the internal metrics hooks to the public RegionHooks type.
func (rm *RegionMetrics) Hooks() RegionHooks {
	if rm == nil || rm.inner == nil {
		return RegionHooks{}
	}
	h := rm.inner.Hooks()
	return RegionHooks{
		OnRegionUpdate: h.OnRegionUpdate,
		OnRegionRemove: h.OnRegionRemove,
	}
}

// Snapshot returns the current counts.
func (rm *RegionMetrics) Snapshot() RegionMetricsSnapshot {
	if rm == nil || rm.inner == nil {
		return RegionMetricsSnapshot{}
	}
	return rm.inner.Snapshot()
}
