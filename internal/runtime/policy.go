package runtime

import (
	"math"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/hotring"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// HotWriteConfig captures the runtime hot-key tracking knobs consumed by the
// DB write path without depending on the root Options type.
type HotWriteConfig struct {
	Enabled          bool
	Bits             uint8
	WindowSlots      int
	WindowSlotPeriod time.Duration
	DecayInterval    time.Duration
	DecayShift       uint32
	NodeCap          uint64
	NodeSampleBits   uint8
	RotationInterval time.Duration
}

// NewHotWriteRing builds the optional hot-key tracker used by the DB write
// runtime. A nil ring means the feature is disabled.
func NewHotWriteRing(cfg HotWriteConfig) *hotring.RotatingHotRing {
	if !cfg.Enabled {
		return nil
	}
	ring := hotring.NewRotatingHotRing(cfg.Bits, nil)
	if cfg.WindowSlots > 0 && cfg.WindowSlotPeriod > 0 {
		ring.EnableSlidingWindow(cfg.WindowSlots, cfg.WindowSlotPeriod)
	}
	if cfg.DecayInterval > 0 && cfg.DecayShift > 0 {
		ring.EnableDecay(cfg.DecayInterval, cfg.DecayShift)
	}
	if cfg.NodeCap > 0 {
		ring.EnableNodeSampling(cfg.NodeCap, cfg.NodeSampleBits)
	}
	if cfg.RotationInterval > 0 {
		ring.EnableRotation(cfg.RotationInterval)
	}
	return ring
}

// CFHotKey encodes a column-family-aware hot-key identity for write tracking.
func CFHotKey(cf kv.ColumnFamily, key []byte) string {
	if !cf.Valid() || cf == kv.CFDefault {
		return string(key)
	}
	buf := make([]byte, len(key)+1)
	buf[0] = byte(cf)
	copy(buf[1:], key)
	return string(buf)
}

// ShouldThrottleHotWrite reports whether the next write should be rejected due
// to repeated writes against the same hot key.
func ShouldThrottleHotWrite(ring *hotring.RotatingHotRing, limit int32, cf kv.ColumnFamily, key []byte) bool {
	if ring == nil || len(key) == 0 || limit <= 0 {
		return false
	}
	skey := CFHotKey(cf, key)
	if skey == "" {
		return false
	}
	_, limited := ring.TouchAndClamp(skey, limit)
	return limited
}

// NormalizeWriteThrottleState clamps unknown states back to WriteThrottleNone.
func NormalizeWriteThrottleState(state lsm.WriteThrottleState) lsm.WriteThrottleState {
	switch state {
	case lsm.WriteThrottleNone, lsm.WriteThrottleSlowdown, lsm.WriteThrottleStop:
		return state
	default:
		return lsm.WriteThrottleNone
	}
}

// SlowdownDelay computes the pacing delay for one batch under the current
// slowdown rate. Zero means no pacing is needed.
func SlowdownDelay(batchSize int64, rate uint64) time.Duration {
	if batchSize <= 0 || rate == 0 {
		return 0
	}
	delayNs := (uint64(batchSize) * uint64(time.Second)) / rate
	if delayNs == 0 {
		return 0
	}
	if delayNs > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(delayNs)
}

// WALGCPolicyConfig captures the DB-provided hooks needed to decide whether a
// WAL segment is safe to remove.
type WALGCPolicyConfig struct {
	RaftPointers   func() map[uint64]localmeta.RaftLogPointer
	SegmentMetrics func(segmentID uint32) wal.RecordMetrics
	Warn           func(msg string, args ...any)
}

// WALGCPolicy adapts raft pointer snapshots and WAL segment metrics into the
// LSM-level WAL GC policy interface.
type WALGCPolicy struct {
	raftPointers   func() map[uint64]localmeta.RaftLogPointer
	segmentMetrics func(segmentID uint32) wal.RecordMetrics
	warn           func(msg string, args ...any)
}

// NewWALGCPolicy builds the runtime WAL GC policy used by the DB-backed LSM.
func NewWALGCPolicy(cfg WALGCPolicyConfig) lsm.WALGCPolicy {
	return &WALGCPolicy{
		raftPointers:   cfg.RaftPointers,
		segmentMetrics: cfg.SegmentMetrics,
		warn:           cfg.Warn,
	}
}

// CanRemoveSegment reports whether the target WAL segment can be garbage-collected.
func (p *WALGCPolicy) CanRemoveSegment(segmentID uint32) bool {
	if p == nil {
		return true
	}
	if p.raftPointers != nil {
		ptrs := p.raftPointers()
		for _, ptr := range ptrs {
			if ptr.SegmentIndex > 0 && segmentID >= uint32(ptr.SegmentIndex) {
				return false
			}
			if ptr.Segment > 0 && segmentID >= ptr.Segment {
				return false
			}
		}
	}
	if p.segmentMetrics != nil {
		metrics := p.segmentMetrics(segmentID)
		if metrics.RaftRecords() > 0 && p.warn != nil {
			p.warn(
				"wal segment retains raft records during GC eligibility",
				"segment", segmentID,
				"raft_entries", metrics.RaftEntries,
				"raft_states", metrics.RaftStates,
				"raft_snapshots", metrics.RaftSnapshots,
			)
		}
	}
	return true
}
