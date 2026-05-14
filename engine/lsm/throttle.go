// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package lsm

import "sync/atomic"

// WriteThrottleState reports the LSM's view of write admission pressure.
// It is **advisory**: the LSM write path (SetBatchGroup → applyWriteBatches)
// does NOT consult or block on this state. Callers above the LSM (typically
// the local commit pipeline) read State / PressurePermille / RateBytesPerSec
// and decide whether to delay or pace their submissions.
//
//   - WriteThrottleNone:     no admission pressure; submit at full rate.
//   - WriteThrottleSlowdown: callers should pace per RateBytesPerSec.
//   - WriteThrottleStop:     callers should hold submissions until pressure recovers.
//
// State transitions are produced by the compactor (compactor.adjustThrottle)
// based on L0 table count and per-level scoring; see compaction.go.
type WriteThrottleState int32

const (
	WriteThrottleNone WriteThrottleState = iota
	WriteThrottleSlowdown
	WriteThrottleStop
)

// writeThrottle owns the write-admission state machine: the current state,
// the in-effect pacing pressure (permille), the slowdown rate target, and
// the optional callback fired on state transitions.
type writeThrottle struct {
	state    atomic.Int32
	pressure atomic.Uint32
	rate     atomic.Uint64
	onChange func(WriteThrottleState)
}

func newWriteThrottle() *writeThrottle { return &writeThrottle{} }

// SetCallback registers fn to be invoked on state transitions. Pass nil to
// clear. Not safe to call concurrently with Apply; intended for setup time.
func (t *writeThrottle) SetCallback(fn func(WriteThrottleState)) {
	if t == nil {
		return
	}
	t.onChange = fn
}

// State reports the current admission state, normalized to a known value.
func (t *writeThrottle) State() WriteThrottleState {
	if t == nil {
		return WriteThrottleNone
	}
	return normalizeWriteThrottleState(WriteThrottleState(t.state.Load()))
}

// PressurePermille returns current write pacing pressure in [0, 1000].
func (t *writeThrottle) PressurePermille() uint32 {
	if t == nil {
		return 0
	}
	p := t.pressure.Load()
	if p > 1000 {
		return 1000
	}
	return p
}

// RateBytesPerSec returns the current slowdown target in bytes/sec.
func (t *writeThrottle) RateBytesPerSec() uint64 {
	if t == nil {
		return 0
	}
	return t.rate.Load()
}

// Apply atomically updates the state machine. The callback fires only when
// the state value actually changes; pressure/rate updates without a state
// transition are silent.
func (t *writeThrottle) Apply(state WriteThrottleState, pressure uint32, rate uint64) {
	if t == nil {
		return
	}
	state = normalizeWriteThrottleState(state)
	if pressure > 1000 {
		pressure = 1000
	}
	switch state {
	case WriteThrottleNone:
		pressure = 0
		rate = 0
	case WriteThrottleStop:
		pressure = 1000
		rate = 0
	default:
	}
	t.pressure.Store(pressure)
	t.rate.Store(rate)
	prev := normalizeWriteThrottleState(WriteThrottleState(t.state.Swap(int32(state))))
	if prev == state {
		return
	}
	if t.onChange != nil {
		t.onChange(state)
	}
}

func normalizeWriteThrottleState(state WriteThrottleState) WriteThrottleState {
	switch state {
	case WriteThrottleNone, WriteThrottleSlowdown, WriteThrottleStop:
		return state
	default:
		return WriteThrottleNone
	}
}
