package lsm

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteThrottleZeroValueIsNone(t *testing.T) {
	wt := newWriteThrottle()
	require.Equal(t, WriteThrottleNone, wt.State())
	require.Equal(t, uint32(0), wt.PressurePermille())
	require.Equal(t, uint64(0), wt.RateBytesPerSec())
}

func TestWriteThrottleApplyTransitionsAndClamps(t *testing.T) {
	wt := newWriteThrottle()

	// Slowdown with custom pressure + rate.
	wt.Apply(WriteThrottleSlowdown, 400, 256<<20)
	require.Equal(t, WriteThrottleSlowdown, wt.State())
	require.Equal(t, uint32(400), wt.PressurePermille())
	require.Equal(t, uint64(256<<20), wt.RateBytesPerSec())

	// Stop forces pressure=1000 and rate=0 regardless of inputs.
	wt.Apply(WriteThrottleStop, 50, 999)
	require.Equal(t, WriteThrottleStop, wt.State())
	require.Equal(t, uint32(1000), wt.PressurePermille())
	require.Equal(t, uint64(0), wt.RateBytesPerSec())

	// None forces pressure=0 and rate=0.
	wt.Apply(WriteThrottleNone, 999, 999)
	require.Equal(t, WriteThrottleNone, wt.State())
	require.Equal(t, uint32(0), wt.PressurePermille())
	require.Equal(t, uint64(0), wt.RateBytesPerSec())

	// Pressure > 1000 in slowdown clamps to 1000.
	wt.Apply(WriteThrottleSlowdown, 5000, 1)
	require.Equal(t, uint32(1000), wt.PressurePermille())
}

func TestWriteThrottleCallbackFiresOnlyOnTransition(t *testing.T) {
	wt := newWriteThrottle()
	var (
		mu     sync.Mutex
		events []WriteThrottleState
	)
	wt.SetCallback(func(state WriteThrottleState) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, state)
	})

	wt.Apply(WriteThrottleStop, 1000, 0)
	wt.Apply(WriteThrottleStop, 1000, 0) // duplicate, no callback
	wt.Apply(WriteThrottleSlowdown, 400, 256<<20)
	wt.Apply(WriteThrottleNone, 0, 0)
	wt.Apply(WriteThrottleNone, 0, 0) // duplicate, no callback

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []WriteThrottleState{
		WriteThrottleStop,
		WriteThrottleSlowdown,
		WriteThrottleNone,
	}, events)
}

func TestWriteThrottleNilSafe(t *testing.T) {
	var wt *writeThrottle
	require.Equal(t, WriteThrottleNone, wt.State())
	require.Equal(t, uint32(0), wt.PressurePermille())
	require.Equal(t, uint64(0), wt.RateBytesPerSec())
	require.NotPanics(t, func() { wt.Apply(WriteThrottleStop, 0, 0) })
	require.NotPanics(t, func() { wt.SetCallback(nil) })
}

func TestNormalizeWriteThrottleStateUnknownBecomesNone(t *testing.T) {
	require.Equal(t, WriteThrottleNone, normalizeWriteThrottleState(WriteThrottleNone))
	require.Equal(t, WriteThrottleSlowdown, normalizeWriteThrottleState(WriteThrottleSlowdown))
	require.Equal(t, WriteThrottleStop, normalizeWriteThrottleState(WriteThrottleStop))
	require.Equal(t, WriteThrottleNone, normalizeWriteThrottleState(WriteThrottleState(99)))
	require.Equal(t, WriteThrottleNone, normalizeWriteThrottleState(WriteThrottleState(-1)))
}
