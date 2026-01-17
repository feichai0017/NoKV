package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCMSketchLifecycle(t *testing.T) {
	require.Panics(t, func() {
		_ = newCmSketch(0)
	})

	s := newCmSketch(16)
	s.Increment(1)
	s.Increment(2)
	s.Increment(2)

	require.GreaterOrEqual(t, s.Estimate(1), int64(1))
	require.GreaterOrEqual(t, s.Estimate(2), int64(1))

	s.Reset()
	require.LessOrEqual(t, s.Estimate(2), int64(1))

	s.Clear()
	require.Equal(t, int64(0), s.Estimate(2))
}

func TestCMRowHelpers(t *testing.T) {
	row := newCmRow(16)
	require.Equal(t, byte(0), row.get(2))
	row.increment(2)
	require.Equal(t, byte(1), row.get(2))
	row.reset()
	row.clear()
	require.Equal(t, byte(0), row.get(2))
}
