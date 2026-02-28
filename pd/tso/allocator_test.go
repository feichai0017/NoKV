package tso

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/pd/core"
)

func TestAllocatorNextMonotonic(t *testing.T) {
	a := NewAllocator(50)
	require.Equal(t, uint64(50), a.Next())
	require.Equal(t, uint64(51), a.Next())
	require.Equal(t, uint64(51), a.Current())
}

func TestAllocatorReserve(t *testing.T) {
	a := NewAllocator(1)
	first, count, err := a.Reserve(3)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(3), count)
	require.Equal(t, uint64(3), a.Current())
	require.Equal(t, uint64(4), a.Next())
}

func TestAllocatorReserveRejectsZero(t *testing.T) {
	a := NewAllocator(1)
	_, _, err := a.Reserve(0)
	require.Error(t, err)
	require.ErrorIs(t, err, core.ErrInvalidBatch)
}
