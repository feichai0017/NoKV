package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDAllocatorAllocMonotonic(t *testing.T) {
	a := NewIDAllocator(100)
	require.Equal(t, uint64(100), a.Alloc())
	require.Equal(t, uint64(101), a.Alloc())
	require.Equal(t, uint64(101), a.Current())
}

func TestIDAllocatorReserve(t *testing.T) {
	a := NewIDAllocator(1)
	first, last, err := a.Reserve(4)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(4), last)
	require.Equal(t, uint64(4), a.Current())

	next := a.Alloc()
	require.Equal(t, uint64(5), next)
}

func TestIDAllocatorReserveRejectsZero(t *testing.T) {
	a := NewIDAllocator(1)
	_, _, err := a.Reserve(0)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidBatch)
}
