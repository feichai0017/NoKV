package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClockProCache(t *testing.T) {
	require.Nil(t, NewClockProCache[int](0))

	cache := NewClockProCache[string](2)
	cache.Promote(1, "a")
	cache.Promote(2, "b")

	val, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, "a", val)

	cache.Promote(3, "c")
	_, ok = cache.Get(1)
	require.False(t, ok)

	cache.Delete(2)
	_, ok = cache.Get(2)
	require.False(t, ok)
}
