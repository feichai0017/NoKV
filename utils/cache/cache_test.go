package cache

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(5)
	for i := range 10 {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set %s: %s\n", key, cache)
	}

	for i := range 1000 {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			fmt.Printf("get %s: %s\n", key, cache)
			assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)
	}
	fmt.Printf("at last: %s\n", cache)
}

func TestCacheDeleteAndHashHelpers(t *testing.T) {
	c := NewCache(2)
	c.Set("k1", "v1")
	conflict, ok := c.Del("k1")
	require.True(t, ok)
	require.NotZero(t, conflict)

	_, ok = c.Del("missing")
	require.False(t, ok)

	h, ch := c.keyToHash("abc")
	require.NotZero(t, h)
	require.NotZero(t, ch)

	h, ch = c.keyToHash([]byte("abc"))
	require.NotZero(t, h)
	require.NotZero(t, ch)

	h, ch = c.keyToHash(uint64(42))
	require.Equal(t, uint64(42), h)
	require.Zero(t, ch)

	h, ch = c.keyToHash(byte(7))
	require.Equal(t, uint64(7), h)
	require.Zero(t, ch)

	require.Panics(t, func() {
		c.keyToHash(struct{}{})
	})
}

func TestMemHashConsistency(t *testing.T) {
	first := MemHashString("abc")
	second := MemHashString("abc")
	require.Equal(t, first, second)

	data := []byte("xyz")
	h1 := MemHash(data)
	h2 := MemHash(data)
	require.Equal(t, h1, h2)
}

func TestCacheLargeUint64Key(t *testing.T) {
	// Test that cache handles uint64 keys larger than math.MaxUint32 correctly
	// and that bloom filter interaction is properly skipped
	c := NewCache(10)

	// Use a uint64 key larger than math.MaxUint32
	largeKey := uint64(math.MaxUint32) + 1000
	testValue := "test_value_for_large_key"

	// Set a value with the large key
	success := c.Set(largeKey, testValue)
	require.True(t, success, "Set should succeed for large uint64 key")

	// Get the value back
	val, ok := c.Get(largeKey)
	require.True(t, ok, "Get should find the value for large uint64 key")
	require.Equal(t, testValue, val, "Retrieved value should match the stored value")

	// Get again to verify the large key code path where bloom filter is skipped
	val, ok = c.Get(largeKey)
	require.True(t, ok, "Second Get should still find the value")
	require.Equal(t, testValue, val, "Retrieved value should still match")

	// Test with a missing large key to ensure bloom filter skip works for cache misses
	missingLargeKey := uint64(math.MaxUint32) + 2000
	val, ok = c.Get(missingLargeKey)
	require.False(t, ok, "Get should return false for missing large uint64 key")
	require.Nil(t, val, "Retrieved value should be nil for missing key")

	// Delete the value
	conflict, ok := c.Del(largeKey)
	require.True(t, ok, "Delete should succeed")
	require.Zero(t, conflict, "Conflict hash should be zero for uint64 keys")

	// Verify deletion
	val, ok = c.Get(largeKey)
	require.False(t, ok, "Get should return false after deletion")
	require.Nil(t, val, "Retrieved value should be nil after deletion")
}
