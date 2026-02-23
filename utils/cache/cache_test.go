package cache

import (
	"fmt"
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
