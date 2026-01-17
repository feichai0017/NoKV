package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomFilterAllowAndReset(t *testing.T) {
	bf := newFilter(10, 0.01)
	key := []byte("k1")
	require.False(t, bf.AllowKey(key))
	require.True(t, bf.AllowKey(key))

	bf.reset()
	require.False(t, bf.MayContainKey(key))
}

func TestBloomFilterEdges(t *testing.T) {
	short := &BloomFilter{bitmap: []byte{0x01}, k: 1}
	require.False(t, short.MayContain(123))

	reserved := &BloomFilter{bitmap: []byte{0x00, 0x00}, k: 31}
	require.True(t, reserved.MayContain(123))
	require.True(t, reserved.Insert(123))
}

func TestBloomBitsPerKey(t *testing.T) {
	require.Greater(t, bloomBitsPerKey(100, 0.01), 0)
}
