package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisValueGetExpiresAt(t *testing.T) {
	var v *redisValue
	require.Equal(t, uint64(0), v.GetExpiresAt())

	v = &redisValue{ExpiresAt: 42}
	require.Equal(t, uint64(42), v.GetExpiresAt())
}
