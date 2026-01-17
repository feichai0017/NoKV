package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandHelpers(t *testing.T) {
	require.GreaterOrEqual(t, Int63n(10), int64(0))
	require.GreaterOrEqual(t, RandN(10), 0)
	require.GreaterOrEqual(t, Float64(), 0.0)

	require.Equal(t, "", randStr(0))
	require.Len(t, randStr(4), 4)

	entry := BuildEntry()
	require.NotNil(t, entry)
	require.Greater(t, len(entry.Key), 8)
	require.NotEmpty(t, entry.Value)
}
