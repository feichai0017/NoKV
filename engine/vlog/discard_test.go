package vlog

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/stretchr/testify/require"
)

func TestDiscardStatsEncodeDecodeRoundTrip(t *testing.T) {
	stats := map[manifest.ValueLogID]int64{
		{Bucket: 0, FileID: 1}: 12,
		{Bucket: 2, FileID: 9}: 44,
	}

	encoded, err := EncodeDiscardStats(stats)
	require.NoError(t, err)

	decoded, err := DecodeDiscardStats(encoded)
	require.NoError(t, err)
	require.Equal(t, stats, decoded)

	empty, err := DecodeDiscardStats(nil)
	require.NoError(t, err)
	require.Nil(t, empty)
}

func TestDecodeDiscardStatsRejectsInvalidKeys(t *testing.T) {
	_, err := DecodeDiscardStats([]byte(`{"broken":1}`))
	require.Error(t, err)

	_, err = DecodeDiscardStats([]byte(`{"x:1":1}`))
	require.Error(t, err)

	_, err = DecodeDiscardStats([]byte(`{"1:y":1}`))
	require.Error(t, err)
}
