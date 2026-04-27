package vlog

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/stretchr/testify/require"
)

func TestIteratorCountAndFilterPendingDeletes(t *testing.T) {
	var c Consumer
	c.numActiveIterators.Store(3)
	require.Equal(t, 3, c.IteratorCount())

	input := []manifest.ValueLogID{
		{Bucket: 0, FileID: 1},
		{Bucket: 0, FileID: 2},
		{Bucket: 1, FileID: 1},
	}

	require.Equal(t, input, c.FilterPendingDeletes(input))

	c.filesToBeDeleted = []manifest.ValueLogID{
		{Bucket: 0, FileID: 2},
	}
	require.Equal(t, []manifest.ValueLogID{
		{Bucket: 0, FileID: 1},
		{Bucket: 1, FileID: 1},
	}, c.FilterPendingDeletes(input))
}

func TestGCSampleRatiosDefaultAndConfigured(t *testing.T) {
	c := &Consumer{}
	require.Equal(t, 0.10, c.GCSampleSizeRatio())
	require.Equal(t, 0.01, c.GCSampleCountRatio())

	c.cfg.GCSampleSizeRatio = 0.25
	c.cfg.GCSampleCountRatio = 0.15
	require.Equal(t, 0.25, c.GCSampleSizeRatio())
	require.Equal(t, 0.15, c.GCSampleCountRatio())
}
