package NoKV

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/stretchr/testify/require"
)

func TestDiscardStatsEncodeDecodeRoundTrip(t *testing.T) {
	stats := map[manifest.ValueLogID]int64{
		{Bucket: 0, FileID: 1}: 12,
		{Bucket: 2, FileID: 9}: 44,
	}

	encoded, err := encodeDiscardStats(stats)
	require.NoError(t, err)

	decoded, err := decodeDiscardStats(encoded)
	require.NoError(t, err)
	require.Equal(t, stats, decoded)

	empty, err := decodeDiscardStats(nil)
	require.NoError(t, err)
	require.Nil(t, empty)
}

func TestDecodeDiscardStatsRejectsInvalidKeys(t *testing.T) {
	_, err := decodeDiscardStats([]byte(`{"broken":1}`))
	require.Error(t, err)

	_, err = decodeDiscardStats([]byte(`{"x:1":1}`))
	require.Error(t, err)

	_, err = decodeDiscardStats([]byte(`{"1:y":1}`))
	require.Error(t, err)
}

func TestValueLogPopulateDiscardStatsLoadsPersistedEntry(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	stats := map[manifest.ValueLogID]int64{
		{Bucket: 0, FileID: 7}: 99,
		{Bucket: 1, FileID: 4}: 12,
	}
	encoded, err := encodeDiscardStats(stats)
	require.NoError(t, err)

	entry := kv.NewInternalEntry(kv.CFDefault, lfDiscardStatsKey, nonTxnMaxVersion, encoded, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))

	testVlog := &valueLog{
		db: db,
		lfDiscardStats: &lfDiscardStats{
			flushChan: make(chan map[manifest.ValueLogID]int64, 1),
		},
	}

	require.NoError(t, testVlog.populateDiscardStats())

	select {
	case got := <-testVlog.lfDiscardStats.flushChan:
		require.Equal(t, stats, got)
	default:
		t.Fatal("expected discard stats to be queued for replay")
	}
}

func TestValueLogIteratorCountAndFilterPendingDeletes(t *testing.T) {
	var vlog valueLog
	vlog.numActiveIterators.Store(3)
	require.Equal(t, 3, vlog.iteratorCount())

	input := []manifest.ValueLogID{
		{Bucket: 0, FileID: 1},
		{Bucket: 0, FileID: 2},
		{Bucket: 1, FileID: 1},
	}

	require.Equal(t, input, vlog.filterPendingDeletes(input))

	vlog.filesToBeDeleted = []manifest.ValueLogID{
		{Bucket: 0, FileID: 2},
	}
	require.Equal(t, []manifest.ValueLogID{
		{Bucket: 0, FileID: 1},
		{Bucket: 1, FileID: 1},
	}, vlog.filterPendingDeletes(input))
}

func TestValueLogGCSampleRatiosDefaultAndConfigured(t *testing.T) {
	vlog := &valueLog{}
	require.Equal(t, 0.10, vlog.gcSampleSizeRatio())
	require.Equal(t, 0.01, vlog.gcSampleCountRatio())

	vlog.opt.ValueLogGCSampleSizeRatio = 0.25
	vlog.opt.ValueLogGCSampleCountRatio = 0.15
	require.Equal(t, 0.25, vlog.gcSampleSizeRatio())
	require.Equal(t, 0.15, vlog.gcSampleCountRatio())
}
