package lsm

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func openShardHintTestLSM(t *testing.T, shardCount int) (*LSM, []*wal.Manager) {
	t.Helper()
	dir := t.TempDir()
	opts := newTestLSMOptions(dir, nil)
	wals := make([]*wal.Manager, shardCount)
	for i := range wals {
		mgr, err := wal.Open(wal.Config{Dir: filepath.Join(dir, fmt.Sprintf("wal-%02d", i))})
		require.NoError(t, err)
		wals[i] = mgr
	}
	lsm, err := NewLSM(opts, wals)
	require.NoError(t, err)
	return lsm, wals
}

func closeShardHintTestLSM(t *testing.T, lsm *LSM, wals []*wal.Manager) {
	t.Helper()
	require.NoError(t, lsm.Close())
	for _, mgr := range wals {
		require.NoError(t, mgr.Close())
	}
}

func TestShardHintTracksSuccessfulWrites(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("hint-key")
	v1 := kv.NewInternalEntry(kv.CFDefault, userKey, 1, []byte("v1"), 0, 0)
	_, err := lsm.SetBatchGroup(1, [][]*kv.Entry{{v1}})
	require.NoError(t, err)

	query := kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
	shardID, ok := lsm.lookupShardHint(query)
	require.True(t, ok)
	require.Equal(t, 1, shardID)

	v2 := kv.NewInternalEntry(kv.CFDefault, userKey, 2, []byte("v2"), 0, 0)
	_, err = lsm.SetBatchGroup(3, [][]*kv.Entry{{v2}})
	require.NoError(t, err)

	shardID, ok = lsm.lookupShardHint(query)
	require.True(t, ok)
	require.Equal(t, 3, shardID)

	got, err := lsm.Get(query)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got.Value)
	got.DecrRef()
}

func TestShardHintFallsBackWhenHintMissesMemtable(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("fallback-key")
	entry := kv.NewInternalEntry(kv.CFDefault, userKey, 7, []byte("visible"), 0, 0)
	_, err := lsm.SetBatchGroup(2, [][]*kv.Entry{{entry}})
	require.NoError(t, err)

	query := kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
	lsm.shardHints.remember(query, 0)

	got, err := lsm.Get(query)
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), got.Value)
	got.DecrRef()
}

func TestShardHintDoesNotShortCircuitHistoricalRead(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("historical-key")
	v20 := kv.NewInternalEntry(kv.CFDefault, userKey, 20, []byte("v20"), 0, 0)
	v10 := kv.NewInternalEntry(kv.CFDefault, userKey, 10, []byte("v10"), 0, 0)
	v30 := kv.NewInternalEntry(kv.CFDefault, userKey, 30, []byte("v30"), 0, 0)
	_, err := lsm.SetBatchGroup(1, [][]*kv.Entry{{v20}})
	require.NoError(t, err)
	_, err = lsm.SetBatchGroup(2, [][]*kv.Entry{{v10}})
	require.NoError(t, err)
	_, err = lsm.SetBatchGroup(2, [][]*kv.Entry{{v30}})
	require.NoError(t, err)

	query := kv.InternalKey(kv.CFDefault, userKey, 25)
	got, err := lsm.Get(query)
	require.NoError(t, err)
	require.Equal(t, uint64(20), got.Version)
	require.Equal(t, []byte("v20"), got.Value)
	got.DecrRef()
}

func TestGetChoosesHighestVisibleVersionAcrossMemtablesAndLevels(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("mem-level-history")
	levelPut := kv.NewInternalEntry(kv.CFDefault, userKey, 396, []byte("v396"), 0, 0)
	memDelete := kv.NewInternalEntry(kv.CFDefault, userKey, 393, nil, kv.BitDelete, 0)
	t1 := buildTableWithEntries(t, lsm, 91, levelPut)
	lsm.levels.levels[1].tables = []*table{t1}
	lsm.levels.levels[1].Sort()
	_, err := lsm.SetBatchGroup(0, [][]*kv.Entry{{memDelete}})
	require.NoError(t, err)

	got, err := lsm.Get(kv.InternalKey(kv.CFDefault, userKey, 396))
	require.NoError(t, err)
	require.Equal(t, uint64(396), got.Version)
	require.Equal(t, []byte("v396"), got.Value)
	got.DecrRef()

	require.NoError(t, t1.DecrRef())
}

func TestShardHintDoesNotBypassRangeTombstones(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	point := kv.NewInternalEntry(kv.CFDefault, []byte("m"), 1, []byte("old"), 0, 0)
	_, err := lsm.SetBatchGroup(1, [][]*kv.Entry{{point}})
	require.NoError(t, err)
	query := kv.InternalKey(kv.CFDefault, []byte("m"), kv.MaxVersion)
	shardID, ok := lsm.lookupShardHint(query)
	require.True(t, ok)
	require.Equal(t, 1, shardID)

	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	_, err = lsm.SetBatchGroup(3, [][]*kv.Entry{{rt}})
	require.NoError(t, err)
	require.True(t, lsm.hasRangeTombstones())

	got, err := lsm.Get(query)
	if got != nil {
		got.DecrRef()
	}
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}
