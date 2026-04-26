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
