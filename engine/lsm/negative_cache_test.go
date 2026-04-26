package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestNegativeCacheRemembersAndInvalidatesMiss(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	query := kv.InternalKey(kv.CFDefault, []byte("negative-key"), kv.MaxVersion)
	got, err := lsm.Get(query)
	require.Nil(t, got)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(query))

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("negative-key"), 1, []byte("visible"), 0, 0)
	_, err = lsm.SetBatchGroup(2, [][]*kv.Entry{{entry}})
	require.NoError(t, err)
	require.False(t, lsm.negativeHit(query))

	got, err = lsm.Get(query)
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), got.Value)
	got.DecrRef()
}

func TestNegativeCacheKeysIncludeReadVersion(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("versioned-negative")
	missAtOne := kv.InternalKey(kv.CFDefault, userKey, 1)
	missAtTwo := kv.InternalKey(kv.CFDefault, userKey, 2)

	_, err := lsm.Get(missAtOne)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(missAtOne))
	require.False(t, lsm.negativeHit(missAtTwo))
}

func TestNegativeCacheDisabledByRangeTombstones(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	_, err := lsm.SetBatchGroup(1, [][]*kv.Entry{{rt}})
	require.NoError(t, err)

	query := kv.InternalKey(kv.CFDefault, []byte("m"), kv.MaxVersion)
	got, err := lsm.Get(query)
	require.Nil(t, got)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.False(t, lsm.negativeHit(query))
}

func TestNegativeCacheClearDropsRememberedMisses(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	query := kv.InternalKey(kv.CFDefault, []byte("clear-negative"), kv.MaxVersion)
	_, err := lsm.Get(query)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(query))

	lsm.clearNegativeCache()
	require.False(t, lsm.negativeHit(query))
}
