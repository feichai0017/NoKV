package lsm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/wal"
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

// TestNegativeCachePersistsAcrossOpen exercises Phase 3 of the slab
// substrate redesign: with NegativeCachePersistent enabled, a process
// restart should replay the slab snapshot back into the in-memory cache
// so previously-known not-found keys do not have to re-warm via the LSM.
func TestNegativeCachePersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()

	// First open: trigger misses to populate cache, then Close to snapshot.
	queries := [][]byte{
		kv.InternalKey(kv.CFDefault, []byte("missing-1"), kv.MaxVersion),
		kv.InternalKey(kv.CFDefault, []byte("missing-2"), kv.MaxVersion),
		kv.InternalKey(kv.CFDefault, []byte("missing-3"), kv.MaxVersion),
	}

	openLSM := func() (*LSM, []*walManagerHandle) {
		t.Helper()
		opts := newTestLSMOptions(dir, nil)
		opts.NegativeCachePersistent = true
		opts.NegativeCacheSlabMaxSize = 1 << 20
		walDir := filepath.Join(dir, "wal-00")
		require.NoError(t, os.MkdirAll(walDir, 0o755))
		mgr, err := wal.Open(wal.Config{Dir: walDir})
		require.NoError(t, err)
		lsm, err := NewLSM(opts, []*wal.Manager{mgr})
		require.NoError(t, err)
		return lsm, []*walManagerHandle{{mgr: mgr}}
	}
	closeLSM := func(lsm *LSM, wals []*walManagerHandle) {
		t.Helper()
		require.NoError(t, lsm.Close())
		for _, w := range wals {
			require.NoError(t, w.mgr.Close())
		}
	}

	lsm1, wals1 := openLSM()
	for _, q := range queries {
		_, err := lsm1.Get(q)
		require.ErrorIs(t, err, utils.ErrKeyNotFound)
		require.True(t, lsm1.negativeHit(q))
	}
	closeLSM(lsm1, wals1)

	// Snapshot must exist.
	_, err := os.Stat(filepath.Join(dir, "negative-slab", "negative.slab"))
	require.NoError(t, err, "snapshot file should exist after Close")

	// Second open: cache should be warm without any explicit miss.
	lsm2, wals2 := openLSM()
	defer closeLSM(lsm2, wals2)
	for _, q := range queries {
		require.True(t, lsm2.negativeHit(q),
			"key %q should be warm in negative cache after restore", q)
	}
}

type walManagerHandle struct {
	mgr *wal.Manager
}
