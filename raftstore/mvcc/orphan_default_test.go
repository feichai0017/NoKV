package mvcc_test

import (
	"context"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/stretchr/testify/require"
)

func TestApplyOrphanDefaultsDeletesUnownedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("orphan")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)

	stats, err := storemvcc.ApplyOrphanDefaults(context.Background(), db, storemvcc.OrphanDefaultOptions{BatchEntries: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.OrphanDefaults)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 10)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestApplyOrphanDefaultsRetainsWriteOwnedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("owned")
	applyMVCCGCPutVersion(t, db, key, 20, 10, "value")

	stats, err := storemvcc.ApplyOrphanDefaults(context.Background(), db, storemvcc.OrphanDefaultOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.RetainedDefaults)
	require.Zero(t, stats.AppliedDefaultDeletes)
}

func TestApplyOrphanDefaultsRetainsLockOwnedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("locked")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, 10, 100, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ApplyOrphanDefaults(context.Background(), db, storemvcc.OrphanDefaultOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.RetainedDefaults)
	require.Zero(t, stats.AppliedDefaultDeletes)
}
