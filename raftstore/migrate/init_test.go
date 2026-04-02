package migrate

import (
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/stretchr/testify/require"
)

func prepareStandaloneWorkdir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = dir
	db, err := NoKV.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("alpha"), []byte("value")))
	require.NoError(t, db.Close())
	return dir
}

func TestInitFailpointAfterModePreparing(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	failpoints.Set(failpoints.AfterInitModePreparing)
	defer failpoints.Set(failpoints.None)

	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 11, PeerID: 101})
	require.Error(t, err)
	require.Contains(t, err.Error(), "after init mode preparing")

	status, err := ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, ModePreparing, status.Mode)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointPreparingWritten, status.Checkpoint.Stage)
	require.Contains(t, status.ResumeHint, "re-run nokv migrate init")

	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = metaStore.Close() }()
	require.Empty(t, metaStore.Snapshot())
}

func TestInitFailpointAfterCatalogPersist(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	failpoints.Set(failpoints.AfterInitCatalogPersist)
	defer failpoints.Set(failpoints.None)

	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 12, PeerID: 102})
	require.Error(t, err)
	require.Contains(t, err.Error(), "after init catalog persist")

	status, err := ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, ModePreparing, status.Mode)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointCatalogPersisted, status.Checkpoint.Stage)
	require.Contains(t, status.ResumeHint, "catalog persistence")

	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = metaStore.Close() }()
	snapshotMap := metaStore.Snapshot()
	require.Contains(t, snapshotMap, uint64(12))

	_, err = snapshot.ReadMeta(SeedSnapshotDir(dir, 12), nil)
	require.Error(t, err)
}

func TestInitFailpointAfterSeedSnapshot(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	failpoints.Set(failpoints.AfterInitSeedSnapshot)
	defer failpoints.Set(failpoints.None)

	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 13, PeerID: 103})
	require.Error(t, err)
	require.Contains(t, err.Error(), "after init seed snapshot")

	status, err := ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, ModePreparing, status.Mode)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointSeedExported, status.Checkpoint.Stage)
	require.Contains(t, status.ResumeHint, "seed snapshot export")

	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = metaStore.Close() }()
	snapshotMap := metaStore.Snapshot()
	require.Contains(t, snapshotMap, uint64(13))

	meta, err := snapshot.ReadMeta(SeedSnapshotDir(dir, 13), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(13), meta.Region.ID)

	_, ok := metaStore.RaftPointer(13)
	require.False(t, ok)
}

func TestInitWritesFinalCheckpoint(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)

	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 14, PeerID: 104})
	require.NoError(t, err)

	status, err := ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, ModeSeeded, status.Mode)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointSeededFinalized, status.Checkpoint.Stage)
	require.Contains(t, status.ResumeHint, "promotion already completed")
}
