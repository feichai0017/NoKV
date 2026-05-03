package snapshot_test

import (
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/stretchr/testify/require"
)

func TestDBStoreRequiresOpenDB(t *testing.T) {
	var nilDB *NoKV.DB
	_, err := snapshot.NewDBStore(nilDB).ExportSnapshot(localmeta.RegionMeta{})
	require.ErrorContains(t, err, "requires open db")

	db := openSnapshotDB(t)
	require.NoError(t, db.Close())
	_, err = snapshot.NewDBStore(db).ImportSnapshot([]byte("not-a-real-payload"))
	require.ErrorContains(t, err, "requires open db")
}
