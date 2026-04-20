package namespace_test

import (
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	ns "github.com/feichai0017/NoKV/namespace"
	"github.com/stretchr/testify/require"
)

func TestNoKVStoreApplyGetAndScanPrefix(t *testing.T) {
	db := openTestNoKVDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	kv := ns.NewNoKVStore(db)
	require.NoError(t, kv.Apply([]ns.Mutation{
		{Kind: ns.MutationPut, Key: []byte("aa/1"), Value: []byte("v1")},
		{Kind: ns.MutationPut, Key: []byte("aa/2"), Value: []byte("v2")},
		{Kind: ns.MutationPut, Key: []byte("bb/1"), Value: []byte("v3")},
	}))

	val, err := kv.Get([]byte("aa/1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	pairs, err := kv.ScanPrefix([]byte("aa/"), nil, 0)
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	require.Equal(t, []byte("aa/1"), pairs[0].Key)
	require.Equal(t, []byte("aa/2"), pairs[1].Key)

	pairs, err = kv.ScanPrefix([]byte("aa/"), []byte("aa/2"), 1)
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, []byte("aa/2"), pairs[0].Key)

	require.NoError(t, kv.Apply([]ns.Mutation{
		{Kind: ns.MutationDelete, Key: []byte("aa/1")},
	}))
	val, err = kv.Get([]byte("aa/1"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func openTestNoKVDB(t *testing.T) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "nokv")
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.ThermosEnabled = false
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}
