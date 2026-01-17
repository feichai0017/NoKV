package benchmark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngineCRUD(t *testing.T) {
	engine := newBadgerEngine(ycsbEngineOptions{
		BaseDir:           t.TempDir(),
		ValueThreshold:    32,
		SyncWrites:        false,
		BlockCacheMB:      4,
		BadgerCompression: "none",
	})

	require.Equal(t, "Badger", engine.Name())
	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()

	key := []byte("user000000000001")
	val := []byte("value")
	require.NoError(t, engine.Insert(key, val))

	out, err := engine.Read(key, nil)
	require.NoError(t, err)
	require.Equal(t, val, out)

	newVal := []byte("value2")
	require.NoError(t, engine.Update(key, newVal))
	out, err = engine.Read(key, nil)
	require.NoError(t, err)
	require.Equal(t, newVal, out)

	count, err := engine.Scan(key, 5)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
