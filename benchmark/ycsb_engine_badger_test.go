package benchmark

import (
	"fmt"
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

func TestBadgerBatchInsert(t *testing.T) {
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

	batchWriter, supportBatch := engine.(BatchWriter)
	require.True(t, supportBatch)

	keys := make([][]byte, 0, 1000000)
	vals := make([][]byte, 0, 1000000)
	for i := range 5 {
		key := fmt.Sprintf("user%012d", i+1)
		value := fmt.Sprintf("value%012d", i+1)
		keys = append(keys, []byte(key))
		vals = append(vals, []byte(value))
	}
	require.NoError(t, batchWriter.BatchInsert(keys, vals))

	for i := range 5 {
		key := fmt.Sprintf("user%012d", i+1)
		out, err := engine.Read([]byte(key), nil)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("value%012d", i+1), string(out))
	}
}
