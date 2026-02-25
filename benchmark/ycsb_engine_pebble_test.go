package benchmark

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestPebbleEngineCRUD(t *testing.T) {
	engine := newPebbleEngine(ycsbEngineOptions{
		BaseDir:            t.TempDir(),
		SyncWrites:         false,
		BlockCacheMB:       16,
		MemtableMB:         8,
		SSTableMB:          8,
		PebbleCompression:  "none",
		RocksDBCompression: "none",
	})

	require.Equal(t, "Pebble", engine.Name())
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

	read, err := engine.Scan(key, 4)
	require.NoError(t, err)
	require.Equal(t, 1, read)
}

func TestParsePebbleCompression(t *testing.T) {
	require.Equal(t, pebble.NoCompression, parsePebbleCompression("NONE"))
	require.Equal(t, pebble.SnappyCompression, parsePebbleCompression("snappy"))
	require.Equal(t, pebble.ZstdCompression, parsePebbleCompression("zstd"))
	require.Equal(t, pebble.NoCompression, parsePebbleCompression("invalid"))
}
