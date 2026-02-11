package benchmark

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoKVEngineCRUD(t *testing.T) {
	engine := newNoKVEngine(ycsbEngineOptions{
		BaseDir:        t.TempDir(),
		ValueSize:      8,
		ValueThreshold: 32,
		MemtableMB:     1,
		SSTableMB:      4,
		VlogFileMB:     4,
	})

	require.Equal(t, "NoKV", engine.Name())
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

	count, err := engine.Scan(key, 10)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestNoKVEnginePrintStats(t *testing.T) {
	eng := newNoKVEngine(ycsbEngineOptions{
		BaseDir:        t.TempDir(),
		ValueSize:      8,
		ValueThreshold: 32,
		MemtableMB:     1,
		SSTableMB:      4,
		VlogFileMB:     4,
	})
	engine, ok := eng.(*nokvEngine)
	require.True(t, ok)

	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()
	require.NoError(t, engine.Insert([]byte("key"), []byte("val")))

	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = old
	})

	engine.printStats()
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(out), "NoKV Stats")
}

func TestNoKVEngineScanWithValueLogEntries(t *testing.T) {
	engine := newNoKVEngine(ycsbEngineOptions{
		BaseDir:        t.TempDir(),
		ValueSize:      256,
		ValueThreshold: 1,
		MemtableMB:     1,
		SSTableMB:      4,
		VlogFileMB:     4,
	})
	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()

	val := bytes.Repeat([]byte("v"), 256)
	for i := 0; i < 5; i++ {
		key := formatYCSBKey(int64(i))
		require.NoError(t, engine.Insert(key, val))
	}

	count, err := engine.Scan(formatYCSBKey(1), 3)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}
