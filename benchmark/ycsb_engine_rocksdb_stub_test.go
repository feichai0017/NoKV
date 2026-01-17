package benchmark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRocksDBStubErrors(t *testing.T) {
	engine := newRocksDBEngine(ycsbEngineOptions{})
	require.Equal(t, "RocksDB", engine.Name())

	require.Error(t, engine.Open(true))
	require.NoError(t, engine.Close())
	_, err := engine.Read([]byte("k"), nil)
	require.Error(t, err)
	require.Error(t, engine.Insert([]byte("k"), []byte("v")))
	require.Error(t, engine.Update([]byte("k"), []byte("v")))
	_, err = engine.Scan([]byte("k"), 1)
	require.Error(t, err)
}
