package lsm

import (
	"os"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func buildTestLSM(t *testing.T, opt *Options) *LSM {
	c := make(chan map[manifest.ValueLogID]int64, 16)
	opt.DiscardStatsCh = &c
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	require.NoError(t, err)
	lsm := NewLSM(opt, wlog)
	lsm.SetDiscardStatsCh(&c)
	return lsm
}

// TestTableReverseIteration tests the reverse iteration feature added in commit ae51fce
func TestTableReverseIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	// Create test data
	builder := newTableBuiler(opt)
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.KeyWithTs(key, 1), []byte("value")))
	}

	tableName := utils.FileNameSSTable(dir, 1)
	tbl := openTable(lsm.levels, tableName, builder)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse iteration with Rewind", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, kv.ParseKey(it.Item().Entry().Key)...)
		}
		require.Equal(t, "jihgfedcba", string(keys))
	})

	t.Run("reverse iteration with Seek", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.KeyWithTs([]byte("f"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("f"), kv.ParseKey(it.Item().Entry().Key))

		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, kv.ParseKey(it.Item().Entry().Key)...)
		}
		require.Equal(t, "fedcba", string(keys))
	})

	t.Run("forward iteration for comparison", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, kv.ParseKey(it.Item().Entry().Key)...)
		}
		require.Equal(t, "abcdefghij", string(keys))
	})

	t.Run("reverse seek to first key", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.KeyWithTs([]byte("a"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("a"), kv.ParseKey(it.Item().Entry().Key))
		it.Next()
		require.False(t, it.Valid())
	})

	t.Run("reverse seek to last key", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.KeyWithTs([]byte("j"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("j"), kv.ParseKey(it.Item().Entry().Key))
	})
}

// TestTableReverseIterationMultiBlock tests reverse iteration across multiple blocks
func TestTableReverseIterationMultiBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-multiblock")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          128, // Small block to force multiple blocks
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	// Create enough data to span multiple blocks
	builder := newTableBuiler(opt)
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.KeyWithTs(key, 1), []byte("value-with-more-data")))
	}

	tableName := utils.FileNameSSTable(dir, 2)
	tbl := openTable(lsm.levels, tableName, builder)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse across multiple blocks", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Rewind()
		count := 0
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, kv.ParseKey(it.Item().Entry().Key)...)
			count++
		}
		require.Equal(t, 20, count)
		// Verify reverse order: t, s, r, ..., b, a
		require.Equal(t, byte('t'), keys[0])
		require.Equal(t, byte('a'), keys[19])
	})

	t.Run("forward across multiple blocks", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		it.Rewind()
		count := 0
		for ; it.Valid(); it.Next() {
			count++
		}
		require.Equal(t, 20, count)
	})
}
