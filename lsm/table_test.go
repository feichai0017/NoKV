package lsm

import (
	"fmt"
	"os"
	"sync/atomic"
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
	return lsm
}

// TestTableReverseIteration tests reverse iteration behavior on a single table.
func TestTableReverseIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

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

// TestTableReverseIterationMultiBlock tests reverse iteration across multiple blocks.
func TestTableReverseIterationMultiBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-multiblock")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          128, // Force multiple blocks.
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

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
		require.Equal(t, byte('t'), keys[0])
		require.Equal(t, byte('a'), keys[19])
		for i := 0; i < 19; i++ {
			require.Greater(t, keys[i], keys[i+1], "keys should be in descending order")
		}
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

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &table{fid: 1, ref: 2}
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, int32(1), atomic.LoadInt32(&tbl.ref))

	// Avoid the 1->0 path in this unit test (which requires a real table handle).
	atomic.StoreInt32(&tbl.ref, 0)
	require.PanicsWithError(t, fmt.Errorf("table refcount underflow: fid %d, current_ref %d", tbl.fid, int32(0)).Error(), func() {
		_ = tbl.DecrRef()
	})
}
