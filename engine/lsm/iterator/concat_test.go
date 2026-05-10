package iterator

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/stretchr/testify/require"
)

type concatTestRuntime struct {
	opts  table.Options
	cache *cache.Cache
}

func (r *concatTestRuntime) Cache() *cache.Cache    { return r.cache }
func (r *concatTestRuntime) Options() table.Options { return r.opts }

func newConcatTestRuntime(dir string) *concatTestRuntime {
	opts := table.Options{
		WorkDir:            dir,
		SSTableMaxSize:     1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.01,
	}
	c := cache.New(cache.Options{IndexBytes: 1 << 20, BlockBytes: 1 << 20})
	return &concatTestRuntime{opts: opts, cache: c}
}

func TestConcatIteratorSeekAndNext(t *testing.T) {
	dir := t.TempDir()
	rt := newConcatTestRuntime(dir)

	builder := table.NewBuilder(rt.opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b"), 1), []byte("vb")))
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("d"), 1), []byte("vd")))
	tbl, err := table.Open(rt, vfs.FileNameSSTable(dir, 100), builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	ci := NewConcatIterator([]*table.Table{tbl}, &index.Options{IsAsc: true})

	ci.Rewind()
	require.True(t, ci.Valid(), "expected concat iterator valid after rewind")
	require.NotNil(t, ci.Item())

	ci.Seek(kv.InternalKey(kv.CFDefault, []byte("c"), 1))
	require.True(t, ci.Valid(), "expected concat iterator valid after seek")
	require.Equal(t, "d", string(splitIterUserKey(t, ci.Item().Entry().Key)))

	ci.Next()
	require.False(t, ci.Valid(), "expected iterator exhausted after stepping past d")

	require.NoError(t, ci.Close())
}
