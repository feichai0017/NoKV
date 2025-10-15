package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestTableBuilderPersistsStaleDataSizeInIndex(t *testing.T) {
	opt := &Options{
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := newTableBuiler(opt)
	entry := utils.NewEntry([]byte("stale-key"), []byte("stale-value"))
	builder.AddStaleKey(entry)

	bd := builder.done()
	require.NotNil(t, bd.index)
	require.NotEmpty(t, bd.index)

	var tableIndex pb.TableIndex
	require.NoError(t, tableIndex.Unmarshal(bd.index))
	require.Equal(t, uint32(builder.staleDataSize), tableIndex.GetStaleDataSize())
}