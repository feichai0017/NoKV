package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"
)

func TestTableBuilderPersistsStaleDataSizeInIndex(t *testing.T) {
	opt := &Options{
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := newTableBuiler(opt)
	entry := kv.NewEntry([]byte("stale-key"), []byte("stale-value"))
	builder.AddStaleKey(entry)

	bd := builder.done()
	require.NotNil(t, bd.index)
	require.NotEmpty(t, bd.index)

	var tableIndex pb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.Equal(t, uint32(builder.staleDataSize), tableIndex.GetStaleDataSize())
	require.Equal(t, uint64(builder.valueSize), tableIndex.GetValueSize())
}
