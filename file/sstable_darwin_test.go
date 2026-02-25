//go:build darwin

package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func writeTestSSTable(t *testing.T, path string) ([]byte, []byte) {
	t.Helper()
	dataBlock := []byte("data-block")
	key := kv.InternalKey(kv.CFDefault, []byte("a"), 1)
	index := &pb.TableIndex{
		Offsets: []*pb.BlockOffset{{
			Key:    key,
			Offset: 0,
			Len:    uint32(len(dataBlock)),
		}},
	}
	indexData, err := proto.Marshal(index)
	require.NoError(t, err)
	checksum := utils.CalculateChecksum(indexData)

	var buf bytes.Buffer
	buf.Write(dataBlock)
	buf.Write(indexData)
	buf.Write(kv.U32ToBytes(uint32(len(indexData))))
	buf.Write(kv.U64ToBytes(checksum))
	buf.Write(kv.U32ToBytes(uint32(len(kv.U64ToBytes(checksum)))))

	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))
	return key, dataBlock
}

func TestSSTableInitAndAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	key, dataBlock := writeTestSSTable(t, path)

	ss := OpenSStable(&Options{
		FileName: path,
		Flag:     os.O_RDWR,
		MaxSz:    0,
	})
	require.NotNil(t, ss)
	defer func() { _ = ss.Close() }()

	require.NoError(t, ss.Init())
	require.Equal(t, uint64(0), ss.FID())

	require.NotNil(t, ss.Indexs())
	require.Len(t, ss.Indexs().GetOffsets(), 1)
	require.Equal(t, key, ss.MinKey())
	require.Equal(t, key, ss.MaxKey())
	require.False(t, ss.HasBloomFilter())

	newMax := kv.InternalKey(kv.CFDefault, []byte("b"), 1)
	ss.SetMaxKey(newMax)
	require.Equal(t, newMax, ss.MaxKey())

	view, err := ss.View(0, len(dataBlock))
	require.NoError(t, err)
	require.Equal(t, dataBlock, view)

	out, err := ss.Bytes(0, len(dataBlock))
	require.NoError(t, err)
	require.Equal(t, dataBlock, out)

	created := ss.GetCreatedAt()
	require.NotNil(t, created)
	now := time.Now().Add(-time.Hour)
	ss.SetCreatedAt(&now)
	require.Equal(t, now, *ss.GetCreatedAt())

	require.NoError(t, ss.Advise(utils.AccessPatternSequential))
	require.NoError(t, ss.Truncature(int64(len(dataBlock))))
}

func TestSSTableDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "delete.sst")
	_, _ = writeTestSSTable(t, path)

	ss := OpenSStable(&Options{
		FileName: path,
		Flag:     os.O_RDWR,
		MaxSz:    0,
	})
	require.NotNil(t, ss)

	require.NoError(t, ss.Detele())
	_, err := os.Stat(path)
	require.Error(t, err)
}
