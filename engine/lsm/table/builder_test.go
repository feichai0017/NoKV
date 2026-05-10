package table

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/vfs"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"
)

func TestTableBuilderPersistsStaleDataSizeInIndex(t *testing.T) {
	opts := Options{
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := NewBuilder(opts)
	entry := kv.NewEntry([]byte("stale-key"), []byte("stale-value"))
	builder.AddStaleKey(entry)

	bd, err := builder.Done()
	require.NoError(t, err)
	require.NotNil(t, bd.index)
	require.NotEmpty(t, bd.index)

	var tableIndex storagepb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.Equal(t, uint32(builder.staleDataSize), tableIndex.GetStaleDataSize())
	require.Equal(t, uint64(builder.valueSize), tableIndex.GetValueSize())
}

func TestTableBuilderPersistsRangeTombstoneCount(t *testing.T) {
	opts := Options{
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := NewBuilder(opts)
	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	builder.AddKey(rt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b"), 9), []byte("value")))

	bd, err := builder.Done()
	require.NoError(t, err)

	var tableIndex storagepb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.Equal(t, uint32(1), tableIndex.GetRangeTombstoneCount())
}

func TestTableBuilderPersistsPrefixBloomInIndex(t *testing.T) {
	opts := Options{
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.01,
		PrefixExtractor: func(key []byte) []byte {
			if len(key) < 2 {
				return nil
			}
			return key[:2]
		},
	}

	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("aa-1"), 1), []byte("value-a")))
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("bb-1"), 1), []byte("value-b")))

	bd, err := builder.Done()
	require.NoError(t, err)

	var tableIndex storagepb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.NotEmpty(t, tableIndex.GetBloomFilter())
	require.NotEmpty(t, tableIndex.GetPrefixBloomFilter())
	require.True(t, utils.Filter(tableIndex.GetPrefixBloomFilter()).MayContainKey([]byte("aa")))
	require.True(t, utils.Filter(tableIndex.GetPrefixBloomFilter()).MayContainKey([]byte("bb")))
}

func TestTableBuilderCompressesBlocksWhenConfigured(t *testing.T) {
	opts := Options{
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
		BlockCompression:   CompressionSnappy,
	}

	builder := NewBuilder(opts)
	for i := range 16 {
		key := fmt.Appendf(nil, "key-%02d", i)
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), bytes.Repeat([]byte("metadata-value-"), 32)))
	}

	bd, err := builder.Done()
	require.NoError(t, err)

	var tableIndex storagepb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.NotEmpty(t, tableIndex.GetOffsets())
	for _, offset := range tableIndex.GetOffsets() {
		require.Equal(t, uint32(CompressionSnappy), offset.GetCompression())
		require.Greater(t, offset.GetRawLen(), offset.GetLen())
	}
}

func TestBuildDataCopyChargesCompactionPacerPerBlock(t *testing.T) {
	opts := Options{
		BlockSize:          128,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := NewBuilder(opts)
	for i := range 8 {
		key := fmt.Appendf(nil, "pace-%02d", i)
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), bytes.Repeat([]byte("v"), 32)))
	}
	bd, err := builder.Done()
	require.NoError(t, err)

	p := pacer.New(1 << 30)
	p.PrefillForTest(1 << 30)
	bd.pacer = p
	var expected int64
	for _, bl := range bd.blockList {
		expected += int64(bl.diskEnd)
	}

	dst := make([]byte, bd.Size)
	require.Equal(t, bd.Size, bd.Copy(dst))
	require.Equal(t, expected, p.Stats().BytesCharged)
}

func TestTableBuilderFinishAndEntryValueLen(t *testing.T) {
	opts := Options{
		BlockSize:          128,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b"), 1), []byte("value-b")))

	buf, err := builder.Finish()
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	builder.Close()

	entry := kv.NewEntry([]byte("inline"), []byte("inline-value"))
	require.Equal(t, uint32(len("inline-value")), entryValueLen(entry))
}

func TestTableBuilderFlushRenameFailureCleansTempFile(t *testing.T) {
	dir := t.TempDir()
	tableName := vfs.FileNameSSTable(dir, 1)
	injected := errors.New("rename injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRenameRule("", tableName, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opts := Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	rt := &testRuntime{opts: opts}
	_, err := builder.Flush(rt, tableName)
	require.ErrorIs(t, err, injected)

	tmpFiles, globErr := filepath.Glob(tableName + ".tmp.*")
	require.NoError(t, globErr)
	require.Empty(t, tmpFiles)
}

func TestTableBuilderFlushStrictPathDoesNotReopenFinalSST(t *testing.T) {
	dir := t.TempDir()
	tableName := vfs.FileNameSSTable(dir, 2)
	injected := errors.New("open final injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpOpenFile, tableName, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opts := Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	rt := &testRuntime{opts: opts}
	tbl, err := builder.Flush(rt, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.ss)
	require.NoError(t, tbl.ss.Close())
}

func TestTableBuilderFlushFastPathSkipsPreStat(t *testing.T) {
	dir := t.TempDir()
	tableName := vfs.FileNameSSTable(dir, 3)
	injected := errors.New("stat injected")
	policy := vfs.NewFaultPolicy(vfs.FailAfterNthRule(vfs.OpStat, tableName, 1, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opts := Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       false,
	}
	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	rt := &testRuntime{opts: opts}
	tbl, err := builder.Flush(rt, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NoError(t, tbl.ss.Close())

	_, statErr := os.Stat(tableName)
	require.NoError(t, statErr)
}

func TestTableBuilderFlushStrictPathSkipsPreStat(t *testing.T) {
	dir := t.TempDir()
	tableName := vfs.FileNameSSTable(dir, 4)
	injected := errors.New("stat injected")
	policy := vfs.NewFaultPolicy(vfs.FailAfterNthRule(vfs.OpStat, tableName, 1, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opts := Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSize:     1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := NewBuilder(opts)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	rt := &testRuntime{opts: opts}
	tbl, err := builder.Flush(rt, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NoError(t, tbl.ss.Close())
}
