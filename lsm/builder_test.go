package lsm

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
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

	bd, err := builder.done()
	require.NoError(t, err)
	require.NotNil(t, bd.index)
	require.NotEmpty(t, bd.index)

	var tableIndex pb.TableIndex
	require.NoError(t, proto.Unmarshal(bd.index, &tableIndex))
	require.Equal(t, uint32(builder.staleDataSize), tableIndex.GetStaleDataSize())
	require.Equal(t, uint64(builder.valueSize), tableIndex.GetValueSize())
}

func TestTableBuilderFinishAndEntryValueLen(t *testing.T) {
	opt := &Options{
		BlockSize:          128,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
	}

	builder := newTableBuiler(opt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b"), 1), []byte("value-b")))

	buf, err := builder.finish()
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	builder.Close()

	var vp kv.ValuePtr
	vp.Fid = 1
	vp.Len = 123
	ptrBytes := vp.Encode()
	entry := kv.NewEntry([]byte("ptr"), ptrBytes)
	entry.Meta |= kv.BitValuePointer
	require.Equal(t, uint32(123), entryValueLen(entry))
}

func TestTableBuilderFlushRenameFailureCleansTempFile(t *testing.T) {
	dir := t.TempDir()
	tableName := utils.FileNameSSTable(dir, 1)
	injected := errors.New("rename injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRenameRule("", tableName, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opt := &Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := newTableBuiler(opt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	lm := &levelsRuntime{opt: opt}
	_, err := builder.flush(lm, tableName)
	require.ErrorIs(t, err, injected)

	tmpFiles, globErr := filepath.Glob(tableName + ".tmp.*")
	require.NoError(t, globErr)
	require.Empty(t, tmpFiles)
}

func TestTableBuilderFlushStrictPathDoesNotReopenFinalSST(t *testing.T) {
	dir := t.TempDir()
	tableName := utils.FileNameSSTable(dir, 2)
	injected := errors.New("open final injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpOpenFile, tableName, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opt := &Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := newTableBuiler(opt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	lm := &levelsRuntime{opt: opt}
	tbl, err := builder.flush(lm, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.ss)
	require.NoError(t, tbl.ss.Close())
}

func TestTableBuilderFlushFastPathSkipsPreStat(t *testing.T) {
	dir := t.TempDir()
	tableName := utils.FileNameSSTable(dir, 3)
	injected := errors.New("stat injected")
	policy := vfs.NewFaultPolicy(vfs.FailAfterNthRule(vfs.OpStat, tableName, 1, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opt := &Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       false,
	}
	builder := newTableBuiler(opt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	lm := &levelsRuntime{opt: opt}
	tbl, err := builder.flush(lm, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NoError(t, tbl.ss.Close())

	_, statErr := os.Stat(tableName)
	require.NoError(t, statErr)
}

func TestTableBuilderFlushStrictPathSkipsPreStat(t *testing.T) {
	dir := t.TempDir()
	tableName := utils.FileNameSSTable(dir, 4)
	injected := errors.New("stat injected")
	policy := vfs.NewFaultPolicy(vfs.FailAfterNthRule(vfs.OpStat, tableName, 1, injected))
	fs := vfs.NewFaultFSWithPolicy(nil, policy)

	opt := &Options{
		FS:                 fs,
		WorkDir:            dir,
		BlockSize:          4 << 10,
		SSTableMaxSz:       1 << 20,
		BloomFalsePositive: 0.0,
		ManifestSync:       true,
	}
	builder := newTableBuiler(opt)
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 1), []byte("value-a")))

	lm := &levelsRuntime{opt: opt}
	tbl, err := builder.flush(lm, tableName)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	require.NoError(t, tbl.ss.Close())

	_, statErr := os.Stat(tableName)
	require.NoError(t, statErr)
}
