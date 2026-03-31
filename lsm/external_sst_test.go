package lsm

import (
	"bytes"
	"path/filepath"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestBuildExternalSSTRoundTripImport(t *testing.T) {
	dir := t.TempDir()
	opt := newTestLSMOptions(dir, nil)
	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	largeValue := bytes.Repeat([]byte("v"), 4096)
	entries := []*kv.Entry{
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("alpha"), 3), []byte("a")),
		kv.NewEntry(kv.InternalKey(kv.CFWrite, []byte("alpha"), 3), []byte("aw")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("beta"), 2), largeValue),
	}
	sort.Slice(entries, func(i, j int) bool {
		return utils.CompareInternalKeys(entries[i].Key, entries[j].Key) < 0
	})

	sstPath := filepath.Join(t.TempDir(), "external.sst")
	meta, err := BuildExternalSST(sstPath, entries, opt)
	require.NoError(t, err)
	require.Equal(t, uint64(len(entries)), meta.EntryCount)
	require.NotZero(t, meta.SizeBytes)
	require.True(t, bytes.Equal(entries[0].Key, meta.SmallestKey))
	require.True(t, bytes.Equal(entries[len(entries)-1].Key, meta.LargestKey))

	require.NoError(t, lsm.ImportExternalSST([]string{sstPath}))

	for _, entry := range entries {
		got, err := lsm.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, entry.Value, got.Value)
		got.DecrRef()
	}
}

func TestBuildExternalSSTRejectsUnsortedEntries(t *testing.T) {
	opt := newTestLSMOptions(t.TempDir(), nil)
	entries := []*kv.Entry{
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("beta"), 2), []byte("b")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("alpha"), 3), []byte("a")),
	}

	_, err := BuildExternalSST(filepath.Join(t.TempDir(), "external.sst"), entries, opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not strictly increasing")
}
