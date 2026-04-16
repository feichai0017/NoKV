package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergePageEntriesWithDeltaPairs(t *testing.T) {
	parent := []byte("/bucket/hot")
	base := []Entry{
		{Name: []byte("file1"), Kind: EntryKindFile, MetaKey: encodeTruthKey([]byte("/bucket/hot/file1"))},
		{Name: []byte("file3"), Kind: EntryKindFile, MetaKey: encodeTruthKey([]byte("/bucket/hot/file3"))},
	}
	addRaw, err := encodePageDeltaRecord(ListingDelta{
		Parent: parent,
		PageID: []byte("rp00000000"),
		Name:   []byte("file2"),
		Kind:   EntryKindFile,
		Op:     DeltaOpAdd,
	})
	require.NoError(t, err)
	removeRaw, err := encodePageDeltaRecord(ListingDelta{
		Parent: parent,
		PageID: []byte("rp00000000"),
		Name:   []byte("file3"),
		Op:     DeltaOpRemove,
	})
	require.NoError(t, err)
	merged, err := mergePageEntriesWithDeltaPairs(parent, base, []KVPair{
		{Key: encodePageDeltaLogKey(parent, []byte("rp00000000"), 1), Value: addRaw},
		{Key: encodePageDeltaLogKey(parent, []byte("rp00000000"), 2), Value: removeRaw},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2"}, []string{
		string(merged[0].Name),
		string(merged[1].Name),
	})
}

func TestMergePageEntriesWithDeltaPairsUsesLatestRecord(t *testing.T) {
	parent := []byte("/bucket/hot")
	base := []Entry{
		{Name: []byte("file1"), Kind: EntryKindFile, MetaKey: encodeTruthKey([]byte("/bucket/hot/file1"))},
	}
	removeRaw, err := encodePageDeltaRecord(ListingDelta{
		Parent: parent,
		PageID: []byte("rp00000000"),
		Name:   []byte("file1"),
		Op:     DeltaOpRemove,
	})
	require.NoError(t, err)
	addRaw, err := encodePageDeltaRecord(ListingDelta{
		Parent: parent,
		PageID: []byte("rp00000000"),
		Name:   []byte("file1"),
		Kind:   EntryKindDirectory,
		Op:     DeltaOpAdd,
	})
	require.NoError(t, err)
	merged, err := mergePageEntriesWithDeltaPairs(parent, base, []KVPair{
		{Key: encodePageDeltaLogKey(parent, []byte("rp00000000"), 1), Value: removeRaw},
		{Key: encodePageDeltaLogKey(parent, []byte("rp00000000"), 2), Value: addRaw},
	})
	require.NoError(t, err)
	require.Len(t, merged, 1)
	require.Equal(t, "file1", string(merged[0].Name))
	require.Equal(t, EntryKindDirectory, merged[0].Kind)
}
