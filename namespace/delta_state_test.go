package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeltaSnapshotSelectPairsChoosesHottestPages(t *testing.T) {
	parent := []byte("/bucket/hot")
	root := ReadRoot{
		Parent: parent,
		Pages: []ReadPageRef{
			{FenceKey: []byte("a"), PageID: []byte("rp00000000")},
			{FenceKey: []byte("m"), PageID: []byte("rp00000001")},
		},
	}
	makePair := func(pageID, name string) KVPair {
		raw, err := encodePageDeltaRecord(ListingDelta{
			Parent: parent,
			PageID: []byte(pageID),
			Name:   []byte(name),
			Kind:   EntryKindFile,
			Op:     DeltaOpAdd,
		})
		require.NoError(t, err)
		return KVPair{Key: encodePageDeltaLogKey(parent, []byte(pageID), uint64(len(name))), Value: raw}
	}
	snapshot := deltaSnapshot{
		parent:    cloneBytes(parent),
		pageLocal: []KVPair{makePair("rp00000000", "a1"), makePair("rp00000001", "z1"), makePair("rp00000001", "z2")},
		pageLocalByPage: map[string][]KVPair{
			"rp00000000": {makePair("rp00000000", "a1")},
			"rp00000001": {makePair("rp00000001", "z1"), makePair("rp00000001", "z2")},
		},
		pageCounts: map[string]int{
			"rp00000000": 1,
			"rp00000001": 2,
		},
		pageBytes: map[string]int{
			"rp00000000": 32,
			"rp00000001": 64,
		},
	}
	selected, pages, err := snapshot.selectPairs(root, true, 1, 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	_, ok := pages["rp00000001"]
	require.True(t, ok)
}
