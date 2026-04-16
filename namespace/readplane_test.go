package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadRootCodecRoundTrip(t *testing.T) {
	root := ReadRoot{
		Parent:         []byte("/bucket/run"),
		RootGeneration: 4,
		Pages: []ReadPageRef{
			{FenceKey: []byte("a"), HighFence: []byte("m"), PageID: []byte("p0"), Count: 128, CoverageState: PageCoverageStateCovered, PublishedFrontier: 7, Generation: 3},
			{FenceKey: []byte("m"), PageID: []byte("p1"), Count: 256, CoverageState: PageCoverageStateDirty, PublishedFrontier: 9, Generation: 4},
		},
	}
	raw, err := encodeReadRoot(root)
	require.NoError(t, err)
	decoded, err := decodeReadRoot(raw)
	require.NoError(t, err)
	require.Equal(t, root.Parent, decoded.Parent)
	require.Equal(t, root.RootGeneration, decoded.RootGeneration)
	require.Equal(t, root.Pages, decoded.Pages)
}

func TestReadPageCodecRoundTrip(t *testing.T) {
	page := ReadPage{
		Parent:            []byte("/bucket/run"),
		PageID:            []byte("p0"),
		LowFence:          []byte("a"),
		HighFence:         []byte("m"),
		NextPageID:        []byte("p1"),
		PublishedFrontier: 7,
		Generation:        5,
		Entries: []Entry{
			{Name: []byte("alpha"), Kind: EntryKindFile},
			{Name: []byte("beta"), Kind: EntryKindDirectory},
		},
	}
	raw, err := encodeReadPage(page)
	require.NoError(t, err)
	decoded, err := decodeReadPage(raw)
	require.NoError(t, err)
	require.Equal(t, page.PageID, decoded.PageID)
	require.Equal(t, page.LowFence, decoded.LowFence)
	require.Equal(t, page.HighFence, decoded.HighFence)
	require.Equal(t, page.NextPageID, decoded.NextPageID)
	require.Equal(t, page.Entries, decoded.Entries)
}

func TestBuildReadPlaneSplitsOrderedMicroPages(t *testing.T) {
	root, pages, err := BuildReadPlane([]byte("/bucket/run"), []Entry{
		{Name: []byte("delta"), Kind: EntryKindFile, MetaKey: []byte("ignored")},
		{Name: []byte("alpha"), Kind: EntryKindDirectory, MetaKey: []byte("ignored")},
		{Name: []byte("charlie"), Kind: EntryKindFile, MetaKey: []byte("ignored")},
		{Name: []byte("bravo"), Kind: EntryKindFile, MetaKey: []byte("ignored")},
	}, 2)
	require.NoError(t, err)
	require.Len(t, pages, 2)
	require.Equal(t, []byte("/bucket/run"), root.Parent)
	require.Equal(t, []byte("alpha"), root.Pages[0].FenceKey)
	require.Equal(t, []byte("charlie"), root.Pages[0].HighFence)
	require.Equal(t, []byte("charlie"), root.Pages[1].FenceKey)
	require.Nil(t, root.Pages[1].HighFence)
	require.Equal(t, []byte("alpha"), pages[0].LowFence)
	require.Equal(t, []byte("charlie"), pages[0].HighFence)
	require.Equal(t, []byte("charlie"), pages[1].LowFence)
	require.Nil(t, pages[1].HighFence)
	require.Equal(t, []byte("rp00000001"), pages[0].NextPageID)
	require.Equal(t, []string{"alpha", "bravo"}, []string{string(pages[0].Entries[0].Name), string(pages[0].Entries[1].Name)})
	require.Equal(t, []string{"charlie", "delta"}, []string{string(pages[1].Entries[0].Name), string(pages[1].Entries[1].Name)})
	require.Nil(t, pages[0].Entries[0].MetaKey)
}

func TestListReadPlanePaginatesOrderedEntries(t *testing.T) {
	root, pages, err := BuildReadPlane([]byte("/bucket/run"), []Entry{
		{Name: []byte("alpha"), Kind: EntryKindDirectory},
		{Name: []byte("bravo"), Kind: EntryKindFile},
		{Name: []byte("charlie"), Kind: EntryKindFile},
		{Name: []byte("delta"), Kind: EntryKindDirectory},
	}, 2)
	require.NoError(t, err)
	root, pages = assignReadPlanePublication(root, pages, 1, 0)

	view, err := NewReadPlaneView(root, pages)
	require.NoError(t, err)
	first, cursor, stats, err := view.List(Cursor{}, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "bravo", "charlie"}, []string{string(first[0].Name), string(first[1].Name), string(first[2].Name)})
	require.Equal(t, []byte("rp00000001"), cursor.PageID)
	require.Equal(t, uint32(1), cursor.EntryOffset)
	require.Equal(t, 2, stats.PagesVisited)
	require.Equal(t, []byte("M|/bucket/run/alpha"), first[0].MetaKey)

	second, next, stats2, err := view.List(cursor, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"delta"}, []string{string(second[0].Name)})
	require.Equal(t, Cursor{}, next)
	require.Equal(t, 1, stats2.PagesVisited)
	require.Equal(t, []byte("M|/bucket/run/delta"), second[0].MetaKey)
}

func TestReadPlaneViewListMatchesPrototype(t *testing.T) {
	root, pages, err := BuildReadPlane([]byte("/bucket/run"), []Entry{
		{Name: []byte("alpha"), Kind: EntryKindDirectory},
		{Name: []byte("bravo"), Kind: EntryKindFile},
		{Name: []byte("charlie"), Kind: EntryKindFile},
		{Name: []byte("delta"), Kind: EntryKindDirectory},
	}, 2)
	require.NoError(t, err)
	root, pages = assignReadPlanePublication(root, pages, 1, 0)
	view, err := NewReadPlaneView(root, pages)
	require.NoError(t, err)

	got, gotNext, _, err := view.List(Cursor{}, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "bravo", "charlie"}, []string{string(got[0].Name), string(got[1].Name), string(got[2].Name)})
	require.Equal(t, []byte("rp00000001"), gotNext.PageID)
}

func TestReadPlaneViewRejectsMismatchedRootInterval(t *testing.T) {
	root, pages, err := BuildReadPlane([]byte("/bucket/run"), []Entry{
		{Name: []byte("alpha"), Kind: EntryKindDirectory},
		{Name: []byte("bravo"), Kind: EntryKindFile},
		{Name: []byte("charlie"), Kind: EntryKindFile},
	}, 2)
	require.NoError(t, err)
	root, pages = assignReadPlanePublication(root, pages, 1, 0)
	root.Pages[0].HighFence = []byte("wrong")

	_, err = NewReadPlaneView(root, pages)
	require.ErrorIs(t, err, ErrCodecCorrupted)
}
