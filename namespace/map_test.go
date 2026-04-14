package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListingMapListReturnsSnapshotConsistentPageLocalOrdering(t *testing.T) {
	idx := NewListingMap(4)
	parent := []byte("/bucket/a")
	for _, name := range []string{"gamma", "alpha", "beta", "omega"} {
		require.NoError(t, idx.AddChild(parent, Entry{
			Name:    []byte(name),
			Kind:    EntryKindFile,
			MetaKey: []byte("/bucket/a/" + name),
		}))
	}

	entries, cursor, err := idx.List(parent, Cursor{}, 8)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.NotEmpty(t, cursor.PageID)

	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		seen[string(entry.Name)] = struct{}{}
	}
	require.Contains(t, seen, "alpha")
	require.Contains(t, seen, "beta")
	require.Contains(t, seen, "gamma")
	require.Contains(t, seen, "omega")
}

func TestListingMapPaginationAdvancesWithinAndAcrossPages(t *testing.T) {
	idx := NewListingMap(2)
	parent := []byte("/bucket/hot")
	for _, name := range []string{"a0", "a1", "b0", "b1", "c0", "c1"} {
		require.NoError(t, idx.AddChild(parent, Entry{
			Name:    []byte(name),
			Kind:    EntryKindFile,
			MetaKey: []byte("/bucket/hot/" + name),
		}))
	}

	first, cursor, err := idx.List(parent, Cursor{}, 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotEmpty(t, cursor.PageID)
	require.NotEmpty(t, cursor.LastName)

	second, next, err := idx.List(parent, cursor, 4)
	require.NoError(t, err)
	require.NotEmpty(t, second)

	seen := map[string]struct{}{}
	for _, entry := range append(first, second...) {
		seen[string(entry.Name)] = struct{}{}
	}
	require.GreaterOrEqual(t, len(seen), 4)
	if len(next.PageID) > 0 {
		require.NotEmpty(t, next.LastName)
	}
}

func TestListingMapRemoveChild(t *testing.T) {
	idx := NewListingMap(4)
	parent := []byte("/bucket/a")
	require.NoError(t, idx.AddChild(parent, Entry{
		Name:    []byte("file1"),
		Kind:    EntryKindFile,
		MetaKey: []byte("/bucket/a/file1"),
	}))

	require.NoError(t, idx.RemoveChild(parent, []byte("file1")))
	entries, _, err := idx.List(parent, Cursor{}, 8)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.ErrorIs(t, idx.RemoveChild(parent, []byte("file1")), ErrChildNotFound)
}
