package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreCreateLookupListDelete(t *testing.T) {
	kv := NewMapStore()
	store := NewStore(kv, 4)

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))

	meta, err := store.Lookup([]byte("/bucket/a/file1"))
	require.NoError(t, err)
	require.Equal(t, []byte("m1"), meta)

	entries, cursor, err := store.List([]byte("/bucket/a"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Empty(t, cursor.PageID)

	require.NoError(t, store.Delete([]byte("/bucket/a/file1")))
	entries, _, err = store.List([]byte("/bucket/a"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file2", string(entries[0].Name))
}

func TestStoreCreateRejectsDuplicatePath(t *testing.T) {
	kv := NewMapStore()
	store := NewStore(kv, 4)

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.ErrorIs(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m2")), ErrPathExists)
}

func TestStoreListPaginatesAcrossPages(t *testing.T) {
	kv := NewMapStore()
	store := NewStore(kv, 2)
	for _, name := range []string{"a0", "a1", "b0", "b1", "c0", "c1"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}

	first, cursor, err := store.List([]byte("/bucket/hot"), Cursor{}, 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotEmpty(t, cursor.PageID)
	require.NotEmpty(t, cursor.LastName)

	second, next, err := store.List([]byte("/bucket/hot"), cursor, 8)
	require.NoError(t, err)
	require.NotEmpty(t, second)
	if len(next.PageID) > 0 {
		require.NotEmpty(t, next.LastName)
	}
}
