package namespace

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreCreateLookupListDelete(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))

	meta, err := store.Lookup([]byte("/bucket/a/file1"))
	require.NoError(t, err)
	require.Equal(t, []byte("m1"), meta)

	entries, cursor, _, err := store.RepairAndList([]byte("/bucket/a"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Empty(t, cursor.PageID)

	require.NoError(t, store.Delete([]byte("/bucket/a/file1")))
	entries, _, _, err = store.RepairAndList([]byte("/bucket/a"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file2", string(entries[0].Name))
}

func TestStoreCreateRejectsDuplicatePath(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.ErrorIs(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m2")), ErrPathExists)
}

func TestStoreListPaginatesAcrossPages(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()
	for _, name := range []string{"a0", "a1", "b0", "b1", "c0", "c1"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}

	first, cursor, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.True(t, len(cursor.PageID) > 0 || len(cursor.LastName) > 0)
	require.NotEmpty(t, cursor.LastName)

	second, next, _, err := store.RepairAndList([]byte("/bucket/hot"), cursor, 8)
	require.NoError(t, err)
	require.NotEmpty(t, second)
	if len(next.PageID) > 0 {
		require.NotEmpty(t, next.LastName)
	}
}

func TestStoreCreatePersistsListingDelta(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))

	deltas, err := store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/a")), nil, 0)
	require.NoError(t, err)
	require.Len(t, deltas, 1)

	delta, err := decodeListingDeltaFromKV([]byte("/bucket/a"), deltas[0].Key, deltas[0].Value)
	require.NoError(t, err)
	require.Equal(t, DeltaOpAdd, delta.Op)
	require.Equal(t, []byte("/bucket/a"), delta.Parent)
	require.Equal(t, []byte("file1"), delta.Name)
}

func TestStoreMaterializeFoldsDeltasIntoPages(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))

	deltas, err := store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/a")), nil, 0)
	require.NoError(t, err)
	require.Len(t, deltas, 2)

	before, err := store.scanTruthEntries([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Len(t, before, 2)

	stats, err := store.Materialize([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Equal(t, 2, stats.DeltasFolded)
	require.Equal(t, 2, stats.EntriesMaterialized)
	require.GreaterOrEqual(t, stats.PagesWritten, 1)

	deltas, err = store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/a")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, deltas)

	root, pages, ok, err := store.LoadReadPlane([]byte("/bucket/a"))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, root.Pages)
	require.NotEmpty(t, pages)

	after, _, _, err := store.RepairAndList([]byte("/bucket/a"), Cursor{}, 16)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestStoreMaterializeDeltaPagesLeavesUnprocessedDeltasVisible(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	var shard0Name, shard1Name string
	for i := 0; i < 128 && (shard0Name == "" || shard1Name == ""); i++ {
		name := "entry-" + string(rune('a'+i))
		switch store.shardFor([]byte(name)) {
		case 0:
			if shard0Name == "" {
				shard0Name = name
			}
		case 1:
			if shard1Name == "" {
				shard1Name = name
			}
		}
	}
	require.NotEmpty(t, shard0Name)
	require.NotEmpty(t, shard1Name)

	require.NoError(t, store.Create([]byte("/bucket/hot/"+shard0Name), EntryKindFile, []byte("m0")))
	require.NoError(t, store.Create([]byte("/bucket/hot/"+shard1Name), EntryKindFile, []byte("m1")))

	stats, err := store.MaterializeDeltaPages([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, stats.DeltaPagesFolded)

	remaining, err := store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Len(t, remaining, 1)

	entries, cursor, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Empty(t, cursor.PageID)
}

func TestStoreCreateAfterMaterializeListRepairsWithoutDeltaReads(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, err := store.Materialize([]byte("/bucket/hot"))
	require.NoError(t, err)

	entries, _, beforeStats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, 0, beforeStats.DeltasRead)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))

	entries, _, afterStats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, 0, afterStats.DeltasRead)
}

func TestStoreCreateAfterMaterializeRepairListRecertifiesPageLocalDelta(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))

	bootstrap, err := store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, bootstrap)

	pageLocal, err := store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Len(t, pageLocal, 1)

	delta, err := decodePageDeltaFromKV([]byte("/bucket/hot"), pageLocal[0].Key, pageLocal[0].Value)
	require.NoError(t, err)
	require.Equal(t, DeltaOpAdd, delta.Op)
	require.Equal(t, []byte("file3"), delta.Name)

	entries, _, stats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, 0, stats.DeltasRead)

	pageLocal, err = store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, pageLocal)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestStoreListRejectsUncoveredParent(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	_, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStoreListRejectsDirtyPageUntilMaterialized(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	entries, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))
	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	entries, _, _, err = store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestStoreListFailsStopOnCorruptedCoverageMetadata(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadRootKey([]byte("/bucket/hot")),
		Value: []byte("corrupted-root"),
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)

	_, _, _, err = store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)

	rebuildStats, err := store.Rebuild([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, rebuildStats.PagesWritten, 1)

	entries, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStoreListFailsStopOnMissingCoveredPage(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind: MutationDelete,
		Key:  encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey),
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)
}

func TestStoreListRejectsStaleButValidCoverageMetadata(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))

	pageLocal, err := store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Len(t, pageLocal, 1)

	dirtyRoot, ok, err := store.loadReadRoot([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	for i := range dirtyRoot.Pages {
		dirtyRoot.Pages[i].CoverageState = PageCoverageStateCovered
	}
	raw, err := encodeReadRoot(dirtyRoot)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadRootKey([]byte("/bucket/hot")),
		Value: raw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestStoreListFailsStopOnPartialFreshPublish(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	kv, ok := store.kv.(*testKV)
	require.True(t, ok)

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))

	kv.failApplyAfter = 1
	kv.failApplyErr = errors.New("namespace: injected fresh publish failure")
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.Error(t, err)
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)
	_, _, _, err = store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)

	rebuildStats, err := store.Rebuild([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, rebuildStats.PagesWritten, 1)

	entries, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestStoreListFailsStopOnPartialPageFoldPublish(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	kv, ok := store.kv.(*testKV)
	require.True(t, ok)

	for _, name := range []string{"file1", "file2", "file5", "file6"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 2)
	require.NoError(t, err)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("file3")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file4"), EntryKindFile, []byte("file4")))

	kv.failApplyAfter = 1
	kv.failApplyErr = errors.New("namespace: injected fold publish failure")
	_, _, _, err = store.materializeReadPlane([]byte("/bucket/hot"), 2, 0)
	require.Error(t, err)
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.True(t, errors.Is(err, ErrCodecCorrupted) || errors.Is(err, ErrCoverageIncomplete))

	_, _, _, err = store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)

	rebuildStats, err := store.Rebuild([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, rebuildStats.PagesWritten, 1)

	entries, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 6)
}

func TestStoreRepairAndListRecertifiesBootstrapParent(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	for _, name := range []string{"file1", "file2", "file3"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}

	_, _, _, err := store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	root, _, ok, err := store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, root.Pages)
	for _, ref := range root.Pages {
		require.True(t, ref.CoverageState.IsCovered())
	}

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestStoreRepairAndListRecertifiesDirtyMaterializedPage(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	for _, name := range []string{"file1", "file2"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("file3")))
	root, _, ok, err := store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	dirtySeen := false
	for _, ref := range root.Pages {
		if ref.CoverageState == PageCoverageStateDirty {
			dirtySeen = true
		}
	}
	require.True(t, dirtySeen)
	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	root, _, ok, err = store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, root.Pages)
	for _, ref := range root.Pages {
		require.True(t, ref.CoverageState.IsCovered())
	}

	pageLocal, err := store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, pageLocal)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestStoreRepairAndListAdvancesPublishedFrontier(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	for _, name := range []string{"file1", "file2"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	before := make(map[string]uint64, len(root.Pages))
	for _, ref := range root.Pages {
		require.True(t, ref.CoverageState.IsCovered())
		before[string(ref.PageID)] = ref.PublishedFrontier
	}

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("file3")))
	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	_, _, _, err = store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)

	root, _, ok, err := store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, root.Pages)

	advanced := false
	for _, ref := range root.Pages {
		require.True(t, ref.CoverageState.IsCovered())
		if prev, ok := before[string(ref.PageID)]; ok && ref.PublishedFrontier > prev {
			advanced = true
		}
	}
	require.True(t, advanced)
}

func TestStoreListReadPlaneLoadsCurrentPageByKey(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	kv, ok := store.kv.(*testKV)
	require.True(t, ok)

	for _, name := range []string{"file1", "file2", "file3", "file4"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}
	root, pages, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 2)
	require.NoError(t, err)
	require.Len(t, root.Pages, 2)
	require.Len(t, pages, 2)

	store.invalidateReadRoot([]byte("/bucket/hot"))

	lpPrefix := encodeReadPagePrefix([]byte("/bucket/hot"))
	beforePrefixScans := kv.scanPrefixCallCount(lpPrefix)
	firstPageKey := encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey)
	beforeFirstPageGets := kv.getCallCount(firstPageKey)

	entries, cursor, stats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2"}, []string{string(entries[0].Name), string(entries[1].Name)})
	require.NotEmpty(t, cursor.PageID)
	require.Equal(t, 1, stats.PagesLoaded)
	require.Equal(t, beforePrefixScans, kv.scanPrefixCallCount(lpPrefix))
	require.Equal(t, beforeFirstPageGets+1, kv.getCallCount(firstPageKey))
}

func TestStoreMaterializeFoldsPageLocalDeltaWithoutFullRebuild(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 2)
	require.NoError(t, err)

	require.NoError(t, store.Create([]byte("/bucket/hot/file4"), EntryKindFile, []byte("m4")))
	pageLocal, err := store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Len(t, pageLocal, 1)

	_, _, stats, err := store.materializeReadPlane([]byte("/bucket/hot"), 2, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.DeltasFolded)
	require.Equal(t, 1, stats.DeltaPagesFolded)
	require.Equal(t, 1, stats.PagesWritten)

	pageLocal, err = store.kv.ScanPrefix(encodePageDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, pageLocal)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2", "file3", "file4"}, []string{
		string(entries[0].Name),
		string(entries[1].Name),
		string(entries[2].Name),
		string(entries[3].Name),
	})
}

func TestStoreMaterializePageLocalDeltaCanSplitPage(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	for _, name := range []string{"file1", "file2", "file5", "file6"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}
	root, pages, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 2)
	require.NoError(t, err)
	require.Len(t, root.Pages, 2)
	require.Len(t, pages, 2)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("file3")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file4"), EntryKindFile, []byte("file4")))

	_, _, stats, err := store.materializeReadPlane([]byte("/bucket/hot"), 2, 0)
	require.NoError(t, err)
	require.Equal(t, 2, stats.DeltasFolded)
	require.Equal(t, 1, stats.DeltaPagesFolded)
	require.GreaterOrEqual(t, stats.PagesWritten, 1)

	persistedRoot, persistedPages, ok, err := store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, persistedRoot.Pages, 3)
	require.Len(t, persistedPages, 3)

	entries, _, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2", "file3", "file4", "file5", "file6"}, []string{
		string(entries[0].Name),
		string(entries[1].Name),
		string(entries[2].Name),
		string(entries[3].Name),
		string(entries[4].Name),
		string(entries[5].Name),
	})
}

func TestStoreMaterializePersistedParentRejectsBootstrapDelta(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 2)
	require.NoError(t, err)

	raw, err := encodeListingDelta(ListingDelta{
		Parent: []byte("/bucket/hot"),
		Name:   []byte("orphan"),
		Kind:   EntryKindFile,
		Op:     DeltaOpAdd,
	})
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeListingDeltaKey([]byte("/bucket/hot"), encodePageID(0), []byte("orphan")),
		Value: raw,
	}}))

	_, err = store.Materialize([]byte("/bucket/hot"))
	require.ErrorIs(t, err, ErrRebuildRequired)
}

func TestStoreStatsReflectMaterializedAndDeltaState(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))

	stats, err := store.Stats([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Equal(t, 0, stats.MaterializedPages)
	require.Equal(t, 0, stats.MaterializedEntries)
	require.Equal(t, 2, stats.DeltaRecords)
	require.GreaterOrEqual(t, stats.DistinctDeltaPages, 1)

	materialized, err := store.Materialize([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Equal(t, 2, materialized.DeltasFolded)
	require.Equal(t, 2, materialized.EntriesMaterialized)
	require.GreaterOrEqual(t, materialized.PagesWritten, 1)

	stats, err = store.Stats([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Equal(t, 0, stats.DeltaRecords)
	require.Equal(t, 2, stats.MaterializedEntries)
	require.GreaterOrEqual(t, stats.MaterializedPages, 1)
}

func TestStoreListAdvancesCursorToNextPageWhenCurrentPageIsExhausted(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	first, cursor, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NotEmpty(t, cursor.PageID)

	second, next, _, err := store.RepairAndList([]byte("/bucket/hot"), cursor, 1)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Empty(t, next.PageID)
}

func TestStoreListColdTruthFirstPageBootstrap(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	kv, ok := store.kv.(*testKV)
	require.True(t, ok)

	require.NoError(t, store.kv.Apply([]Mutation{
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/a")), Value: encodeTruthValue(EntryKindDirectory, []byte("a"))},
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/a/x1")), Value: encodeTruthValue(EntryKindFile, []byte("x1"))},
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/a/x2")), Value: encodeTruthValue(EntryKindFile, []byte("x2"))},
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/b")), Value: encodeTruthValue(EntryKindDirectory, []byte("b"))},
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/b/y1")), Value: encodeTruthValue(EntryKindFile, []byte("y1"))},
		{Kind: MutationPut, Key: encodeTruthKey([]byte("/bucket/hot/c")), Value: encodeTruthValue(EntryKindFile, []byte("c"))},
	}))

	bootstrap, err := store.kv.ScanPrefix(encodeListingDeltaParentPrefix([]byte("/bucket/hot")), nil, 0)
	require.NoError(t, err)
	require.Empty(t, bootstrap)

	truthPrefix := encodeTruthKey([]byte("/bucket/hot"))
	if truthPrefix[len(truthPrefix)-1] != '/' {
		truthPrefix = append(truthPrefix, '/')
	}
	beforeScans := kv.scanPrefixCallCount(truthPrefix)
	beforeWindowScans := kv.windowedScanCount(truthPrefix)

	first, cursor, stats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, []string{string(first[0].Name), string(first[1].Name)})
	require.NotEmpty(t, cursor.LastName)
	require.True(t, len(cursor.PageID) > 0 || len(cursor.LastName) > 0)
	require.Greater(t, stats.EntriesScanned, 0)
	require.Greater(t, kv.scanPrefixCallCount(truthPrefix), beforeScans)
	require.Greater(t, kv.windowedScanCount(truthPrefix), beforeWindowScans)

	second, next, _, err := store.RepairAndList([]byte("/bucket/hot"), cursor, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, []string{string(second[0].Name)})
	require.Empty(t, next.LastName)
	require.Empty(t, next.PageID)
}

func TestStoreRepairListBootstrapsCertifiedReadPlane(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	kv, ok := store.kv.(*testKV)
	require.True(t, ok)

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))

	ldPrefix := encodeListingDeltaParentPrefix([]byte("/bucket/hot"))
	beforeLDScans := kv.scanPrefixCallCount(ldPrefix)

	entries, cursor, stats, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2"}, []string{string(entries[0].Name), string(entries[1].Name)})
	require.Empty(t, cursor.PageID)
	require.Empty(t, cursor.LastName)
	require.Equal(t, 0, stats.DeltasRead)
	require.GreaterOrEqual(t, kv.scanPrefixCallCount(ldPrefix), beforeLDScans)

	entries, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2"}, []string{string(entries[0].Name), string(entries[1].Name)})
}

func openTestStore(t *testing.T, shards int) (*Store, func()) {
	t.Helper()
	store := NewStore(newTestKV(), shards)
	mustStoreCreate(t, store, "/bucket", EntryKindDirectory, []byte("bucket"))
	mustStoreCreate(t, store, "/bucket/a", EntryKindDirectory, []byte("a"))
	mustStoreCreate(t, store, "/bucket/hot", EntryKindDirectory, []byte("hot"))
	return store, func() {}
}

type testKV struct {
	data            map[string][]byte
	getCalls        map[string]int
	scanPrefixCalls map[string]int
	windowedScans   map[string]int
	failApplyAfter  int
	failApplyErr    error
}

func newTestKV() *testKV {
	return &testKV{
		data:            make(map[string][]byte),
		getCalls:        make(map[string]int),
		scanPrefixCalls: make(map[string]int),
		windowedScans:   make(map[string]int),
	}
}

func (m *testKV) Apply(batch []Mutation) error {
	limit := len(batch)
	fail := false
	failErr := m.failApplyErr
	if m.failApplyAfter > 0 && m.failApplyAfter < limit {
		limit = m.failApplyAfter
		fail = true
	}
	for _, mut := range batch[:limit] {
		key := string(mut.Key)
		switch mut.Kind {
		case MutationPut:
			m.data[key] = append([]byte(nil), mut.Value...)
		case MutationDelete:
			delete(m.data, key)
		}
	}
	if fail {
		m.failApplyAfter = 0
		if failErr == nil {
			failErr = errors.New("namespace: injected partial apply failure")
		}
		return failErr
	}
	return nil
}

func (m *testKV) Get(key []byte) ([]byte, error) {
	m.getCalls[string(key)]++
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), val...), nil
}

func (m *testKV) ScanPrefix(prefix, start []byte, limit int) ([]KVPair, error) {
	m.scanPrefixCalls[string(prefix)]++
	if len(start) > 0 || limit > 0 {
		m.windowedScans[string(prefix)]++
	}
	keys := make([]string, 0, len(m.data))
	lowerBound := string(prefix)
	if len(start) > 0 && string(start) > lowerBound {
		lowerBound = string(start)
	}
	for key := range m.data {
		if strings.HasPrefix(key, string(prefix)) && key >= lowerBound {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]KVPair, 0, len(keys))
	for _, key := range keys {
		out = append(out, KVPair{
			Key:   []byte(key),
			Value: append([]byte(nil), m.data[key]...),
		})
	}
	return out, nil
}

func (m *testKV) getCallCount(key []byte) int {
	return m.getCalls[string(key)]
}

func (m *testKV) scanPrefixCallCount(prefix []byte) int {
	return m.scanPrefixCalls[string(prefix)]
}

func (m *testKV) windowedScanCount(prefix []byte) int {
	return m.windowedScans[string(prefix)]
}

func TestStoreMaterializeReadPlanePersistsOrderedMicroPages(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file3"), EntryKindFile, []byte("m3")))
	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))

	root, pages, err := store.MaterializeReadPlane([]byte("/bucket/a"), 2)
	require.NoError(t, err)
	require.Len(t, root.Pages, 2)
	require.Len(t, pages, 2)

	persistedRoot, persistedPages, ok, err := store.LoadReadPlane([]byte("/bucket/a"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, root, persistedRoot)
	require.Equal(t, pages, persistedPages)

	view, err := NewReadPlaneView(persistedRoot, persistedPages)
	require.NoError(t, err)
	entries, next, _, err := view.List(Cursor{}, 8)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2", "file3"}, []string{string(entries[0].Name), string(entries[1].Name), string(entries[2].Name)})
	require.Empty(t, next.PageID)
}

func TestStoreMaterializeReadPlaneReflectsDeletes(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))
	require.NoError(t, store.Create([]byte("/bucket/a/file3"), EntryKindFile, []byte("m3")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/a"), 2)
	require.NoError(t, err)

	require.NoError(t, store.Delete([]byte("/bucket/a/file2")))
	_, _, err = store.MaterializeReadPlane([]byte("/bucket/a"), 2)
	require.NoError(t, err)

	root, pages, ok, err := store.LoadReadPlane([]byte("/bucket/a"))
	require.NoError(t, err)
	require.True(t, ok)
	view, err := NewReadPlaneView(root, pages)
	require.NoError(t, err)
	entries, _, _, err := view.List(Cursor{}, 8)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file3"}, []string{string(entries[0].Name), string(entries[1].Name)})
}

func TestStoreVerifyDetectsDriftAndRebuildRestoresReadPlane(t *testing.T) {
	store, cleanup := openTestStore(t, 4)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/a/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/a"), 2)
	require.NoError(t, err)

	okStats, err := store.Verify([]byte("/bucket/a"))
	require.NoError(t, err)
	require.True(t, okStats.Consistent)
	require.Empty(t, okStats.Membership.MissingNames)
	require.Empty(t, okStats.Membership.ExtraNames)

	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind: MutationDelete,
		Key:  encodeReadRootKey([]byte("/bucket/a")),
	}}))
	store.invalidateReadRoot([]byte("/bucket/a"))

	drifted, err := store.Verify([]byte("/bucket/a"))
	require.NoError(t, err)
	require.False(t, drifted.Consistent)
	require.Equal(t, []string{"file1", "file2"}, drifted.Membership.MissingNames)

	rebuilt, err := store.Rebuild([]byte("/bucket/a"))
	require.NoError(t, err)
	require.Equal(t, 2, rebuilt.TruthEntries)
	require.Equal(t, 0, rebuilt.DeltaRecordsCleared)
	require.GreaterOrEqual(t, rebuilt.PagesWritten, 1)

	after, err := store.Verify([]byte("/bucket/a"))
	require.NoError(t, err)
	require.True(t, after.Consistent)
}

func TestStoreVerifyReportsDirtyPageAsUncovered(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)

	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, stats.Membership.Consistent)
	require.True(t, stats.Certificate.Consistent)
	require.True(t, stats.Publication.Consistent)
	require.True(t, stats.Consistent)
	require.Greater(t, stats.Certificate.DirtyPages, 0)
	require.Greater(t, stats.Certificate.UncoveredPages, 0)
	require.Empty(t, stats.Certificate.CoveredPendingDeltaIDs)
}

func TestStoreVerifyCertificateDetectsCoveredPendingDelta(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	_, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NoError(t, store.Create([]byte("/bucket/hot/file3"), EntryKindFile, []byte("m3")))

	root, ok, err := store.loadReadRoot([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	for i := range root.Pages {
		root.Pages[i].CoverageState = PageCoverageStateCovered
	}
	raw, err := encodeReadRoot(root)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadRootKey([]byte("/bucket/hot")),
		Value: raw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, stats.Membership.Consistent)
	require.False(t, stats.Certificate.Consistent)
	require.False(t, stats.Consistent)
	require.NotEmpty(t, stats.Certificate.CoveredPendingDeltaIDs)
}

func TestStoreVerifyCertificateDetectsRootPageMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind: MutationDelete,
		Key:  encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey),
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, stats.Certificate.RootPageMismatch)
	require.False(t, stats.Certificate.Consistent)
	require.False(t, stats.Consistent)
}

func TestStoreVerifyCertificateDetectsGenerationMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	raw, err := store.kv.Get(encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey))
	require.NoError(t, err)
	page, err := decodeReadPage(raw)
	require.NoError(t, err)
	page.Generation++
	pageRaw, err := encodeReadPage(page)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey),
		Value: pageRaw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, stats.Membership.Consistent)
	require.False(t, stats.Certificate.Consistent)
	require.False(t, stats.Consistent)
	require.NotEmpty(t, stats.Certificate.GenerationMismatchIDs)
}

func TestStoreVerifyCertificateDetectsRootGenerationMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	root.RootGeneration = 0
	raw, err := encodeReadRoot(root)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadRootKey([]byte("/bucket/hot")),
		Value: raw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.False(t, stats.Certificate.Consistent)
	require.False(t, stats.Publication.Consistent)
	require.False(t, stats.Consistent)
	require.True(t, stats.Certificate.RootGenerationMismatch)
	require.NotEmpty(t, stats.Publication.GenerationRollbackIDs)
}

func TestStoreListFailsStopOnFrontierMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	raw, err := store.kv.Get(encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey))
	require.NoError(t, err)
	page, err := decodeReadPage(raw)
	require.NoError(t, err)
	page.PublishedFrontier++
	pageRaw, err := encodeReadPage(page)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey),
		Value: pageRaw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)
}

func TestStoreListFailsStopOnRootGenerationMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	root.RootGeneration = 0
	raw, err := encodeReadRoot(root)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadRootKey([]byte("/bucket/hot")),
		Value: raw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	_, _, _, err = store.List([]byte("/bucket/hot"), Cursor{}, 16)
	require.ErrorIs(t, err, ErrCodecCorrupted)
}

func TestStoreVerifyCertificateDetectsFrontierMismatch(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	require.NoError(t, store.Create([]byte("/bucket/hot/file1"), EntryKindFile, []byte("m1")))
	require.NoError(t, store.Create([]byte("/bucket/hot/file2"), EntryKindFile, []byte("m2")))
	root, _, err := store.MaterializeReadPlane([]byte("/bucket/hot"), 1)
	require.NoError(t, err)
	require.NotEmpty(t, root.Pages)

	raw, err := store.kv.Get(encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey))
	require.NoError(t, err)
	page, err := decodeReadPage(raw)
	require.NoError(t, err)
	page.PublishedFrontier++
	pageRaw, err := encodeReadPage(page)
	require.NoError(t, err)
	require.NoError(t, store.kv.Apply([]Mutation{{
		Kind:  MutationPut,
		Key:   encodeReadPageKey([]byte("/bucket/hot"), root.Pages[0].FenceKey),
		Value: pageRaw,
	}}))
	store.invalidateReadRoot([]byte("/bucket/hot"))

	stats, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, stats.Membership.Consistent)
	require.True(t, stats.Certificate.Consistent)
	require.False(t, stats.Publication.Consistent)
	require.False(t, stats.Consistent)
	require.NotEmpty(t, stats.Publication.CohortMismatchIDs)
	require.Empty(t, stats.Publication.FrontierLagIDs)
}

func TestStoreRepairAndListBootstrapsColdIntervalsIncrementally(t *testing.T) {
	store, cleanup := openTestStore(t, 2)
	defer cleanup()

	store.readPageEntries = 2
	for _, name := range []string{"file1", "file2", "file3", "file4", "file5"} {
		require.NoError(t, store.Create([]byte("/bucket/hot/"+name), EntryKindFile, []byte(name)))
	}

	first, cursor, _, err := store.RepairAndList([]byte("/bucket/hot"), Cursor{}, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2"}, []string{string(first[0].Name), string(first[1].Name)})
	require.NotEmpty(t, cursor.PageID)

	root, pages, ok, err := store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, root.Pages, 2)
	require.Len(t, pages, 2)
	require.True(t, root.Pages[0].CoverageState.IsCovered())
	require.Equal(t, PageCoverageStateCold, root.Pages[1].CoverageState)
	require.Empty(t, pages[1].Entries)

	verify, err := store.Verify([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, verify.Consistent)
	require.Equal(t, 1, verify.Certificate.ColdPages)
	require.Equal(t, 1, verify.Certificate.UncoveredPages)

	_, _, _, err = store.List([]byte("/bucket/hot"), cursor, 2)
	require.ErrorIs(t, err, ErrCoverageIncomplete)

	second, next, _, err := store.RepairAndList([]byte("/bucket/hot"), cursor, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"file3", "file4"}, []string{string(second[0].Name), string(second[1].Name)})
	require.NotEmpty(t, next.PageID)

	root, pages, ok, err = store.LoadReadPlane([]byte("/bucket/hot"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, root.Pages, 3)
	require.Len(t, pages, 3)
	require.True(t, root.Pages[0].CoverageState.IsCovered())
	require.True(t, root.Pages[1].CoverageState.IsCovered())
	require.Equal(t, PageCoverageStateCold, root.Pages[2].CoverageState)
	require.Empty(t, pages[2].Entries)

	last, tail, _, err := store.RepairAndList([]byte("/bucket/hot"), next, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"file5"}, []string{string(last[0].Name)})
	require.Empty(t, tail.PageID)
}

func TestStoreCreateRejectsMissingParent(t *testing.T) {
	store := NewStore(newTestKV(), 4)
	require.ErrorIs(t, store.Create([]byte("/bucket/a/file1"), EntryKindFile, []byte("m1")), ErrParentNotFound)
}

func TestStoreCreateRejectsFileParent(t *testing.T) {
	store := NewStore(newTestKV(), 4)
	mustStoreCreate(t, store, "/bucket", EntryKindDirectory, []byte("bucket"))
	mustStoreCreate(t, store, "/bucket/file-parent", EntryKindFile, []byte("fp"))
	require.ErrorIs(t, store.Create([]byte("/bucket/file-parent/child"), EntryKindFile, []byte("m1")), ErrParentNotDir)
}

func mustStoreCreate(t *testing.T, store *Store, path string, kind EntryKind, meta []byte) {
	t.Helper()
	require.NoError(t, store.Create([]byte(path), kind, meta))
}
