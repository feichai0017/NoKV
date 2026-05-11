package peras

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/txn/percolator"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestMVCCReplayStoreMaterializesReaderVisibleValues(t *testing.T) {
	db := openPerasReplayDB(t)
	plan := versionedReplayPlanForTest(t, 100)
	store, err := NewMVCCReplayStoreForPlan(db, plan)
	require.NoError(t, err)

	stats, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)
	require.Equal(t, ApplyStats{Operations: 3, Mutations: 6}, stats)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
	value, _, err = reader.GetValue([]byte("inode/7"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("attrs"), value)
}

func TestMVCCReplayStoreMaterializesDelete(t *testing.T) {
	db := openPerasReplayDB(t)
	putStore, err := NewMVCCReplayStore(db, 100)
	require.NoError(t, err)
	_, err = ApplyReplayPlan(putStore, replayPlanForTest(t))
	require.NoError(t, err)

	deleteStore, err := NewMVCCReplayStore(db, 200)
	require.NoError(t, err)
	_, err = ApplyReplayPlan(deleteStore, ReplayPlan{
		EpochID: 2,
		Operations: []ReplayOperation{{
			OpID: opID("delete", 1),
			Mutations: []ReplayMutation{{
				Key:    []byte("dentry/a"),
				Delete: true,
			}},
		}},
	})
	require.NoError(t, err)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 150)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
	_, _, err = reader.GetValue([]byte("dentry/a"), 250)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestBuildMVCCReplayEntriesUsesShortValueForSmallPuts(t *testing.T) {
	entries, err := BuildMVCCReplayEntries(ReplayOperation{
		OpID: opID("client-a", 1),
		Mutations: []ReplayMutation{{
			Key:   []byte("short"),
			Value: []byte("value"),
		}},
	}, 100)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	require.Len(t, entries, 1)
	require.Equal(t, entrykv.CFWrite, entries[0].CF)
}

func TestBuildMVCCReplayEntriesKeepsLargeValuesInDefaultCF(t *testing.T) {
	entries, err := BuildMVCCReplayEntries(ReplayOperation{
		OpID: opID("client-a", 1),
		Mutations: []ReplayMutation{{
			Key:   []byte("large"),
			Value: bytes.Repeat([]byte("x"), 129),
		}},
	}, 100)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	require.Len(t, entries, 3)
	require.Equal(t, entrykv.CFDefault, entries[0].CF)
	require.Equal(t, byte(entrykv.BitDelete), entries[0].Meta&entrykv.BitDelete)
	require.Equal(t, entrykv.CFDefault, entries[1].CF)
	require.Equal(t, entrykv.CFWrite, entries[2].CF)
}

func TestBuildMVCCSegmentInstallEntriesUsesOneInstallVersion(t *testing.T) {
	segment := fsmetaSegmentForTest(t)

	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	catalogKey, err := PerasSegmentCatalogKey(segment)
	require.NoError(t, err)
	var catalogFound bool
	for _, entry := range entries {
		require.Equal(t, uint64(99), entry.Version)
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if cf == entrykv.CFDefault && bytes.Equal(userKey, catalogKey) {
			catalogFound = true
			catalog, err := DecodePerasSegmentCatalogRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, catalog.Root)
			require.Equal(t, uint64(99), catalog.InstallVersion)
			require.Equal(t, uint64(len(segment.Completions)), catalog.CompletionCount)
		}
	}
	require.True(t, catalogFound)
}

func TestBuildMVCCSegmentInstallEntriesRequiresFSMetaKeys(t *testing.T) {
	segment, err := BuildPerasSegmentFromReplayPlan(replayPlanForTest(t))
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.ErrorIs(t, err, ErrInvalidPerasSegment)
	require.Empty(t, entries)
}

func TestLoadPerasSegmentCatalogsScansInstalledSegments(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaSegmentForTest(t)
	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	require.NoError(t, db.ApplyInternalEntries(entries))
	releaseMVCCReplayEntries(entries)

	records, err := LoadPerasSegmentCatalogs(db)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, segment.Root, records[0].Root)
	require.Equal(t, uint64(99), records[0].InstallVersion)
	require.Equal(t, segment.Stats().OperationCount, records[0].OperationCount)
	require.Len(t, records[0].Completions, len(segment.Completions))
}

func TestLoadPerasSegmentCatalogFindsInstalledSegment(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaSegmentForTest(t)
	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	require.NoError(t, db.ApplyInternalEntries(entries))
	releaseMVCCReplayEntries(entries)

	record, ok, err := LoadPerasSegmentCatalog(db, segment)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segment.Root, record.Root)
	require.Equal(t, uint64(99), record.InstallVersion)
	require.Equal(t, segment.Stats().EntryCount, record.EntryCount)
	require.Len(t, record.Completions, len(segment.Completions))
}

func TestMVCCReplayStoreKeepsVersionOnApplyFailure(t *testing.T) {
	storeErr := errors.New("apply failed")
	failing := &failingInternalEntryApplier{err: storeErr}
	plan := versionedReplayPlanForTest(t, 100)
	store, err := NewMVCCReplayStoreForPlan(failing, plan)
	require.NoError(t, err)

	_, err = ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, storeErr)

	failing.err = nil
	_, err = ApplyReplayPlan(store, ReplayPlan{
		EpochID: 2,
		Operations: []ReplayOperation{{
			OpID: opID("client-z", 1),
			Mutations: []ReplayMutation{{
				Key:   []byte("z"),
				Value: []byte("value"),
			}},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(100), failing.lastVersion())
}

func TestNewMVCCReplayStoreForPlanRequiresExactVersionRange(t *testing.T) {
	plan := versionedReplayPlanForTest(t, 100)
	plan.Versions.Count--

	_, err := NewMVCCReplayStoreForPlan(noopInternalEntryApplier{}, plan)
	require.ErrorIs(t, err, ErrReplayVersionRequired)
}

func BenchmarkMVCCReplayStoreApply64(b *testing.B) {
	plan := replayPlanForCount(b, 64)
	plan.Versions = ReplayVersionRange{First: 1, Count: 64}
	db := noopInternalEntryApplier{}

	b.ReportAllocs()
	for b.Loop() {
		store, err := NewMVCCReplayStoreForPlan(db, plan)
		if err != nil {
			b.Fatal(err)
		}
		stats, err := ApplyReplayPlan(store, plan)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Operations != 64 {
			b.Fatalf("unexpected operation count %d", stats.Operations)
		}
	}
}

type failingInternalEntryApplier struct {
	err      error
	versions []uint64
}

func (a *failingInternalEntryApplier) ApplyInternalEntries(entries []*entrykv.Entry) error {
	if a.err != nil {
		return a.err
	}
	for _, entry := range entries {
		a.versions = append(a.versions, entry.Version)
	}
	return nil
}

func (a *failingInternalEntryApplier) lastVersion() uint64 {
	if len(a.versions) == 0 {
		return 0
	}
	return a.versions[len(a.versions)-1]
}

type noopInternalEntryApplier struct{}

func (noopInternalEntryApplier) ApplyInternalEntries([]*entrykv.Entry) error {
	return nil
}

func openPerasReplayDB(t *testing.T) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func versionedReplayPlanForTest(t *testing.T, firstVersion uint64) ReplayPlan {
	t.Helper()
	return ReplayPlan{
		EpochID:  1,
		Versions: ReplayVersionRange{First: firstVersion, Count: 3},
		Operations: []ReplayOperation{
			replayOpForTest(opID("client-a", 1), "dentry/a", "inode=7", "inode/7", "attrs"),
			replayOpForTest(opID("client-c", 1), "dentry/c", "inode=9", "inode/9", "attrs"),
			replayOpForTest(opID("client-b", 1), "dentry/b", "inode=8", "inode/8", "attrs"),
		},
	}
}

func fsmetaSegmentForTest(t *testing.T) PerasSegment {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryA, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	dentryB, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "b")
	require.NoError(t, err)
	inodeA, err := fsmeta.EncodeInodeKey(mount, 7)
	require.NoError(t, err)
	segment, err := BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{
				OpID: opID("client-a", 1),
				Kind: fsmeta.OperationCreate,
				Mutations: []ReplayMutation{
					{Key: dentryA, Value: []byte("inode=7")},
					{Key: inodeA, Value: []byte("attrs")},
				},
			},
			{
				OpID: opID("client-b", 1),
				Kind: fsmeta.OperationCreate,
				Mutations: []ReplayMutation{
					{Key: dentryB, Value: []byte("inode=8")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}
