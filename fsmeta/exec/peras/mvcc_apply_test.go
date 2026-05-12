package peras

import (
	"bytes"
	"path/filepath"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/local"
	"github.com/stretchr/testify/require"
)

func TestBuildMVCCSegmentInstallEntriesUsesOneInstallVersion(t *testing.T) {
	segment := fsmetaSegmentForTest(t)
	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	objectKey, err := PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	require.NoError(t, err)
	indexFound := make(map[string]bool, len(indexKeys))
	var catalogFound bool
	for _, entry := range entries {
		require.Equal(t, uint64(99), entry.Version)
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if cf != entrykv.CFDefault {
			continue
		}
		if bytes.Equal(userKey, objectKey) {
			catalogFound = true
			catalog, err := DecodePerasSegmentCatalogRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, catalog.Root)
			require.Equal(t, uint64(99), catalog.InstallVersion)
			require.Equal(t, uint64(len(segment.Completions)), catalog.CompletionCount)
			require.Equal(t, digest, catalog.SegmentPayloadDigest)
			require.Equal(t, uint64(len(payload)), catalog.SegmentPayloadSize)
			require.Equal(t, payload, catalog.SegmentPayload)
			continue
		}
		for _, indexKey := range indexKeys {
			if !bytes.Equal(userKey, indexKey) {
				continue
			}
			indexFound[string(indexKey)] = true
			index, err := DecodePerasSegmentCatalogIndexRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, index.Root)
			require.Equal(t, objectKey, index.ObjectKey)
		}
	}
	require.True(t, catalogFound)
	require.Len(t, indexFound, len(indexKeys))
}

func TestBuildMVCCSegmentCatalogInstallEntriesStoresObjectAndBucketIndexes(t *testing.T) {
	segment := fsmetaSegmentForTest(t)
	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentCatalogInstallEntries(segment, 99)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	require.NoError(t, err)
	objectKey, err := PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	require.Len(t, entries, len(indexKeys)+1)

	var objectFound bool
	indexFound := make(map[string]bool, len(indexKeys))
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		require.Equal(t, entrykv.CFDefault, cf)
		if bytes.Equal(userKey, objectKey) {
			objectFound = true
			catalog, err := DecodePerasSegmentCatalogRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, catalog.Root)
			require.Equal(t, digest, catalog.SegmentPayloadDigest)
			require.Equal(t, uint64(len(payload)), catalog.SegmentPayloadSize)
			decoded, err := VerifyPerasSegmentPayload(catalog.SegmentPayload, segment.Root, digest)
			require.NoError(t, err)
			require.Equal(t, segment.Stats(), decoded.Stats())
			continue
		}
		for _, indexKey := range indexKeys {
			if !bytes.Equal(userKey, indexKey) {
				continue
			}
			indexFound[string(indexKey)] = true
			index, err := DecodePerasSegmentCatalogIndexRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, index.Root)
			require.Equal(t, objectKey, index.ObjectKey)
		}
	}
	require.True(t, objectFound)
	require.Len(t, indexFound, len(indexKeys))
}

func TestBuildMVCCSegmentCatalogInstallEntriesForObjectKeyStoresPayloadOnce(t *testing.T) {
	segment := fsmetaMultiBucketSegmentForTest(t)
	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	objectKeys, err := PerasSegmentCatalogObjectKeys(segment)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objectKeys), 2)
	canonicalObjectKey, err := PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	require.Equal(t, objectKeys[0], canonicalObjectKey)

	canonicalEntries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, canonicalObjectKey)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(canonicalEntries)
	require.Len(t, canonicalEntries, 2)
	require.True(t, entriesContainUserKey(canonicalEntries, canonicalObjectKey))

	nonCanonicalEntries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKeys[1])
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(nonCanonicalEntries)
	require.Len(t, nonCanonicalEntries, 1)
	require.False(t, entriesContainUserKey(nonCanonicalEntries, objectKeys[1]))

	canonicalIndex := decodeOnlyCatalogIndex(t, canonicalEntries)
	require.Equal(t, canonicalObjectKey, canonicalIndex.ObjectKey)
	nonCanonicalIndex := decodeOnlyCatalogIndex(t, nonCanonicalEntries)
	require.Equal(t, canonicalObjectKey, nonCanonicalIndex.ObjectKey)
}

func TestBuildMVCCSegmentInstallEntriesRequiresFSMetaKeys(t *testing.T) {
	segment, err := BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{{
			OpID: opID("client-a", 1),
			Mutations: []ReplayMutation{{
				Key:   []byte("raw-key"),
				Value: []byte("value"),
			}},
		}},
	})
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

func TestLoadPerasSegmentCatalogInstallForObjectKeyUsesBucketIndex(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaMultiBucketSegmentForTest(t)
	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	objectKeys, err := PerasSegmentCatalogObjectKeys(segment)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objectKeys), 2)

	for _, objectKey := range objectKeys {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKey)
		require.NoError(t, err)
		require.NoError(t, db.ApplyInternalEntries(entries))
		releaseMVCCReplayEntries(entries)
	}

	for _, objectKey := range objectKeys {
		installed, err := LoadPerasSegmentCatalogInstallForObjectKey(db, segment, objectKey)
		require.NoError(t, err)
		require.True(t, installed)
	}
	canonical, ok, err := LoadPerasSegmentCatalogForObjectKey(db, segment, objectKeys[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segment.Root, canonical.Root)
	_, ok, err = LoadPerasSegmentCatalogForObjectKey(db, segment, objectKeys[1])
	require.NoError(t, err)
	require.False(t, ok)
}

func BenchmarkBuildMVCCSegmentCatalogInstallEntries1000(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	payload, err := EncodePerasSegment(segment)
	if err != nil {
		b.Fatal(err)
	}
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		b.Fatal(err)
	}
	catalogKeys, err := PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		b.Fatal(err)
	}
	wantEntries := len(catalogKeys) + 1

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment, 99, payload, digest)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != wantEntries {
			b.Fatalf("unexpected catalog entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func BenchmarkBuildMVCCSegmentMaterializationEntries1000(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) <= 1 {
			b.Fatalf("unexpected materialized entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
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

func fsmetaMultiBucketSegmentForTest(t *testing.T) PerasSegment {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	var inode fsmeta.InodeID
	for candidate := fsmeta.InodeID(2); candidate < 1_000_000; candidate++ {
		if fsmeta.BucketForInodeID(candidate) != fsmeta.RootAffinityBucket {
			inode = candidate
			break
		}
	}
	require.NotZero(t, inode)
	dentry, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "artifact")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	segment, err := BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{{
			OpID: opID("client-a", 1),
			Kind: fsmeta.OperationCreate,
			Mutations: []ReplayMutation{
				{Key: dentry, Value: []byte("inode")},
				{Key: inodeKey, Value: []byte("attrs")},
			},
		}},
	})
	require.NoError(t, err)
	return segment
}

func entriesContainUserKey(entries []*entrykv.Entry, want []byte) bool {
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if ok && cf == entrykv.CFDefault && bytes.Equal(userKey, want) {
			return true
		}
	}
	return false
}

func decodeOnlyCatalogIndex(t *testing.T, entries []*entrykv.Entry) SegmentCatalogIndexRecord {
	t.Helper()
	var record SegmentCatalogIndexRecord
	var found bool
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if cf != entrykv.CFDefault {
			continue
		}
		parts, ok := fsmeta.InspectKey(userKey)
		if !ok || parts.Kind != fsmeta.KeyKindPeras || parts.PerasRecord != fsmeta.PerasSegmentRecordIndex {
			continue
		}
		next, err := DecodePerasSegmentCatalogIndexRecord(entry.Value)
		require.NoError(t, err)
		require.False(t, found)
		record = next
		found = true
	}
	require.True(t, found)
	return record
}
