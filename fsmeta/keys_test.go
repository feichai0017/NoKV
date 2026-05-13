package fsmeta

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testMount      = MountIdentity{MountID: "vol", MountKeyID: 1}
	testOtherMount = MountIdentity{MountID: "volume", MountKeyID: 2}
)

func TestKeyLayoutStable(t *testing.T) {
	key, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)

	require.Equal(t, "66736d00010000000000000001000769000000000000002a", hex.EncodeToString(key))

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindInode, kind)
}

func TestUsageKeyAllowsMountWideScope(t *testing.T) {
	key, err := EncodeUsageKey(testMount, 0)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindUsage, kind)
}

func TestPerasSegmentCatalogIndexKeyUsesRootedMountAndBucket(t *testing.T) {
	var root [32]byte
	root[0] = 7
	key, err := EncodePerasSegmentCatalogIndexKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindPeras, kind)
	require.Equal(t, "peras", kind.String())

	parts, ok := InspectKey(key)
	require.True(t, ok)
	require.Equal(t, testMount.MountKeyID, parts.MountKeyID)
	require.Equal(t, AffinityBucket(3), parts.Bucket)
	require.Equal(t, PerasSegmentRecordIndex, parts.PerasRecord)
	require.Equal(t, root, parts.PerasRoot)

	object, err := EncodePerasSegmentObjectKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	parts, ok = InspectKey(object)
	require.True(t, ok)
	require.Equal(t, PerasSegmentRecordObject, parts.PerasRecord)
	require.Equal(t, root, parts.PerasRoot)
}

func TestPerasSegmentCatalogIndexPrefixCoversCatalogKeys(t *testing.T) {
	var root [32]byte
	root[0] = 7
	key, err := EncodePerasSegmentCatalogIndexKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	prefix, err := EncodePerasSegmentCatalogIndexPrefix(testMount.MountKeyID, 3)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(key, prefix))
	object, err := EncodePerasSegmentObjectKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	require.False(t, bytes.HasPrefix(object, prefix))
	otherBucketPrefix, err := EncodePerasSegmentCatalogIndexPrefix(testMount.MountKeyID, 4)
	require.NoError(t, err)
	require.False(t, bytes.HasPrefix(key, otherBucketPrefix))
}

func TestAuxiliaryKeyEncoders(t *testing.T) {
	mount, err := EncodeMountKey(testMount)
	require.NoError(t, err)
	kind, err := KeyKindOf(mount)
	require.NoError(t, err)
	require.Equal(t, KeyKindMount, kind)
	require.Equal(t, "mount", kind.String())
	require.Equal(t, "unknown(120)", KeyKind('x').String())

	chunk, err := EncodeChunkKey(testMount, 22, 3)
	require.NoError(t, err)
	kind, err = KeyKindOf(chunk)
	require.NoError(t, err)
	require.Equal(t, KeyKindChunk, kind)
	require.Equal(t, "chunk", kind.String())
}

func TestBucketPrefixAndRangeCoverOneAffinityBucket(t *testing.T) {
	start, end, err := EncodeBucketRange(testMount, 3)
	require.NoError(t, err)
	require.NotEmpty(t, end)

	var inBucket InodeID
	for inode := InodeID(2); inode < 10_000; inode++ {
		if BucketForInodeID(inode) == 3 {
			inBucket = inode
			break
		}
	}
	require.NotZero(t, inBucket)
	key, err := EncodeInodeKey(testMount, inBucket)
	require.NoError(t, err)
	require.GreaterOrEqual(t, bytes.Compare(key, start), 0)
	require.Less(t, bytes.Compare(key, end), 0)
	bucket, ok := BucketOfKey(key)
	require.True(t, ok)
	require.Equal(t, AffinityBucket(3), bucket)
}

func TestMountPrefixAndRangeCoverOnlyOneMount(t *testing.T) {
	start, end, err := EncodeMountKeyRange(testMount)
	require.NoError(t, err)
	require.NotEmpty(t, end)

	inode, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	dentry, err := EncodeDentryKey(testMount, 7, "name")
	require.NoError(t, err)
	other, err := EncodeInodeKey(testOtherMount, 42)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(inode, start))
	require.True(t, bytes.HasPrefix(dentry, start))
	require.GreaterOrEqual(t, bytes.Compare(inode, start), 0)
	require.Less(t, bytes.Compare(inode, end), 0)
	require.GreaterOrEqual(t, bytes.Compare(dentry, start), 0)
	require.Less(t, bytes.Compare(dentry, end), 0)
	require.False(t, bytes.HasPrefix(other, start))
	require.GreaterOrEqual(t, bytes.Compare(other, end), 0)

	mountKey, ok := MountKeyIDOfKey(dentry)
	require.True(t, ok)
	require.Equal(t, MountKeyID(1), mountKey)
	name, ok := DentryNameOfKey(dentry)
	require.True(t, ok)
	require.Equal(t, "name", name)

	_, ok = MountKeyIDOfKey(start)
	require.False(t, ok)
}

func TestInspectKeyExtractsAuthorityParts(t *testing.T) {
	dentry, err := EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	parts, ok := InspectKey(dentry)
	require.True(t, ok)
	require.Equal(t, testMount.MountKeyID, parts.MountKeyID)
	require.Equal(t, BucketForInodeID(7), parts.Bucket)
	require.Equal(t, KeyKindDentry, parts.Kind)
	require.Equal(t, InodeID(7), parts.Parent)

	inode, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	parts, ok = InspectKey(inode)
	require.True(t, ok)
	require.Equal(t, KeyKindInode, parts.Kind)
	require.Equal(t, InodeID(42), parts.Inode)

	chunk, err := EncodeChunkKey(testMount, 42, 3)
	require.NoError(t, err)
	parts, ok = InspectKey(chunk)
	require.True(t, ok)
	require.Equal(t, KeyKindChunk, parts.Kind)
	require.Equal(t, InodeID(42), parts.Inode)

	session, err := EncodeSessionKey(testMount, 42, "writer")
	require.NoError(t, err)
	parts, ok = InspectKey(session)
	require.True(t, ok)
	require.Equal(t, KeyKindSession, parts.Kind)
	require.Equal(t, InodeID(42), parts.Inode)

	usage, err := EncodeUsageKey(testMount, 9)
	require.NoError(t, err)
	parts, ok = InspectKey(usage)
	require.True(t, ok)
	require.Equal(t, KeyKindUsage, parts.Kind)
	require.Equal(t, InodeID(9), parts.UsageScope)

	_, ok = InspectKey([]byte("bad"))
	require.False(t, ok)
}

func TestDentryPrefixGroupsDirectoryEntries(t *testing.T) {
	prefix, err := EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)
	a, err := EncodeDentryKey(testMount, 9, "a")
	require.NoError(t, err)
	b, err := EncodeDentryKey(testMount, 9, "b")
	require.NoError(t, err)
	otherParent, err := EncodeDentryKey(testMount, 10, "a")
	require.NoError(t, err)
	otherMountKey, err := EncodeDentryKey(testOtherMount, 9, "a")
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(a, prefix))
	require.True(t, bytes.HasPrefix(b, prefix))
	require.False(t, bytes.HasPrefix(otherParent, prefix))
	require.False(t, bytes.HasPrefix(otherMountKey, prefix))
}

func TestShardForUserKeyKeepsWorkspaceMutationsLocal(t *testing.T) {
	const shards = 4
	createDentry, err := EncodeDentryKey(testMount, RootInode, "workspace-a")
	require.NoError(t, err)
	rootBucket, ok := BucketOfKey(createDentry)
	require.True(t, ok)
	require.Equal(t, RootAffinityBucket, rootBucket)

	targetBucket := ChooseWorkspaceBucket(testMount, "workspace-a")
	inode := findInodeOnBucket(t, testMount, targetBucket)
	createInode, err := EncodeInodeKey(testMount, inode)
	require.NoError(t, err)

	childA, err := EncodeDentryKey(testMount, inode, "scratch")
	require.NoError(t, err)
	childB, err := EncodeDentryKey(testMount, inode, "checkpoint")
	require.NoError(t, err)
	require.Equal(t, ShardForUserKey(createInode, shards), ShardForUserKey(childA, shards))
	require.Equal(t, ShardForUserKey(createInode, shards), ShardForUserKey(childB, shards))
}

func TestUserKeyShapeExtractsAffinityAndBloomPrefixes(t *testing.T) {
	mount := testMount
	const inode InodeID = 42

	bucketPrefix, err := EncodeBucketPrefix(mount, BucketForInodeID(inode))
	require.NoError(t, err)

	dentryPrefix, err := EncodeDentryPrefix(mount, inode)
	require.NoError(t, err)
	dentry, err := EncodeDentryKey(mount, inode, "checkpoint")
	require.NoError(t, err)
	dentryShape := UserKeyShape(dentry)
	require.Equal(t, bucketPrefix, dentryShape.LocalityPrefix)
	require.Equal(t, bucketPrefix, dentryShape.ShardKey)
	require.Equal(t, byte(KeyKindDentry), dentryShape.Family)
	require.Equal(t, dentryPrefix, dentryShape.BloomPrefix)

	session, err := EncodeSessionKey(mount, inode, "writer-1")
	require.NoError(t, err)
	owner, err := EncodeInodeSessionKey(mount, inode)
	require.NoError(t, err)
	sessionShape := UserKeyShape(session)
	require.Equal(t, bucketPrefix, sessionShape.LocalityPrefix)
	require.Equal(t, byte(KeyKindSession), sessionShape.Family)
	require.Equal(t, owner[:len(owner)-1], sessionShape.BloomPrefix)

	inodeKey, err := EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	inodeShape := UserKeyShape(inodeKey)
	require.Equal(t, bucketPrefix, inodeShape.LocalityPrefix)
	require.Equal(t, inodeKey, inodeShape.BloomPrefix)
}

func TestShardForUserKeyUsesShapeLocalityAcrossFamilies(t *testing.T) {
	const shards = 4
	mount := testMount
	inode := findInodeOnBucket(t, mount, 7)
	keys := make([][]byte, 0, 5)

	inodeKey, err := EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	keys = append(keys, inodeKey)
	dentry, err := EncodeDentryKey(mount, inode, "scratch")
	require.NoError(t, err)
	keys = append(keys, dentry)
	chunk, err := EncodeChunkKey(mount, inode, 3)
	require.NoError(t, err)
	keys = append(keys, chunk)
	session, err := EncodeSessionKey(mount, inode, "writer-1")
	require.NoError(t, err)
	keys = append(keys, session)
	usage, err := EncodeUsageKey(mount, inode)
	require.NoError(t, err)
	keys = append(keys, usage)

	target := ShardForUserKey(keys[0], shards)
	for _, key := range keys[1:] {
		require.Equal(t, target, ShardForUserKey(key, shards))
	}
}

func TestShardForUserKeyKeepsSessionIndexesWithInode(t *testing.T) {
	const shards = 8
	session, err := EncodeSessionKey(testMount, 42, "writer-1")
	require.NoError(t, err)
	owner, err := EncodeInodeSessionKey(testMount, 42)
	require.NoError(t, err)
	inode, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)

	targetShard := ShardForUserKey(inode, shards)
	require.Equal(t, targetShard, ShardForUserKey(session, shards))
	require.Equal(t, targetShard, ShardForUserKey(owner, shards))
}

func TestNameValidationRejectsUnsafePathComponents(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b"} {
		_, err := EncodeDentryKey(testMount, RootInode, name)
		require.ErrorIs(t, err, ErrInvalidName)
	}
}

func TestKeyKindOfRejectsInvalidKeys(t *testing.T) {
	_, err := KeyKindOf([]byte("not-fsmeta"))
	require.ErrorIs(t, err, ErrInvalidKey)

	key, err := encodeKey(testMount, RootAffinityBucket, KeyKind('x'), []byte("body"))
	require.NoError(t, err)
	_, err = KeyKindOf(key)
	require.ErrorIs(t, err, ErrInvalidKeyKind)
}

func findInodeOnBucket(t *testing.T, mount MountIdentity, bucket AffinityBucket) InodeID {
	t.Helper()
	for inode := InodeID(2); inode < 10_000; inode++ {
		key, err := EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		got, ok := BucketOfKey(key)
		require.True(t, ok)
		if got == bucket {
			return inode
		}
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return 0
}
