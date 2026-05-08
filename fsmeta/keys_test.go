package fsmeta

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyLayoutStable(t *testing.T) {
	key, err := EncodeInodeKey("vol", 42)
	require.NoError(t, err)

	require.Equal(t, "66736d000103766f6c69000000000000002a", hex.EncodeToString(key))

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindInode, kind)
}

func TestUsageKeyAllowsMountWideScope(t *testing.T) {
	key, err := EncodeUsageKey("vol", 0)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindUsage, kind)
}

func TestAuxiliaryKeyEncoders(t *testing.T) {
	mount, err := EncodeMountKey("vol")
	require.NoError(t, err)
	kind, err := KeyKindOf(mount)
	require.NoError(t, err)
	require.Equal(t, KeyKindMount, kind)
	require.Equal(t, "mount", kind.String())
	require.Equal(t, "unknown(120)", KeyKind('x').String())

	chunk, err := EncodeChunkKey("vol", 22, 3)
	require.NoError(t, err)
	kind, err = KeyKindOf(chunk)
	require.NoError(t, err)
	require.Equal(t, KeyKindChunk, kind)
	require.Equal(t, "chunk", kind.String())
}

func TestMountPrefixAndRangeCoverOnlyOneMount(t *testing.T) {
	start, end, err := EncodeMountKeyRange("vol")
	require.NoError(t, err)
	require.NotEmpty(t, end)

	inode, err := EncodeInodeKey("vol", 42)
	require.NoError(t, err)
	dentry, err := EncodeDentryKey("vol", 7, "name")
	require.NoError(t, err)
	other, err := EncodeInodeKey("volume", 42)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(inode, start))
	require.True(t, bytes.HasPrefix(dentry, start))
	require.GreaterOrEqual(t, bytes.Compare(inode, start), 0)
	require.Less(t, bytes.Compare(inode, end), 0)
	require.GreaterOrEqual(t, bytes.Compare(dentry, start), 0)
	require.Less(t, bytes.Compare(dentry, end), 0)
	require.False(t, bytes.HasPrefix(other, start))
	require.GreaterOrEqual(t, bytes.Compare(other, end), 0)

	mount, ok := MountIDOfKey(dentry)
	require.True(t, ok)
	require.Equal(t, MountID("vol"), mount)

	_, ok = MountIDOfKey(start)
	require.False(t, ok)
}

func TestDentryPrefixGroupsDirectoryEntries(t *testing.T) {
	prefix, err := EncodeDentryPrefix("vol", 9)
	require.NoError(t, err)
	a, err := EncodeDentryKey("vol", 9, "a")
	require.NoError(t, err)
	b, err := EncodeDentryKey("vol", 9, "b")
	require.NoError(t, err)
	otherParent, err := EncodeDentryKey("vol", 10, "a")
	require.NoError(t, err)
	otherMount, err := EncodeDentryKey("other", 9, "a")
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(a, prefix))
	require.True(t, bytes.HasPrefix(b, prefix))
	require.False(t, bytes.HasPrefix(otherParent, prefix))
	require.False(t, bytes.HasPrefix(otherMount, prefix))
}

func TestShardForUserKeyKeepsWorkspaceMutationsLocal(t *testing.T) {
	const shards = 4
	createDentry, err := EncodeDentryKey("vol", RootInode, "workspace-a")
	require.NoError(t, err)
	targetShard := ShardForUserKey(createDentry, shards)

	inode := findInodeOnShard(t, "vol", targetShard, shards)
	createInode, err := EncodeInodeKey("vol", inode)
	require.NoError(t, err)
	require.Equal(t, targetShard, ShardForUserKey(createInode, shards))

	childA, err := EncodeDentryKey("vol", inode, "scratch")
	require.NoError(t, err)
	childB, err := EncodeDentryKey("vol", inode, "checkpoint")
	require.NoError(t, err)
	require.Equal(t, targetShard, ShardForUserKey(childA, shards))
	require.Equal(t, targetShard, ShardForUserKey(childB, shards))
}

func TestShardForUserKeyKeepsSessionIndexesWithInode(t *testing.T) {
	const shards = 8
	session, err := EncodeSessionKey("vol", 42, "writer-1")
	require.NoError(t, err)
	owner, err := EncodeInodeSessionKey("vol", 42)
	require.NoError(t, err)
	inode, err := EncodeInodeKey("vol", 42)
	require.NoError(t, err)

	targetShard := ShardForUserKey(inode, shards)
	require.Equal(t, targetShard, ShardForUserKey(session, shards))
	require.Equal(t, targetShard, ShardForUserKey(owner, shards))
}

func TestNameValidationRejectsUnsafePathComponents(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b"} {
		_, err := EncodeDentryKey("vol", RootInode, name)
		require.ErrorIs(t, err, ErrInvalidName)
	}
}

func TestKeyKindOfRejectsInvalidKeys(t *testing.T) {
	_, err := KeyKindOf([]byte("not-fsmeta"))
	require.ErrorIs(t, err, ErrInvalidKey)

	key, err := encodeKey("vol", KeyKind('x'), []byte("body"))
	require.NoError(t, err)
	_, err = KeyKindOf(key)
	require.ErrorIs(t, err, ErrInvalidKeyKind)
}

func findInodeOnShard(t *testing.T, mount MountID, shard int, shardCount int) InodeID {
	t.Helper()
	for inode := InodeID(2); inode < 10_000; inode++ {
		key, err := EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		if ShardForUserKey(key, shardCount) == shard {
			return inode
		}
	}
	t.Fatalf("no inode found for shard %d", shard)
	return 0
}
