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
