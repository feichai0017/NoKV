package fsmeta

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDentryValueRoundTrip(t *testing.T) {
	value, err := EncodeDentryValue(DentryRecord{
		Parent: RootInode,
		Name:   "file",
		Inode:  22,
		Type:   InodeTypeFile,
	})
	require.NoError(t, err)
	require.Equal(t, "66737600016400000000000000010466696c65000000000000001601", hex.EncodeToString(value))

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindDentry, kind)

	record, err := DecodeDentryValue(value)
	require.NoError(t, err)
	require.Equal(t, DentryRecord{
		Parent: RootInode,
		Name:   "file",
		Inode:  22,
		Type:   InodeTypeFile,
	}, record)
}

func TestInodeValueRoundTrip(t *testing.T) {
	value, err := EncodeInodeValue(InodeRecord{
		Inode:     22,
		Type:      InodeTypeFile,
		Size:      4096,
		LinkCount: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "6673760001690000000000000016010000000000001000000000000000000100000000000000000000000000000000", hex.EncodeToString(value))

	record, err := DecodeInodeValue(value)
	require.NoError(t, err)
	require.Equal(t, InodeRecord{
		Inode:     22,
		Type:      InodeTypeFile,
		Size:      4096,
		LinkCount: 1,
	}, record)
}

func TestSessionValueRoundTrip(t *testing.T) {
	value, err := EncodeSessionValue(SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	})
	require.NoError(t, err)

	record, err := DecodeSessionValue(value)
	require.NoError(t, err)
	require.Equal(t, SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	}, record)
}

func TestValueDecodersRejectWrongKind(t *testing.T) {
	value, err := EncodeDentryValue(DentryRecord{Parent: RootInode, Name: "file", Inode: 22})
	require.NoError(t, err)

	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueKindOfRejectsInvalidValues(t *testing.T) {
	_, err := ValueKindOf([]byte("not-fsmeta-value"))
	require.ErrorIs(t, err, ErrInvalidValue)

	value := encodeValue(ValueKind('x'), []byte("body"))
	_, err = ValueKindOf(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueCodecsRejectInvalidType(t *testing.T) {
	_, err := EncodeInodeValue(InodeRecord{Inode: 22, Type: InodeType("symlink")})
	require.ErrorIs(t, err, ErrInvalidValue)

	value := encodeValue(ValueKindInode, append([]byte{
		0, 0, 0, 0, 0, 0, 0, 22,
		99,
	}, make([]byte, 32)...))
	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValue)
}
