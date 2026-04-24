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
	require.Equal(t, "667376000164", hex.EncodeToString(value[:6]))

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

	value, err := encodeValue(ValueKind('x'), map[string]string{"x": "y"})
	require.NoError(t, err)
	_, err = ValueKindOf(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}
