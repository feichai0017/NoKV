// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import (
	"encoding/hex"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestDentryValueRoundTrip(t *testing.T) {
	value, err := EncodeDentryValue(model.DentryRecord{
		Parent: model.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   model.InodeTypeFile,
	})
	require.NoError(t, err)
	require.Equal(t, "66737600016400000000000000010466696c65000000000000001601", hex.EncodeToString(value))

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindDentry, kind)

	record, err := DecodeDentryValue(value)
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: model.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}, record)

	_, _, _, ok, err := DecodeDentryValueWithProjection(value)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDentryValueWithProjectionRoundTrip(t *testing.T) {
	dentry := model.DentryRecord{
		Parent: model.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}
	inode := model.InodeRecord{
		Inode:         22,
		Type:          model.InodeTypeFile,
		Size:          4096,
		Mode:          0o644,
		LinkCount:     1,
		CreatedUnixNs: 10,
		UpdatedUnixNs: 20,
		OpaqueAttrs:   []byte(`{"body_ref":"s3://bucket/checkpoint"}`),
	}
	value, err := EncodeDentryValueWithProjection(dentry, inode, 99)
	require.NoError(t, err)

	record, err := DecodeDentryValue(value)
	require.NoError(t, err)
	require.Equal(t, dentry, record)

	record, projection, version, ok, err := DecodeDentryValueWithProjection(value)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, dentry, record)
	require.Equal(t, inode, projection)
	require.Equal(t, uint64(99), version)
}

func TestParentLinkValueRoundTrip(t *testing.T) {
	record := model.ParentLinkRecord{
		Child:  22,
		Parent: model.RootInode,
		Name:   "file",
		Type:   model.InodeTypeFile,
	}
	value, err := EncodeParentLinkValue(record)
	require.NoError(t, err)
	require.Equal(t, "parent", ValueKindParent.String())

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindParent, kind)

	decoded, err := DecodeParentLinkValue(value)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func TestPathIndexValueRoundTrip(t *testing.T) {
	record := model.PathIndexRecord{
		RootInode:     model.RootInode,
		Path:          "runs/1/artifact.json",
		Parent:        7,
		Name:          "artifact.json",
		Inode:         22,
		Type:          model.InodeTypeFile,
		DentryVersion: 99,
	}
	value, err := EncodePathIndexValue(record)
	require.NoError(t, err)

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindPath, kind)

	decoded, err := DecodePathIndexValue(value)
	require.NoError(t, err)
	require.Equal(t, record, decoded)

	_, err = EncodePathIndexValue(model.PathIndexRecord{RootInode: model.RootInode, Path: "../bad"})
	require.ErrorIs(t, err, model.ErrInvalidName)
}

func TestInodeValueRoundTrip(t *testing.T) {
	value, err := EncodeInodeValue(model.InodeRecord{
		Inode:       22,
		Type:        model.InodeTypeFile,
		Size:        4096,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"s3://bucket/checkpoint"}`),
	})
	require.NoError(t, err)
	require.Equal(t, "6673760001690000000000000016010000000000001000000000000000000100000000000000000000000000000000257b22626f64795f726566223a2273333a2f2f6275636b65742f636865636b706f696e74227d00", hex.EncodeToString(value))

	record, err := DecodeInodeValue(value)
	require.NoError(t, err)
	require.Equal(t, model.InodeRecord{
		Inode:       22,
		Type:        model.InodeTypeFile,
		Size:        4096,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"s3://bucket/checkpoint"}`),
	}, record)
}

func TestSessionValueRoundTrip(t *testing.T) {
	value, err := EncodeSessionValue(model.SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	})
	require.NoError(t, err)

	record, err := DecodeSessionValue(value)
	require.NoError(t, err)
	require.Equal(t, model.SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	}, record)
}

func TestUsageValueRoundTrip(t *testing.T) {
	value, err := EncodeUsageValue(model.UsageRecord{
		Bytes:  4096,
		Inodes: 12,
	})
	require.NoError(t, err)
	require.Equal(t, "usage", ValueKindUsage.String())
	require.Equal(t, "unknown(122)", ValueKind('z').String())

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindUsage, kind)

	record, err := DecodeUsageValue(value)
	require.NoError(t, err)
	require.Equal(t, model.UsageRecord{Bytes: 4096, Inodes: 12}, record)
}

func TestSnapshotValueRoundTrip(t *testing.T) {
	var ref model.SnapshotEvidenceRef
	ref.EpochID = 7
	ref.EvidenceRoot[0] = 1
	ref.PayloadDigest[0] = 2
	token := model.SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      9,
		RootInode:       model.RootInode,
		ReadVersion:     42,
		RuntimeEvidence: []model.SnapshotEvidenceRef{ref},
	}
	value, err := EncodeSnapshotValue(token)
	require.NoError(t, err)
	require.Equal(t, "snapshot", ValueKindSnapshot.String())

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindSnapshot, kind)

	decoded, err := DecodeSnapshotValue(value)
	require.NoError(t, err)
	require.Equal(t, token, decoded)
}

func TestValueDecodersRejectWrongKind(t *testing.T) {
	value, err := EncodeDentryValue(model.DentryRecord{Parent: model.RootInode, Name: "file", Inode: 22})
	require.NoError(t, err)

	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueKindOfRejectsInvalidValues(t *testing.T) {
	_, err := ValueKindOf([]byte("not-fsmeta-value"))
	require.ErrorIs(t, err, model.ErrInvalidValue)

	value := encodeValue(ValueKind('z'), []byte("body"))
	_, err = ValueKindOf(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueCodecsRejectInvalidType(t *testing.T) {
	_, err := EncodeInodeValue(model.InodeRecord{Inode: 22, Type: model.InodeType("symlink")})
	require.ErrorIs(t, err, model.ErrInvalidValue)

	_, err = EncodeInodeValue(model.InodeRecord{
		Inode:       22,
		Type:        model.InodeTypeFile,
		OpaqueAttrs: make([]byte, model.MaxInodeOpaqueAttrsBytes+1),
	})
	require.ErrorIs(t, err, model.ErrInvalidValue)

	body := append([]byte{
		0, 0, 0, 0, 0, 0, 0, 22,
		99,
	}, make([]byte, 32)...)
	body = append(body, 0)
	value := encodeValue(ValueKindInode, body)
	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, model.ErrInvalidValue)

	value = encodeValue(ValueKindInode, append([]byte{
		0, 0, 0, 0, 0, 0, 0, 22,
		1,
	}, make([]byte, 32)...))
	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, model.ErrInvalidValue)
}
