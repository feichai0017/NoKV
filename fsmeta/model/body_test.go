// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBodyDescriptorRoundTrip(t *testing.T) {
	desc := BodyDescriptor{
		Producer:    "mlflow",
		DigestURI:   "sha256:abc",
		Size:        42,
		ContentType: "application/json",
		BodyRef:     "s3://bucket/run/artifact",
		Generation:  7,
	}

	encoded, err := EncodeBodyDescriptor(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"producer": "mlflow",
		"digest_uri": "sha256:abc",
		"size": 42,
		"content_type": "application/json",
		"body_ref": "s3://bucket/run/artifact",
		"generation": 7
	}`, string(encoded))

	decoded, err := DecodeBodyDescriptor(encoded)
	require.NoError(t, err)
	require.Equal(t, desc, decoded)
}

func TestBodyDescriptorRejectsMalformedOpaqueAttrs(t *testing.T) {
	_, err := DecodeBodyDescriptor([]byte(`{"body_ref":"cas://1","unknown":true}`))
	require.ErrorIs(t, err, ErrInvalidValue)

	_, err = DecodeBodyDescriptor([]byte(`{"producer":"","digest_uri":"","size":0,"content_type":"","body_ref":"","generation":0}`))
	require.ErrorIs(t, err, ErrInvalidValue)

	_, err = EncodeBodyDescriptor(BodyDescriptor{BodyRef: "cas://bad\x00ref"})
	require.ErrorIs(t, err, ErrInvalidValue)
}

func TestInodeBodyDescriptorDistinguishesEmptyAndMalformed(t *testing.T) {
	desc, ok, err := InodeBodyDescriptor(InodeRecord{})
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, desc)

	desc, ok, err = InodeBodyDescriptor(InodeRecord{
		OpaqueAttrs: []byte(`{"producer":"","digest_uri":"","size":0,"content_type":"","body_ref":"cas://1","generation":0}`),
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, BodyDescriptor{BodyRef: "cas://1"}, desc)

	_, _, err = InodeBodyDescriptor(InodeRecord{OpaqueAttrs: []byte("not-json")})
	require.ErrorIs(t, err, ErrInvalidValue)
}
