// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeViewPathRejectsEscapesAndAbsolutePaths(t *testing.T) {
	got, err := NormalizeViewPath("")
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = NormalizeViewPath("workspace/input")
	require.NoError(t, err)
	require.Equal(t, "workspace/input", got)

	for _, path := range []string{"/workspace", "workspace//input", "workspace/../secret", "workspace/.", "bad/name/"} {
		_, err := NormalizeViewPath(path)
		require.ErrorIs(t, err, ErrInvalidName)
	}
}

func TestValidateCreateViewRequestRejectsWritableSnapshotRule(t *testing.T) {
	err := ValidateCreateViewRequest(CreateViewRequest{
		Mount:           "vol",
		RootInode:       RootInode,
		SnapshotVersion: 42,
		AccessRules: []ViewAccessRule{{
			Prefix: "",
			Mode:   ViewAccessReadWrite,
		}},
	})
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestViewDescriptorCloneDetachesRules(t *testing.T) {
	desc := ViewDescriptor{
		Ref:       NamespaceRef{Mount: "vol", ViewToken: "tok"},
		RootInode: RootInode,
		AccessRules: []ResolvedViewAccessRule{{
			Prefix:    "",
			RootInode: RootInode,
			Mode:      ViewAccessReadOnly,
		}},
	}
	cloned := desc.Clone()
	cloned.AccessRules[0].Mode = ViewAccessReadWrite
	require.Equal(t, ViewAccessReadOnly, desc.AccessRules[0].Mode)
}
