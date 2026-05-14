// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestGeneratedReadProgramsCarryPointPlans(t *testing.T) {
	lookup, err := CompileLookupReadProgram(fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "file"}, testMount)
	require.NoError(t, err)
	require.Equal(t, ReadProgramLookup, lookup.Kind)
	require.Equal(t, fsmeta.OperationLookup, lookup.Plan.Kind)
	require.Len(t, lookup.Footprint.Reads, 1)
	require.Equal(t, lookup.Key, lookup.Footprint.Reads[0].Key)
	require.Equal(t, []fsmeta.InodeID{7}, lookup.Authority.Scope.Parents)

	attr, err := CompileGetAttrReadProgram(testMount, 44)
	require.NoError(t, err)
	require.Equal(t, ReadProgramGetAttr, attr.Kind)
	require.Equal(t, fsmeta.OperationGetAttr, attr.Plan.Kind)
	require.Equal(t, fsmeta.KeyKindInode, attr.Footprint.Reads[0].Kind)
	require.Equal(t, []fsmeta.InodeID{44}, attr.Authority.Scope.Inodes)

	session, err := CompileReadSessionProgram(testMount, 44, "writer-1")
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, session.Kind)
	require.Equal(t, fsmeta.OperationReadSession, session.Plan.Kind)
	require.Equal(t, fsmeta.KeyKindSession, session.Footprint.Reads[0].Kind)
	require.Equal(t, []fsmeta.InodeID{44}, session.Authority.Scope.Inodes)

	owner, err := CompileReadSessionOwnerProgram(testMount, 44)
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, owner.Kind)
	require.Equal(t, fsmeta.OperationReadSession, owner.Plan.Kind)
	require.Equal(t, fsmeta.KeyKindSession, owner.Footprint.Reads[0].Kind)
	require.Equal(t, []fsmeta.InodeID{44}, owner.Authority.Scope.Inodes)
}

func TestGeneratedReadSessionKeyProgramValidatesMountAndKeyShape(t *testing.T) {
	key, err := fsmeta.EncodeSessionKey(testMount, 44, "writer-1")
	require.NoError(t, err)

	program, err := CompileReadSessionKeyProgram(testMount, key)
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, program.Kind)
	require.Equal(t, fsmeta.OperationReadSession, program.Plan.Kind)
	require.Equal(t, fsmeta.KeyKindSession, program.Footprint.Reads[0].Kind)
	require.Equal(t, []fsmeta.InodeID{44}, program.Authority.Scope.Inodes)

	wrongMount := testMount
	wrongMount.MountKeyID++
	_, err = CompileReadSessionKeyProgram(wrongMount, key)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	dentryKey, err := fsmeta.EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	_, err = CompileReadSessionKeyProgram(testMount, dentryKey)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestGeneratedReadDirPlusInodeKeysPreserveDentryOrder(t *testing.T) {
	dentries := []fsmeta.DentryRecord{
		{Parent: 7, Name: "b", Inode: 44},
		{Parent: 7, Name: "a", Inode: 42},
	}

	keys, err := CompileReadDirPlusInodeKeys(testMount, dentries)
	require.NoError(t, err)
	require.Len(t, keys, len(dentries))

	first, err := fsmeta.EncodeInodeKey(testMount, 44)
	require.NoError(t, err)
	second, err := fsmeta.EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	require.Equal(t, first, keys[0])
	require.Equal(t, second, keys[1])
}

func TestGeneratedDirectoryReadPlanNormalizesPerasSource(t *testing.T) {
	base, err := CompileDirectoryReadPlan(fsmeta.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, false, false)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourceBase, base.Source)
	require.False(t, base.IncludePeras)
	require.False(t, base.PerasOnly)
	require.Len(t, base.Footprint.Reads, 1)
	require.True(t, base.Footprint.HasPrefixRead)

	overlay, err := CompileDirectoryReadPlan(fsmeta.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, true, false)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourcePerasOverlay, overlay.Source)
	require.True(t, overlay.IncludePeras)
	require.False(t, overlay.PerasOnly)

	perasOnly, err := CompileDirectoryReadPlan(fsmeta.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, false, true)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourcePerasOnly, perasOnly.Source)
	require.True(t, perasOnly.IncludePeras)
	require.True(t, perasOnly.PerasOnly)
}
