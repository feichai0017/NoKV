// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorRenameSubtreeMovesDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: 8,
		Name:   "new",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}, record)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 4)
	require.Equal(t, backend.MutationDelete, runner.mutations[0][0].Op)
	require.Equal(t, backend.MutationPut, runner.mutations[0][1].Op)
	require.True(t, runner.mutations[0][1].AssertionNotExist)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: model.RootInode, frontier: 2}}, publisher.starts)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: model.RootInode, frontier: 2}}, publisher.completes)
}

func TestExecutorRenameRejectsCrossAuthority(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	authority := &fakeAuthorityResolver{same: false}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, model.ErrCrossAuthorityRename)
	require.Empty(t, runner.mutations)
	require.Equal(t, 1, authority.calls)
}

func TestExecutorRenameSubtreeRejectsMissingSource(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "missing",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Empty(t, runner.mutations)
}

func TestExecutorRenameSubtreeRejectsExistingDestination(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDentry(t, runner, "vol", 8, "existing", 23)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "existing",
	})
	require.ErrorIs(t, err, model.ErrExists)
	require.Empty(t, runner.mutations)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
}

func BenchmarkExecutorRenameDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorRename(b, runner, executor)
}
