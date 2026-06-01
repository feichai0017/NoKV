// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorUpdateInodeUsesMetadataPredicateCommitWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakePredicateRunner{fakeRunner: base}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Mode: 0o644, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Len(t, runner.predicateCalls, 1)
	require.Empty(t, base.mutations)
	requireMetadataPredicateStatUint(t, executor.Stats(), model.OperationUpdateInode, "success_total", 1)
	require.Equal(t, backend.PredicateValueEquals, runner.predicateCalls[0].predicates[0].Kind)
	require.Equal(t, backend.PredicateValueEquals, runner.predicateCalls[0].predicates[1].Kind)

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(0o600), stored.Mode)
}

func TestExecutorUpdateInodeSkipsMetadataPredicateCommitWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakePredicateRunner{fakeRunner: base}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Size: 1024, LinkCount: 1})
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    2048,
	})
	require.NoError(t, err)
	require.Empty(t, runner.predicateCalls)
	require.Len(t, base.mutations, 1)
	requireMetadataPredicateStatUint(t, executor.Stats(), model.OperationUpdateInode, "skip_total", 1)
}

func TestExecutorUpdateInodeUpdatesMutableFieldsAndQuota(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:         22,
		Type:          model.InodeTypeFile,
		Size:          4096,
		Mode:          0o644,
		LinkCount:     1,
		CreatedUnixNs: 10,
		UpdatedUnixNs: 20,
	})
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           7,
		Inode:            22,
		Name:             "file",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    30,
		SetOpaqueAttrs:   true,
		OpaqueAttrs:      []byte("body=cas://1"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8192), updated.Size)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Equal(t, int64(30), updated.UpdatedUnixNs)
	require.Equal(t, []byte("body=cas://1"), updated.OpaqueAttrs)
	require.Equal(t, int64(10), updated.CreatedUnixNs)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][1].Key)

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
}

func TestExecutorUpdateInodeRejectsHardLinkedInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Size: 4096, LinkCount: 2})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    8192,
	})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorUpdateInodeRejectsDentryTypeMismatch(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "file", 22, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Size: 4096, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.ErrorIs(t, err, model.ErrInvalidValue)
	require.Empty(t, runner.mutations)
}

func BenchmarkExecutorUpdateInodeDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUpdateInode(b, runner, executor)
}
