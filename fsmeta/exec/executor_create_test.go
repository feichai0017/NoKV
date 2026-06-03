// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorCreateRequiresInodeAllocator(t *testing.T) {
	executor, err := newTestExecutor(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, errInodeAllocatorRequired)
}

func TestExecutorCreateUsesMetadataPredicateCommit(t *testing.T) {
	base := newFakeRunner()
	runner := &fakePredicateRunner{fakeRunner: base}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	req := model.CreateRequest{Mount: "vol", Parent: model.RootInode, Name: "file", Attrs: model.CreateAttrs{Type: model.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	plan, err := layout.PlanCreate(req, testMountIdentity, 22)
	require.NoError(t, err)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "attempt_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "success_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "skip_total", 0)
	require.Len(t, runner.predicateCalls, 1)
	call := runner.predicateCalls[0]
	require.Equal(t, plan.PrimaryKey, call.primary)
	require.Equal(t, uint64(1), call.startVersion)
	require.Equal(t, uint64(2), call.commitVersion)
	require.Len(t, call.predicates, 4)
	require.Equal(t, plan.ReadKeys[0], call.predicates[0].Key)
	require.Equal(t, backend.PredicateValueEquals, call.predicates[0].Kind)
	require.Equal(t, plan.MutateKeys[1], call.predicates[1].Key)
	require.Equal(t, backend.PredicateNotExists, call.predicates[1].Kind)
	require.Equal(t, plan.MutateKeys[2], call.predicates[2].Key)
	require.Equal(t, backend.PredicateNotExists, call.predicates[2].Kind)
	require.Equal(t, backend.PredicateNotExists, call.predicates[3].Kind)
	require.Len(t, call.mutations, 4)
	require.True(t, call.mutations[1].AssertionNotExist)
	require.True(t, call.mutations[2].AssertionNotExist)
	require.True(t, call.mutations[3].AssertionNotExist)
	require.Equal(t, backend.CommandKindCreate, runner.commands[0].Kind)
	require.Empty(t, base.mutations)
	require.Equal(t, []backend.ReadPurpose{backend.ReadPurposeWritePlanLocal}, base.readPurposes)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), record.Inode)
	require.Equal(t, backend.ReadPurposeUserStrong, base.readPurposes[len(base.readPurposes)-1])
}

func TestExecutorCreateUsesMetadataPredicatesWithDefaultRunner(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "attempt_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "success_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "skip_total", 0)
	require.Len(t, runner.mutations, 1)
}

func TestExecutorCreateSkipsMetadataPredicateCommitWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	seedDirectory(t, base, "vol", 7)
	runner := &fakePredicateRunner{fakeRunner: base}
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	// Quota reservation adds an extra key outside Create's compiled predicate
	// set, so Create records a predicate-commit skip and submits the full
	// metadata command without generated dentry/inode predicates.
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "attempt_total", 0)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "success_total", 0)
	requireMetadataPredicateStatUint(t, stats, model.OperationCreate, "skip_total", 1)
	require.Empty(t, runner.predicateCalls)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 5)
	require.Equal(t, quotaKey, base.mutations[0][4].Key)
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22, 23}}))
	require.NoError(t, err)

	req := model.CreateRequest{Mount: "vol", Parent: model.RootInode, Name: "file", Attrs: model.CreateAttrs{Type: model.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), req)
	require.ErrorIs(t, err, model.ErrExists)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 2, runner.getCalls)
}

func TestExecutorCreateRequiresActiveMountWhenResolverConfigured(t *testing.T) {
	t.Run("active mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.NoError(t, err)
		require.Equal(t, 1, resolver.calls)
		require.Len(t, runner.mutations, 1)
	})

	t.Run("missing mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "missing",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.ErrorIs(t, err, model.ErrMountNotRegistered)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})

	t.Run("retired mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1, Retired: true},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.ErrorIs(t, err, model.ErrMountRetired)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})
}

func TestExecutorCreateReservesQuotaInsideMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][4].Key)
}

func TestExecutorCreateRejectsQuotaExceededBeforeMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quota := &fakeQuotaResolver{err: model.ErrQuotaExceeded}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.ErrorIs(t, err, model.ErrQuotaExceeded)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorCreateTranslatesAlreadyExistsConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErr = fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
		Kind: nokverrors.KindAlreadyExists,
		Key:  []byte("dentry"),
	}}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, model.ErrExists)
	require.Equal(t, 1, runner.getCalls)
}

func BenchmarkExecutorCreateDefaultPath(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorCheckpointStormDefaultPath100(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, 100)
}
