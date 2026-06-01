// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"fmt"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorGetReadVersionReservesEphemeralTimestamp(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), model.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Equal(t, uint64(2), runner.nextTS)
}

func TestExecutorRetriesTimestampAuthorityRefreshBeforeMutate(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})

	require.NoError(t, err)
	require.Empty(t, runner.timestampErrs)
	require.Len(t, runner.mutations, 1)
	requireStatUint(t, executor.Stats(), "commit_retries_total", 1)
}

func TestExecutorRetriesReadTimestampAuthorityRefresh(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale authority era")}
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), model.ReadVersionRequest{Mount: "vol"})

	require.NoError(t, err)
	require.NotZero(t, version)
	require.Empty(t, runner.timestampErrs)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
}

func TestExecutorRetriesLostCommitLock(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
			Kind:    nokverrors.KindRetryable,
			Message: "backend: lock not found",
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesCommitTsExpired(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
			Kind:             nokverrors.KindCommitTsExpired,
			Key:              []byte("dentry"),
			CommitVersion:    2,
			MinCommitVersion: 5,
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesRouteUnavailable(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		nokverrors.New(nokverrors.KindRouteUnavailable, "route lookup refreshing"),
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesNotLeaderRouteRefresh(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		nokverrors.New(nokverrors.KindNotLeader, "metadata route points at old leader"),
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesWriteConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
			Kind:            nokverrors.KindWriteConflict,
			Key:             []byte("dentry"),
			ConflictVersion: 4,
			StartVersion:    2,
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesLockedCommitContention(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
			Kind:        nokverrors.KindLockConflict,
			Primary:     []byte("dentry"),
			Key:         []byte("dentry"),
			LockVersion: 2,
			LockTTL:     defaultLockTTL,
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestExecutorRetriesSustainedLiveCommitContention(t *testing.T) {
	runner := newFakeRunner()
	for range 9 {
		runner.mutateErrs = append(runner.mutateErrs, fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
			Kind:        nokverrors.KindLockConflict,
			Primary:     []byte("dentry"),
			Key:         []byte("dentry"),
			LockVersion: 2,
			LockTTL:     defaultLockTTL,
		}}})
	}
	runner.mutateErrs = append(runner.mutateErrs, nil)
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(21), runner.nextTS)
	require.Equal(t, uint64(9), executor.Stats()["commit_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["commit_retry_exhausted_total"])
}

func TestCommitContentionRetryPolicyUsesLockTTLAfterFixedAttempts(t *testing.T) {
	lockErr := fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
		Kind:        nokverrors.KindLockConflict,
		Primary:     []byte("dentry"),
		Key:         []byte("dentry"),
		LockVersion: 2,
		LockTTL:     uint64(5 * time.Second / time.Millisecond),
	}}}
	budget := commitRetryBudget(lockErr, defaultLockTTL)

	require.Equal(t, 5*time.Second+commitContentionRetryMaxBackoff, budget)
	require.True(t, canRetryCommitAttempt(maxCommitContentionRetries+8, time.Now(), lockErr, defaultLockTTL))
	require.False(t, canRetryCommitAttempt(maxCommitContentionRetries+8, time.Now().Add(-budget-time.Millisecond), lockErr, defaultLockTTL))
}

func TestCommitContentionRetryPolicyKeepsCountBoundForNonLockConflicts(t *testing.T) {
	writeConflictErr := fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
		Kind:            nokverrors.KindWriteConflict,
		Key:             []byte("dentry"),
		ConflictVersion: 4,
		StartVersion:    2,
	}}}

	require.Zero(t, commitRetryBudget(writeConflictErr, defaultLockTTL))
	require.True(t, canRetryCommitAttempt(maxCommitContentionRetries-1, time.Now(), writeConflictErr, defaultLockTTL))
	require.False(t, canRetryCommitAttempt(maxCommitContentionRetries, time.Now(), writeConflictErr, defaultLockTTL))
}

func TestCommitRetryBudgetFallsBackWhenLockDetailsAreUnavailable(t *testing.T) {
	err := nokverrors.New(nokverrors.KindLockConflict, "lock conflict translated across rpc boundary")

	require.Equal(t, 25*time.Millisecond+commitContentionRetryMaxBackoff, commitRetryBudget(err, 25))
}

func TestCommitRetryBudgetCoversRetryableStartTSLoss(t *testing.T) {
	err := fakeMetadataKeyError{errors: []nokverrors.MetadataKeyIssue{{
		Kind:    nokverrors.KindRetryable,
		Message: "backend: lock not found",
	}}}

	require.Equal(t, 50*time.Millisecond+commitContentionRetryMaxBackoff, commitRetryBudget(err, 50))
	require.True(t, canRetryCommitAttempt(maxCommitContentionRetries+1, time.Now(), err, 50))
}

func TestExecutorMetadataPredicateCommitRecordsEveryAttempt(t *testing.T) {
	base := newFakeRunner()
	runner := &fakePredicateRunner{fakeRunner: base}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	seedDirectory(t, runner.fakeRunner, "vol", 8)
	total := 20
	for i := range total {
		oldName := fmt.Sprintf("old-%d", i)
		newName := fmt.Sprintf("new-%d", i)
		seedDentry(t, runner.fakeRunner, "vol", 7, oldName, model.InodeID(100+i))
		err := executor.Rename(context.Background(), model.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   8,
			ToName:     newName,
		})
		require.NoError(t, err)
	}

	require.Len(t, runner.predicateCalls, total)
	stats := executor.Stats()
	requireMetadataPredicateStatUint(t, stats, model.OperationRename, "attempt_total", uint64(total))
	requireMetadataPredicateStatUint(t, stats, model.OperationRename, "success_total", uint64(total))
}
