// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	cpebble "github.com/cockroachdb/pebble"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestRunnerProvidesSnapshotReads(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("alpha")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.Mutate(ctx, key, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   key,
		Value: []byte("one"),
	}}, start, start+1, 0)
	require.NoError(t, err)
	require.Greater(t, commit, start)

	_, ok, err := runner.Get(ctx, key, start)
	require.NoError(t, err)
	require.False(t, ok)
	value, ok, err := runner.Get(ctx, key, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)

	rows, err := runner.Scan(ctx, []byte("a"), 8, commit)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, key, rows[0].Key)
	require.Equal(t, []byte("one"), rows[0].Value)
}

func TestRunnerMutateHandlesMultiKeyPebbleBatch(t *testing.T) {
	db := openTestDB(t, t.TempDir(), nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("multi-key-a"), []byte("multi-key-b")
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	commit, err := runner.Mutate(context.Background(), first, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, commit, start+1)
}

func TestRunnerInstallMutationsAtCommitAcceptsMultiKeyGroup(t *testing.T) {
	db := openTestDB(t, t.TempDir(), nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("install-key-a"), []byte("install-key-b")
	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, first, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1)
	require.NoError(t, err, "install must tolerate multi-key groups")
	require.Equal(t, start+1, commit)

	got, ok, err := runner.Get(ctx, first, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), got)
	got, ok, err = runner.Get(ctx, second, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), got)
}

func TestRunnerInstallMutationsAtCommitAllowsMissingDeletePrimary(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("install-delete-missing")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, key, []*backend.Mutation{{
		Op:  backend.MutationDelete,
		Key: key,
	}}, start, start+1)
	require.NoError(t, err, "segment install must accept tombstones for keys already absent")
	require.Equal(t, start+1, commit)

	_, ok, err := runner.Get(ctx, key, commit)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRunnerInstallMutationsAtCommitRejectsBadCommitVersion(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	// commitVersion must be strictly greater than startVersion — same
	// contract as MutateAtCommit so callers can't accidentally clobber
	// MVCC ordering through the install path.
	_, err = runner.InstallMutationsAtCommit(ctx, []byte("x"), []*backend.Mutation{
		{Op: backend.MutationPut, Key: []byte("x"), Value: []byte("v")},
	}, start, start)
	require.Error(t, err)
}

func TestRunnerInstallMutationsAtCommitEmptyGroupIsNoop(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, []byte("primary"), nil, start, start+1)
	require.NoError(t, err)
	// An empty install must still complete cleanly; the chain layers that
	// produce zero mutations (e.g., a future SealedTrackingLayer that only
	// updates in-memory state) need this path to succeed.
	require.GreaterOrEqual(t, commit, uint64(0))
}

// Note: in-process retry of InstallMutationsAtCommit at the same
// commitVersion is intentionally not supported — the runner's nextTS
// advances past the commit on the first successful call and the second
// call returns commit_ts_expired. The crash-recovery story uses a fresh
// Runner instance (process restart), where nextTS is rebuilt from disk state.

func TestRunnerTryAtomicMutateAppliesPredicateCheckedGroup(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("alpha"), []*backend.Predicate{
		{Key: []byte("alpha"), Kind: backend.PredicateNotExists},
		{Key: []byte("beta"), Kind: backend.PredicateNotExists},
	}, []*backend.Mutation{
		{Op: backend.MutationPut, Key: []byte("alpha"), Value: []byte("one")},
		{Op: backend.MutationPut, Key: []byte("beta"), Value: []byte("two")},
	}, start, start+1)
	require.NoError(t, err)
	require.True(t, handled)

	value, ok, err := runner.Get(ctx, []byte("alpha"), start+1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)
	stats := runner.Stats()
	require.Equal(t, uint64(1), stats["atomic_mutate_total"])
	require.Equal(t, uint64(0), stats["atomic_predicate_rejected_total"])
}

func TestRunnerTryAtomicMutateRejectsValuePredicateMismatch(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, []byte("alpha"), []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("alpha"),
		Value: []byte("one"),
	}}, start, start+1, 0)
	require.NoError(t, err)

	nextStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("beta"), []*backend.Predicate{{
		Key:           []byte("alpha"),
		Kind:          backend.PredicateValueEquals,
		ExpectedValue: []byte("stale"),
	}}, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("beta"),
		Value: []byte("two"),
	}}, nextStart, nextStart+1)
	require.Error(t, err)
	require.True(t, handled)
	require.True(t, nokverrors.Retryable(err))
	_, ok, getErr := runner.Get(ctx, []byte("beta"), nextStart+1)
	require.NoError(t, getErr)
	require.False(t, ok)
	require.Equal(t, uint64(1), runner.Stats()["atomic_predicate_rejected_total"])
}

func TestRunnerTryAtomicMutateHandlesMultiKeyPebbleBatch(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("atomic-key-a"), []byte("atomic-key-b")
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(context.Background(), first, nil, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1)
	require.NoError(t, err)
	require.True(t, handled)
}

func TestRunnerTryAtomicMutateSerializesConcurrentSameKey(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("same-dentry")
	start := make(chan struct{})
	errCh := make(chan error, 32)
	var successes atomic.Uint64
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ts, reserveErr := runner.ReserveTimestamp(ctx, 2)
			if reserveErr != nil {
				errCh <- reserveErr
				return
			}
			handled, mutateErr := runner.TryAtomicMutate(ctx, key, []*backend.Predicate{{
				Key:  key,
				Kind: backend.PredicateNotExists,
			}}, []*backend.Mutation{{
				Op:    backend.MutationPut,
				Key:   key,
				Value: []byte{byte(i)},
			}}, ts, ts+1)
			if !handled {
				errCh <- errInvalidAtomicMutate
				return
			}
			if mutateErr == nil {
				successes.Add(1)
				return
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, uint64(1), successes.Load())
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, ok, err := runner.Get(ctx, key, readVersion)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRunnerRestartsAboveObservedTimestamp(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, nil)
	runner, err := NewRunner(db)
	require.NoError(t, err)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	_, err = runner.Mutate(context.Background(), []byte("k"), []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("k"),
		Value: []byte("v"),
	}}, start, start+1, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db = openTestDB(t, dir, nil)
	defer func() { require.NoError(t, db.Close()) }()
	recovered, err := NewRunner(db)
	require.NoError(t, err)
	next, err := recovered.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	require.Greater(t, next, start+1)
}

func openTestDB(t *testing.T, dir string, opts *cpebble.Options) *cpebble.DB {
	t.Helper()
	if opts == nil {
		opts = &cpebble.Options{}
	}
	if dir == "" {
		dir = t.TempDir()
	}
	db, err := cpebble.Open(dir, opts)
	require.NoError(t, err)
	return db
}

func testMount() model.MountIdentity {
	return model.MountIdentity{MountID: "vol", MountKeyID: 1}
}
