// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	localdb "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestRunnerProvidesSnapshotReads(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("alpha")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.Mutate(ctx, key, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
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

func TestRunnerRejectsCrossShardMutationWhenCallerOwnedDBIsNotAtomic(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	_, err = runner.Mutate(context.Background(), first, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: first, Value: []byte("a")},
		{Op: kvrpcpb.Mutation_Put, Key: second, Value: []byte("b")},
	}, start, start+1, 0)
	require.ErrorIs(t, err, errNonAtomicApplyGroup)
}

func TestRunnerTryAtomicMutateAppliesPredicateCheckedGroup(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("alpha"), []*kvrpcpb.AtomicPredicate{
		{Key: []byte("alpha"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
		{Key: []byte("beta"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
	}, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alpha"), Value: []byte("one")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("beta"), Value: []byte("two")},
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
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, []byte("alpha"), []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("alpha"),
		Value: []byte("one"),
	}}, start, start+1, 0)
	require.NoError(t, err)

	nextStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("beta"), []*kvrpcpb.AtomicPredicate{{
		Key:           []byte("alpha"),
		Kind:          kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS,
		ExpectedValue: []byte("stale"),
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
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

func TestRunnerTryAtomicMutateFallsBackWhenCallerOwnedDBIsNotAtomic(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(context.Background(), first, nil, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: first, Value: []byte("a")},
		{Op: kvrpcpb.Mutation_Put, Key: second, Value: []byte("b")},
	}, start, start+1)
	require.NoError(t, err)
	require.False(t, handled)
	require.Equal(t, uint64(1), runner.Stats()["atomic_apply_group_rejected_total"])
}

func TestRunnerTryAtomicMutateSerializesConcurrentSameKey(t *testing.T) {
	db := openTestDB(t, nil)
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
			handled, mutateErr := runner.TryAtomicMutate(ctx, key, []*kvrpcpb.AtomicPredicate{{
				Key:  key,
				Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
			}}, []*kvrpcpb.Mutation{{
				Op:    kvrpcpb.Mutation_Put,
				Key:   key,
				Value: []byte{byte(i)},
			}}, ts, ts+1)
			if !handled {
				errCh <- errNonAtomicApplyGroup
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

func keysOnDifferentLocalShards(t *testing.T, db *localdb.DB, shards int) ([]byte, []byte) {
	t.Helper()
	var first []byte
	for i := range 1024 {
		key := []byte{byte('a' + i%26), byte(i / 26)}
		entry := kv.NewInternalEntry(kv.CFDefault, key, 1, []byte("v"), 0, 0)
		same := db.CanApplyInternalEntriesAtomically([]*kv.Entry{entry})
		entry.DecrRef()
		require.True(t, same)
		if first == nil {
			first = append([]byte(nil), key...)
			continue
		}
		a := kv.NewInternalEntry(kv.CFDefault, first, 1, []byte("a"), 0, 0)
		b := kv.NewInternalEntry(kv.CFDefault, key, 1, []byte("b"), 0, 0)
		atomic := db.CanApplyInternalEntriesAtomically([]*kv.Entry{a, b})
		a.DecrRef()
		b.DecrRef()
		if !atomic {
			return first, append([]byte(nil), key...)
		}
	}
	t.Fatalf("failed to find keys on different local shards for shard count %d", shards)
	return nil, nil
}

func TestRunnerRestartsAboveObservedTimestamp(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, testDBOptions(dir, 1))
	runner, err := NewRunner(db)
	require.NoError(t, err)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	_, err = runner.Mutate(context.Background(), []byte("k"), []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("k"),
		Value: []byte("v"),
	}}, start, start+1, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db = openTestDB(t, testDBOptions(dir, 1))
	defer func() { require.NoError(t, db.Close()) }()
	recovered, err := NewRunner(db)
	require.NoError(t, err)
	next, err := recovered.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	require.Greater(t, next, start+1)
}

func openTestDB(t *testing.T, opts *localdb.Options) *localdb.DB {
	t.Helper()
	if opts == nil {
		opts = localdb.NewDefaultOptions()
		opts.WorkDir = t.TempDir()
		opts.LSMShardCount = 1
	}
	db, err := localdb.Open(opts)
	require.NoError(t, err)
	return db
}

func testDBOptions(dir string, shards int) *localdb.Options {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = dir
	opts.LSMShardCount = shards
	return opts
}

func testMount() fsmeta.MountIdentity {
	return fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
}
