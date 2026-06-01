// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

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
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    key,
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   key,
			Value: []byte("one"),
		}},
	})
	require.NoError(t, err)
	commit := result.CommitVersion
	require.Greater(t, commit, start)

	_, ok, err := runner.Get(ctx, key, start)
	require.NoError(t, err)
	require.False(t, ok)
	value, ok, err := runner.Get(ctx, key, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)

	rows, err := runner.Scan(ctx, []byte("a"), nil, 8, commit)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, key, rows[0].Key)
	require.Equal(t, []byte("one"), rows[0].Value)
}

func TestRunnerScanHonorsPrefix(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    []byte("a/1"),
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("a/1"), Value: []byte("one")},
			{Op: backend.MutationPut, Key: []byte("a/2"), Value: []byte("two")},
			{Op: backend.MutationPut, Key: []byte("b/1"), Value: []byte("other")},
		},
	})
	require.NoError(t, err)

	rows, err := runner.Scan(ctx, []byte("a/"), []byte("a/"), 8, result.CommitVersion)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, []byte("a/1"), rows[0].Key)
	require.Equal(t, []byte("a/2"), rows[1].Key)
}

func TestRunnerCommitMetadataHandlesMultiKeyBadgerTransaction(t *testing.T) {
	db := openTestDB(t, t.TempDir(), nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("multi-key-a"), []byte("multi-key-b")
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		PrimaryKey:    first,
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: first, Value: []byte("a")},
			{Op: backend.MutationPut, Key: second, Value: []byte("b")},
		},
	})
	require.NoError(t, err)
	require.Equal(t, start+1, result.CommitVersion)
}

func TestRunnerCommitMetadataAcceptsExplicitMultiKeyGroup(t *testing.T) {
	db := openTestDB(t, t.TempDir(), nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("install-key-a"), []byte("install-key-b")
	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    first,
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: first, Value: []byte("a")},
			{Op: backend.MutationPut, Key: second, Value: []byte("b")},
		},
	})
	require.NoError(t, err, "metadata commit must tolerate multi-key groups")
	require.Equal(t, start+1, result.CommitVersion)

	got, ok, err := runner.Get(ctx, first, result.CommitVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), got)
	got, ok, err = runner.Get(ctx, second, result.CommitVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), got)
}

func TestRunnerCommitMetadataAllowsMissingDeletePrimary(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("install-delete-missing")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    key,
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{{
			Op:  backend.MutationDelete,
			Key: key,
		}},
	})
	require.NoError(t, err, "metadata commit must accept tombstones for keys already absent")
	require.Equal(t, start+1, result.CommitVersion)

	_, ok, err := runner.Get(ctx, key, result.CommitVersion)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRunnerCommitMetadataRejectsBadCommitVersion(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    []byte("x"),
		ReadVersion:   start,
		CommitVersion: start,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("x"), Value: []byte("v")},
		},
	})
	require.Error(t, err)
}

func TestRunnerCommitMetadataEmptyGroupIsNoop(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:  []byte("primary"),
		ReadVersion: start,
	})
	require.NoError(t, err)
	require.Equal(t, start, result.CommitVersion)
}

func TestRunnerCommitMetadataRequestIDAppliesPredicateCheckedGroup(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:  []byte("alpha"),
		ReadVersion: start,
		Predicates: []*backend.Predicate{
			{Key: []byte("alpha"), Kind: backend.PredicateNotExists},
			{Key: []byte("beta"), Kind: backend.PredicateNotExists},
		},
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("alpha"), Value: []byte("one")},
			{Op: backend.MutationPut, Key: []byte("beta"), Value: []byte("two")},
		},
	})
	require.NoError(t, err)

	value, ok, err := runner.Get(ctx, []byte("alpha"), result.CommitVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)
	stats := runner.Stats()
	require.Equal(t, uint64(1), stats["metadata_commit_total"])
	require.Equal(t, uint64(0), stats["metadata_predicate_rejected_total"])
}

func TestRunnerCommitMetadataAppliesPredicateCheckedGroup(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	result, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		RequestID:   []byte("metadata-command-1"),
		Mount:       "vol",
		MountKeyID:  1,
		ReadVersion: readVersion,
		Predicates: []*backend.Predicate{
			{Key: []byte("alpha"), Kind: backend.PredicateNotExists},
			{Key: []byte("beta"), Kind: backend.PredicateNotExists},
		},
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("alpha"), Value: []byte("one")},
			{Op: backend.MutationPut, Key: []byte("beta"), Value: []byte("two")},
		},
	})
	require.NoError(t, err)
	require.Greater(t, result.CommitVersion, readVersion)
	require.Equal(t, uint64(2), result.AppliedMutations)

	value, ok, err := runner.Get(ctx, []byte("alpha"), result.CommitVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)
	stats := runner.Stats()
	require.Equal(t, uint64(1), stats["metadata_commit_total"])
	require.Equal(t, uint64(0), stats["metadata_predicate_rejected_total"])
}

func TestRunnerCommitMetadataRejectsPredicateWithoutPartialApply(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    []byte("alpha"),
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("alpha"),
			Value: []byte("one"),
		}},
	})
	require.NoError(t, err)

	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		RequestID:   []byte("metadata-command-reject"),
		ReadVersion: readVersion,
		Predicates: []*backend.Predicate{{
			Key:           []byte("alpha"),
			Kind:          backend.PredicateValueEquals,
			ExpectedValue: []byte("stale"),
		}},
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("beta"),
			Value: []byte("two"),
		}},
	})
	require.Error(t, err)
	require.True(t, nokverrors.Retryable(err))
	_, ok, getErr := runner.Get(ctx, []byte("beta"), localMaxVersion)
	require.NoError(t, getErr)
	require.False(t, ok)
	require.Equal(t, uint64(1), runner.Stats()["metadata_predicate_rejected_total"])
}

func TestRunnerCommitMetadataRequestIDIsIdempotent(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	first, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		RequestID:   []byte("metadata-command-idempotent"),
		ReadVersion: readVersion,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("alpha"),
			Value: []byte("one"),
		}},
	})
	require.NoError(t, err)
	second, err := runner.CommitMetadata(ctx, backend.MetadataCommand{
		RequestID:   []byte("metadata-command-idempotent"),
		ReadVersion: readVersion,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("alpha"),
			Value: []byte("two"),
		}},
	})
	require.NoError(t, err)
	require.Equal(t, first, second)
	value, ok, err := runner.Get(ctx, []byte("alpha"), first.CommitVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)
	require.Equal(t, uint64(1), runner.Stats()["metadata_commit_total"])
}

func TestRunnerCommitMetadataRejectsValuePredicateMismatch(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    []byte("alpha"),
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("alpha"),
			Value: []byte("one"),
		}},
	})
	require.NoError(t, err)

	nextStart, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:  []byte("beta"),
		ReadVersion: nextStart,
		Predicates: []*backend.Predicate{{
			Key:           []byte("alpha"),
			Kind:          backend.PredicateValueEquals,
			ExpectedValue: []byte("stale"),
		}},
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("beta"),
			Value: []byte("two"),
		}},
	})
	require.Error(t, err)
	require.True(t, nokverrors.Retryable(err))
	_, ok, getErr := runner.Get(ctx, []byte("beta"), nextStart+1)
	require.NoError(t, getErr)
	require.False(t, ok)
	require.Equal(t, uint64(1), runner.Stats()["metadata_predicate_rejected_total"])
}

func TestRunnerCommitMetadataHandlesPredicateFreeMultiKeyBadgerTransaction(t *testing.T) {
	db := openTestDB(t, "", nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := []byte("atomic-key-a"), []byte("atomic-key-b")
	start, err := runner.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	_, err = runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		PrimaryKey:  first,
		ReadVersion: start,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: first, Value: []byte("a")},
			{Op: backend.MutationPut, Key: second, Value: []byte("b")},
		},
	})
	require.NoError(t, err)
}

func TestRunnerCommitMetadataSerializesConcurrentSameKey(t *testing.T) {
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
			_, mutateErr := runner.CommitMetadata(ctx, backend.MetadataCommand{
				PrimaryKey:  key,
				ReadVersion: ts,
				Predicates: []*backend.Predicate{{
					Key:  key,
					Kind: backend.PredicateNotExists,
				}},
				Mutations: []*backend.Mutation{{
					Op:    backend.MutationPut,
					Key:   key,
					Value: []byte{byte(i)},
				}},
			})
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
	_, err = runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		PrimaryKey:    []byte("k"),
		ReadVersion:   start,
		CommitVersion: start + 1,
		Mutations: []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   []byte("k"),
			Value: []byte("v"),
		}},
	})
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

func openTestDB(t *testing.T, dir string, opts *badger.Options) *badger.DB {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	cfg := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(false)
	if opts != nil {
		cfg = *opts
		if cfg.Dir == "" {
			cfg.Dir = dir
		}
		if cfg.ValueDir == "" {
			cfg.ValueDir = cfg.Dir
		}
	}
	db, err := badger.Open(cfg)
	require.NoError(t, err)
	return db
}

func testMount() model.MountIdentity {
	return model.MountIdentity{MountID: "vol", MountKeyID: 1}
}
