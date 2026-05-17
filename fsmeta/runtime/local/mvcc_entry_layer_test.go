// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	localdb "github.com/feichai0017/NoKV/local"
	"github.com/stretchr/testify/require"
)

func TestNewMVCCEntryLayerRejectsNilRunner(t *testing.T) {
	require.Nil(t, newMVCCEntryLayer(nil))
}

func TestMVCCEntryLayerNoopWhenMaterializeFalse(t *testing.T) {
	// Catalog-mode requests must not touch base MVCC; the catalog
	// installer is the cursor producer for that mode. Returning a zero
	// cursor lets the chain skip past this layer.
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)
	layer := newMVCCEntryLayer(runner)

	segment := mvccEntryTestSegment(t, "client", 1, []byte("k1"), []byte("v1"))
	cursor, err := layer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		MaterializeMVCC: false,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{}, cursor, "materialize=false must return a zero cursor")

	readVersion, err := runner.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	_, ok, err := runner.Get(context.Background(), segment.EntriesView()[0].Key, readVersion)
	require.NoError(t, err)
	require.False(t, ok, "no entries should be written when materialize=false")
}

func TestMVCCEntryLayerWritesEntriesWhenMaterializeTrue(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)
	layer := newMVCCEntryLayer(runner)

	segment := mvccEntryTestSegment(t, "client", 1, []byte("k1"), []byte("v1"))
	cursor, err := layer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		MaterializeMVCC: true,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid(), "materialize=true install must produce a valid cursor")
	require.Equal(t, uint64(localPerasRegionID), cursor.RegionID)
	require.Equal(t, uint64(localPerasTerm), cursor.Term)
	require.Equal(t, cursor.Index, cursor.InstallVersion,
		"local install uses commit version as both raft index and install version")

	value, ok, err := runner.Get(context.Background(), segment.EntriesView()[0].Key, cursor.InstallVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segment.EntriesView()[0].Value, value)
}

func TestMVCCEntryLayerEmptyEntriesIsNoop(t *testing.T) {
	// Defensive: an empty segment (no entries to install) must not
	// reserve a timestamp or invoke InstallMutationsAtCommit, otherwise
	// the call burns a commit version with zero work to show for it.
	// Two consecutive ReserveTimestamp(1) calls before and after the
	// install must yield consecutive values — meaning the install did
	// not slip a reservation in between.
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)
	layer := newMVCCEntryLayer(runner)

	before, err := runner.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	cursor, err := layer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         fsperas.PerasSegment{EpochID: 1},
		MaterializeMVCC: true,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{}, cursor)
	after, err := runner.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, before+1, after, "empty segment install must not consume a timestamp")
}

func TestMVCCEntryLayerCrossShardEntriesAreAtomic(t *testing.T) {
	// MVCCEntryLayer's defining capability vs the percolator
	// MutateAtCommit path: install groups can span LSM shards because
	// InstallMutationsAtCommit relaxes the cross-shard atomicity guard.
	// Asserts the layer wires that path through (not the percolator
	// commit path, which would reject the multi-shard write).
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)
	layer := newMVCCEntryLayer(runner)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	segment := mvccEntryTestSegmentMulti(t, "client", 1, [][2][]byte{
		{first, []byte("alpha")},
		{second, []byte("beta")},
	})

	cursor, err := layer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		MaterializeMVCC: true,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid())

	for _, want := range [][2][]byte{{first, []byte("alpha")}, {second, []byte("beta")}} {
		value, ok, err := runner.Get(context.Background(), want[0], cursor.InstallVersion)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want[1], value)
	}
}

func TestMVCCEntryLayerNilReceiverReturnsInvalid(t *testing.T) {
	var layer *mvccEntryLayer
	_, err := layer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{MaterializeMVCC: true})
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func mvccEntryTestSegment(t *testing.T, client string, seq uint64, key, value []byte) fsperas.PerasSegment {
	t.Helper()
	return mvccEntryTestSegmentMulti(t, client, seq, [][2][]byte{{key, value}})
}

func mvccEntryTestSegmentMulti(t *testing.T, client string, seq uint64, pairs [][2][]byte) fsperas.PerasSegment {
	t.Helper()
	mutations := make([]fsperas.ReplayMutation, 0, len(pairs))
	for _, p := range pairs {
		mutations = append(mutations, fsperas.ReplayMutation{Key: p[0], Value: p[1]})
	}
	plan := fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID:      fsperas.OperationID{ClientID: client, Seq: seq},
			Kind:      fsmeta.OperationCreate,
			Mutations: mutations,
		}},
	}
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	return segment
}
