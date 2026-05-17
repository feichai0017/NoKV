// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync"
	"testing"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/stretchr/testify/require"
)

func TestNewSealedTrackingLayerRejectsNilRuntime(t *testing.T) {
	require.Nil(t, newSealedTrackingLayer(nil))
}

func TestSealedTrackingLayerFinalizesRuntimeReadView(t *testing.T) {
	committer, err := NewRuntime(Config{
		Authority:         &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:        &recordingVisibleLog{},
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	key := testRuntimeDentryKeyForLabel("sealed")
	segment := testRuntimePerasSegmentForOverlay(key, []byte("sealed-value"))
	layer := newSealedTrackingLayer(committer)
	require.NoError(t, layer.FinalizeSegment(context.Background(), SegmentFinalizeRequest{
		Plan:    fsperas.ReplayPlan{},
		Segment: segment,
	}))

	value, deleted, ok := committer.GetPerasOverlay(key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("sealed-value"), value)

	committer.read.mu.RLock()
	defer committer.read.mu.RUnlock()
	require.Len(t, committer.read.sealedSegments, 1)
	require.Len(t, committer.read.segments, 1)
}

func TestSealedTrackingLayerMaterializedSegmentDoesNotEnterSealedOverlay(t *testing.T) {
	committer, err := NewRuntime(Config{
		Authority:         &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:        &recordingVisibleLog{},
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	key := testRuntimeDentryKeyForLabel("materialized")
	layer := newSealedTrackingLayer(committer)
	require.NoError(t, layer.FinalizeSegment(context.Background(), SegmentFinalizeRequest{
		Segment:         testRuntimePerasSegmentForOverlay(key, []byte("base-value")),
		MaterializeMVCC: true,
	}))

	_, _, ok := committer.GetPerasOverlay(key)
	require.False(t, ok)

	committer.read.mu.RLock()
	defer committer.read.mu.RUnlock()
	require.Empty(t, committer.read.sealedSegments)
	require.Len(t, committer.read.segments, 1)
}

func TestNewCompletionIndexLayerRejectsNilReadState(t *testing.T) {
	require.Nil(t, newCompletionIndexLayer(nil))
}

func TestCompletionIndexLayerFinalizesCompletionsUnderRuntimeLock(t *testing.T) {
	read := newReadState()
	layer := newCompletionIndexLayer(read)
	req := SegmentFinalizeRequest{
		Segment: fsperas.PerasSegment{
			EpochID: 7,
			Completions: []fsperas.SegmentCompletion{
				{OpID: fsperas.OperationID{ClientID: "c", Seq: 1}, Version: 11},
				{OpID: fsperas.OperationID{ClientID: "c", Seq: 2}, Version: 12},
			},
		},
	}

	err := layer.FinalizeSegment(context.Background(), req)
	require.NoError(t, err)

	read.mu.RLock()
	defer read.mu.RUnlock()
	require.Len(t, read.completed, 2)
	first, ok := read.completed[fsperas.OperationID{ClientID: "c", Seq: 1}]
	require.True(t, ok)
	require.Equal(t, uint64(7), first.epochID)
	require.Equal(t, uint64(11), first.completion.Version)
	second, ok := read.completed[fsperas.OperationID{ClientID: "c", Seq: 2}]
	require.True(t, ok)
	require.Equal(t, uint64(7), second.epochID)
}

func TestCompletionIndexLayerEmptyCompletionsIsNoop(t *testing.T) {
	read := newReadState()
	layer := newCompletionIndexLayer(read)
	err := layer.FinalizeSegment(context.Background(), SegmentFinalizeRequest{Segment: fsperas.PerasSegment{EpochID: 3}})
	require.NoError(t, err)
	require.Empty(t, read.completed)
}

func TestCompletionIndexLayerLaterEpochOverwritesEarlier(t *testing.T) {
	// The live install pipeline guarantees segment-level ordering, so when
	// two segments report a completion for the same OpID the later install
	// reflects the authoritative epoch. The layer must overwrite rather
	// than merge.
	read := newReadState()
	layer := newCompletionIndexLayer(read)
	opID := fsperas.OperationID{ClientID: "c", Seq: 1}

	earlier := SegmentFinalizeRequest{Segment: fsperas.PerasSegment{
		EpochID:     2,
		Completions: []fsperas.SegmentCompletion{{OpID: opID, Version: 100}},
	}}
	err := layer.FinalizeSegment(context.Background(), earlier)
	require.NoError(t, err)

	later := SegmentFinalizeRequest{Segment: fsperas.PerasSegment{
		EpochID:     5,
		Completions: []fsperas.SegmentCompletion{{OpID: opID, Version: 200}},
	}}
	err = layer.FinalizeSegment(context.Background(), later)
	require.NoError(t, err)

	read.mu.RLock()
	defer read.mu.RUnlock()
	entry, ok := read.completed[opID]
	require.True(t, ok)
	require.Equal(t, uint64(5), entry.epochID)
	require.Equal(t, uint64(200), entry.completion.Version)
}

func TestCompletionIndexLayerConcurrentInstallsAreSafe(t *testing.T) {
	read := newReadState()
	layer := newCompletionIndexLayer(read)
	const workers = 16
	const perWorker = 64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	wg.Add(workers)
	for w := range workers {
		go func(client int) {
			defer wg.Done()
			completions := make([]fsperas.SegmentCompletion, perWorker)
			for i := range completions {
				completions[i] = fsperas.SegmentCompletion{
					OpID:    fsperas.OperationID{ClientID: clientLabel(client), Seq: uint64(i + 1)},
					Version: uint64(client*1_000 + i),
				}
			}
			req := SegmentFinalizeRequest{Segment: fsperas.PerasSegment{EpochID: 1, Completions: completions}}
			errCh <- layer.FinalizeSegment(context.Background(), req)
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	read.mu.RLock()
	defer read.mu.RUnlock()
	require.Len(t, read.completed, workers*perWorker)
}

func TestCompletionIndexLayerNilReceiverReturnsInvalid(t *testing.T) {
	var layer *completionIndexLayer
	err := layer.FinalizeSegment(context.Background(), SegmentFinalizeRequest{})
	require.ErrorIs(t, err, ErrRuntimeInvalid)
}

func clientLabel(id int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	if id >= 0 && id < len(alphabet) {
		return string(alphabet[id])
	}
	return "x"
}
