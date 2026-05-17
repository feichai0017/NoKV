// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"testing"
	"time"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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
