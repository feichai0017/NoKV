// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeInstallLayer struct {
	id     string
	cursor InstallCursor
	err    error
	calls  *[]string
}

func (f *fakeInstallLayer) InstallSegment(_ context.Context, _ SegmentInstallRequest) (InstallCursor, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, f.id)
	}
	return f.cursor, f.err
}

type fakePayloadAwareInstallLayer struct {
	fakeInstallLayer
	needsPayload bool
}

func (f *fakePayloadAwareInstallLayer) NeedsSegmentPayload() bool {
	return f.needsPayload
}

type fakeMaterializingInstallLayer struct {
	fakeInstallLayer
	materializes bool
}

func (f *fakeMaterializingInstallLayer) MaterializesSegments() bool {
	return f.materializes
}

func TestNewInstallChainReturnsNilWhenEmpty(t *testing.T) {
	require.Nil(t, NewInstallChain())
}

func TestNewInstallChainPassesThroughSingleLayer(t *testing.T) {
	layer := &fakeInstallLayer{id: "only"}
	chain := NewInstallChain(layer)
	require.Same(t, SegmentInstaller(layer), chain, "single-layer chain must not wrap")
}

func TestInstallChainRunsLayersInOrderAndReturnsFirstValidCursor(t *testing.T) {
	calls := []string{}
	a := &fakeInstallLayer{id: "a", cursor: InstallCursor{RegionID: 1, Term: 2, Index: 3, InstallVersion: 7}, calls: &calls}
	b := &fakeInstallLayer{id: "b", cursor: InstallCursor{RegionID: 9, Term: 9, Index: 9, InstallVersion: 99}, calls: &calls}
	c := &fakeInstallLayer{id: "c", calls: &calls}
	chain := NewInstallChain(a, b, c)
	cursor, err := chain.InstallSegment(context.Background(), SegmentInstallRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, calls)
	require.Equal(t, InstallCursor{RegionID: 1, Term: 2, Index: 3, InstallVersion: 7}, cursor,
		"chain reports the first valid cursor — a wins even though b also reports one")
}

func TestInstallChainSkipsZeroCursorLayers(t *testing.T) {
	// Some durable install layers may validate or attach evidence without
	// owning the InstallCursor. The chain must keep looking and pick up the
	// next valid cursor. This is the semantic that lets MVCCEntryLayer and the
	// catalog installer take turns owning the cursor based on
	// req.MaterializeMVCC.
	calls := []string{}
	zero := &fakeInstallLayer{id: "zero", calls: &calls}
	owner := &fakeInstallLayer{id: "owner", cursor: InstallCursor{RegionID: 4, Term: 5, Index: 6, InstallVersion: 8}, calls: &calls}
	tail := &fakeInstallLayer{id: "tail", calls: &calls}
	chain := NewInstallChain(zero, owner, tail)
	cursor, err := chain.InstallSegment(context.Background(), SegmentInstallRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"zero", "owner", "tail"}, calls,
		"every layer still runs — chain composition is not short-circuit on cursor")
	require.Equal(t, InstallCursor{RegionID: 4, Term: 5, Index: 6, InstallVersion: 8}, cursor)
}

func TestInstallChainReturnsZeroCursorWhenNoLayerProducesOne(t *testing.T) {
	// A chain may contain layers that validate durable evidence without owning
	// the InstallCursor. The chain reports zero cursor, callers that need a
	// cursor — flush pipeline's seal step in particular — already gate on
	// cursor.Valid() before consuming it.
	a := &fakeInstallLayer{id: "a"}
	b := &fakeInstallLayer{id: "b"}
	chain := NewInstallChain(a, b)
	cursor, err := chain.InstallSegment(context.Background(), SegmentInstallRequest{})
	require.NoError(t, err)
	require.Equal(t, InstallCursor{}, cursor)
}

func TestInstallChainAbortsOnFirstError(t *testing.T) {
	calls := []string{}
	boom := errors.New("boom")
	a := &fakeInstallLayer{id: "a", calls: &calls}
	b := &fakeInstallLayer{id: "b", err: boom, calls: &calls}
	c := &fakeInstallLayer{id: "c", calls: &calls}
	chain := NewInstallChain(a, b, c)
	cursor, err := chain.InstallSegment(context.Background(), SegmentInstallRequest{})
	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{"a", "b"}, calls, "layers after the failing one must not run")
	require.Equal(t, InstallCursor{}, cursor)
}

func TestInstallChainPropagatesRequestUnchanged(t *testing.T) {
	// Phase 1 contract: every layer receives the same SegmentInstallRequest
	// the runtime builds. Future phases will fan-out by reading specific
	// fields (Payload for CatalogLayer, Segment for WitnessSignLayer, etc.);
	// the chain itself must not mutate the request between layers.
	var seenScopes []string
	captured := func(id string) *fakeRequestObservingLayer {
		return &fakeRequestObservingLayer{id: id, collector: &seenScopes}
	}
	chain := NewInstallChain(captured("a"), captured("b"), captured("c"))
	req := SegmentInstallRequest{MaterializeMVCC: true}
	req.Scope.Mount = "fixture"
	_, err := chain.InstallSegment(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []string{"a:fixture", "b:fixture", "c:fixture"}, seenScopes,
		"every layer must observe the same Scope.Mount the runtime supplied")
}

func TestInstallChainSingleLayerErrorBubblesUp(t *testing.T) {
	// Single-layer chains are returned as the layer itself (no wrapper). A
	// layer-level error must surface intact — the chain abstraction is not
	// supposed to swallow or wrap errors.
	boom := errors.New("single-layer boom")
	layer := &fakeInstallLayer{id: "solo", err: boom}
	chain := NewInstallChain(layer)
	_, err := chain.InstallSegment(context.Background(), SegmentInstallRequest{})
	require.ErrorIs(t, err, boom)
}

func TestInstallChainReportsPayloadRequirement(t *testing.T) {
	require.False(t, segmentInstallerNeedsPayload(nil))
	require.True(t, segmentInstallerNeedsPayload(&fakeInstallLayer{}), "unknown installers keep the legacy encoded-payload contract")
	require.False(t, segmentInstallerNeedsPayload(&fakePayloadAwareInstallLayer{}))

	chain := NewInstallChain(
		&fakePayloadAwareInstallLayer{},
		&fakePayloadAwareInstallLayer{needsPayload: true},
	)
	require.True(t, segmentInstallerNeedsPayload(chain))

	chain = NewInstallChain(
		&fakePayloadAwareInstallLayer{},
		&fakePayloadAwareInstallLayer{},
	)
	require.False(t, segmentInstallerNeedsPayload(chain))
}

func TestInstallChainReportsMaterializationRequirement(t *testing.T) {
	require.False(t, segmentInstallerMaterializes(nil))
	require.False(t, segmentInstallerMaterializes(&fakeInstallLayer{}))
	require.False(t, segmentInstallerMaterializes(&fakeMaterializingInstallLayer{}))

	chain := NewInstallChain(
		&fakeMaterializingInstallLayer{},
		&fakeMaterializingInstallLayer{materializes: true},
	)
	require.True(t, segmentInstallerMaterializes(chain))

	chain = NewInstallChain(
		&fakeMaterializingInstallLayer{},
		&fakeMaterializingInstallLayer{},
	)
	require.False(t, segmentInstallerMaterializes(chain))
}

type fakeRequestObservingLayer struct {
	id        string
	collector *[]string
}

func (f *fakeRequestObservingLayer) InstallSegment(_ context.Context, req SegmentInstallRequest) (InstallCursor, error) {
	*f.collector = append(*f.collector, f.id+":"+string(req.Scope.Mount))
	return InstallCursor{}, nil
}
