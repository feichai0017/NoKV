// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeFinalizeLayer struct {
	id    string
	err   error
	calls *[]string
}

func (f *fakeFinalizeLayer) FinalizeSegment(_ context.Context, _ SegmentFinalizeRequest) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, f.id)
	}
	return f.err
}

func TestNewFinalizeChainReturnsNilWhenEmpty(t *testing.T) {
	require.Nil(t, NewFinalizeChain())
	require.Nil(t, NewFinalizeChain(nil, nil))
}

func TestNewFinalizeChainPassesThroughSingleLayer(t *testing.T) {
	layer := &fakeFinalizeLayer{id: "only"}
	chain := NewFinalizeChain(nil, layer)
	require.Same(t, SegmentFinalizer(layer), chain)
}

func TestFinalizeChainRunsLayersInOrder(t *testing.T) {
	calls := []string{}
	chain := NewFinalizeChain(
		&fakeFinalizeLayer{id: "sealed", calls: &calls},
		&fakeFinalizeLayer{id: "completion", calls: &calls},
	)
	require.NoError(t, chain.FinalizeSegment(context.Background(), SegmentFinalizeRequest{}))
	require.Equal(t, []string{"sealed", "completion"}, calls)
}

func TestFinalizeChainAbortsOnFirstError(t *testing.T) {
	calls := []string{}
	boom := errors.New("boom")
	chain := NewFinalizeChain(
		&fakeFinalizeLayer{id: "sealed", calls: &calls},
		&fakeFinalizeLayer{id: "completion", err: boom, calls: &calls},
		&fakeFinalizeLayer{id: "applied", calls: &calls},
	)
	err := chain.FinalizeSegment(context.Background(), SegmentFinalizeRequest{})
	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{"sealed", "completion"}, calls)
}
