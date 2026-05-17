// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import "context"

// SegmentFinalizeLayer names one step of the post-publish segment finalize
// pipeline. Finalize layers must not create new durability evidence; they only
// update runtime read/dedup state after a segment is safe to observe.
type SegmentFinalizeLayer = SegmentFinalizer

// NewFinalizeChain composes finalize layers into one SegmentFinalizer that
// runs each layer in order. nil layers are ignored so optional runtime pieces
// can be wired without caller-side branching.
func NewFinalizeChain(layers ...SegmentFinalizeLayer) SegmentFinalizer {
	filtered := make([]SegmentFinalizeLayer, 0, len(layers))
	for _, layer := range layers {
		if layer != nil {
			filtered = append(filtered, layer)
		}
	}
	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		return &finalizeChain{layers: filtered}
	}
}

type finalizeChain struct {
	layers []SegmentFinalizeLayer
}

func (c *finalizeChain) FinalizeSegment(ctx context.Context, req SegmentFinalizeRequest) error {
	for _, layer := range c.layers {
		if err := layer.FinalizeSegment(ctx, req); err != nil {
			return err
		}
	}
	return nil
}
