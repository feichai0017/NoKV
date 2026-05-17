// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import "context"

// SegmentInstallLayer names one step of the segment install pipeline.
// It is an alias for SegmentInstaller so that today's single-installer
// callers keep working while future phases can compose multiple durable
// install layers (MVCC entry write, catalog write, witness sign) with the
// same contract. See docs/guide/development/peras_install_layers.md for the
// migration plan.
type SegmentInstallLayer = SegmentInstaller

// NewInstallChain composes layers into one SegmentInstaller that runs
// each layer in order. The first layer that returns a valid (non-zero)
// InstallCursor is the chain's reported cursor; subsequent layers'
// cursors are ignored. Layers that do not own the cursor return a zero cursor
// and the chain skips past them when picking the reported cursor. The first
// layer that returns an error aborts the chain.
//
// A chain of length 1 returns the layer directly so the existing
// single-installer code paths incur no indirection. nil or zero-length
// input returns nil.
func NewInstallChain(layers ...SegmentInstallLayer) SegmentInstaller {
	switch len(layers) {
	case 0:
		return nil
	case 1:
		return layers[0]
	default:
		return &installChain{layers: append([]SegmentInstallLayer(nil), layers...)}
	}
}

type installChain struct {
	layers []SegmentInstallLayer
}

// InstallSegment runs each configured layer in declaration order.
//
// Cursor selection: the chain returns the first valid InstallCursor any
// layer produces (Valid() == true). Layers that do not own segment durability
// return a zero cursor and the chain keeps looking. Phase 1 layers all owned
// the cursor so this is a pure superset of the pre-Phase-2 contract.
//
// Failure-mode contract: if layer i returns an error, layers j>i are not
// invoked. Side effects from layers 0..i-1 are left in place; callers
// are responsible for retry / compensation policy. The chain matches
// the existing single-installer contract where any install error aborts
// the segment install pipeline.
func (c *installChain) InstallSegment(ctx context.Context, req SegmentInstallRequest) (InstallCursor, error) {
	var primary InstallCursor
	for _, layer := range c.layers {
		cursor, err := layer.InstallSegment(ctx, req)
		if err != nil {
			return InstallCursor{}, err
		}
		if !primary.Valid() && cursor.Valid() {
			primary = cursor
		}
	}
	return primary, nil
}

func (c *installChain) NeedsSegmentPayload() bool {
	for _, layer := range c.layers {
		if segmentInstallerNeedsPayload(layer) {
			return true
		}
	}
	return false
}

func segmentInstallerNeedsPayload(installer SegmentInstaller) bool {
	if installer == nil {
		return false
	}
	requirement, ok := installer.(SegmentPayloadRequirement)
	if !ok {
		return true
	}
	return requirement.NeedsSegmentPayload()
}
