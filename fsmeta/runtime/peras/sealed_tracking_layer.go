// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import "context"

// sealedTrackingLayer finalizes the runtime read view for an installed
// segment. Non-materialized segments are added to the sealed overlay; all
// segments are removed from the visible overlay and counted in segment stats.
type sealedTrackingLayer struct {
	runtime *Runtime
}

func newSealedTrackingLayer(runtime *Runtime) SegmentFinalizeLayer {
	if runtime == nil {
		return nil
	}
	return &sealedTrackingLayer{runtime: runtime}
}

func (l *sealedTrackingLayer) FinalizeSegment(_ context.Context, req SegmentFinalizeRequest) error {
	if l == nil || l.runtime == nil {
		return ErrRuntimeInvalid
	}
	return l.runtime.installSegment(req.Plan, req.Segment, req.MaterializeMVCC)
}
