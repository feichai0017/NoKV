// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import "context"

// completionIndexLayer records each segment's per-operation completion in the
// runtime's read state so duplicate-operation submits (SubmitVisible retries)
// can short-circuit without re-issuing the work. The layer is in-memory only
// and runs after the segment is safe to observe. See
// docs/guide/development/peras_install_layers.md.
type completionIndexLayer struct {
	read *readState
}

func newCompletionIndexLayer(read *readState) SegmentFinalizeLayer {
	if read == nil {
		return nil
	}
	return &completionIndexLayer{read: read}
}

// FinalizeSegment merges the segment's completions into the runtime's
// completion index via readState.mergeCompletions. Durability ownership
// belongs to the pre-publish installer; this layer only updates retry dedup
// after the runtime read path is valid.
func (l *completionIndexLayer) FinalizeSegment(_ context.Context, req SegmentFinalizeRequest) error {
	if l == nil || l.read == nil {
		return ErrRuntimeInvalid
	}
	l.read.mergeCompletions(req.Segment)
	return nil
}
