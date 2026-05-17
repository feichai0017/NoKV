// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"fmt"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// mvccEntryLayer applies a segment's entries directly to base MVCC for
// the local runtime when req.MaterializeMVCC=true. It is the chain
// counterpart of the materialize branch that used to live inside
// localPerasSegmentInstaller.
//
// The local runtime now wires this as its only install layer and enables
// MaterializeSegments. The false branch remains a no-op so tests or temporary
// callers that use a non-materialized request fail closed instead of writing a
// catalog record through the local path.
type mvccEntryLayer struct {
	runner *Runner
}

func newMVCCEntryLayer(runner *Runner) runtimeperas.SegmentInstallLayer {
	if runner == nil {
		return nil
	}
	return &mvccEntryLayer{runner: runner}
}

func (l *mvccEntryLayer) InstallSegment(ctx context.Context, req runtimeperas.SegmentInstallRequest) (runtimeperas.InstallCursor, error) {
	if l == nil || l.runner == nil {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	if !req.MaterializeMVCC {
		return runtimeperas.InstallCursor{}, nil
	}
	mutations := buildMVCCEntryMutations(req.Segment)
	if len(mutations) == 0 {
		return runtimeperas.InstallCursor{}, nil
	}
	startVersion, err := l.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	commitVersion := startVersion + 1
	primary := cloneBytes(mutations[0].GetKey())
	if _, err := l.runner.InstallMutationsAtCommit(ctx, primary, mutations, startVersion, commitVersion); err != nil {
		return runtimeperas.InstallCursor{}, fmt.Errorf("local peras mvcc entry install entries=%d: %w", len(mutations), err)
	}
	return runtimeperas.InstallCursor{
		RegionID:       localPerasRegionID,
		Term:           localPerasTerm,
		Index:          commitVersion,
		InstallVersion: commitVersion,
	}, nil
}

func (l *mvccEntryLayer) NeedsSegmentPayload() bool {
	return false
}

// buildMVCCEntryMutations folds a segment's entries (across all kinds
// — dentry, inode, chunk, session, usage, other, tombstone) into a
// single MVCC mutation list. Duplicate keys collapse to the latest
// write via appendLocalPerasMutation, matching the original materialize
// branch's behavior.
func buildMVCCEntryMutations(segment fsperas.PerasSegment) []*kvrpcpb.Mutation {
	entries := segment.EntriesView()
	if len(entries) == 0 {
		return nil
	}
	mutations := make([]*kvrpcpb.Mutation, 0, len(entries))
	for _, entry := range entries {
		m := &kvrpcpb.Mutation{Key: cloneBytes(entry.Key)}
		if entry.Delete {
			m.Op = kvrpcpb.Mutation_Delete
		} else {
			m.Op = kvrpcpb.Mutation_Put
			m.Value = cloneBytes(entry.Value)
		}
		mutations = appendLocalPerasMutation(mutations, m)
	}
	return mutations
}
