// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

// VisibleLogStateRecord is a visible WAL operation plus the applied-marker
// state that covers it during replay.
type VisibleLogStateRecord struct {
	Record  fsperas.VisibleOperationRecord
	Applied bool
}

type visibleLogStateReplayer interface {
	ReplayVisibleState(context.Context) ([]VisibleLogStateRecord, error)
}

func (c *Runtime) recoverVisibleLog(ctx context.Context) (int, error) {
	if c == nil || c.visibleLog == nil {
		return 0, nil
	}
	if c.recoverAppliedVisibleLogState() {
		if replayer, ok := c.visibleLog.(visibleLogStateReplayer); ok {
			return c.recoverVisibleLogState(ctx, replayer)
		}
	}
	replayer, ok := c.visibleLog.(fsperas.VisibleLogReplayer)
	if !ok {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := replayer.ReplayVisible(ctx)
	if err != nil {
		return 0, c.recordErrorf("replay peras visible log: %w", err)
	}
	recovered := 0
	for _, record := range records {
		ok, err := c.recoverVisiblePendingRecord(ctx, record)
		if err != nil {
			return recovered, err
		}
		if ok {
			recovered++
		}
	}
	return recovered, nil
}

func (c *Runtime) recoverAppliedVisibleLogState() bool {
	return c != nil && c.materialize && c.catalog == nil
}

func (c *Runtime) recoverVisibleLogState(ctx context.Context, replayer visibleLogStateReplayer) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := replayer.ReplayVisibleState(ctx)
	if err != nil {
		return 0, c.recordErrorf("replay peras visible log: %w", err)
	}
	recovered := 0
	for _, state := range records {
		if state.Applied {
			ok, err := c.recoverVisibleAppliedCompletion(ctx, state.Record)
			if err != nil {
				return recovered, err
			}
			if ok {
				recovered++
			}
			continue
		}
		ok, err := c.recoverVisiblePendingRecord(ctx, state.Record)
		if err != nil {
			return recovered, err
		}
		if ok {
			recovered++
		}
	}
	return recovered, nil
}

func (c *Runtime) recoverVisiblePendingRecord(ctx context.Context, record fsperas.VisibleOperationRecord) (bool, error) {
	if record.HolderID != c.authority.HolderID() {
		c.metrics.visibleLogRecoverSkipTotal.Add(1)
		return false, nil
	}
	active, owned, err := c.authority.Acquire(ctx, record.Scope)
	if err != nil {
		return false, c.recordErrorf("recover peras visible authority: %w", err)
	}
	if !owned || active.HolderID != record.HolderID {
		return false, c.recordErrorf("recover peras visible authority transferred: %w", ErrNotHeld)
	}
	if !visibleLogLineageCovers(record, active) {
		c.metrics.visibleLogRecoverSkipTotal.Add(1)
		return false, nil
	}
	grant := visibleRecordGrant(record)
	if active.EpochID == record.EpochID && active.GrantID == record.GrantID {
		grant = active
	} else {
		c.metrics.visibleLogRecoverOldEpochTotal.Add(1)
	}
	holder, err := c.holderForGrant(ctx, grant, record.Scope)
	if err != nil {
		return false, c.recordErrorf("recover peras visible holder: %w", err)
	}
	if err := holder.RestoreVisible(record.Scope, record.Operation); err != nil {
		return false, c.recordErrorf("restore peras visible holder: %w", err)
	}
	if err := c.read.overlay.AddReplayOperation(record.Operation); err != nil {
		return false, c.recordErrorf("restore peras visible overlay: %w", err)
	}
	c.metrics.visibleLogRecoverTotal.Add(1)
	return true, nil
}

func (c *Runtime) recoverVisibleAppliedCompletion(ctx context.Context, record fsperas.VisibleOperationRecord) (bool, error) {
	if record.HolderID != c.authority.HolderID() {
		c.metrics.visibleLogRecoverSkipTotal.Add(1)
		return false, nil
	}
	active, owned, err := c.authority.Acquire(ctx, record.Scope)
	if err != nil {
		return false, c.recordErrorf("recover peras visible authority: %w", err)
	}
	if !owned || active.HolderID != record.HolderID {
		return false, c.recordErrorf("recover peras visible authority transferred: %w", ErrNotHeld)
	}
	if !visibleLogLineageCovers(record, active) {
		c.metrics.visibleLogRecoverSkipTotal.Add(1)
		return false, nil
	}
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID:    record.EpochID,
		Operations: []fsperas.ReplayOperation{record.Operation},
	})
	if err != nil {
		return false, c.recordErrorf("recover peras visible completion: %w", err)
	}
	c.read.mergeCompletions(segment)
	c.metrics.visibleLogRecoverTotal.Add(1)
	return true, nil
}

func visibleRecordGrant(record fsperas.VisibleOperationRecord) rootproto.PerasAuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:           record.GrantID,
		EpochID:           record.EpochID,
		HolderID:          record.HolderID,
		Scope:             AuthorityScopeFromDelta(record.Scope),
		ExpiresUnixNano:   record.GrantExpiresNanos,
		PredecessorDigest: record.PredecessorDigest,
		RootClusterEpoch:  record.RootLineage.ClusterEpoch,
		IssuedRootToken: rootproto.AuthorityRootToken{
			Term:     record.RootLineage.Term,
			Index:    record.RootLineage.Index,
			Revision: record.RootLineage.Revision,
		},
	}
}

func visibleLogLineageCovers(record fsperas.VisibleOperationRecord, active rootproto.PerasAuthorityGrant) bool {
	if !record.RootLineage.Valid() {
		return false
	}
	activeLineage := visibleRootLineageFromGrant(active)
	if !activeLineage.Valid() || record.RootLineage.ClusterEpoch != activeLineage.ClusterEpoch {
		return false
	}
	if active.PredecessorDigest != record.PredecessorDigest {
		return false
	}
	if active.EpochID < record.EpochID {
		return false
	}
	if active.EpochID == record.EpochID && active.GrantID != record.GrantID {
		return false
	}
	return visibleRootLineageAtOrAfter(activeLineage, record.RootLineage)
}

func visibleRootLineageFromGrant(grant rootproto.PerasAuthorityGrant) fsperas.VisibleRootLineage {
	return fsperas.VisibleRootLineage{
		ClusterEpoch: grant.RootClusterEpoch,
		Term:         grant.IssuedRootToken.Term,
		Index:        grant.IssuedRootToken.Index,
		Revision:     grant.IssuedRootToken.Revision,
	}
}

func visibleRootLineageAtOrAfter(active, record fsperas.VisibleRootLineage) bool {
	if active.Term != record.Term {
		return active.Term > record.Term
	}
	if active.Index != record.Index {
		return active.Index > record.Index
	}
	return active.Revision >= record.Revision
}

type visibleReplayPlanApplier interface {
	AppendVisibleReplayPlanApplied(context.Context, uint64, string, fsperas.ReplayPlan) error
}

func (c *Runtime) markVisibleLogApplied(ctx context.Context, holder *fsperas.Holder, plan fsperas.ReplayPlan) error {
	if c == nil || c.visibleLog == nil || holder == nil || len(plan.Operations) == 0 {
		return nil
	}
	applier, ok := c.visibleLog.(visibleReplayPlanApplier)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := applier.AppendVisibleReplayPlanApplied(context.WithoutCancel(ctx), holder.EpochID(), holder.HolderID(), plan)
	if err != nil {
		c.metrics.visibleLogApplyErrorTotal.Add(1)
		return c.recordErrorf("mark peras visible log applied: %w", err)
	}
	c.metrics.visibleLogApplyMarkerTotal.Add(1)
	return nil
}
