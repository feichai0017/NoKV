// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (c *Runtime) recoverVisibleLog(ctx context.Context) (int, error) {
	if c == nil || c.visibleLog == nil {
		return 0, nil
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
		if record.HolderID != c.authority.HolderID() {
			c.metrics.visibleLogRecoverSkipTotal.Add(1)
			continue
		}
		active, owned, err := c.authority.Acquire(ctx, record.Scope)
		if err != nil {
			return recovered, c.recordErrorf("recover peras visible authority: %w", err)
		}
		if !owned || active.HolderID != record.HolderID {
			return recovered, c.recordErrorf("recover peras visible authority transferred: %w", ErrNotHeld)
		}
		if !visibleLogLineageCovers(record, active) {
			c.metrics.visibleLogRecoverSkipTotal.Add(1)
			continue
		}
		grant := visibleRecordGrant(record)
		if active.EpochID == record.EpochID && active.GrantID == record.GrantID {
			grant = active
		} else {
			c.metrics.visibleLogRecoverOldEpochTotal.Add(1)
		}
		holder, err := c.holderForGrant(ctx, grant, record.Scope)
		if err != nil {
			return recovered, c.recordErrorf("recover peras visible holder: %w", err)
		}
		if err := holder.RestoreVisible(record.Scope, record.Operation); err != nil {
			return recovered, c.recordErrorf("restore peras visible holder: %w", err)
		}
		if err := c.read.overlay.AddReplayOperation(record.Operation); err != nil {
			return recovered, c.recordErrorf("restore peras visible overlay: %w", err)
		}
		c.metrics.visibleLogRecoverTotal.Add(1)
		recovered++
	}
	return recovered, nil
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

func (c *Runtime) markVisibleLogApplied(ctx context.Context, holder *fsperas.Holder, plan fsperas.ReplayPlan) error {
	if c == nil || c.visibleLog == nil || holder == nil || len(plan.Operations) == 0 {
		return nil
	}
	applier, ok := c.visibleLog.(fsperas.VisibleLogApplier)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	operations := make([]fsperas.VisibleOperationReference, 0, len(plan.Operations))
	for _, op := range plan.Operations {
		if !op.OpID.Valid() {
			return fsperas.ErrInvalidOperationID
		}
		ref, err := fsperas.VisibleOperationReferenceFromReplay(op)
		if err != nil {
			return err
		}
		operations = append(operations, ref)
	}
	err := applier.AppendVisibleApplied(context.WithoutCancel(ctx), fsperas.VisibleAppliedRecord{
		EpochID:    holder.EpochID(),
		HolderID:   holder.HolderID(),
		Operations: operations,
	})
	if err != nil {
		c.metrics.visibleLogApplyErrorTotal.Add(1)
		return c.recordErrorf("mark peras visible log applied: %w", err)
	}
	c.metrics.visibleLogApplyMarkerTotal.Add(1)
	return nil
}
