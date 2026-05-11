package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type VersionAllocator interface {
	ReserveTimestamp(context.Context, uint64) (uint64, error)
}

type DirectCommitterConfig struct {
	Holder    *Holder
	Versions  VersionAllocator
	ReplayDB  InternalEntryApplier
	ApplyHook func(ReplayPlan, ApplyStats)
}

type DirectCommitter struct {
	holder   *Holder
	versions VersionAllocator
	replayDB InternalEntryApplier
	hook     func(ReplayPlan, ApplyStats)
}

func NewDirectCommitter(cfg DirectCommitterConfig) (*DirectCommitter, error) {
	if cfg.Holder == nil || cfg.Versions == nil || cfg.ReplayDB == nil {
		return nil, ErrHolderConfigInvalid
	}
	return &DirectCommitter{
		holder:   cfg.Holder,
		versions: cfg.Versions,
		replayDB: cfg.ReplayDB,
		hook:     cfg.ApplyHook,
	}, nil
}

func (c *DirectCommitter) CommitPeras(ctx context.Context, id OperationID, delta compile.SemanticDelta) (VisibleAck, error) {
	if c == nil || c.holder == nil || c.versions == nil || c.replayDB == nil {
		return VisibleAck{}, ErrHolderConfigInvalid
	}
	ack, err := c.holder.Submit(ctx, id, delta)
	if err != nil {
		return VisibleAck{}, err
	}
	pending := c.holder.PendingIDs()
	if len(pending) == 0 {
		return VisibleAck{}, ErrInvalidPerasSegment
	}
	firstVersion, err := c.versions.ReserveTimestamp(ctx, uint64(len(pending)))
	if err != nil {
		return VisibleAck{}, err
	}
	plan, _, err := c.holder.BuildPendingReplayPlan(firstVersion)
	if err != nil {
		return VisibleAck{}, err
	}
	if len(plan.Operations) != len(pending) {
		return VisibleAck{}, ErrInvalidPerasSegment
	}
	store, err := NewMVCCReplayStoreForPlan(c.replayDB, plan)
	if err != nil {
		return VisibleAck{}, err
	}
	stats, err := ApplyReplayPlan(store, plan)
	if err != nil {
		return VisibleAck{}, err
	}
	if c.hook != nil {
		c.hook(plan, stats)
	}
	if err := c.holder.MarkReplayPlanApplied(plan); err != nil {
		return VisibleAck{}, err
	}
	return ack, nil
}
