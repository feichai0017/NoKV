package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type WitnessSnapshotSource interface {
	Probe(context.Context, uint64) (WitnessSnapshot, error)
}

type VersionAllocator interface {
	ReserveTimestamp(context.Context, uint64) (uint64, error)
}

type DirectCommitterConfig struct {
	Holder    *Holder
	Snapshot  WitnessSnapshotSource
	Versions  VersionAllocator
	ReplayDB  InternalEntryApplier
	ApplyHook func(ReplayPlan, ApplyStats)
}

type DirectCommitter struct {
	holder   *Holder
	snapshot WitnessSnapshotSource
	versions VersionAllocator
	replayDB InternalEntryApplier
	hook     func(ReplayPlan, ApplyStats)
}

func NewDirectCommitter(cfg DirectCommitterConfig) (*DirectCommitter, error) {
	if cfg.Holder == nil || cfg.Snapshot == nil || cfg.Versions == nil || cfg.ReplayDB == nil {
		return nil, ErrHolderConfigInvalid
	}
	return &DirectCommitter{
		holder:   cfg.Holder,
		snapshot: cfg.Snapshot,
		versions: cfg.Versions,
		replayDB: cfg.ReplayDB,
		hook:     cfg.ApplyHook,
	}, nil
}

func (c *DirectCommitter) CommitPeras(ctx context.Context, id OperationID, delta compile.SemanticDelta) (PerasSeal, error) {
	if c == nil || c.holder == nil || c.snapshot == nil || c.versions == nil || c.replayDB == nil {
		return PerasSeal{}, ErrHolderConfigInvalid
	}
	if _, err := c.holder.Submit(ctx, id, delta); err != nil {
		return PerasSeal{}, err
	}
	pending := c.holder.PendingIDs()
	if len(pending) == 0 {
		return PerasSeal{}, ErrInvalidPerasSeal
	}
	firstVersion, err := c.versions.ReserveTimestamp(ctx, uint64(len(pending)))
	if err != nil {
		return PerasSeal{}, err
	}
	snapshot, err := c.snapshot.Probe(ctx, c.holder.EpochID())
	if err != nil {
		return PerasSeal{}, err
	}
	seal, err := c.holder.BuildPendingSealWithVersions(firstVersion, snapshot)
	if err != nil {
		return PerasSeal{}, err
	}
	if len(seal.Certificates) != len(pending) {
		return PerasSeal{}, ErrInvalidPerasSeal
	}
	plan, err := BuildReplayPlan(seal)
	if err != nil {
		return PerasSeal{}, err
	}
	store, err := NewMVCCReplayStoreForPlan(c.replayDB, plan)
	if err != nil {
		return PerasSeal{}, err
	}
	stats, err := ApplyReplayPlan(store, plan)
	if err != nil {
		return PerasSeal{}, err
	}
	if c.hook != nil {
		c.hook(plan, stats)
	}
	if err := c.holder.MarkSealApplied(seal); err != nil {
		return PerasSeal{}, err
	}
	return seal, nil
}
