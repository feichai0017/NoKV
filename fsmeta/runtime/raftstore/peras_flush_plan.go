package raftstore

import (
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *RemotePerasCommitter) freezeFlushBatchesLocked(target *compile.AuthorityScope, materialize bool, maxOpsPerHolder int) ([]perasFlushBatch, error) {
	plans, err := c.freezeReplayPlansLocked(target, maxOpsPerHolder)
	if err != nil {
		return nil, err
	}
	return c.buildFlushBatches(plans, materialize)
}

func (c *RemotePerasCommitter) buildFlushBatches(plans []perasFrozenPlan, materialize bool) ([]perasFlushBatch, error) {
	batches := make([]perasFlushBatch, 0, len(plans))
	for _, frozen := range plans {
		installPlans, err := fsperas.SplitReplayPlanByFSMetaBucket(frozen.plan)
		if !materialize {
			installPlans, err = fsperas.SplitReplayPlanForCatalogInstall(frozen.plan)
		}
		if err != nil {
			return nil, c.recordErrorf("split peras replay plan: %w", err)
		}
		batch := perasFlushBatch{
			holder: frozen.holder,
			plan:   frozen.plan,
			jobs:   make([]perasFlushJob, 0, len(installPlans)),
		}
		for _, installPlan := range installPlans {
			sized, err := fsperas.SplitReplayPlanByMutationBudget(installPlan, c.replayMutationBudget(materialize))
			if err != nil {
				return nil, c.recordErrorf("split peras replay plan by install budget: %w", err)
			}
			for _, plan := range sized {
				segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
				if err != nil {
					return nil, c.recordErrorf("build peras segment: %w", err)
				}
				payload, err := fsperas.EncodePerasSegment(segment)
				if err != nil {
					return nil, c.recordErrorf("encode peras segment: %w", err)
				}
				digest, err := fsperas.PerasSegmentPayloadDigest(payload)
				if err != nil {
					return nil, c.recordErrorf("digest peras segment: %w", err)
				}
				batch.jobs = append(batch.jobs, perasFlushJob{
					scope:       frozen.scope,
					plan:        plan,
					segment:     segment,
					payload:     payload,
					digest:      digest,
					materialize: materialize,
				})
			}
		}
		if len(batch.jobs) > 0 {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (c *RemotePerasCommitter) replayMutationBudget(materialize bool) int {
	if !materialize {
		return c.maxReplay
	}
	if c.maxReplay > 0 && c.maxReplay < defaultPerasMaterializeMaxReplayMutations {
		return c.maxReplay
	}
	return defaultPerasMaterializeMaxReplayMutations
}

func (c *RemotePerasCommitter) freezeReplayPlansLocked(target *compile.AuthorityScope, maxOpsPerHolder int) ([]perasFrozenPlan, error) {
	holders := c.holderSnapshot()
	plans := make([]perasFrozenPlan, 0, len(holders))
	for _, holder := range holders {
		plan, scope, ok, err := c.buildFlushPlan(holder, target, maxOpsPerHolder)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		plans = append(plans, perasFrozenPlan{
			holder: holder,
			scope:  scope,
			plan:   plan,
		})
	}
	return plans, nil
}

func (c *RemotePerasCommitter) buildFlushPlan(holder *fsperas.Holder, target *compile.AuthorityScope, maxOps int) (fsperas.ReplayPlan, compile.AuthorityScope, bool, error) {
	if target != nil {
		plan, scope, ok, err := holder.BuildPendingReplayPlanForScope(0, *target)
		if err != nil {
			return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
		}
		return plan, scope, ok, nil
	}
	pending := holder.PendingIDs()
	if len(pending) == 0 {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, nil
	}
	plan, scope, err := holder.BuildPendingReplayPlanLimit(0, maxOps)
	if err != nil {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
	}
	if maxOps <= 0 && len(plan.Operations) != len(pending) {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordError(fsperas.ErrInvalidPerasSegment)
	}
	return plan, scope, true, nil
}

func (c *RemotePerasCommitter) holderSnapshot() []*fsperas.Holder {
	c.holdersMu.Lock()
	defer c.holdersMu.Unlock()
	out := make([]*fsperas.Holder, 0, len(c.holders))
	for _, holder := range c.holders {
		out = append(out, holder)
	}
	return out
}
