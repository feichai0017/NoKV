package raftstore

import (
	"github.com/feichai0017/NoKV/fsmeta"
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
			sized, err := splitReplayPlanByCompilerBudget(installPlan, c.replayMutationBudget(materialize))
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

func splitReplayPlanByCompilerBudget(plan fsperas.ReplayPlan, maxMutations int) ([]fsperas.ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 || maxMutations <= 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, fsperas.ErrReplayVersionRequired
	}
	catalogInstall := replayPlanUsesCatalogInstall(plan)
	out := make([]fsperas.ReplayPlan, 0, len(plan.Operations))
	current := fsperas.ReplayPlan{EpochID: plan.EpochID}
	var currentPlan compile.CompiledOp
	flush := func() {
		if len(current.Operations) == 0 {
			return
		}
		out = append(out, fsperas.ReplayPlan{
			EpochID:    current.EpochID,
			Operations: cloneRuntimeReplayOperations(current.Operations),
		})
		current.Operations = current.Operations[:0]
		currentPlan = compile.CompiledOp{}
	}
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		nextPlan := compiledReplayOperation(op)
		if catalogInstall {
			forceCatalogInstallPlan(&nextPlan)
		}
		if len(current.Operations) > 0 {
			decision := compile.CanAppendSegment(currentPlan, nextPlan, compile.SegmentBudget{
				MaxMutations: uint32(maxMutations),
			})
			if decision.Kind != compile.SegmentDecisionAppend {
				flush()
			}
		}
		current.Operations = append(current.Operations, cloneRuntimeReplayOperation(op))
		if len(current.Operations) == 1 {
			currentPlan = nextPlan
		} else {
			currentPlan = mergeCompilerSegmentPlan(currentPlan, nextPlan)
		}
	}
	flush()
	return out, nil
}

func compiledReplayOperation(op fsperas.ReplayOperation) compile.CompiledOp {
	delta := compile.SemanticDelta{
		Kind:         op.Kind,
		Eligibility:  compile.EligibilityVisibleCommit,
		WriteEffects: make([]compile.WriteEffect, 0, len(op.Mutations)),
	}
	for _, mutation := range op.Mutations {
		effect := compile.WriteEffect{
			Kind:  compile.EffectPut,
			Key:   mutation.Key,
			Value: mutation.Value,
		}
		if mutation.Delete {
			effect.Kind = compile.EffectDelete
			effect.Value = nil
		}
		delta.WriteEffects = append(delta.WriteEffects, effect)
	}
	return compile.CompileDelta(delta)
}

func replayPlanUsesCatalogInstall(plan fsperas.ReplayPlan) bool {
	var mount fsmeta.MountKeyID
	buckets := make(map[fsmeta.AffinityBucket]struct{})
	for _, op := range plan.Operations {
		for _, mutation := range op.Mutations {
			parts, ok := fsmeta.InspectKey(mutation.Key)
			if !ok {
				return false
			}
			if mount == 0 {
				mount = parts.MountKeyID
			} else if mount != parts.MountKeyID {
				return false
			}
			buckets[parts.Bucket] = struct{}{}
			if len(buckets) > 1 {
				return true
			}
		}
	}
	return false
}

func forceCatalogInstallPlan(op *compile.CompiledOp) {
	if op == nil {
		return
	}
	op.Placement.Install = compile.SegmentInstallCatalog
	op.Placement.SingleBucket = false
	op.Placement.MergeKey.PrimaryBucket = 0
	op.Placement.MergeKey.Install = compile.SegmentInstallCatalog
	op.Segment.Install = compile.SegmentInstallCatalog
	op.Segment.MergeKey = op.Placement.MergeKey
}

func mergeCompilerSegmentPlan(current, next compile.CompiledOp) compile.CompiledOp {
	out := current
	out.Segment.OperationCount += next.Segment.OperationCount
	out.Segment.MutationCount += next.Segment.MutationCount
	out.Segment.EstimatedPayloadBytes += next.Segment.EstimatedPayloadBytes
	return out
}

func cloneRuntimeReplayOperations(ops []fsperas.ReplayOperation) []fsperas.ReplayOperation {
	out := make([]fsperas.ReplayOperation, 0, len(ops))
	for _, op := range ops {
		out = append(out, cloneRuntimeReplayOperation(op))
	}
	return out
}

func cloneRuntimeReplayOperation(op fsperas.ReplayOperation) fsperas.ReplayOperation {
	mutations := make([]fsperas.ReplayMutation, 0, len(op.Mutations))
	for _, mutation := range op.Mutations {
		mutations = append(mutations, fsperas.ReplayMutation{
			Key:    append([]byte(nil), mutation.Key...),
			Value:  append([]byte(nil), mutation.Value...),
			Delete: mutation.Delete,
		})
	}
	return fsperas.ReplayOperation{
		OpID:                 op.OpID,
		Kind:                 op.Kind,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: op.PredicateProofDigest,
		Mutations:            mutations,
	}
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
