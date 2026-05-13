package peras

import (
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *Runtime) freezeFlushBatchesLocked(target *compile.AuthorityScope, materialize bool, maxOpsPerHolder int) ([]perasFlushBatch, error) {
	plans, err := c.freezeReplayPlansLocked(target, maxOpsPerHolder)
	if err != nil {
		return nil, err
	}
	return c.buildFlushBatches(plans, materialize)
}

func (c *Runtime) buildFlushBatches(plans []perasFrozenPlan, materialize bool) ([]perasFlushBatch, error) {
	batches := make([]perasFlushBatch, 0, len(plans))
	for _, frozen := range plans {
		batch := perasFlushBatch{
			holder: frozen.holder,
			plan:   frozen.plan,
			jobs:   make([]perasFlushJob, 0, 1),
		}
		sized, err := splitReplayPlanByCompilerBudget(frozen.plan, materialize, c.replaySegmentBudget(materialize))
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
		if len(batch.jobs) > 0 {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (c *Runtime) replaySegmentBudget(materialize bool) compile.SegmentBudget {
	budget := compile.SegmentBudget{
		MaxOperations:   uint32(c.maxOps),
		MaxMutations:    uint32(c.maxReplay),
		MaxPayloadBytes: c.maxPayload,
	}
	if !materialize {
		return budget
	}
	if c.maxReplay > 0 && c.maxReplay < defaultPerasMaterializeMaxReplayMutations {
		budget.MaxMutations = uint32(c.maxReplay)
	} else {
		budget.MaxMutations = defaultPerasMaterializeMaxReplayMutations
	}
	return budget
}

func splitReplayPlanByCompilerBudget(plan fsperas.ReplayPlan, materialize bool, budget compile.SegmentBudget) ([]fsperas.ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, fsperas.ErrReplayVersionRequired
	}
	out := make([]fsperas.ReplayPlan, 0, len(plan.Operations))
	current := fsperas.ReplayPlan{EpochID: plan.EpochID}
	var currentPlan compile.SegmentPlan
	flush := func() {
		if len(current.Operations) == 0 {
			return
		}
		out = append(out, fsperas.ReplayPlan{
			EpochID:    current.EpochID,
			Operations: cloneRuntimeReplayOperations(current.Operations),
		})
		current.Operations = current.Operations[:0]
		currentPlan = compile.SegmentPlan{}
	}
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		nextPlan, ok := compile.SegmentPlanForInstall(op.Segment, materialize)
		if !ok {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		if len(current.Operations) > 0 {
			decision := compile.CanAppendSegmentPlans(currentPlan, nextPlan, op.Durability, budget)
			if decision.Kind != compile.SegmentDecisionAppend {
				flush()
			}
		}
		current.Operations = append(current.Operations, cloneRuntimeReplayOperation(op))
		if len(current.Operations) == 1 {
			currentPlan = nextPlan
		} else {
			currentPlan = compile.MergeSegmentPlans(currentPlan, nextPlan)
		}
	}
	flush()
	return out, nil
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
		ExecutionPlanDigest:  op.ExecutionPlanDigest,
		Segment:              op.Segment,
		Atomicity:            cloneRuntimeReplayAtomicity(op.Atomicity),
		Durability:           op.Durability,
		Mutations:            mutations,
	}
}

func cloneRuntimeReplayAtomicity(group compile.AtomicityGroup) compile.AtomicityGroup {
	group.Members = append([]compile.MutationID(nil), group.Members...)
	return group
}

func (c *Runtime) freezeReplayPlansLocked(target *compile.AuthorityScope, maxOpsPerHolder int) ([]perasFrozenPlan, error) {
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

func (c *Runtime) buildFlushPlan(holder *fsperas.Holder, target *compile.AuthorityScope, maxOps int) (fsperas.ReplayPlan, compile.AuthorityScope, bool, error) {
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

func (c *Runtime) holderSnapshot() []*fsperas.Holder {
	if c == nil || c.epochs == nil {
		return nil
	}
	return c.epochs.holderSnapshot()
}
