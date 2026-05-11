package peras

import "errors"

var ErrReplayStoreRequired = errors.New("fsmeta peras: replay store required")

// ReplayStore is the narrow apply boundary for a sealed Peras replay plan.
// Peras replay is intentionally linear here: raftstore owns committed-entry
// dependency scheduling and async apply.
type ReplayStore interface {
	ApplyPerasReplay([]ReplayOperation) error
}

type ApplyStats struct {
	Operations uint64
	Mutations  uint64
}

func ApplyReplayPlan(store ReplayStore, plan ReplayPlan) (ApplyStats, error) {
	if store == nil {
		return ApplyStats{}, ErrReplayStoreRequired
	}
	if err := validateReplayPlanForApply(plan); err != nil {
		return ApplyStats{}, err
	}
	var stats ApplyStats
	cloned := cloneReplayOperations(plan.Operations)
	if err := store.ApplyPerasReplay(cloned); err != nil {
		return ApplyStats{}, err
	}
	stats.Operations = uint64(len(plan.Operations))
	for _, op := range plan.Operations {
		stats.Mutations += uint64(len(op.Mutations))
	}
	return stats, nil
}

func validateReplayPlanForApply(plan ReplayPlan) error {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return ErrInvalidPerasSegment
	}
	seen := make(map[OperationID]struct{})
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return ErrInvalidPerasSegment
		}
		if _, ok := seen[op.OpID]; ok {
			return ErrInvalidPerasSegment
		}
		seen[op.OpID] = struct{}{}
		for _, mutation := range op.Mutations {
			if len(mutation.Key) == 0 {
				return ErrInvalidPerasSegment
			}
			switch {
			case mutation.Delete:
				if mutation.Value != nil {
					return ErrInvalidPerasSegment
				}
			case mutation.Value == nil:
				return ErrInvalidPerasSegment
			}
		}
	}
	return nil
}

func cloneReplayOperations(ops []ReplayOperation) []ReplayOperation {
	if len(ops) == 0 {
		return nil
	}
	out := make([]ReplayOperation, 0, len(ops))
	for _, op := range ops {
		out = append(out, cloneReplayOperation(op))
	}
	return out
}

func cloneReplayOperation(op ReplayOperation) ReplayOperation {
	mutations := make([]ReplayMutation, 0, len(op.Mutations))
	for _, mutation := range op.Mutations {
		mutations = append(mutations, ReplayMutation{
			Key:    cloneBytes(mutation.Key),
			Value:  cloneBytes(mutation.Value),
			Delete: mutation.Delete,
		})
	}
	return ReplayOperation{
		OpID:      op.OpID,
		Kind:      op.Kind,
		Mutations: mutations,
	}
}
