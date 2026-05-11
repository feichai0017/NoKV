package capsule

import "errors"

var ErrReplayStoreRequired = errors.New("fsmeta capsule: replay store required")

// ReplayStore is the narrow apply boundary for a sealed Capsule replay plan.
// Waves are delivered sequentially; the implementation may atomically apply or
// internally parallelize operations within one wave because the conflict DAG
// already proves that wave members are independent.
type ReplayStore interface {
	ApplyCapsuleReplayWave([]ReplayOperation) error
}

type ApplyStats struct {
	Waves      uint64
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
	for _, wave := range plan.Waves {
		cloned := cloneReplayWave(wave)
		if err := store.ApplyCapsuleReplayWave(cloned); err != nil {
			return stats, err
		}
		stats.Waves++
		stats.Operations += uint64(len(wave))
		for _, op := range wave {
			stats.Mutations += uint64(len(op.Mutations))
		}
	}
	return stats, nil
}

func validateReplayPlanForApply(plan ReplayPlan) error {
	if plan.EpochID == 0 || len(plan.Waves) == 0 {
		return ErrInvalidCapsuleSeal
	}
	seen := make(map[OperationID]struct{})
	for _, wave := range plan.Waves {
		if len(wave) == 0 {
			return ErrInvalidCapsuleSeal
		}
		for _, op := range wave {
			if !op.OpID.Valid() || len(op.Mutations) == 0 {
				return ErrInvalidCapsuleSeal
			}
			if _, ok := seen[op.OpID]; ok {
				return ErrInvalidCapsuleSeal
			}
			seen[op.OpID] = struct{}{}
			for _, mutation := range op.Mutations {
				if len(mutation.Key) == 0 {
					return ErrInvalidCapsuleSeal
				}
				switch {
				case mutation.Delete:
					if mutation.Value != nil {
						return ErrInvalidCapsuleSeal
					}
				case mutation.Value == nil:
					return ErrInvalidCapsuleSeal
				}
			}
		}
	}
	return nil
}

func cloneReplayWave(wave []ReplayOperation) []ReplayOperation {
	if len(wave) == 0 {
		return nil
	}
	out := make([]ReplayOperation, 0, len(wave))
	for _, op := range wave {
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
