package peras

import (
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type ReplayMutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type ReplayOperation struct {
	OpID      OperationID
	Kind      fsmeta.OperationKind
	Mutations []ReplayMutation
}

type ReplayPlan struct {
	EpochID    uint64
	Versions   ReplayVersionRange
	Operations []ReplayOperation
}

func ReplayPlanOperationCount(plan ReplayPlan) uint64 {
	return uint64(len(plan.Operations))
}

func replayOperationFromDelta(id OperationID, delta compile.SemanticDelta) (ReplayOperation, error) {
	if delta.Eligibility != compile.EligibilityVisibleCommit || len(delta.WriteEffects) == 0 {
		return ReplayOperation{}, ErrInvalidPerasSegment
	}
	mutations := make([]ReplayMutation, 0, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ReplayOperation{}, ErrInvalidPerasSegment
			}
			mutations = append(mutations, ReplayMutation{
				Key:   cloneBytes(effect.Key),
				Value: cloneBytes(effect.Value),
			})
		case compile.EffectDelete:
			mutations = append(mutations, ReplayMutation{
				Key:    cloneBytes(effect.Key),
				Delete: true,
			})
		default:
			return ReplayOperation{}, ErrInvalidPerasSegment
		}
	}
	return ReplayOperation{
		OpID:      id,
		Kind:      delta.Kind,
		Mutations: mutations,
	}, nil
}
