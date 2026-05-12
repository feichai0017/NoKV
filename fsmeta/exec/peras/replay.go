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

func replayOperationFromCompiled(id OperationID, op compile.CompiledOp) (ReplayOperation, error) {
	delta := op.Delta
	if delta.Eligibility != compile.EligibilityVisibleCommit || len(op.Effects) == 0 {
		return ReplayOperation{}, ErrInvalidPerasSegment
	}
	mutations := make([]ReplayMutation, 0, len(op.Effects))
	for _, effect := range op.Effects {
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
