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
	OpID                 OperationID
	Kind                 fsmeta.OperationKind
	DescriptorDigest     [32]byte
	PredicateProofDigest [32]byte
	ExecutionPlanDigest  [32]byte
	PredicateProofs      []compile.PredicateProof
	GuardProofs          []compile.GuardProof
	Segment              compile.SegmentPlan
	Atomicity            compile.AtomicityGroup
	Durability           compile.DurabilityClass
	Mutations            []ReplayMutation
}

type ReplayPlan struct {
	EpochID    uint64
	Versions   ReplayVersionRange
	Operations []ReplayOperation
}

func ReplayPlanOperationCount(plan ReplayPlan) uint64 {
	return uint64(len(plan.Operations))
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
		OpID:                 op.OpID,
		Kind:                 op.Kind,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: op.PredicateProofDigest,
		ExecutionPlanDigest:  op.ExecutionPlanDigest,
		PredicateProofs:      clonePredicateProofs(op.PredicateProofs),
		GuardProofs:          cloneGuardProofs(op.GuardProofs),
		Segment:              op.Segment,
		Atomicity:            cloneReplayAtomicity(op.Atomicity),
		Durability:           op.Durability,
		Mutations:            mutations,
	}
}

func replayOperationFromMaterialized(id OperationID, op compile.MaterializedOp) (ReplayOperation, error) {
	if err := op.ValidateForAdmission(); err != nil {
		return ReplayOperation{}, ErrInvalidPerasSegment
	}
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
		OpID:                 id,
		Kind:                 delta.Kind,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: compile.AdmissionProofSetDigest(op.PredicateProofs, op.GuardProofs),
		ExecutionPlanDigest:  compile.ExecutionPlanDigest(op.Segment, op.Atomicity, op.Durability),
		PredicateProofs:      clonePredicateProofs(op.PredicateProofs),
		GuardProofs:          cloneGuardProofs(op.GuardProofs),
		Segment:              op.Segment,
		Atomicity:            cloneReplayAtomicity(op.Atomicity),
		Durability:           op.Durability,
		Mutations:            mutations,
	}, nil
}

func replayOperationExecutionPlanDigest(op ReplayOperation) [32]byte {
	if op.ExecutionPlanDigest != ([32]byte{}) {
		return op.ExecutionPlanDigest
	}
	return compile.ExecutionPlanDigest(op.Segment, op.Atomicity, op.Durability)
}

func cloneReplayAtomicity(group compile.AtomicityGroup) compile.AtomicityGroup {
	group.Members = append([]compile.MutationID(nil), group.Members...)
	return group
}

func clonePredicateProofs(proofs []compile.PredicateProof) []compile.PredicateProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]compile.PredicateProof, len(proofs))
	for i, proof := range proofs {
		out[i] = compile.PredicateProof{
			Key:           cloneBytes(proof.Key),
			Present:       proof.Present,
			Value:         cloneBytes(proof.Value),
			Version:       proof.Version,
			Source:        proof.Source,
			ProofFrontier: proof.ProofFrontier,
			Digest:        proof.Digest,
		}
	}
	return out
}

func cloneGuardProofs(proofs []compile.GuardProof) []compile.GuardProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]compile.GuardProof, len(proofs))
	copy(out, proofs)
	return out
}
