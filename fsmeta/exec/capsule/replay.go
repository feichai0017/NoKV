package capsule

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
	EpochID uint64
	Waves   [][]ReplayOperation
}

func BuildReplayPlan(seal CapsuleSeal) (ReplayPlan, error) {
	if seal.EpochID == 0 || len(seal.Certificates) == 0 {
		return ReplayPlan{}, ErrInvalidCapsuleSeal
	}
	levels := make(map[OperationID]int, len(seal.Certificates))
	waves := make([][]ReplayOperation, 0)
	for _, cert := range seal.Certificates {
		if err := validateSealedCertificate(seal.EpochID, cert); err != nil {
			return ReplayPlan{}, err
		}
		delta, err := DecodeSemanticDeltaPayload(cert.Prepare.DeltaPayload)
		if err != nil {
			return ReplayPlan{}, err
		}
		op, err := replayOperationFromDelta(cert.Prepare.OpID, delta)
		if err != nil {
			return ReplayPlan{}, err
		}
		level := 0
		for _, predecessor := range cert.Prepare.ConflictDAGFrontier {
			predecessorLevel, ok := levels[predecessor]
			if !ok {
				return ReplayPlan{}, ErrInvalidCapsuleSeal
			}
			if predecessorLevel+1 > level {
				level = predecessorLevel + 1
			}
		}
		for len(waves) <= level {
			waves = append(waves, nil)
		}
		waves[level] = append(waves[level], op)
		levels[cert.Prepare.OpID] = level
	}
	return ReplayPlan{EpochID: seal.EpochID, Waves: waves}, nil
}

func validateSealedCertificate(epochID uint64, cert SealedCertificate) error {
	if cert.Prepare.EpochID != epochID || cert.Commit.EpochID != epochID || cert.Prepare.OpID != cert.Commit.OpID {
		return ErrInvalidCapsuleSeal
	}
	prepareDigest, err := PrepareDigest(cert.Prepare)
	if err != nil {
		return err
	}
	if prepareDigest != cert.Commit.PrepareDigest {
		return ErrInvalidCapsuleSeal
	}
	return nil
}

func replayOperationFromDelta(id OperationID, delta compile.SemanticDelta) (ReplayOperation, error) {
	if delta.Eligibility != compile.EligibilityFastPath || len(delta.WriteEffects) == 0 {
		return ReplayOperation{}, ErrInvalidCapsuleSeal
	}
	mutations := make([]ReplayMutation, 0, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ReplayOperation{}, ErrInvalidCapsuleSeal
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
			return ReplayOperation{}, ErrInvalidCapsuleSeal
		}
	}
	return ReplayOperation{
		OpID:      id,
		Kind:      delta.Kind,
		Mutations: mutations,
	}, nil
}
