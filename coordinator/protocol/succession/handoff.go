package succession

import (
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Frontiers materializes the built-in duty-frontier projection from the rooted
// allocator and descriptor state.
func Frontiers(state rootstate.State, descriptorRevision uint64) rootproto.MandateFrontiers {
	return rootproto.NewMandateFrontiers(
		rootproto.MandateFrontier{Mandate: rootproto.MandateAllocID, Frontier: state.IDFence},
		rootproto.MandateFrontier{Mandate: rootproto.MandateTSO, Frontier: state.TSOFence},
		rootproto.MandateFrontier{Mandate: rootproto.MandateGetRegionByKey, Frontier: descriptorRevision},
	)
}

// HandoffRecord projects the current rooted tenure plus handoff frontier view into the portable
// handoff record used by checker and diagnostics code.
func HandoffRecord(current rootstate.Tenure, frontiers rootproto.MandateFrontiers) rootproto.AuthorityHandoffRecord {
	if current.HolderID == "" || current.Epoch == 0 {
		return rootproto.AuthorityHandoffRecord{}
	}
	return rootproto.MustNewAuthorityHandoffRecord(
		current.HolderID,
		current.ExpiresUnixNano,
		current.Epoch,
		current.IssuedAt,
		current.Mandate,
		current.LineageDigest,
		frontiers,
	)
}

// BuildHandoverWitness derives the base successor-coverage witness from the
// current rooted lease, seal, and duty frontiers.
func BuildHandoverWitness(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, nowUnixNano int64) rootproto.HandoverWitness {
	if !seal.Present() {
		return rootproto.HandoverWitness{}
	}
	witness := rootproto.HandoverWitness{
		LegacyEpoch:  seal.Epoch,
		LegacyDigest: rootstate.DigestOfLegacy(seal),
		Stage:        rootproto.HandoverStageUnspecified,
	}
	witness.SuccessorPresent = current.Epoch > seal.Epoch
	witness.Inheritance = rootstate.EvaluateInheritance(current, seal, currentFrontiers)
	witness.SuccessorLineageSatisfied = witness.SuccessorPresent && current.LineageDigest == witness.LegacyDigest
	witness.SealedGenerationRetired = current.Epoch != seal.Epoch || !current.ActiveAt(nowUnixNano)
	return witness
}

func ValidateHandoverConfirmation(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, nowUnixNano int64) (rootproto.HandoverWitness, error) {
	witness := BuildHandoverWitness(current, currentFrontiers, seal, nowUnixNano)
	if !witness.FinalitySatisfied() {
		return witness, rootstate.ErrFinality
	}
	return witness, nil
}

func EvaluateHandoverStage(current rootstate.Tenure, handover rootstate.Handover, holderID string, nowUnixNano int64) rootproto.HandoverStatus {
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return rootproto.HandoverStatus{}
	}
	if !handover.Present() || handover.HolderID != holderID {
		return rootproto.HandoverStatus{}
	}
	if handover.SuccessorEpoch <= handover.LegacyEpoch ||
		handover.SuccessorEpoch != current.Epoch ||
		current.LineageDigest != handover.LegacyDigest {
		return rootproto.HandoverStatus{}
	}
	return rootproto.HandoverStatus{Stage: handover.Stage}
}

func BuildHandoverWitnessForStage(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, handover rootstate.Handover, holderID string, nowUnixNano int64) rootproto.HandoverWitness {
	witness := BuildHandoverWitness(current, currentFrontiers, seal, nowUnixNano)
	handoverStatus := EvaluateHandoverStage(current, handover, holderID, nowUnixNano)
	return witness.WithStage(handoverStatus.Stage)
}

func AdvanceHandover(current rootstate.Tenure, existing rootstate.Handover, stage rootproto.HandoverStage, holderID string, legacyEpoch uint64, legacyDigest string, cursor rootstate.Cursor) rootstate.Handover {
	handover := existing
	if handover.HolderID == "" || stage == rootproto.HandoverStageConfirmed {
		handover = rootstate.Handover{
			HolderID:       holderID,
			LegacyEpoch:    legacyEpoch,
			SuccessorEpoch: current.Epoch,
			LegacyDigest:   legacyDigest,
		}
	}
	handover.Stage = stage
	switch stage {
	case rootproto.HandoverStageConfirmed:
		handover.ConfirmedAt = cursor
		handover.ClosedAt = rootstate.Cursor{}
		handover.ReattachedAt = rootstate.Cursor{}
	case rootproto.HandoverStageClosed:
		handover.ClosedAt = cursor
		handover.ReattachedAt = rootstate.Cursor{}
	case rootproto.HandoverStageReattached:
		handover.ReattachedAt = cursor
	}
	return handover
}

func ValidateHandoverFinality(current rootstate.Tenure, handover rootstate.Handover, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrPrimacy
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidTenure
	}
	status := EvaluateHandoverStage(current, handover, holderID, nowUnixNano)
	if !rootproto.HandoverStageAtLeast(status.Stage, rootproto.HandoverStageConfirmed) {
		return rootstate.ErrFinality
	}
	return nil
}

func ValidateHandoverReattach(current rootstate.Tenure, handover rootstate.Handover, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrPrimacy
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidTenure
	}
	status := EvaluateHandoverStage(current, handover, holderID, nowUnixNano)
	if !rootproto.HandoverStageAtLeast(status.Stage, rootproto.HandoverStageClosed) {
		return rootstate.ErrFinality
	}
	return nil
}
