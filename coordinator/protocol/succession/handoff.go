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

// BuildTransitWitness derives the base successor-coverage witness from the
// current rooted lease, seal, and duty frontiers.
func BuildTransitWitness(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, nowUnixNano int64) rootproto.TransitWitness {
	if !seal.Present() {
		return rootproto.TransitWitness{}
	}
	witness := rootproto.TransitWitness{
		LegacyEpoch: seal.Epoch,
		LegacyDigest:     rootstate.DigestOfLegacy(seal),
		Stage:          rootproto.TransitStageUnspecified,
	}
	witness.SuccessorPresent = current.Epoch > seal.Epoch
	witness.Inheritance = rootstate.EvaluateInheritance(current, seal, currentFrontiers)
	witness.SuccessorLineageSatisfied = witness.SuccessorPresent && current.LineageDigest == witness.LegacyDigest
	witness.SealedGenerationRetired = current.Epoch != seal.Epoch || !current.ActiveAt(nowUnixNano)
	return witness
}

func ValidateTransitConfirmation(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, nowUnixNano int64) (rootproto.TransitWitness, error) {
	witness := BuildTransitWitness(current, currentFrontiers, seal, nowUnixNano)
	if !witness.ClosureSatisfied() {
		return witness, rootstate.ErrClosure
	}
	return witness, nil
}

func EvaluateTransitStage(current rootstate.Tenure, closure rootstate.Transit, holderID string, nowUnixNano int64) rootproto.TransitStatus {
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return rootproto.TransitStatus{}
	}
	if !closure.Present() || closure.HolderID != holderID {
		return rootproto.TransitStatus{}
	}
	if closure.SuccessorEpoch <= closure.LegacyEpoch ||
		closure.SuccessorEpoch != current.Epoch ||
		current.LineageDigest != closure.LegacyDigest {
		return rootproto.TransitStatus{}
	}
	return rootproto.TransitStatus{Stage: closure.Stage}
}

func BuildTransitWitnessForStage(current rootstate.Tenure, currentFrontiers rootproto.MandateFrontiers, seal rootstate.Legacy, closure rootstate.Transit, holderID string, nowUnixNano int64) rootproto.TransitWitness {
	witness := BuildTransitWitness(current, currentFrontiers, seal, nowUnixNano)
	closureStatus := EvaluateTransitStage(current, closure, holderID, nowUnixNano)
	return witness.WithStage(closureStatus.Stage)
}

func AdvanceTransit(current rootstate.Tenure, existing rootstate.Transit, stage rootproto.TransitStage, holderID string, legacyEpoch uint64, legacyDigest string, cursor rootstate.Cursor) rootstate.Transit {
	transit := existing
	if transit.HolderID == "" || stage == rootproto.TransitStageConfirmed {
		transit = rootstate.Transit{
			HolderID:            holderID,
			LegacyEpoch:         legacyEpoch,
			SuccessorEpoch: current.Epoch,
			LegacyDigest:        legacyDigest,
		}
	}
	transit.Stage = stage
	switch stage {
	case rootproto.TransitStageConfirmed:
		transit.ConfirmedAt = cursor
		transit.ClosedAt = rootstate.Cursor{}
		transit.ReattachedAt = rootstate.Cursor{}
	case rootproto.TransitStageClosed:
		transit.ClosedAt = cursor
		transit.ReattachedAt = rootstate.Cursor{}
	case rootproto.TransitStageReattached:
		transit.ReattachedAt = cursor
	}
	return transit
}

func ValidateTransitClosure(current rootstate.Tenure, closure rootstate.Transit, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrPrimacy
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidTenure
	}
	status := EvaluateTransitStage(current, closure, holderID, nowUnixNano)
	if !rootproto.TransitStageAtLeast(status.Stage, rootproto.TransitStageConfirmed) {
		return rootstate.ErrClosure
	}
	return nil
}

func ValidateTransitReattach(current rootstate.Tenure, closure rootstate.Transit, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrPrimacy
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidTenure
	}
	status := EvaluateTransitStage(current, closure, holderID, nowUnixNano)
	if !rootproto.TransitStageAtLeast(status.Stage, rootproto.TransitStageClosed) {
		return rootstate.ErrClosure
	}
	return nil
}
