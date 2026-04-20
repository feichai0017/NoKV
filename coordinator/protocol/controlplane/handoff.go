package controlplane

import (
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Frontiers materializes the built-in duty-frontier projection from the rooted
// allocator and descriptor state.
func Frontiers(state rootstate.State, descriptorRevision uint64) rootproto.CoordinatorDutyFrontiers {
	return rootproto.NewCoordinatorDutyFrontiers(
		rootproto.CoordinatorDutyFrontier{DutyMask: rootproto.CoordinatorDutyAllocID, Frontier: state.IDFence},
		rootproto.CoordinatorDutyFrontier{DutyMask: rootproto.CoordinatorDutyTSO, Frontier: state.TSOFence},
		rootproto.CoordinatorDutyFrontier{DutyMask: rootproto.CoordinatorDutyGetRegionByKey, Frontier: descriptorRevision},
	)
}

// HandoffRecord projects the current rooted lease plus handoff frontier view into the portable
// handoff record used by checker and diagnostics code.
func HandoffRecord(current rootstate.CoordinatorLease, frontiers rootproto.CoordinatorDutyFrontiers) rootproto.AuthorityHandoffRecord {
	if current.HolderID == "" || current.CertGeneration == 0 {
		return rootproto.AuthorityHandoffRecord{}
	}
	return rootproto.MustNewAuthorityHandoffRecord(
		current.HolderID,
		current.ExpiresUnixNano,
		current.CertGeneration,
		current.IssuedCursor,
		current.DutyMask,
		current.PredecessorDigest,
		frontiers,
	)
}

// BuildClosureWitness derives the base successor-coverage witness from the
// current rooted lease, seal, and duty frontiers.
func BuildClosureWitness(current rootstate.CoordinatorLease, currentFrontiers rootproto.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) rootproto.ClosureWitness {
	if !seal.Present() {
		return rootproto.ClosureWitness{}
	}
	witness := rootproto.ClosureWitness{
		SealGeneration: seal.CertGeneration,
		SealDigest:     rootstate.CoordinatorSealDigest(seal),
		Stage:          rootproto.CoordinatorClosureStageUnspecified,
	}
	witness.SuccessorPresent = current.CertGeneration > seal.CertGeneration
	witness.SuccessorCoverage = rootstate.EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, currentFrontiers)
	witness.SuccessorLineageSatisfied = witness.SuccessorPresent && current.PredecessorDigest == witness.SealDigest
	witness.SealedGenerationRetired = current.CertGeneration != seal.CertGeneration || !current.ActiveAt(nowUnixNano)
	return witness
}

func ValidateClosureConfirmation(current rootstate.CoordinatorLease, currentFrontiers rootproto.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) (rootproto.ClosureWitness, error) {
	witness := BuildClosureWitness(current, currentFrontiers, seal, nowUnixNano)
	if !witness.ClosureSatisfied() {
		return witness, rootstate.ErrCoordinatorLeaseAudit
	}
	return witness, nil
}

func EvaluateClosureStage(current rootstate.CoordinatorLease, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootproto.CoordinatorClosureStatus {
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return rootproto.CoordinatorClosureStatus{}
	}
	if !closure.Present() || closure.HolderID != holderID {
		return rootproto.CoordinatorClosureStatus{}
	}
	if closure.SuccessorGeneration <= closure.SealGeneration ||
		closure.SuccessorGeneration != current.CertGeneration ||
		current.PredecessorDigest != closure.SealDigest {
		return rootproto.CoordinatorClosureStatus{}
	}
	return rootproto.CoordinatorClosureStatus{Stage: closure.Stage}
}

func BuildClosureWitnessForClosure(current rootstate.CoordinatorLease, currentFrontiers rootproto.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootproto.ClosureWitness {
	witness := BuildClosureWitness(current, currentFrontiers, seal, nowUnixNano)
	closureStatus := EvaluateClosureStage(current, closure, holderID, nowUnixNano)
	return witness.WithStage(closureStatus.Stage)
}

func AdvanceClosure(current rootstate.CoordinatorLease, existing rootstate.CoordinatorClosure, stage rootproto.CoordinatorClosureStage, holderID string, sealGeneration uint64, sealDigest string, cursor rootstate.Cursor) rootstate.CoordinatorClosure {
	closure := existing
	if closure.HolderID == "" || stage == rootproto.CoordinatorClosureStageConfirmed {
		closure = rootstate.CoordinatorClosure{
			HolderID:            holderID,
			SealGeneration:      sealGeneration,
			SuccessorGeneration: current.CertGeneration,
			SealDigest:          sealDigest,
		}
	}
	closure.Stage = stage
	switch stage {
	case rootproto.CoordinatorClosureStageConfirmed:
		closure.ConfirmedAtCursor = cursor
		closure.ClosedAtCursor = rootstate.Cursor{}
		closure.ReattachedAtCursor = rootstate.Cursor{}
	case rootproto.CoordinatorClosureStageClosed:
		closure.ClosedAtCursor = cursor
		closure.ReattachedAtCursor = rootstate.Cursor{}
	case rootproto.CoordinatorClosureStageReattached:
		closure.ReattachedAtCursor = cursor
	}
	return closure
}

func ValidateClosureClose(current rootstate.CoordinatorLease, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrCoordinatorLeaseOwner
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidCoordinatorLease
	}
	status := EvaluateClosureStage(current, closure, holderID, nowUnixNano)
	if !rootproto.ClosureStageAtLeast(status.Stage, rootproto.CoordinatorClosureStageConfirmed) {
		return rootstate.ErrCoordinatorLeaseClose
	}
	return nil
}

func ValidateClosureReattach(current rootstate.CoordinatorLease, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) error {
	if holderID == "" || holderID != current.HolderID {
		return rootstate.ErrCoordinatorLeaseOwner
	}
	if !current.ActiveAt(nowUnixNano) {
		return rootstate.ErrInvalidCoordinatorLease
	}
	status := EvaluateClosureStage(current, closure, holderID, nowUnixNano)
	if !rootproto.ClosureStageAtLeast(status.Stage, rootproto.CoordinatorClosureStageClosed) {
		return rootstate.ErrCoordinatorLeaseReattach
	}
	return nil
}
