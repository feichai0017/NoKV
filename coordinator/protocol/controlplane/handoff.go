package controlplane

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Frontiers materializes the built-in duty-frontier projection from the rooted
// allocator and descriptor state.
func Frontiers(idFence, tsoFence, descriptorRevision uint64) rootstate.CoordinatorDutyFrontiers {
	return rootstate.NewCoordinatorDutyFrontiers(
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyAllocID, DutyName: rootstate.CoordinatorDutyName(rootstate.CoordinatorDutyAllocID), Frontier: idFence},
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyTSO, DutyName: rootstate.CoordinatorDutyName(rootstate.CoordinatorDutyTSO), Frontier: tsoFence},
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyGetRegionByKey, DutyName: rootstate.CoordinatorDutyName(rootstate.CoordinatorDutyGetRegionByKey), Frontier: descriptorRevision},
	)
}

// FrontiersFromState projects the current rooted allocator and descriptor state
// into the generic duty-frontier protocol object.
func FrontiersFromState(state rootstate.State, descriptorRevision uint64) rootstate.CoordinatorDutyFrontiers {
	return Frontiers(state.IDFence, state.TSOFence, descriptorRevision)
}

// HandoffRecord projects the current rooted lease plus handoff frontier view into the portable
// handoff record used by checker and diagnostics code.
func HandoffRecord(current rootstate.CoordinatorLease, frontiers rootstate.CoordinatorDutyFrontiers) rootstate.AuthorityHandoffRecord {
	if current.HolderID == "" || current.CertGeneration == 0 {
		return rootstate.AuthorityHandoffRecord{}
	}
	return rootstate.MustNewAuthorityHandoffRecord(
		current.HolderID,
		current.ExpiresUnixNano,
		current.CertGeneration,
		current.IssuedCursor,
		rootstate.ResolvedCoordinatorDutyMask(current.DutyMask),
		current.PredecessorDigest,
		frontiers,
	)
}

// BuildClosureWitness derives the base successor-coverage witness from the
// current rooted lease, seal, and duty frontiers.
func BuildClosureWitness(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) rootstate.ClosureWitness {
	if !seal.Present() {
		return rootstate.ClosureWitness{}
	}
	witness := rootstate.ClosureWitness{
		SealGeneration: seal.CertGeneration,
		SealDigest:     rootstate.CoordinatorSealDigest(seal),
		Stage:          rootstate.CoordinatorClosureStageUnspecified,
	}
	witness.SuccessorPresent = current.CertGeneration > seal.CertGeneration
	witness.SuccessorCoverage = rootstate.EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, currentFrontiers)
	witness.SuccessorLineageSatisfied = witness.SuccessorPresent && current.PredecessorDigest == witness.SealDigest
	witness.SealedGenerationRetired = current.CertGeneration != seal.CertGeneration || !current.ActiveAt(nowUnixNano)
	return witness
}

func ValidateClosureConfirmation(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) (rootstate.ClosureWitness, error) {
	witness := BuildClosureWitness(current, currentFrontiers, seal, nowUnixNano)
	if !witness.ClosureSatisfied() {
		return witness, rootstate.ErrCoordinatorLeaseAudit
	}
	return witness, nil
}

func EvaluateClosureStage(current rootstate.CoordinatorLease, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootstate.CoordinatorClosureStatus {
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return rootstate.CoordinatorClosureStatus{}
	}
	if !closure.Present() || closure.HolderID != holderID {
		return rootstate.CoordinatorClosureStatus{}
	}
	if closure.SuccessorGeneration <= closure.SealGeneration ||
		closure.SuccessorGeneration != current.CertGeneration ||
		current.PredecessorDigest != closure.SealDigest {
		return rootstate.CoordinatorClosureStatus{}
	}
	return rootstate.CoordinatorClosureStatus{Stage: closure.Stage}
}

func BuildClosureWitnessForClosure(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootstate.ClosureWitness {
	witness := BuildClosureWitness(current, currentFrontiers, seal, nowUnixNano)
	closureStatus := EvaluateClosureStage(current, closure, holderID, nowUnixNano)
	return witness.WithStage(closureStatus.Stage)
}

func AdvanceClosure(current rootstate.CoordinatorLease, existing rootstate.CoordinatorClosure, stage rootstate.CoordinatorClosureStage, holderID string, sealGeneration uint64, sealDigest string, cursor rootstate.Cursor) rootstate.CoordinatorClosure {
	closure := existing
	if closure.HolderID == "" || stage == rootstate.CoordinatorClosureStageConfirmed {
		closure = rootstate.CoordinatorClosure{
			HolderID:            holderID,
			SealGeneration:      sealGeneration,
			SuccessorGeneration: current.CertGeneration,
			SealDigest:          sealDigest,
		}
	}
	closure.Stage = stage
	switch stage {
	case rootstate.CoordinatorClosureStageConfirmed:
		closure.ConfirmedAtCursor = cursor
		closure.ClosedAtCursor = rootstate.Cursor{}
		closure.ReattachedAtCursor = rootstate.Cursor{}
	case rootstate.CoordinatorClosureStageClosed:
		closure.ClosedAtCursor = cursor
		closure.ReattachedAtCursor = rootstate.Cursor{}
	case rootstate.CoordinatorClosureStageReattached:
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
	if !rootstate.ClosureStageAtLeast(status.Stage, rootstate.CoordinatorClosureStageConfirmed) {
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
	if !rootstate.ClosureStageAtLeast(status.Stage, rootstate.CoordinatorClosureStageClosed) {
		return rootstate.ErrCoordinatorLeaseReattach
	}
	return nil
}
