package controlplane

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
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
	return rootstate.AuthorityHandoffRecord{
		HolderID:          current.HolderID,
		ExpiresUnixNano:   current.ExpiresUnixNano,
		CertGeneration:    current.CertGeneration,
		IssuedCursor:      current.IssuedCursor,
		DutyMask:          rootstate.ResolvedCoordinatorDutyMask(current.DutyMask),
		PredecessorDigest: current.PredecessorDigest,
		Frontiers:         frontiers,
	}
}

// MaxDescriptorRevision returns the maximum rooted descriptor epoch currently
// materialized in descriptors.
func MaxDescriptorRevision(descriptors map[uint64]descriptor.Descriptor) uint64 {
	var maxEpoch uint64
	for _, desc := range descriptors {
		if desc.RootEpoch > maxEpoch {
			maxEpoch = desc.RootEpoch
		}
	}
	return maxEpoch
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
		Stage:          rootstate.CoordinatorClosureStagePendingConfirm,
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
	if closure.HolderID != holderID ||
		closure.SealGeneration == 0 ||
		closure.SuccessorGeneration == 0 ||
		closure.SealDigest == "" {
		return rootstate.CoordinatorClosureStatus{Stage: rootstate.CoordinatorClosureStagePendingConfirm}
	}
	if closure.SuccessorGeneration <= closure.SealGeneration ||
		closure.SuccessorGeneration != current.CertGeneration ||
		current.PredecessorDigest != closure.SealDigest {
		return rootstate.CoordinatorClosureStatus{Stage: rootstate.CoordinatorClosureStagePendingConfirm}
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
	if status.Stage < rootstate.CoordinatorClosureStageConfirmed {
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
	if status.Stage < rootstate.CoordinatorClosureStageClosed {
		return rootstate.ErrCoordinatorLeaseReattach
	}
	return nil
}
