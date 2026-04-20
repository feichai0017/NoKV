package controlplane

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

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

// EvaluateClosureAudit evaluates the current rooted closure relationship
// between one predecessor seal and the currently installed lease state.
func EvaluateClosureAudit(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) rootstate.CoordinatorClosureAudit {
	if !seal.Present() {
		return rootstate.CoordinatorClosureAudit{}
	}
	audit := rootstate.CoordinatorClosureAudit{
		SealGeneration: seal.CertGeneration,
		SealDigest:     rootstate.CoordinatorSealDigest(seal),
	}
	audit.SuccessorPresent = current.CertGeneration > seal.CertGeneration
	audit.SuccessorCoverage = rootstate.EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, currentFrontiers)
	audit.SuccessorLineageSatisfied = audit.SuccessorPresent && current.PredecessorDigest == audit.SealDigest
	audit.SealedGenerationRetired = current.CertGeneration != seal.CertGeneration || !current.ActiveAt(nowUnixNano)
	return audit
}

func ValidateClosureConfirmation(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, nowUnixNano int64) (rootstate.CoordinatorClosureAudit, error) {
	audit := EvaluateClosureAudit(current, currentFrontiers, seal, nowUnixNano)
	if !audit.ClosureSatisfied() {
		return audit, rootstate.ErrCoordinatorLeaseAudit
	}
	return audit, nil
}

func EvaluateClosure(current rootstate.CoordinatorLease, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootstate.CoordinatorClosureStatus {
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

func EvaluateClosureWitness(current rootstate.CoordinatorLease, currentFrontiers rootstate.CoordinatorDutyFrontiers, seal rootstate.CoordinatorSeal, closure rootstate.CoordinatorClosure, holderID string, nowUnixNano int64) rootstate.ClosureWitness {
	auditStatus := EvaluateClosureAudit(current, currentFrontiers, seal, nowUnixNano)
	closureStatus := EvaluateClosure(current, closure, holderID, nowUnixNano)
	return auditStatus.AsClosureWitness(closureStatus.Stage)
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
	status := EvaluateClosure(current, closure, holderID, nowUnixNano)
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
	status := EvaluateClosure(current, closure, holderID, nowUnixNano)
	if status.Stage < rootstate.CoordinatorClosureStageClosed {
		return rootstate.ErrCoordinatorLeaseReattach
	}
	return nil
}
