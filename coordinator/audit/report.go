package audit

import (
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SnapshotStatus captures the rooted closure and reattach status that can be
// derived from one storage snapshot without consulting live service internals.
type SnapshotStatus struct {
	RootDescriptorRevision uint64
	Handoff                rootstate.AuthorityHandoffRecord
	ClosureAudit           rootstate.CoordinatorClosureAudit
	Closure                rootstate.CoordinatorClosureStatus
	ClosureWitness         rootstate.ClosureWitness
}

// SnapshotAnomalies surfaces the most important closure/audit gaps in a form
// suitable for CLI or standalone checker consumption.
type SnapshotAnomalies struct {
	SuccessorLineageMismatch    bool
	UncoveredMonotoneFrontier   bool
	UncoveredDescriptorRevision bool
	LeaseStartCoverageViolation bool
	SealedGenerationStillLive   bool
	ClosureIncomplete           bool
	MissingConfirm              bool
	MissingClose                bool
	CloseWithoutConfirm         bool
	CloseLineageMismatch        bool
	ReattachWithoutConfirm      bool
	ReattachWithoutClose        bool
	ReattachLineageMismatch     bool
	ReattachIncomplete          bool
}

// Report is the standalone ccc-audit projection for one rooted snapshot.
type Report struct {
	HolderID               string
	NowUnixNano            int64
	RootDescriptorRevision uint64
	CatchUpState           string
	CurrentHolderID        string
	CurrentGeneration      uint64
	Handoff                rootstate.AuthorityHandoffRecord
	ClosureWitness         rootstate.ClosureWitness
	ClosureAudit           rootstate.CoordinatorClosureAudit
	Closure                rootstate.CoordinatorClosureStatus
	Anomalies              SnapshotAnomalies
}

type closureEvidence struct {
	confirmPresent        bool
	confirmMatchesCurrent bool
	lineageSatisfied      bool
	closePresent          bool
	reattachPresent       bool
}

// EvaluateSnapshot projects one rooted storage snapshot into closure-oriented
// audit status. It is the minimal reusable seed for standalone ccc-audit.
func EvaluateSnapshot(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) SnapshotStatus {
	descriptorRevision := controlplane.MaxDescriptorRevision(snapshot.Descriptors)
	currentFrontiers := controlplane.FrontiersFromState(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, descriptorRevision)
	closureAudit := controlplane.EvaluateClosureAudit(snapshot.CoordinatorLease, currentFrontiers, snapshot.CoordinatorSeal, nowUnixNano)
	closure := controlplane.EvaluateClosure(
		snapshot.CoordinatorLease,
		snapshot.CoordinatorClosure,
		holderID,
		nowUnixNano,
	)
	return SnapshotStatus{
		RootDescriptorRevision: descriptorRevision,
		Handoff:                controlplane.HandoffRecord(snapshot.CoordinatorLease, currentFrontiers),
		ClosureAudit:           closureAudit,
		Closure:                closure,
		ClosureWitness:         closureAudit.AsClosureWitness(closure.Stage),
	}
}

func evaluateClosureEvidence(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) closureEvidence {
	current := snapshot.CoordinatorLease
	closure := snapshot.CoordinatorClosure
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return closureEvidence{}
	}
	evidence := closureEvidence{}
	evidence.confirmPresent = closure.HolderID == holderID &&
		closure.SealGeneration != 0 &&
		closure.SuccessorGeneration != 0 &&
		closure.SealDigest != ""
	evidence.confirmMatchesCurrent = evidence.confirmPresent &&
		closure.SuccessorGeneration > closure.SealGeneration &&
		closure.SuccessorGeneration == current.CertGeneration
	evidence.lineageSatisfied = evidence.confirmMatchesCurrent &&
		current.PredecessorDigest == closure.SealDigest
	evidence.closePresent = evidence.confirmPresent && closure.Stage >= rootstate.CoordinatorClosureStageClosed
	evidence.reattachPresent = evidence.confirmPresent && closure.Stage >= rootstate.CoordinatorClosureStageReattached
	return evidence
}

// BuildReport materializes one rooted snapshot into a standalone audit report
// that callers can serialize or render without duplicating anomaly logic.
func BuildReport(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) Report {
	status := EvaluateSnapshot(snapshot, holderID, nowUnixNano)
	closure := evaluateClosureEvidence(snapshot, holderID, nowUnixNano)
	anomalies := SnapshotAnomalies{
		SuccessorLineageMismatch:    status.ClosureAudit.SuccessorPresent && !status.ClosureAudit.SuccessorLineageSatisfied,
		UncoveredMonotoneFrontier:   status.ClosureAudit.SuccessorPresent && !status.ClosureAudit.SuccessorMonotoneCovered(),
		UncoveredDescriptorRevision: status.ClosureAudit.SuccessorPresent && !status.ClosureAudit.SuccessorDescriptorCovered(),
		SealedGenerationStillLive:   status.ClosureAudit.SealGeneration != 0 && !status.ClosureAudit.SealedGenerationRetired,
		ClosureIncomplete:           status.ClosureAudit.SealGeneration != 0 && !status.ClosureAudit.ClosureSatisfied(),
		MissingConfirm:              status.ClosureAudit.ClosureSatisfied() && !closure.confirmPresent,
		MissingClose:                closure.confirmPresent && !closure.closePresent,
		CloseWithoutConfirm:         closure.closePresent && !closure.confirmPresent,
		CloseLineageMismatch:        closure.confirmPresent && !closure.lineageSatisfied,
		ReattachWithoutConfirm:      closure.reattachPresent && !closure.confirmPresent,
		ReattachWithoutClose:        closure.reattachPresent && !closure.closePresent,
		ReattachLineageMismatch:     closure.confirmPresent && !closure.lineageSatisfied,
		ReattachIncomplete:          closure.reattachPresent && status.Closure.Stage != rootstate.CoordinatorClosureStageReattached,
	}
	return Report{
		HolderID:               holderID,
		NowUnixNano:            nowUnixNano,
		RootDescriptorRevision: status.RootDescriptorRevision,
		CatchUpState:           snapshot.CatchUpState.String(),
		CurrentHolderID:        snapshot.CoordinatorLease.HolderID,
		CurrentGeneration:      snapshot.CoordinatorLease.CertGeneration,
		Handoff:                status.Handoff,
		ClosureWitness:         status.ClosureWitness,
		ClosureAudit:           status.ClosureAudit,
		Closure:                status.Closure,
		Anomalies:              anomalies,
	}
}
