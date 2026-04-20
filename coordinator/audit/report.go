package audit

import (
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

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

func evaluateSnapshot(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) Report {
	descriptorRevision := controlplane.MaxDescriptorRevision(snapshot.Descriptors)
	currentFrontiers := controlplane.FrontiersFromState(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, descriptorRevision)
	closureWitness := controlplane.BuildClosureWitness(snapshot.CoordinatorLease, currentFrontiers, snapshot.CoordinatorSeal, nowUnixNano)
	closure := controlplane.EvaluateClosureStage(
		snapshot.CoordinatorLease,
		snapshot.CoordinatorClosure,
		holderID,
		nowUnixNano,
	)
	return Report{
		HolderID:               holderID,
		NowUnixNano:            nowUnixNano,
		RootDescriptorRevision: descriptorRevision,
		CatchUpState:           snapshot.CatchUpState.String(),
		CurrentHolderID:        snapshot.CoordinatorLease.HolderID,
		CurrentGeneration:      snapshot.CoordinatorLease.CertGeneration,
		Handoff:                controlplane.HandoffRecord(snapshot.CoordinatorLease, currentFrontiers),
		ClosureWitness:         closureWitness.WithStage(closure.Stage),
		Closure:                closure,
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
	report := evaluateSnapshot(snapshot, holderID, nowUnixNano)
	closure := evaluateClosureEvidence(snapshot, holderID, nowUnixNano)
	anomalies := SnapshotAnomalies{
		SuccessorLineageMismatch:    report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorLineageSatisfied,
		UncoveredMonotoneFrontier:   report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorMonotoneCovered(),
		UncoveredDescriptorRevision: report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorDescriptorCovered(),
		SealedGenerationStillLive:   report.ClosureWitness.SealGeneration != 0 && !report.ClosureWitness.SealedGenerationRetired,
		ClosureIncomplete:           report.ClosureWitness.SealGeneration != 0 && !report.ClosureWitness.ClosureSatisfied(),
		MissingConfirm:              report.ClosureWitness.ClosureSatisfied() && !closure.confirmPresent,
		MissingClose:                closure.confirmPresent && !closure.closePresent,
		CloseWithoutConfirm:         closure.closePresent && !closure.confirmPresent,
		CloseLineageMismatch:        closure.confirmPresent && !closure.lineageSatisfied,
		ReattachWithoutConfirm:      closure.reattachPresent && !closure.confirmPresent,
		ReattachWithoutClose:        closure.reattachPresent && !closure.closePresent,
		ReattachLineageMismatch:     closure.confirmPresent && !closure.lineageSatisfied,
		ReattachIncomplete:          closure.reattachPresent && report.Closure.Stage != rootstate.CoordinatorClosureStageReattached,
	}
	report.Anomalies = anomalies
	return report
}
