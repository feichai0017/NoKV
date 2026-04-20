package audit

import (
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SnapshotAnomalies surfaces the most important closure/audit gaps in a form
// suitable for CLI or standalone checker consumption.
type ClosureDefect string

const (
	ClosureDefectNone                    ClosureDefect = ""
	ClosureDefectSuccessorIncomplete     ClosureDefect = "successor_incomplete"
	ClosureDefectMissingConfirm          ClosureDefect = "missing_confirm"
	ClosureDefectMissingClose            ClosureDefect = "missing_close"
	ClosureDefectCloseWithoutConfirm     ClosureDefect = "close_without_confirm"
	ClosureDefectCloseLineageMismatch    ClosureDefect = "close_lineage_mismatch"
	ClosureDefectReattachWithoutConfirm  ClosureDefect = "reattach_without_confirm"
	ClosureDefectReattachWithoutClose    ClosureDefect = "reattach_without_close"
	ClosureDefectReattachLineageMismatch ClosureDefect = "reattach_lineage_mismatch"
	ClosureDefectReattachIncomplete      ClosureDefect = "reattach_incomplete"
)

type SnapshotAnomalies struct {
	SuccessorLineageMismatch    bool
	UncoveredMonotoneFrontier   bool
	UncoveredDescriptorRevision bool
	LeaseStartCoverageViolation bool
	SealedGenerationStillLive   bool
	ClosureDefect               ClosureDefect
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

func evaluateSnapshot(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) Report {
	descriptorRevision := rootstate.MaxDescriptorRevision(snapshot.Descriptors)
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

func evaluateClosureDefect(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64, witness rootstate.ClosureWitness, status rootstate.CoordinatorClosureStatus) ClosureDefect {
	current := snapshot.CoordinatorLease
	closure := snapshot.CoordinatorClosure
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return ClosureDefectNone
	}
	if witness.SealGeneration != 0 && !witness.ClosureSatisfied() {
		return ClosureDefectSuccessorIncomplete
	}
	confirmPresent := closure.Present() && closure.HolderID == holderID
	if !confirmPresent {
		if status.Stage == rootstate.CoordinatorClosureStageUnspecified {
			if witness.ClosureSatisfied() {
				return ClosureDefectMissingConfirm
			}
			return ClosureDefectNone
		}
		if rootstate.ClosureStageAtLeast(status.Stage, rootstate.CoordinatorClosureStageReattached) {
			return ClosureDefectReattachWithoutConfirm
		}
		if rootstate.ClosureStageAtLeast(status.Stage, rootstate.CoordinatorClosureStageClosed) {
			return ClosureDefectCloseWithoutConfirm
		}
		return ClosureDefectMissingConfirm
	}
	confirmMatchesCurrent := confirmPresent &&
		closure.SuccessorGeneration > closure.SealGeneration &&
		closure.SuccessorGeneration == current.CertGeneration
	lineageSatisfied := confirmMatchesCurrent &&
		current.PredecessorDigest == closure.SealDigest
	closePresent := confirmPresent && rootstate.ClosureStageAtLeast(closure.Stage, rootstate.CoordinatorClosureStageClosed)
	reattachPresent := confirmPresent && rootstate.ClosureStageAtLeast(closure.Stage, rootstate.CoordinatorClosureStageReattached)

	if reattachPresent {
		if !closePresent {
			return ClosureDefectReattachWithoutClose
		}
		if !lineageSatisfied {
			return ClosureDefectReattachLineageMismatch
		}
		if status.Stage != rootstate.CoordinatorClosureStageReattached {
			return ClosureDefectReattachIncomplete
		}
		return ClosureDefectNone
	}
	if closePresent {
		if !lineageSatisfied {
			return ClosureDefectCloseLineageMismatch
		}
		return ClosureDefectNone
	}
	if !lineageSatisfied {
		return ClosureDefectCloseLineageMismatch
	}
	return ClosureDefectMissingClose
}

// BuildReport materializes one rooted snapshot into a standalone audit report
// that callers can serialize or render without duplicating anomaly logic.
func BuildReport(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) Report {
	report := evaluateSnapshot(snapshot, holderID, nowUnixNano)
	closureDefect := evaluateClosureDefect(snapshot, holderID, nowUnixNano, report.ClosureWitness, report.Closure)
	anomalies := SnapshotAnomalies{
		SuccessorLineageMismatch:    report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorLineageSatisfied,
		UncoveredMonotoneFrontier:   report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorMonotoneCovered(),
		UncoveredDescriptorRevision: report.ClosureWitness.SuccessorPresent && !report.ClosureWitness.SuccessorDescriptorCovered(),
		SealedGenerationStillLive:   report.ClosureWitness.SealGeneration != 0 && !report.ClosureWitness.SealedGenerationRetired,
		ClosureDefect:               closureDefect,
		ClosureIncomplete:           closureDefect == ClosureDefectSuccessorIncomplete,
		MissingConfirm:              closureDefect == ClosureDefectMissingConfirm,
		MissingClose:                closureDefect == ClosureDefectMissingClose,
		CloseWithoutConfirm:         closureDefect == ClosureDefectCloseWithoutConfirm,
		CloseLineageMismatch:        closureDefect == ClosureDefectCloseLineageMismatch,
		ReattachWithoutConfirm:      closureDefect == ClosureDefectReattachWithoutConfirm,
		ReattachWithoutClose:        closureDefect == ClosureDefectReattachWithoutClose,
		ReattachLineageMismatch:     closureDefect == ClosureDefectReattachLineageMismatch,
		ReattachIncomplete:          closureDefect == ClosureDefectReattachIncomplete,
	}
	report.Anomalies = anomalies
	return report
}
