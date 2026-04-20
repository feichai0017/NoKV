package audit

import (
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
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
}

// Report is the standalone ccc-audit projection for one rooted snapshot.
type Report struct {
	HolderID               string
	NowUnixNano            int64
	RootDescriptorRevision uint64
	CatchUpState           string
	CurrentHolderID        string
	CurrentGeneration      uint64
	Handoff                rootproto.AuthorityHandoffRecord
	ClosureWitness         rootproto.ClosureWitness
	Closure                rootproto.CoordinatorClosureStatus
	Anomalies              SnapshotAnomalies
}

func evaluateSnapshot(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64) Report {
	descriptorRevision := rootstate.MaxDescriptorRevision(snapshot.Descriptors)
	currentFrontiers := controlplane.Frontiers(rootstate.State{
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

func evaluateClosureDefect(snapshot coordstorage.Snapshot, holderID string, nowUnixNano int64, witness rootproto.ClosureWitness, status rootproto.CoordinatorClosureStatus) ClosureDefect {
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
		if status.Stage == rootproto.CoordinatorClosureStageUnspecified {
			if witness.ClosureSatisfied() {
				return ClosureDefectMissingConfirm
			}
			return ClosureDefectNone
		}
		if rootproto.ClosureStageAtLeast(status.Stage, rootproto.CoordinatorClosureStageReattached) {
			return ClosureDefectReattachWithoutConfirm
		}
		if rootproto.ClosureStageAtLeast(status.Stage, rootproto.CoordinatorClosureStageClosed) {
			return ClosureDefectCloseWithoutConfirm
		}
		return ClosureDefectMissingConfirm
	}
	confirmMatchesCurrent := confirmPresent &&
		closure.SuccessorGeneration > closure.SealGeneration &&
		closure.SuccessorGeneration == current.CertGeneration
	lineageSatisfied := confirmMatchesCurrent &&
		current.PredecessorDigest == closure.SealDigest
	closePresent := confirmPresent && rootproto.ClosureStageAtLeast(closure.Stage, rootproto.CoordinatorClosureStageClosed)
	reattachPresent := confirmPresent && rootproto.ClosureStageAtLeast(closure.Stage, rootproto.CoordinatorClosureStageReattached)

	if reattachPresent {
		if !closePresent {
			return ClosureDefectReattachWithoutClose
		}
		if !lineageSatisfied {
			return ClosureDefectReattachLineageMismatch
		}
		if status.Stage != rootproto.CoordinatorClosureStageReattached {
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
	}
	report.Anomalies = anomalies
	return report
}
