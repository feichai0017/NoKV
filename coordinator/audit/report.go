package audit

import (
	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/feichai0017/NoKV/coordinator/rootview"
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
	ClosureDefectLineageMismatch         ClosureDefect = "lineage_mismatch"
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

// Report is the standalone succession-audit projection for one rooted snapshot.
type Report struct {
	HolderID               string
	NowUnixNano            int64
	RootDescriptorRevision uint64
	CatchUpState           string
	CurrentHolderID        string
	CurrentGeneration      uint64
	Handoff                rootproto.AuthorityHandoffRecord
	TransitWitness         rootproto.TransitWitness
	Closure                rootproto.TransitStatus
	Anomalies              SnapshotAnomalies
}

func evaluateSnapshot(snapshot rootview.Snapshot, holderID string, nowUnixNano int64) Report {
	descriptorRevision := rootstate.MaxDescriptorRevision(snapshot.Descriptors)
	currentFrontiers := succession.Frontiers(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, descriptorRevision)
	closureWitness := succession.BuildTransitWitness(snapshot.Tenure, currentFrontiers, snapshot.Legacy, nowUnixNano)
	closure := succession.EvaluateTransitStage(
		snapshot.Tenure,
		snapshot.Transit,
		holderID,
		nowUnixNano,
	)
	return Report{
		HolderID:               holderID,
		NowUnixNano:            nowUnixNano,
		RootDescriptorRevision: descriptorRevision,
		CatchUpState:           snapshot.CatchUpState.String(),
		CurrentHolderID:        snapshot.Tenure.HolderID,
		CurrentGeneration:      snapshot.Tenure.Epoch,
		Handoff:                succession.HandoffRecord(snapshot.Tenure, currentFrontiers),
		TransitWitness:         closureWitness.WithStage(closure.Stage),
		Closure:                closure,
	}
}

func evaluateClosureDefect(snapshot rootview.Snapshot, holderID string, nowUnixNano int64, witness rootproto.TransitWitness, status rootproto.TransitStatus) ClosureDefect {
	current := snapshot.Tenure
	closure := snapshot.Transit
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return ClosureDefectNone
	}
	if witness.LegacyEpoch != 0 && !witness.ClosureSatisfied() {
		return ClosureDefectSuccessorIncomplete
	}
	confirmPresent := closure.Present() && closure.HolderID == holderID
	if !confirmPresent {
		if status.Stage == rootproto.TransitStageUnspecified {
			if witness.ClosureSatisfied() {
				return ClosureDefectMissingConfirm
			}
			return ClosureDefectNone
		}
		if rootproto.TransitStageAtLeast(status.Stage, rootproto.TransitStageReattached) {
			return ClosureDefectReattachWithoutConfirm
		}
		if rootproto.TransitStageAtLeast(status.Stage, rootproto.TransitStageClosed) {
			return ClosureDefectCloseWithoutConfirm
		}
		return ClosureDefectMissingConfirm
	}
	confirmMatchesCurrent := confirmPresent &&
		closure.SuccessorEpoch > closure.LegacyEpoch &&
		closure.SuccessorEpoch == current.Epoch
	lineageSatisfied := confirmMatchesCurrent &&
		current.LineageDigest == closure.LegacyDigest
	closePresent := confirmPresent && rootproto.TransitStageAtLeast(closure.Stage, rootproto.TransitStageClosed)
	reattachPresent := confirmPresent && rootproto.TransitStageAtLeast(closure.Stage, rootproto.TransitStageReattached)

	if reattachPresent {
		if !closePresent {
			return ClosureDefectReattachWithoutClose
		}
		if !lineageSatisfied {
			return ClosureDefectReattachLineageMismatch
		}
		if status.Stage != rootproto.TransitStageReattached {
			return ClosureDefectReattachIncomplete
		}
		return ClosureDefectNone
	}
	if closePresent {
		if !lineageSatisfied {
			return ClosureDefectLineageMismatch
		}
		return ClosureDefectNone
	}
	if !lineageSatisfied {
		return ClosureDefectLineageMismatch
	}
	return ClosureDefectMissingClose
}

// BuildReport materializes one rooted snapshot into a standalone audit report
// that callers can serialize or render without duplicating anomaly logic.
func BuildReport(snapshot rootview.Snapshot, holderID string, nowUnixNano int64) Report {
	report := evaluateSnapshot(snapshot, holderID, nowUnixNano)
	closureDefect := evaluateClosureDefect(snapshot, holderID, nowUnixNano, report.TransitWitness, report.Closure)
	anomalies := SnapshotAnomalies{
		SuccessorLineageMismatch:    report.TransitWitness.SuccessorPresent && !report.TransitWitness.SuccessorLineageSatisfied,
		UncoveredMonotoneFrontier:   report.TransitWitness.SuccessorPresent && !report.TransitWitness.SuccessorMonotoneCovered(),
		UncoveredDescriptorRevision: report.TransitWitness.SuccessorPresent && !report.TransitWitness.SuccessorDescriptorCovered(),
		SealedGenerationStillLive:   report.TransitWitness.LegacyEpoch != 0 && !report.TransitWitness.SealedGenerationRetired,
		ClosureDefect:               closureDefect,
	}
	report.Anomalies = anomalies
	return report
}
