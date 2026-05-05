package audit

import (
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	eunomia "github.com/feichai0017/NoKV/meta/root/protocol/eunomia"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SnapshotAnomalies surfaces the most important finality/audit gaps in a form
// suitable for CLI or standalone checker consumption.
type FinalityDefect string
type AuthorityCompletionState string

const (
	FinalityDefectNone                    FinalityDefect = ""
	FinalityDefectSuccessorIncomplete     FinalityDefect = "successor_incomplete"
	FinalityDefectMissingConfirm          FinalityDefect = "missing_confirm"
	FinalityDefectMissingClose            FinalityDefect = "missing_close"
	FinalityDefectCloseWithoutConfirm     FinalityDefect = "close_without_confirm"
	FinalityDefectLineageMismatch         FinalityDefect = "lineage_mismatch"
	FinalityDefectReattachWithoutConfirm  FinalityDefect = "reattach_without_confirm"
	FinalityDefectReattachWithoutClose    FinalityDefect = "reattach_without_close"
	FinalityDefectReattachLineageMismatch FinalityDefect = "reattach_lineage_mismatch"
	FinalityDefectReattachIncomplete      FinalityDefect = "reattach_incomplete"
)

const (
	AuthorityCompletionNone                 AuthorityCompletionState = ""
	AuthorityCompletionSealedPending        AuthorityCompletionState = "sealed_handoff_pending"
	AuthorityCompletionSealedCompleted      AuthorityCompletionState = "sealed_handoff_completed"
	AuthorityCompletionExpiredWithoutLegacy AuthorityCompletionState = "expired_takeover_without_legacy"
)

type SnapshotAnomalies struct {
	SuccessorLineageMismatch    bool
	UncoveredMonotoneFrontier   bool
	UncoveredDescriptorRevision bool
	LeaseStartCoverageViolation bool
	SealedEraStillLive          bool
	FinalityDefect              FinalityDefect
}

// Report is the standalone audit projection for one rooted snapshot.
type Report struct {
	HolderID               string
	NowUnixNano            int64
	RootDescriptorRevision uint64
	CatchUpState           string
	CurrentHolderID        string
	CurrentEra             uint64
	Handoff                rootproto.AuthorityHandoffRecord
	HandoverWitness        rootproto.HandoverWitness
	Handover               rootproto.HandoverStatus
	AuthorityCompletion    AuthorityCompletionState
	Anomalies              SnapshotAnomalies
}

func evaluateSnapshot(snapshot rootview.Snapshot, holderID string, nowUnixNano int64) Report {
	descriptorRevision := rootstate.MaxDescriptorRevision(snapshot.Descriptors)
	currentFrontiers := eunomia.Frontiers(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, descriptorRevision)
	handoverWitness := eunomia.BuildHandoverWitness(snapshot.Tenure, currentFrontiers, snapshot.Legacy, nowUnixNano)
	handover := eunomia.EvaluateHandoverStage(
		snapshot.Tenure,
		snapshot.Handover,
		holderID,
		nowUnixNano,
	)
	return Report{
		HolderID:               holderID,
		NowUnixNano:            nowUnixNano,
		RootDescriptorRevision: descriptorRevision,
		CatchUpState:           snapshot.CatchUpState.String(),
		CurrentHolderID:        snapshot.Tenure.HolderID,
		CurrentEra:             snapshot.Tenure.Era,
		Handoff:                eunomia.HandoffRecord(snapshot.Tenure, currentFrontiers),
		HandoverWitness:        handoverWitness.WithStage(handover.Stage),
		Handover:               handover,
	}
}

func evaluateFinalityDefect(snapshot rootview.Snapshot, holderID string, nowUnixNano int64, witness rootproto.HandoverWitness, status rootproto.HandoverStatus) FinalityDefect {
	current := snapshot.Tenure
	handover := snapshot.Handover
	if holderID == "" || holderID != current.HolderID || !current.ActiveAt(nowUnixNano) {
		return FinalityDefectNone
	}
	if witness.LegacyEra != 0 && !witness.FinalitySatisfied() {
		return FinalityDefectSuccessorIncomplete
	}
	confirmPresent := handover.Present() && handover.HolderID == holderID
	if !confirmPresent {
		if status.Stage == rootproto.HandoverStageUnspecified {
			if witness.FinalitySatisfied() {
				return FinalityDefectMissingConfirm
			}
			return FinalityDefectNone
		}
		if rootproto.HandoverStageAtLeast(status.Stage, rootproto.HandoverStageReattached) {
			return FinalityDefectReattachWithoutConfirm
		}
		if rootproto.HandoverStageAtLeast(status.Stage, rootproto.HandoverStageClosed) {
			return FinalityDefectCloseWithoutConfirm
		}
		return FinalityDefectMissingConfirm
	}
	confirmMatchesCurrent := confirmPresent &&
		handover.SuccessorEra > handover.LegacyEra &&
		handover.SuccessorEra == current.Era
	lineageSatisfied := confirmMatchesCurrent &&
		current.LineageDigest == handover.LegacyDigest
	closePresent := confirmPresent && rootproto.HandoverStageAtLeast(handover.Stage, rootproto.HandoverStageClosed)
	reattachPresent := confirmPresent && rootproto.HandoverStageAtLeast(handover.Stage, rootproto.HandoverStageReattached)

	if reattachPresent {
		if !closePresent {
			return FinalityDefectReattachWithoutClose
		}
		if !lineageSatisfied {
			return FinalityDefectReattachLineageMismatch
		}
		if status.Stage != rootproto.HandoverStageReattached {
			return FinalityDefectReattachIncomplete
		}
		return FinalityDefectNone
	}
	if closePresent {
		if !lineageSatisfied {
			return FinalityDefectLineageMismatch
		}
		return FinalityDefectNone
	}
	if !lineageSatisfied {
		return FinalityDefectLineageMismatch
	}
	return FinalityDefectMissingClose
}

func evaluateAuthorityCompletion(snapshot rootview.Snapshot, witness rootproto.HandoverWitness) AuthorityCompletionState {
	if snapshot.Legacy.Present() {
		if witness.FinalitySatisfied() && rootproto.HandoverStageAtLeast(witness.Stage, rootproto.HandoverStageReattached) {
			return AuthorityCompletionSealedCompleted
		}
		return AuthorityCompletionSealedPending
	}
	if snapshot.Tenure.Era > 1 {
		return AuthorityCompletionExpiredWithoutLegacy
	}
	return AuthorityCompletionNone
}

// BuildReport materializes one rooted snapshot into a standalone audit report
// that callers can serialize or render without duplicating anomaly logic.
func BuildReport(snapshot rootview.Snapshot, holderID string, nowUnixNano int64) Report {
	report := evaluateSnapshot(snapshot, holderID, nowUnixNano)
	finalityDefect := evaluateFinalityDefect(snapshot, holderID, nowUnixNano, report.HandoverWitness, report.Handover)
	anomalies := SnapshotAnomalies{
		SuccessorLineageMismatch:    report.HandoverWitness.SuccessorPresent && !report.HandoverWitness.SuccessorLineageSatisfied,
		UncoveredMonotoneFrontier:   report.HandoverWitness.SuccessorPresent && !report.HandoverWitness.SuccessorMonotoneCovered(),
		UncoveredDescriptorRevision: report.HandoverWitness.SuccessorPresent && !report.HandoverWitness.SuccessorDescriptorCovered(),
		SealedEraStillLive:          report.HandoverWitness.LegacyEra != 0 && !report.HandoverWitness.SealedEraRetired,
		FinalityDefect:              finalityDefect,
	}
	report.Anomalies = anomalies
	report.AuthorityCompletion = evaluateAuthorityCompletion(snapshot, report.HandoverWitness)
	return report
}
