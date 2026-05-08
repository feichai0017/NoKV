package audit

import (
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SnapshotAnomalies surfaces the important grant/finality gaps in a form
// suitable for diagnostics, CLI output, or standalone checker consumption.
type FinalityDefect string
type AuthorityCompletionState string

const (
	FinalityDefectNone                  FinalityDefect = ""
	FinalityDefectRetiredNotInherited   FinalityDefect = "retired_not_inherited"
	FinalityDefectInvalidSuccessorBound FinalityDefect = "invalid_successor_bound"
	FinalityDefectOrphanInheritance     FinalityDefect = "orphan_inheritance"
)

const (
	AuthorityCompletionNone                  AuthorityCompletionState = ""
	AuthorityCompletionSealedExactCompleted  AuthorityCompletionState = "sealed_exact_completed"
	AuthorityCompletionExpiredBoundInherited AuthorityCompletionState = "expired_bound_inherited"
	AuthorityCompletionRetiredNotInherited   AuthorityCompletionState = "retired_not_inherited"
)

type SnapshotAnomalies struct {
	RetiredGrantNotInherited bool
	InvalidSuccessorBound    bool
	OrphanInheritance        bool
	FinalityDefect           FinalityDefect
}

// Report is the standalone audit projection for one rooted snapshot.
type Report struct {
	HolderID               string
	NowUnixNano            int64
	RootDescriptorRevision uint64
	CatchUpState           string
	CurrentHolderID        string
	CurrentEra             uint64
	ActiveGrant            rootproto.AuthorityGrant
	RetiredGrants          []rootproto.GrantRetirement
	GrantInheritances      []rootproto.GrantInheritance
	RetiredEraFloor        uint64
	AuthorityCompletion    AuthorityCompletionState
	Anomalies              SnapshotAnomalies
}

// BuildReport materializes one rooted snapshot into a grant-lifecycle audit
// report. It treats a crash-before-seal takeover as complete only when the
// expired_bound retirement has been inherited by a successor grant.
func BuildReport(snapshot rootview.Snapshot, holderID string, nowUnixNano int64) Report {
	report := Report{
		HolderID:               holderID,
		NowUnixNano:            nowUnixNano,
		RootDescriptorRevision: rootstate.MaxDescriptorRevision(snapshot.Descriptors),
		CatchUpState:           snapshot.CatchUpState.String(),
		CurrentHolderID:        snapshot.ActiveGrant.HolderID,
		CurrentEra:             snapshot.ActiveGrant.Era,
		ActiveGrant:            snapshot.ActiveGrant,
		RetiredGrants:          append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...),
		GrantInheritances:      append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...),
		RetiredEraFloor:        snapshot.RetiredEraFloor,
	}
	for _, retirement := range report.RetiredGrants {
		if retirement.InheritedByGrantID != "" && retirement.Era > report.RetiredEraFloor {
			report.RetiredEraFloor = retirement.Era
		}
		if retirement.Present() && retirement.InheritedByGrantID == "" {
			report.Anomalies.RetiredGrantNotInherited = true
		}
	}
	report.Anomalies.InvalidSuccessorBound = invalidSuccessorBound(report.ActiveGrant, report.RetiredGrants)
	report.Anomalies.OrphanInheritance = orphanInheritance(report.RetiredGrants, report.GrantInheritances)
	switch {
	case report.Anomalies.OrphanInheritance:
		report.Anomalies.FinalityDefect = FinalityDefectOrphanInheritance
	case report.Anomalies.InvalidSuccessorBound:
		report.Anomalies.FinalityDefect = FinalityDefectInvalidSuccessorBound
	case report.Anomalies.RetiredGrantNotInherited:
		report.Anomalies.FinalityDefect = FinalityDefectRetiredNotInherited
	default:
		report.Anomalies.FinalityDefect = FinalityDefectNone
	}
	report.AuthorityCompletion = evaluateAuthorityCompletion(report.RetiredGrants)
	return report
}

func evaluateAuthorityCompletion(retirements []rootproto.GrantRetirement) AuthorityCompletionState {
	var latest rootproto.GrantRetirement
	for _, retirement := range retirements {
		if retirement.Era > latest.Era {
			latest = retirement
		}
	}
	if !latest.Present() {
		return AuthorityCompletionNone
	}
	if latest.InheritedByGrantID == "" {
		return AuthorityCompletionRetiredNotInherited
	}
	switch latest.Mode {
	case rootproto.GrantRetirementSealedExact:
		return AuthorityCompletionSealedExactCompleted
	case rootproto.GrantRetirementExpiredBound:
		return AuthorityCompletionExpiredBoundInherited
	default:
		return AuthorityCompletionRetiredNotInherited
	}
}

func invalidSuccessorBound(grant rootproto.AuthorityGrant, retirements []rootproto.GrantRetirement) bool {
	if !grant.Present() {
		return false
	}
	for _, retirement := range retirements {
		if retirement.InheritedByGrantID != "" {
			continue
		}
		for _, bound := range retirement.Bounds {
			duty, ok := grant.DutyFor(bound.DutyID, bound.Scope)
			if !ok || !rootproto.DutyBoundCovers(duty.Bound, bound.Bound) {
				return true
			}
		}
	}
	return false
}

func orphanInheritance(retirements []rootproto.GrantRetirement, inheritances []rootproto.GrantInheritance) bool {
	retired := make(map[string]rootproto.GrantRetirement, len(retirements))
	for _, retirement := range retirements {
		if retirement.GrantID != "" {
			retired[retirement.GrantID] = retirement
		}
	}
	for _, inheritance := range inheritances {
		retirement, ok := retired[inheritance.PredecessorGrantID]
		if !ok {
			return true
		}
		if retirement.InheritedByGrantID != "" && retirement.InheritedByGrantID != inheritance.SuccessorGrantID {
			return true
		}
	}
	return false
}
