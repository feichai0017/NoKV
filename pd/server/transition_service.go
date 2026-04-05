package server

import (
	"context"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	pdview "github.com/feichai0017/NoKV/pd/view"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListTransitions returns the rooted transition/operator view currently
// materialized inside PD.
func (s *Service) ListTransitions(_ context.Context, _ *pdpb.ListTransitionsRequest) (*pdpb.ListTransitionsResponse, error) {
	if s == nil || s.cluster == nil {
		return &pdpb.ListTransitionsResponse{}, nil
	}
	snapshot := s.cluster.OperatorSnapshot()
	entries := make([]*pdpb.TransitionEntry, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		entries = append(entries, transitionEntryToProto(entry))
	}
	return &pdpb.ListTransitionsResponse{Entries: entries}, nil
}

// AssessRootEvent evaluates one rooted transition event against the current
// rooted view without mutating truth.
func (s *Service) AssessRootEvent(_ context.Context, req *pdpb.AssessRootEventRequest) (*pdpb.AssessRootEventResponse, error) {
	if req == nil || req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "assess root event request missing event")
	}
	event := metacodec.RootEventFromProto(req.GetEvent())
	if event.Kind == rootevent.KindUnknown {
		return nil, status.Error(codes.InvalidArgument, "assess root event requires known kind")
	}
	event, err := s.normalizeRootEvent(event)
	if err != nil {
		return nil, status.Error(codes.Internal, "normalize root event: "+err.Error())
	}
	assessment := s.cluster.ObserveRootEventLifecycle(event)
	return &pdpb.AssessRootEventResponse{
		Assessment: transitionAssessmentToProto(assessment),
	}, nil
}

func transitionEntryToProto(entry rootstate.TransitionEntry) *pdpb.TransitionEntry {
	out := &pdpb.TransitionEntry{
		Key:        entry.Key,
		Kind:       transitionKindToProto(entry.Kind),
		Status:     transitionStatusToProto(entry.Status),
		RetryClass: transitionRetryClassToProto(entry.RetryClass),
		Reason:     transitionReasonToProto(entry.Reason),
	}
	if entry.PeerChange != nil {
		out.PendingPeerChange = metacodec.RootPendingPeerChangeToProto(entry.Key, *entry.PeerChange)
	}
	if entry.RangeChange != nil {
		out.PendingRangeChange = metacodec.RootPendingRangeChangeToProto(entry.Key, *entry.RangeChange)
	}
	return out
}

func transitionAssessmentToProto(assessment pdview.TransitionAssessment) *pdpb.TransitionAssessment {
	return &pdpb.TransitionAssessment{
		Key:        assessment.Key,
		Kind:       transitionKindToProto(assessment.Kind),
		Status:     transitionStatusToProto(assessment.Status),
		RetryClass: transitionRetryClassToProto(assessment.RetryClass),
		Reason:     transitionReasonToProto(assessment.Reason),
		Decision:   transitionDecisionToProto(assessment.Decision),
	}
}

func transitionKindToProto(kind rootstate.TransitionKind) pdpb.TransitionKind {
	switch kind {
	case rootstate.TransitionKindPeerChange:
		return pdpb.TransitionKind_TRANSITION_KIND_PEER_CHANGE
	case rootstate.TransitionKindRangeChange:
		return pdpb.TransitionKind_TRANSITION_KIND_RANGE_CHANGE
	default:
		return pdpb.TransitionKind_TRANSITION_KIND_UNSPECIFIED
	}
}

func transitionStatusToProto(status rootstate.TransitionStatus) pdpb.TransitionStatus {
	switch status {
	case rootstate.TransitionStatusOpen:
		return pdpb.TransitionStatus_TRANSITION_STATUS_OPEN
	case rootstate.TransitionStatusPending:
		return pdpb.TransitionStatus_TRANSITION_STATUS_PENDING
	case rootstate.TransitionStatusCompleted:
		return pdpb.TransitionStatus_TRANSITION_STATUS_COMPLETED
	case rootstate.TransitionStatusConflict:
		return pdpb.TransitionStatus_TRANSITION_STATUS_CONFLICT
	case rootstate.TransitionStatusSuperseded:
		return pdpb.TransitionStatus_TRANSITION_STATUS_SUPERSEDED
	case rootstate.TransitionStatusCancelled:
		return pdpb.TransitionStatus_TRANSITION_STATUS_CANCELLED
	case rootstate.TransitionStatusAborted:
		return pdpb.TransitionStatus_TRANSITION_STATUS_ABORTED
	default:
		return pdpb.TransitionStatus_TRANSITION_STATUS_UNSPECIFIED
	}
}

func transitionRetryClassToProto(class rootstate.TransitionRetryClass) pdpb.TransitionRetryClass {
	switch class {
	case rootstate.TransitionRetryConflict:
		return pdpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_CONFLICT
	case rootstate.TransitionRetryTransient:
		return pdpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_TRANSIENT
	default:
		return pdpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_NONE
	}
}

func transitionReasonToProto(reason rootstate.TransitionReason) pdpb.TransitionReason {
	switch reason {
	case rootstate.TransitionReasonOpenApply:
		return pdpb.TransitionReason_TRANSITION_REASON_OPEN_APPLY
	case rootstate.TransitionReasonMatchingPending:
		return pdpb.TransitionReason_TRANSITION_REASON_MATCHING_PENDING
	case rootstate.TransitionReasonAlreadyCompleted:
		return pdpb.TransitionReason_TRANSITION_REASON_ALREADY_COMPLETED
	case rootstate.TransitionReasonConflictingPending:
		return pdpb.TransitionReason_TRANSITION_REASON_CONFLICTING_PENDING
	case rootstate.TransitionReasonSupersededTarget:
		return pdpb.TransitionReason_TRANSITION_REASON_SUPERSEDED_TARGET
	case rootstate.TransitionReasonCancelledTarget:
		return pdpb.TransitionReason_TRANSITION_REASON_CANCELLED_TARGET
	case rootstate.TransitionReasonAbortedApply:
		return pdpb.TransitionReason_TRANSITION_REASON_ABORTED_APPLY
	default:
		return pdpb.TransitionReason_TRANSITION_REASON_NONE
	}
}

func transitionDecisionToProto(decision rootstate.RootEventLifecycleDecision) pdpb.TransitionDecision {
	switch decision {
	case rootstate.RootEventLifecycleSkip:
		return pdpb.TransitionDecision_TRANSITION_DECISION_SKIP
	default:
		return pdpb.TransitionDecision_TRANSITION_DECISION_APPLY
	}
}
