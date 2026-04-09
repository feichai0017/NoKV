package server

import (
	"context"

	pdoperator "github.com/feichai0017/NoKV/coordinator/operator"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListTransitions returns the rooted transition/operator view currently
// materialized inside Coordinator.
func (s *Service) ListTransitions(_ context.Context, _ *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	if s == nil || s.cluster == nil {
		return &coordpb.ListTransitionsResponse{}, nil
	}
	snapshot := s.cluster.OperatorSnapshot()
	entries := make([]*coordpb.TransitionEntry, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		entries = append(entries, transitionEntryToProto(entry))
	}
	return &coordpb.ListTransitionsResponse{Entries: entries}, nil
}

// AssessRootEvent evaluates one rooted transition event against the current
// rooted view without mutating truth.
func (s *Service) AssessRootEvent(_ context.Context, req *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
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
	return &coordpb.AssessRootEventResponse{
		Assessment: transitionAssessmentToProto(assessment),
	}, nil
}

func transitionEntryToProto(entry pdoperator.RuntimeEntry) *coordpb.TransitionEntry {
	out := &coordpb.TransitionEntry{
		Key:        entry.Transition.Key,
		Kind:       transitionKindToProto(entry.Transition.Kind),
		Status:     transitionStatusToProto(entry.Transition.Status),
		RetryClass: transitionRetryClassToProto(entry.Transition.RetryClass),
		Reason:     transitionReasonToProto(entry.Transition.Reason),
	}
	if entry.Transition.PeerChange != nil {
		out.PendingPeerChange = metacodec.RootPendingPeerChangeToProto(entry.Transition.Key, *entry.Transition.PeerChange)
	}
	if entry.Transition.RangeChange != nil {
		out.PendingRangeChange = metacodec.RootPendingRangeChangeToProto(entry.Transition.Key, *entry.Transition.RangeChange)
	}
	return out
}

func transitionAssessmentToProto(assessment rootstate.TransitionAssessment) *coordpb.TransitionAssessment {
	return &coordpb.TransitionAssessment{
		Key:        assessment.Key,
		Kind:       transitionKindToProto(assessment.Kind),
		Status:     transitionStatusToProto(assessment.Status),
		RetryClass: transitionRetryClassToProto(assessment.RetryClass),
		Reason:     transitionReasonToProto(assessment.Reason),
		Decision:   transitionDecisionToProto(assessment.Decision),
	}
}

func transitionKindToProto(kind rootstate.TransitionKind) coordpb.TransitionKind {
	switch kind {
	case rootstate.TransitionKindPeerChange:
		return coordpb.TransitionKind_TRANSITION_KIND_PEER_CHANGE
	case rootstate.TransitionKindRangeChange:
		return coordpb.TransitionKind_TRANSITION_KIND_RANGE_CHANGE
	default:
		return coordpb.TransitionKind_TRANSITION_KIND_UNSPECIFIED
	}
}

func transitionStatusToProto(status rootstate.TransitionStatus) coordpb.TransitionStatus {
	switch status {
	case rootstate.TransitionStatusOpen:
		return coordpb.TransitionStatus_TRANSITION_STATUS_OPEN
	case rootstate.TransitionStatusPending:
		return coordpb.TransitionStatus_TRANSITION_STATUS_PENDING
	case rootstate.TransitionStatusCompleted:
		return coordpb.TransitionStatus_TRANSITION_STATUS_COMPLETED
	case rootstate.TransitionStatusConflict:
		return coordpb.TransitionStatus_TRANSITION_STATUS_CONFLICT
	case rootstate.TransitionStatusSuperseded:
		return coordpb.TransitionStatus_TRANSITION_STATUS_SUPERSEDED
	case rootstate.TransitionStatusCancelled:
		return coordpb.TransitionStatus_TRANSITION_STATUS_CANCELLED
	case rootstate.TransitionStatusAborted:
		return coordpb.TransitionStatus_TRANSITION_STATUS_ABORTED
	default:
		return coordpb.TransitionStatus_TRANSITION_STATUS_UNSPECIFIED
	}
}

func transitionRetryClassToProto(class rootstate.TransitionRetryClass) coordpb.TransitionRetryClass {
	switch class {
	case rootstate.TransitionRetryConflict:
		return coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_CONFLICT
	case rootstate.TransitionRetryTransient:
		return coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_TRANSIENT
	default:
		return coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_NONE
	}
}

func transitionReasonToProto(reason rootstate.TransitionReason) coordpb.TransitionReason {
	switch reason {
	case rootstate.TransitionReasonOpenApply:
		return coordpb.TransitionReason_TRANSITION_REASON_OPEN_APPLY
	case rootstate.TransitionReasonMatchingPending:
		return coordpb.TransitionReason_TRANSITION_REASON_MATCHING_PENDING
	case rootstate.TransitionReasonAlreadyCompleted:
		return coordpb.TransitionReason_TRANSITION_REASON_ALREADY_COMPLETED
	case rootstate.TransitionReasonConflictingPending:
		return coordpb.TransitionReason_TRANSITION_REASON_CONFLICTING_PENDING
	case rootstate.TransitionReasonSupersededTarget:
		return coordpb.TransitionReason_TRANSITION_REASON_SUPERSEDED_TARGET
	case rootstate.TransitionReasonCancelledTarget:
		return coordpb.TransitionReason_TRANSITION_REASON_CANCELLED_TARGET
	case rootstate.TransitionReasonAbortedApply:
		return coordpb.TransitionReason_TRANSITION_REASON_ABORTED_APPLY
	default:
		return coordpb.TransitionReason_TRANSITION_REASON_NONE
	}
}

func transitionDecisionToProto(decision rootstate.RootEventLifecycleDecision) coordpb.TransitionDecision {
	switch decision {
	case rootstate.RootEventLifecycleSkip:
		return coordpb.TransitionDecision_TRANSITION_DECISION_SKIP
	default:
		return coordpb.TransitionDecision_TRANSITION_DECISION_APPLY
	}
}
