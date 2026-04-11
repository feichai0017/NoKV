package store

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

type transitionTarget struct {
	TransitionID string
	RegionID     uint64
	Event        rootevent.Event
	Action       string
	Noop         bool
	ConfChange   *raftpb.ConfChangeV2
	Admin        *raftcmdpb.AdminCommand
}

type terminalOutcome struct {
	TransitionID string
	RegionID     uint64
	Event        rootevent.Event
	Action       string
	Noop         bool
	Apply        func() error
}

func (s *Store) leaderPeer(regionID uint64) (*peer.Peer, error) {
	if s == nil {
		return nil, fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return nil, fmt.Errorf("raftstore: region id is zero")
	}
	peerRef := s.regionMgr().peer(regionID)
	if peerRef == nil {
		return nil, fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	if status := peerRef.Status(); status.RaftState != myraft.StateLeader {
		return nil, fmt.Errorf("raftstore: peer %d is not leader", peerRef.ID())
	}
	return peerRef, nil
}

func (s *Store) publishPlannedRootEvent(regionID uint64, event rootevent.Event, action string) error {
	if s == nil || s.schedulerClient() == nil || regionID == 0 || event.Kind == rootevent.KindUnknown {
		return nil
	}
	ctx, cancel := s.schedulerPublishContext()
	defer cancel()
	if err := s.schedulerClient().PublishRootEvent(ctx, event); err != nil {
		if action == "" {
			action = "transition"
		}
		return fmt.Errorf("raftstore: publish %s target: %w", action, err)
	}
	return nil
}

func (s *Store) proposeTransition(target transitionTarget) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if target.RegionID == 0 {
		return fmt.Errorf("raftstore: transition region id is zero")
	}
	if target.ConfChange == nil && target.Admin == nil {
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
	peerRef, err := s.leaderPeer(target.RegionID)
	if err != nil {
		return err
	}
	switch {
	case target.ConfChange != nil:
		return peerRef.ProposeConfChange(*target.ConfChange)
	case target.Admin != nil:
		payload, err := proto.Marshal(target.Admin)
		if err != nil {
			return fmt.Errorf("raftstore: marshal transition admin proposal: %w", err)
		}
		return peerRef.ProposeAdmin(payload)
	default:
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
}

func (s *Store) executeTransitionTarget(target transitionTarget) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if target.Noop {
		return nil
	}
	if target.RegionID == 0 {
		s.recordAdmission(Admission{Class: AdmissionClassTopology, Reason: AdmissionReasonInvalid, Detail: "transition region id is zero"})
		s.recordTopologyRejected(target, fmt.Errorf("raftstore: transition region id is zero"))
		return fmt.Errorf("raftstore: transition region id is zero")
	}
	if target.ConfChange == nil && target.Admin == nil {
		s.recordAdmission(Admission{Class: AdmissionClassTopology, Reason: AdmissionReasonInvalid, RegionID: target.RegionID, Detail: "transition proposal is empty"})
		s.recordTopologyRejected(target, fmt.Errorf("raftstore: transition proposal is empty"))
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
	s.recordAdmission(Admission{
		Class:    AdmissionClassTopology,
		Reason:   AdmissionReasonAccepted,
		Accepted: true,
		RegionID: target.RegionID,
		Detail:   target.Action,
	})
	s.recordTopologyQueued(target)
	if err := s.publishPlannedRootEvent(target.RegionID, target.Event, target.Action); err != nil {
		s.recordTopologyFailed(target.TransitionID, target.RegionID, target.Action, PublishStateUnknown, err)
		return err
	}
	if err := s.proposeTransition(target); err != nil {
		s.recordTopologyFailed(target.TransitionID, target.RegionID, target.Action, PublishStatePlannedPublished, err)
		return err
	}
	s.recordTopologyProposed(target)
	return nil
}

func (s *Store) enqueueAppliedRootEvent(event rootevent.Event) {
	if s == nil || s.sched == nil || event.Kind == rootevent.KindUnknown {
		return
	}
	s.enqueueRegionEvent(regionEvent{
		root:         event,
		transitionID: rootstate.TransitionIDFromEvent(event),
	})
}

func (s *Store) applyTerminalOutcome(term terminalOutcome) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if term.Noop {
		return nil
	}
	if term.Apply != nil {
		if err := term.Apply(); err != nil {
			s.recordTopologyFailed(term.TransitionID, term.RegionID, term.Action, PublishStateUnknown, err)
			return err
		}
	}
	if term.TransitionID == "" && term.Event.Kind != rootevent.KindUnknown {
		term.TransitionID = rootstate.TransitionIDFromEvent(term.Event)
	}
	s.recordTopologyApplied(term)
	s.enqueueAppliedRootEvent(term.Event)
	return nil
}
