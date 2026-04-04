package store

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type transitionPlan struct {
	RegionID     uint64
	Event        rootevent.Event
	Action       string
	Noop         bool
	ConfChange   *raftpb.ConfChangeV2
	AdminPayload []byte
}

type terminalTransition struct {
	Event  rootevent.Event
	Action string
	Noop   bool
	Apply  func() error
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
	if err := s.schedulerClient().PublishRootEvent(s.runtimeContext(), event); err != nil {
		if action == "" {
			action = "transition"
		}
		return fmt.Errorf("raftstore: publish %s target: %w", action, err)
	}
	return nil
}

func (s *Store) executeTransitionPlan(plan transitionPlan) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if plan.Noop {
		return nil
	}
	if plan.RegionID == 0 {
		return fmt.Errorf("raftstore: transition region id is zero")
	}
	if plan.ConfChange == nil && len(plan.AdminPayload) == 0 {
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
	if err := s.publishPlannedRootEvent(plan.RegionID, plan.Event, plan.Action); err != nil {
		return err
	}
	peerRef, err := s.leaderPeer(plan.RegionID)
	if err != nil {
		return err
	}
	switch {
	case plan.ConfChange != nil:
		return peerRef.ProposeConfChange(*plan.ConfChange)
	case len(plan.AdminPayload) > 0:
		return peerRef.ProposeAdmin(plan.AdminPayload)
	default:
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
}

func (s *Store) enqueueAppliedRootEvent(event rootevent.Event) {
	if s == nil || s.sched == nil || event.Kind == rootevent.KindUnknown {
		return
	}
	s.enqueueRegionEvent(regionEvent{
		root: event,
	})
}

func (s *Store) applyTerminalTransition(term terminalTransition) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if term.Noop {
		return nil
	}
	if term.Apply != nil {
		if err := term.Apply(); err != nil {
			return err
		}
	}
	s.enqueueAppliedRootEvent(term.Event)
	return nil
}
