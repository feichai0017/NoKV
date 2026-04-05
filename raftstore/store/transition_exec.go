package store

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

type transitionProposal struct {
	ConfChange *raftpb.ConfChangeV2
	Admin      *raftcmdpb.AdminCommand
}

func (p transitionProposal) empty() bool {
	return p.ConfChange == nil && p.Admin == nil
}

type transitionTarget struct {
	RegionID uint64
	Event    rootevent.Event
	Action   string
	Noop     bool
	Proposal transitionProposal
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

func (s *Store) proposeTransition(regionID uint64, proposal transitionProposal) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: transition region id is zero")
	}
	if proposal.empty() {
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
	peerRef, err := s.leaderPeer(regionID)
	if err != nil {
		return err
	}
	switch {
	case proposal.ConfChange != nil:
		return peerRef.ProposeConfChange(*proposal.ConfChange)
	case proposal.Admin != nil:
		payload, err := proto.Marshal(proposal.Admin)
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
		return fmt.Errorf("raftstore: transition region id is zero")
	}
	if target.Proposal.empty() {
		return fmt.Errorf("raftstore: transition proposal is empty")
	}
	if err := s.publishPlannedRootEvent(target.RegionID, target.Event, target.Action); err != nil {
		return err
	}
	return s.proposeTransition(target.RegionID, target.Proposal)
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
