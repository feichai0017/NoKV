package store

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

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
