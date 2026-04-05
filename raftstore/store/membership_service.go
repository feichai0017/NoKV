package store

import (
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

func (s *Store) handlePeerConfChange(ev peer.ConfChangeEvent) error {
	if s == nil {
		return nil
	}
	region := ev.RegionMeta
	if region == nil && ev.Peer != nil {
		region = ev.Peer.RegionMeta()
	}
	if region == nil || region.ID == 0 {
		return nil
	}
	meta := localmeta.CloneRegionMeta(*region)
	changed, err := applyConfChangeToMeta(&meta, ev.ConfChange)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if len(ev.ConfChange.Changes) > 0 {
		meta.Epoch.ConfVersion += uint64(len(ev.ConfChange.Changes))
	}
	appliedEvent, hasAppliedEvent := appliedPeerChangeEvent(meta, ev.ConfChange)
	if ev.Peer != nil && peerIndexByID(meta.Peers, ev.Peer.ID()) == -1 {
		return s.applyTerminalOutcome(terminalOutcome{
			Event:  appliedEvent,
			Action: "peer change",
			Apply: func() error {
				if err := s.applyRegionRemovalSilent(meta.ID); err != nil {
					return err
				}
				s.stopPeer(ev.Peer.ID(), false)
				return nil
			},
		})
	}
	if !hasAppliedEvent {
		return s.applyTerminalOutcome(terminalOutcome{
			Action: "peer change",
			Apply: func() error {
				return s.applyRegionMetaSilent(meta)
			},
		})
	}
	return s.applyTerminalOutcome(terminalOutcome{
		Event:  appliedEvent,
		Action: "peer change",
		Apply: func() error {
			return s.applyRegionMetaSilent(meta)
		},
	})
}

// AddPeer publishes one planned peer-addition target into Meta and then
// proposes the matching data-plane raft configuration change.
func (s *Store) AddPeer(regionID uint64, meta metaregion.Peer) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	if meta.PeerID == 0 {
		return fmt.Errorf("raftstore: peer id is zero")
	}
	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: meta.PeerID,
			},
		},
		Context: encodeConfChangeContext([]metaregion.Peer{meta}),
	}
	target, err := s.buildPeerChangeTarget(regionID, cc)
	if err != nil {
		return err
	}
	return s.executeTransitionTarget(target)
}

// RemovePeer publishes one planned peer-removal target into Meta and then
// proposes the matching data-plane raft configuration change.
func (s *Store) RemovePeer(regionID, peerID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || peerID == 0 {
		return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, peerID)
	}
	ctxMeta := metaregion.Peer{StoreID: peerID, PeerID: peerID}
	if meta, ok := s.RegionMetaByID(regionID); ok {
		if idx := peerIndexByID(meta.Peers, peerID); idx >= 0 {
			ctxMeta = meta.Peers[idx]
		}
	}
	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: peerID,
			},
		},
		Context: encodeConfChangeContext([]metaregion.Peer{ctxMeta}),
	}
	target, err := s.buildPeerChangeTarget(regionID, cc)
	if err != nil {
		return err
	}
	return s.executeTransitionTarget(target)
}

// TransferLeader initiates leadership transfer for the specified region to the
// provided peer ID.
func (s *Store) TransferLeader(regionID, targetPeerID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || targetPeerID == 0 {
		return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, targetPeerID)
	}
	peerRef, err := s.leaderPeer(regionID)
	if err != nil {
		return err
	}
	return peerRef.TransferLeader(targetPeerID)
}
