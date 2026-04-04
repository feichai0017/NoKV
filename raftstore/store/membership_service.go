package store

import (
	"encoding/binary"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
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
		return s.applyTerminalTransition(terminalTransition{
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
		return s.applyTerminalTransition(terminalTransition{
			Action: "peer change",
			Apply: func() error {
				return s.applyRegionMetaSilent(meta)
			},
		})
	}
	return s.applyTerminalTransition(terminalTransition{
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
	plan, err := s.planPeerChange(regionID, cc)
	if err != nil {
		return err
	}
	return s.executeTransitionPlan(plan)
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
	plan, err := s.planPeerChange(regionID, cc)
	if err != nil {
		return err
	}
	return s.executeTransitionPlan(plan)
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

func applyConfChangeToMeta(meta *localmeta.RegionMeta, cc raftpb.ConfChangeV2) (bool, error) {
	if meta == nil {
		return false, fmt.Errorf("raftstore: region meta is nil")
	}
	changed := false
	ctxPeers, err := decodeConfChangeContext(cc.Context)
	if err != nil {
		return false, err
	}
	ctxIndex := 0
	for _, change := range cc.Changes {
		peerMeta := metaregion.Peer{StoreID: change.NodeID, PeerID: change.NodeID}
		if ctxIndex < len(ctxPeers) {
			peerMeta = ctxPeers[ctxIndex]
		}
		ctxIndex++
		switch change.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if idx := peerIndexByID(meta.Peers, peerMeta.PeerID); idx == -1 {
				meta.Peers = append(meta.Peers, peerMeta)
				changed = true
			}
		case raftpb.ConfChangeRemoveNode:
			if idx := peerIndexByID(meta.Peers, change.NodeID); idx >= 0 {
				meta.Peers = append(meta.Peers[:idx], meta.Peers[idx+1:]...)
				changed = true
			}
		case raftpb.ConfChangeUpdateNode:
			if idx := peerIndexByID(meta.Peers, change.NodeID); idx >= 0 {
				meta.Peers[idx] = peerMeta
				changed = true
			}
		default:
			return false, fmt.Errorf("raftstore: unsupported conf change type %v", change.Type)
		}
	}
	return changed, nil
}

func encodeConfChangeContext(peers []metaregion.Peer) []byte {
	if len(peers) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(peers)*16)
	for _, meta := range peers {
		buf = binary.AppendUvarint(buf, meta.StoreID)
		buf = binary.AppendUvarint(buf, meta.PeerID)
	}
	return buf
}

func decodeConfChangeContext(ctx []byte) ([]metaregion.Peer, error) {
	if len(ctx) == 0 {
		return nil, nil
	}
	peers := make([]metaregion.Peer, 0, 2)
	for len(ctx) > 0 {
		storeID, n := binary.Uvarint(ctx)
		if n <= 0 {
			return nil, fmt.Errorf("raftstore: invalid conf change context")
		}
		ctx = ctx[n:]
		peerID, m := binary.Uvarint(ctx)
		if m <= 0 {
			return nil, fmt.Errorf("raftstore: invalid conf change context")
		}
		ctx = ctx[m:]
		peers = append(peers, metaregion.Peer{StoreID: storeID, PeerID: peerID})
	}
	return peers, nil
}

func peerIndexByID(peers []metaregion.Peer, peerID uint64) int {
	for i, meta := range peers {
		if meta.PeerID == peerID {
			return i
		}
	}
	return -1
}

func appliedPeerChangeEvent(meta localmeta.RegionMeta, cc raftpb.ConfChangeV2) (rootevent.Event, bool) {
	if meta.ID == 0 || len(cc.Changes) != 1 {
		return rootevent.Event{}, false
	}
	desc := metacodec.DescriptorFromLocalRegionMeta(meta, 0)
	change := cc.Changes[0]
	peerMeta := confChangeTargetPeer(change, cc.Context)
	switch change.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		return rootevent.PeerAdded(meta.ID, peerMeta.StoreID, peerMeta.PeerID, desc), true
	case raftpb.ConfChangeRemoveNode:
		return rootevent.PeerRemoved(meta.ID, peerMeta.StoreID, peerMeta.PeerID, desc), true
	default:
		return rootevent.Event{}, false
	}
}

func plannedPeerChangeEvent(meta localmeta.RegionMeta, cc raftpb.ConfChangeV2) (rootevent.Event, error) {
	if meta.ID == 0 || len(cc.Changes) != 1 {
		return rootevent.Event{}, fmt.Errorf("raftstore: invalid peer change event")
	}
	desc := metacodec.DescriptorFromLocalRegionMeta(meta, 0)
	change := cc.Changes[0]
	peerMeta := confChangeTargetPeer(change, cc.Context)
	switch change.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		return rootevent.PeerAdditionPlanned(meta.ID, peerMeta.StoreID, peerMeta.PeerID, desc), nil
	case raftpb.ConfChangeRemoveNode:
		return rootevent.PeerRemovalPlanned(meta.ID, peerMeta.StoreID, peerMeta.PeerID, desc), nil
	default:
		return rootevent.Event{}, fmt.Errorf("raftstore: unsupported conf change type %v", change.Type)
	}
}

func confChangeTargetPeer(change raftpb.ConfChangeSingle, ctx []byte) metaregion.Peer {
	peerMeta := metaregion.Peer{StoreID: change.NodeID, PeerID: change.NodeID}
	if peers, err := decodeConfChangeContext(ctx); err == nil && len(peers) > 0 {
		peerMeta = peers[0]
	}
	return peerMeta
}

func (s *Store) planPeerChange(regionID uint64, cc raftpb.ConfChangeV2) (transitionPlan, error) {
	if s == nil {
		return transitionPlan{}, fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || len(cc.Changes) != 1 {
		return transitionPlan{}, fmt.Errorf("raftstore: invalid peer change plan")
	}
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		return transitionPlan{}, fmt.Errorf("raftstore: region %d metadata not found", regionID)
	}
	next := localmeta.CloneRegionMeta(meta)
	changed, err := applyConfChangeToMeta(&next, cc)
	if err != nil || !changed {
		if err != nil {
			return transitionPlan{}, err
		}
		return transitionPlan{RegionID: regionID, Noop: true}, nil
	}
	next.Epoch.ConfVersion += uint64(len(cc.Changes))
	event, err := plannedPeerChangeEvent(next, cc)
	if err != nil {
		return transitionPlan{}, err
	}
	return transitionPlan{
		RegionID: regionID,
		Event:    event,
		Action:   "peer change",
		Propose: func(peerRef *peer.Peer) error {
			return peerRef.ProposeConfChange(cc)
		},
	}, nil
}
