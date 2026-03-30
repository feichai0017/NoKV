package store

import (
	"encoding/binary"
	"fmt"
	myraft "github.com/feichai0017/NoKV/raft"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
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
	meta := raftmeta.CloneRegionMeta(*region)
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
	if ev.Peer != nil && peerIndexByID(meta.Peers, ev.Peer.ID()) == -1 {
		if err := s.applyRegionRemoval(meta.ID); err != nil {
			return err
		}
		s.StopPeer(ev.Peer.ID())
		return nil
	}
	return s.applyRegionMeta(meta)
}

// ProposeAddPeer issues a configuration change to add the provided peer to the
// region's raft group. Local region metadata is updated once the configuration
// change is committed and applied.
func (s *Store) ProposeAddPeer(regionID uint64, meta raftmeta.PeerMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	if meta.PeerID == 0 {
		return fmt.Errorf("raftstore: peer id is zero")
	}
	peerRef := s.regionMgr().peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	if status := peerRef.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peerRef.ID())
	}
	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: meta.PeerID,
			},
		},
		Context: encodeConfChangeContext([]raftmeta.PeerMeta{meta}),
	}
	return peerRef.ProposeConfChange(cc)
}

// ProposeRemovePeer issues a configuration change removing the peer with the
// provided peer ID from the region's raft group.
func (s *Store) ProposeRemovePeer(regionID, peerID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || peerID == 0 {
		return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, peerID)
	}
	peerRef := s.regionMgr().peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	if status := peerRef.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peerRef.ID())
	}
	ctxMeta := raftmeta.PeerMeta{StoreID: peerID, PeerID: peerID}
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
		Context: encodeConfChangeContext([]raftmeta.PeerMeta{ctxMeta}),
	}
	return peerRef.ProposeConfChange(cc)
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
	peerRef := s.regionMgr().peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	if status := peerRef.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peerRef.ID())
	}
	return peerRef.TransferLeader(targetPeerID)
}

func applyConfChangeToMeta(meta *raftmeta.RegionMeta, cc raftpb.ConfChangeV2) (bool, error) {
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
		peerMeta := raftmeta.PeerMeta{StoreID: change.NodeID, PeerID: change.NodeID}
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

func encodeConfChangeContext(peers []raftmeta.PeerMeta) []byte {
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

func decodeConfChangeContext(ctx []byte) ([]raftmeta.PeerMeta, error) {
	if len(ctx) == 0 {
		return nil, nil
	}
	peers := make([]raftmeta.PeerMeta, 0, 2)
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
		peers = append(peers, raftmeta.PeerMeta{StoreID: storeID, PeerID: peerID})
	}
	return peers, nil
}

func peerIndexByID(peers []raftmeta.PeerMeta, peerID uint64) int {
	for i, meta := range peers {
		if meta.PeerID == peerID {
			return i
		}
	}
	return -1
}
