package store

import (
	"bytes"
	"fmt"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	proto "google.golang.org/protobuf/proto"
)

// SplitRegion updates the parent region metadata and bootstraps a new peer for
// the child region. The child metadata must describe the desired child region
// (key range, peers, epoch).
func (s *Store) SplitRegion(parentID uint64, childMeta manifest.RegionMeta) (*peer.Peer, error) {
	if s == nil {
		return nil, fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 {
		return nil, fmt.Errorf("raftstore: parent region id is zero")
	}
	childMeta = manifest.CloneRegionMeta(childMeta)
	if childMeta.ID == 0 {
		return nil, fmt.Errorf("raftstore: child region id is zero")
	}
	if len(childMeta.StartKey) == 0 {
		return nil, fmt.Errorf("raftstore: child region start key required")
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return nil, fmt.Errorf("raftstore: parent region %d not found", parentID)
	}
	originalParent := manifest.CloneRegionMeta(parentMeta)
	if len(parentMeta.EndKey) > 0 && bytes.Compare(childMeta.StartKey, parentMeta.EndKey) >= 0 {
		return nil, fmt.Errorf("raftstore: split key >= parent end key")
	}
	if bytes.Compare(childMeta.StartKey, parentMeta.StartKey) <= 0 {
		return nil, fmt.Errorf("raftstore: split key must be greater than parent start key")
	}
	newParent := parentMeta
	newParent.EndKey = append([]byte(nil), childMeta.StartKey...)
	newParent.Epoch.Version++
	if err := s.UpdateRegion(newParent); err != nil {
		return nil, err
	}
	if childMeta.State == 0 {
		childMeta.State = manifest.RegionStateRunning
	}
	cfg, bootstrapPeers, err := s.buildChildPeerConfig(childMeta)
	if err != nil {
		_ = s.UpdateRegion(originalParent)
		return nil, err
	}
	childPeer, err := s.StartPeer(cfg, bootstrapPeers)
	if err != nil {
		_ = s.UpdateRegion(originalParent)
		return nil, err
	}
	return childPeer, nil
}

// ProposeSplit issues a split command through the raft log of the parent
// region. The child metadata must describe the new region configuration.
func (s *Store) ProposeSplit(parentID uint64, childMeta manifest.RegionMeta, splitKey []byte) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 || childMeta.ID == 0 {
		return fmt.Errorf("raftstore: invalid region identifiers")
	}
	parentPeer := s.regions.peer(parentID)
	if parentPeer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", parentID)
	}
	if status := parentPeer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", parentPeer.ID())
	}
	cmd := &pb.AdminCommand{
		Type: pb.AdminCommand_SPLIT,
		Split: &pb.SplitCommand{
			ParentRegionId: parentID,
			SplitKey:       append([]byte(nil), splitKey...),
			Child:          regionMetaToPB(childMeta),
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return parentPeer.ProposeAdmin(data)
}

// ProposeMerge submits a merge admin command merging source region into target.
func (s *Store) ProposeMerge(targetRegionID, sourceRegionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return fmt.Errorf("raftstore: invalid region identifiers")
	}
	peer := s.regions.peer(targetRegionID)
	if peer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", targetRegionID)
	}
	if status := peer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peer.ID())
	}
	cmd := &pb.AdminCommand{
		Type: pb.AdminCommand_MERGE,
		Merge: &pb.MergeCommand{
			TargetRegionId: targetRegionID,
			SourceRegionId: sourceRegionID,
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return peer.ProposeAdmin(data)
}

func (s *Store) buildChildPeerConfig(child manifest.RegionMeta) (*peer.Config, []myraft.Peer, error) {
	if s.peerBuilder == nil {
		return nil, nil, fmt.Errorf("raftstore: peer builder not configured")
	}
	cfg, err := s.peerBuilder(child)
	if err != nil {
		return nil, nil, err
	}
	if cfg == nil {
		return nil, nil, fmt.Errorf("raftstore: peer builder returned nil config")
	}
	if cfg.Region == nil {
		cfg.Region = &child
	}
	peers := make([]myraft.Peer, 0, len(child.Peers))
	for _, peerMeta := range child.Peers {
		peers = append(peers, myraft.Peer{ID: peerMeta.PeerID})
	}
	return cfg, peers, nil
}

func (s *Store) handleAdminCommand(cmd *pb.AdminCommand) error {
	if s == nil || cmd == nil {
		return nil
	}
	switch cmd.Type {
	case pb.AdminCommand_SPLIT:
		return s.handleSplitCommand(cmd.Split)
	case pb.AdminCommand_MERGE:
		return s.handleMergeCommand(cmd.Merge)
	default:
		return nil
	}
}

func (s *Store) handleSplitCommand(split *pb.SplitCommand) error {
	if split == nil {
		return fmt.Errorf("raftstore: split command missing payload")
	}
	childMeta := pbRegionMetaToManifest(split.GetChild())
	childMeta.State = manifest.RegionStateRunning
	if len(childMeta.StartKey) == 0 {
		childMeta.StartKey = append([]byte(nil), split.GetSplitKey()...)
	}
	_, err := s.SplitRegion(split.GetParentRegionId(), childMeta)
	return err
}

func (s *Store) handleMergeCommand(merge *pb.MergeCommand) error {
	if merge == nil {
		return fmt.Errorf("raftstore: merge command missing payload")
	}
	parentMeta, ok := s.RegionMetaByID(merge.GetTargetRegionId())
	if !ok {
		return fmt.Errorf("raftstore: target region %d not found", merge.GetTargetRegionId())
	}
	sourceMeta, ok := s.RegionMetaByID(merge.GetSourceRegionId())
	if !ok {
		return fmt.Errorf("raftstore: source region %d not found", merge.GetSourceRegionId())
	}
	updated := parentMeta
	updated.Epoch.Version++
	if len(sourceMeta.EndKey) == 0 || bytes.Compare(sourceMeta.EndKey, updated.EndKey) > 0 {
		updated.EndKey = append([]byte(nil), sourceMeta.EndKey...)
	}
	if err := s.UpdateRegion(updated); err != nil {
		return err
	}
	if peer := s.regions.peer(sourceMeta.ID); peer != nil {
		s.StopPeer(peer.ID())
	}
	if err := s.RemoveRegion(sourceMeta.ID); err != nil {
		return err
	}
	return nil
}

func regionMetaToPB(meta manifest.RegionMeta) *pb.RegionMeta {
	peers := make([]*pb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
		Peers:            peers,
	}
}

func pbRegionMetaToManifest(pbMeta *pb.RegionMeta) manifest.RegionMeta {
	if pbMeta == nil {
		return manifest.RegionMeta{}
	}
	meta := manifest.RegionMeta{
		ID:       pbMeta.GetId(),
		StartKey: append([]byte(nil), pbMeta.GetStartKey()...),
		EndKey:   append([]byte(nil), pbMeta.GetEndKey()...),
		Epoch: manifest.RegionEpoch{
			Version:     pbMeta.GetEpochVersion(),
			ConfVersion: pbMeta.GetEpochConfVersion(),
		},
	}
	for _, peerPB := range pbMeta.GetPeers() {
		meta.Peers = append(meta.Peers, manifest.PeerMeta{
			StoreID: peerPB.GetStoreId(),
			PeerID:  peerPB.GetPeerId(),
		})
	}
	return meta
}
