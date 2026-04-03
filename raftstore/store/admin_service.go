package store

import (
	"bytes"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/raftstore/peer"
	proto "google.golang.org/protobuf/proto"
)

// splitRegionLocal updates the parent region metadata and bootstraps a new
// peer for the child region. It is intentionally kept local to the store
// package so callers cannot bypass raft and mutate region layout directly.
func (s *Store) splitRegionLocal(parentID uint64, childMeta localmeta.RegionMeta) (*peer.Peer, error) {
	if s == nil {
		return nil, fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 {
		return nil, fmt.Errorf("raftstore: parent region id is zero")
	}
	childMeta = localmeta.CloneRegionMeta(childMeta)
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
	originalParent := localmeta.CloneRegionMeta(parentMeta)
	if len(parentMeta.EndKey) > 0 && bytes.Compare(childMeta.StartKey, parentMeta.EndKey) >= 0 {
		return nil, fmt.Errorf("raftstore: split key >= parent end key")
	}
	if bytes.Compare(childMeta.StartKey, parentMeta.StartKey) <= 0 {
		return nil, fmt.Errorf("raftstore: split key must be greater than parent start key")
	}
	newParent := parentMeta
	newParent.EndKey = append([]byte(nil), childMeta.StartKey...)
	newParent.Epoch.Version++
	if err := s.applyRegionMeta(newParent); err != nil {
		return nil, err
	}
	if childMeta.State == 0 {
		childMeta.State = metaregion.ReplicaStateRunning
	}
	cfg, bootstrapPeers, err := s.buildChildPeerConfig(childMeta)
	if err != nil {
		_ = s.applyRegionMeta(originalParent)
		return nil, err
	}
	childPeer, err := s.StartPeer(cfg, bootstrapPeers)
	if err != nil {
		_ = s.applyRegionMeta(originalParent)
		return nil, err
	}
	if s.sched != nil {
		parentDesc := metacodec.DescriptorFromLocalRegionMeta(newParent, 0)
		childDesc := metacodec.DescriptorFromLocalRegionMeta(childMeta, 0)
		parentDesc.Lineage = append(parentDesc.Lineage, descriptor.LineageRef{
			RegionID: originalParent.ID,
			Epoch:    originalParent.Epoch,
			Kind:     descriptor.LineageKindSplitParent,
		})
		childDesc.Lineage = append(childDesc.Lineage, descriptor.LineageRef{
			RegionID: originalParent.ID,
			Epoch:    originalParent.Epoch,
			Kind:     descriptor.LineageKindSplitParent,
		})
		s.enqueueRegionEvent(regionEvent{kind: regionEventApply, regionID: parentDesc.RegionID, desc: parentDesc})
		s.enqueueRegionEvent(regionEvent{kind: regionEventApply, regionID: childDesc.RegionID, desc: childDesc})
	}
	return childPeer, nil
}

// ProposeSplit issues a split command through the raft log of the parent
// region. The child metadata must describe the new region configuration.
func (s *Store) ProposeSplit(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 || childMeta.ID == 0 {
		return fmt.Errorf("raftstore: invalid region identifiers")
	}
	parentPeer := s.regionMgr().peer(parentID)
	if parentPeer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", parentID)
	}
	if status := parentPeer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", parentPeer.ID())
	}
	cmd := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_SPLIT,
		Split: &raftcmdpb.SplitCommand{
			ParentRegionId: parentID,
			SplitKey:       append([]byte(nil), splitKey...),
			Child:          metacodec.LocalRegionMetaToDescriptorProto(childMeta),
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
	peer := s.regionMgr().peer(targetRegionID)
	if peer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", targetRegionID)
	}
	if status := peer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peer.ID())
	}
	cmd := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_MERGE,
		Merge: &raftcmdpb.MergeCommand{
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

func (s *Store) buildChildPeerConfig(child localmeta.RegionMeta) (*peer.Config, []myraft.Peer, error) {
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

func (s *Store) handleAdminCommand(cmd *raftcmdpb.AdminCommand) error {
	if s == nil || cmd == nil {
		return nil
	}
	switch cmd.Type {
	case raftcmdpb.AdminCommand_SPLIT:
		return s.handleSplitCommand(cmd.Split)
	case raftcmdpb.AdminCommand_MERGE:
		return s.handleMergeCommand(cmd.Merge)
	default:
		return nil
	}
}

func (s *Store) handleSplitCommand(split *raftcmdpb.SplitCommand) error {
	if split == nil {
		return fmt.Errorf("raftstore: split command missing payload")
	}
	childMeta := pbRegionMetaToManifest(split.GetChild())
	childMeta.State = metaregion.ReplicaStateRunning
	if len(childMeta.StartKey) == 0 {
		childMeta.StartKey = append([]byte(nil), split.GetSplitKey()...)
	}
	if s.splitCommandAlreadyApplied(split.GetParentRegionId(), childMeta) {
		return nil
	}
	_, err := s.splitRegionLocal(split.GetParentRegionId(), childMeta)
	return err
}

func (s *Store) handleMergeCommand(merge *raftcmdpb.MergeCommand) error {
	if merge == nil {
		return fmt.Errorf("raftstore: merge command missing payload")
	}
	parentMeta, ok := s.RegionMetaByID(merge.GetTargetRegionId())
	if !ok {
		return fmt.Errorf("raftstore: target region %d not found", merge.GetTargetRegionId())
	}
	sourceMeta, ok := s.RegionMetaByID(merge.GetSourceRegionId())
	if !ok {
		// Merge apply must be replay-safe across restart. Once the source region
		// has already been removed locally, replaying the committed merge is a
		// no-op instead of a fatal state divergence.
		return nil
	}
	if sourceMeta.State == metaregion.ReplicaStateTombstone {
		return nil
	}
	updated := parentMeta
	updated.Epoch.Version++
	if len(sourceMeta.EndKey) == 0 || bytes.Compare(sourceMeta.EndKey, updated.EndKey) > 0 {
		updated.EndKey = append([]byte(nil), sourceMeta.EndKey...)
	}
	if err := s.applyRegionMeta(updated); err != nil {
		return err
	}
	if s.sched != nil {
		mergedDesc := metacodec.DescriptorFromLocalRegionMeta(updated, 0)
		mergedDesc.Lineage = append(mergedDesc.Lineage, descriptor.LineageRef{
			RegionID: sourceMeta.ID,
			Epoch:    sourceMeta.Epoch,
			Kind:     descriptor.LineageKindMergeSource,
		})
		s.enqueueRegionEvent(regionEvent{kind: regionEventApply, regionID: mergedDesc.RegionID, desc: mergedDesc})
	}
	if peer := s.regionMgr().peer(sourceMeta.ID); peer != nil {
		s.StopPeer(peer.ID())
	}
	if err := s.applyRegionRemoval(sourceMeta.ID); err != nil {
		return err
	}
	return nil
}

func (s *Store) splitCommandAlreadyApplied(parentID uint64, childMeta localmeta.RegionMeta) bool {
	if s == nil || parentID == 0 || childMeta.ID == 0 {
		return false
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return false
	}
	existingChild, ok := s.RegionMetaByID(childMeta.ID)
	if !ok {
		return false
	}
	if !bytes.Equal(parentMeta.EndKey, childMeta.StartKey) {
		return false
	}
	if !bytes.Equal(existingChild.StartKey, childMeta.StartKey) || !bytes.Equal(existingChild.EndKey, childMeta.EndKey) {
		return false
	}
	return regionPeersEqual(existingChild.Peers, childMeta.Peers)
}

func regionPeersEqual(a, b []metaregion.Peer) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func pbRegionMetaToManifest(pbMeta *metapb.RegionDescriptor) localmeta.RegionMeta {
	if pbMeta == nil {
		return localmeta.RegionMeta{}
	}
	meta, _ := metacodec.LocalRegionMetaFromDescriptorProto(pbMeta)
	return meta
}
