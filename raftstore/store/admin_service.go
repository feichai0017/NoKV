package store

import (
	"bytes"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	"github.com/feichai0017/NoKV/raftstore/peer"
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
	plan, err := s.buildSplitPlan(parentID, childMeta, childMeta.StartKey)
	if err != nil {
		return nil, err
	}
	newParent := plan.parent
	childMeta = plan.child
	var childPeer *peer.Peer
	if err := s.applyTerminalOutcome(terminalOutcome{
		Event:  committedSplitEvent(plan),
		Action: "split",
		Apply: func() error {
			if err := s.applyRegionMetaSilent(newParent); err != nil {
				return err
			}
			cfg, bootstrapPeers, err := s.buildChildPeerConfig(childMeta)
			if err != nil {
				_ = s.applyRegionMetaSilent(originalParent)
				return err
			}
			childPeer, err = s.startPeer(cfg, bootstrapPeers, false)
			if err != nil {
				_ = s.applyRegionMetaSilent(originalParent)
				return err
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return childPeer, nil
}

// ProposeSplit issues a split command through the raft log of the parent
// region. The child metadata must describe the new region configuration.
func (s *Store) ProposeSplit(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) error {
	target, err := s.buildSplitTarget(parentID, childMeta, splitKey)
	if err != nil {
		return err
	}
	return s.executeTransitionTarget(target)
}

// ProposeMerge submits a merge admin command merging source region into target.
func (s *Store) ProposeMerge(targetRegionID, sourceRegionID uint64) error {
	target, err := s.buildMergeTarget(targetRegionID, sourceRegionID)
	if err != nil {
		return err
	}
	return s.executeTransitionTarget(target)
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
	childMeta := regionMetaFromDescriptorProto(split.GetChild())
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
	plan, err := s.buildMergePlan(merge.GetTargetRegionId(), merge.GetSourceRegionId())
	if err != nil {
		// Merge apply must be replay-safe across restart. Once the source region
		// has already been removed locally, replaying the committed merge is a
		// no-op instead of a fatal state divergence.
		if _, ok := s.RegionMetaByID(merge.GetSourceRegionId()); !ok {
			return nil
		}
		return err
	}
	sourceMeta := plan.source
	if sourceMeta.State == metaregion.ReplicaStateTombstone {
		return nil
	}
	updated := plan.target
	return s.applyTerminalOutcome(terminalOutcome{
		Event:  committedMergeEvent(plan),
		Action: "merge",
		Apply: func() error {
			if err := s.applyRegionMetaSilent(updated); err != nil {
				return err
			}
			if peer := s.regionMgr().peer(sourceMeta.ID); peer != nil {
				s.stopPeer(peer.ID(), false)
			}
			return s.applyRegionRemovalSilent(sourceMeta.ID)
		},
	})
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

func regionMetaFromDescriptorProto(pbMeta *metapb.RegionDescriptor) localmeta.RegionMeta {
	if pbMeta == nil {
		return localmeta.RegionMeta{}
	}
	meta, _ := metacodec.LocalRegionMetaFromDescriptorProto(pbMeta)
	return meta
}
