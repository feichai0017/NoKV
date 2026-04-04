package store

import (
	"bytes"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/raftstore/peer"
	proto "google.golang.org/protobuf/proto"
)

type splitTransition struct {
	originalParent localmeta.RegionMeta
	parent         localmeta.RegionMeta
	child          localmeta.RegionMeta
	parentDesc     descriptor.Descriptor
	childDesc      descriptor.Descriptor
}

func (s *Store) buildSplitTransition(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) (splitTransition, error) {
	if s == nil {
		return splitTransition{}, fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 || childMeta.ID == 0 {
		return splitTransition{}, fmt.Errorf("raftstore: invalid region identifiers")
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return splitTransition{}, fmt.Errorf("raftstore: parent region %d not found", parentID)
	}
	if len(parentMeta.EndKey) > 0 && bytes.Compare(splitKey, parentMeta.EndKey) >= 0 {
		return splitTransition{}, fmt.Errorf("raftstore: split key >= parent end key")
	}
	if bytes.Compare(splitKey, parentMeta.StartKey) <= 0 {
		return splitTransition{}, fmt.Errorf("raftstore: split key must be greater than parent start key")
	}
	originalParent := localmeta.CloneRegionMeta(parentMeta)
	newParent := parentMeta
	newParent.EndKey = append([]byte(nil), splitKey...)
	newParent.Epoch.Version++
	childMeta = localmeta.CloneRegionMeta(childMeta)
	if len(childMeta.StartKey) == 0 {
		childMeta.StartKey = append([]byte(nil), splitKey...)
	}
	if childMeta.State == 0 {
		childMeta.State = metaregion.ReplicaStateRunning
	}
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
	return splitTransition{
		originalParent: originalParent,
		parent:         newParent,
		child:          childMeta,
		parentDesc:     parentDesc,
		childDesc:      childDesc,
	}, nil
}

func plannedSplitEvent(transition splitTransition) rootevent.Event {
	return rootevent.RegionSplitPlanned(
		transition.originalParent.ID,
		transition.child.StartKey,
		transition.parentDesc,
		transition.childDesc,
	)
}

func committedSplitEvent(transition splitTransition) rootevent.Event {
	return rootevent.RegionSplitCommitted(
		transition.originalParent.ID,
		transition.child.StartKey,
		transition.parentDesc,
		transition.childDesc,
	)
}

type mergeTransition struct {
	target     localmeta.RegionMeta
	source     localmeta.RegionMeta
	mergedDesc descriptor.Descriptor
	leftID     uint64
	rightID    uint64
}

func (s *Store) buildMergeTransition(targetRegionID, sourceRegionID uint64) (mergeTransition, error) {
	if s == nil {
		return mergeTransition{}, fmt.Errorf("raftstore: store is nil")
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return mergeTransition{}, fmt.Errorf("raftstore: invalid region identifiers")
	}
	parentMeta, ok := s.RegionMetaByID(targetRegionID)
	if !ok {
		return mergeTransition{}, fmt.Errorf("raftstore: target region %d not found", targetRegionID)
	}
	sourceMeta, ok := s.RegionMetaByID(sourceRegionID)
	if !ok {
		return mergeTransition{}, fmt.Errorf("raftstore: source region %d not found", sourceRegionID)
	}
	updated := parentMeta
	updated.Epoch.Version++
	if len(sourceMeta.EndKey) == 0 || bytes.Compare(sourceMeta.EndKey, updated.EndKey) > 0 {
		updated.EndKey = append([]byte(nil), sourceMeta.EndKey...)
	}
	mergedDesc := metacodec.DescriptorFromLocalRegionMeta(updated, 0)
	mergedDesc.Lineage = append(mergedDesc.Lineage, descriptor.LineageRef{
		RegionID: sourceMeta.ID,
		Epoch:    sourceMeta.Epoch,
		Kind:     descriptor.LineageKindMergeSource,
	})
	leftID, rightID := mergedDesc.RegionID, sourceMeta.ID
	if bytes.Compare(sourceMeta.StartKey, mergedDesc.StartKey) < 0 {
		leftID, rightID = sourceMeta.ID, mergedDesc.RegionID
	}
	return mergeTransition{
		target:     updated,
		source:     sourceMeta,
		mergedDesc: mergedDesc,
		leftID:     leftID,
		rightID:    rightID,
	}, nil
}

func plannedMergeEvent(transition mergeTransition) rootevent.Event {
	return rootevent.RegionMergePlanned(transition.leftID, transition.rightID, transition.mergedDesc)
}

func committedMergeEvent(transition mergeTransition) rootevent.Event {
	return rootevent.RegionMerged(transition.leftID, transition.rightID, transition.mergedDesc)
}

func (s *Store) planSplit(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) (transitionPlan, error) {
	if s == nil {
		return transitionPlan{}, fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 || childMeta.ID == 0 {
		return transitionPlan{}, fmt.Errorf("raftstore: invalid region identifiers")
	}
	if splitAlreadyAppliedLocal(s, parentID, childMeta, splitKey) {
		return transitionPlan{RegionID: parentID, Noop: true}, nil
	}
	transition, err := s.buildSplitTransition(parentID, childMeta, splitKey)
	if err != nil {
		return transitionPlan{}, err
	}
	command := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_SPLIT,
		Split: &raftcmdpb.SplitCommand{
			ParentRegionId: parentID,
			SplitKey:       append([]byte(nil), splitKey...),
			Child:          metacodec.LocalRegionMetaToDescriptorProto(transition.child),
		},
	}
	data, err := proto.Marshal(command)
	if err != nil {
		return transitionPlan{}, err
	}
	return transitionPlan{
		RegionID:     parentID,
		Event:        plannedSplitEvent(transition),
		Action:       "split",
		AdminPayload: data,
	}, nil
}

func (s *Store) planMerge(targetRegionID, sourceRegionID uint64) (transitionPlan, error) {
	if s == nil {
		return transitionPlan{}, fmt.Errorf("raftstore: store is nil")
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return transitionPlan{}, fmt.Errorf("raftstore: invalid region identifiers")
	}
	transition, err := s.buildMergeTransition(targetRegionID, sourceRegionID)
	if err != nil {
		return transitionPlan{}, err
	}
	command := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_MERGE,
		Merge: &raftcmdpb.MergeCommand{
			TargetRegionId: targetRegionID,
			SourceRegionId: sourceRegionID,
		},
	}
	data, err := proto.Marshal(command)
	if err != nil {
		return transitionPlan{}, err
	}
	return transitionPlan{
		RegionID:     targetRegionID,
		Event:        plannedMergeEvent(transition),
		Action:       "merge",
		AdminPayload: data,
	}, nil
}

func splitAlreadyAppliedLocal(s *Store, parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) bool {
	if s == nil || parentID == 0 || childMeta.ID == 0 {
		return false
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return false
	}
	if !bytes.Equal(parentMeta.EndKey, splitKey) {
		return false
	}
	currentChild, ok := s.RegionMetaByID(childMeta.ID)
	if !ok {
		return false
	}
	nextChild := localmeta.CloneRegionMeta(childMeta)
	if len(nextChild.StartKey) == 0 {
		nextChild.StartKey = append([]byte(nil), splitKey...)
	}
	if nextChild.State == 0 {
		nextChild.State = metaregion.ReplicaStateRunning
	}
	got := metacodec.DescriptorFromLocalRegionMeta(currentChild, 0)
	want := metacodec.DescriptorFromLocalRegionMeta(nextChild, 0)
	return got.Equal(want)
}

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
	transition, err := s.buildSplitTransition(parentID, childMeta, childMeta.StartKey)
	if err != nil {
		return nil, err
	}
	newParent := transition.parent
	childMeta = transition.child
	var childPeer *peer.Peer
	if err := s.applyTerminalTransition(terminalTransition{
		Event:  committedSplitEvent(transition),
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
	plan, err := s.planSplit(parentID, childMeta, splitKey)
	if err != nil {
		return err
	}
	return s.executeTransitionPlan(plan)
}

// ProposeMerge submits a merge admin command merging source region into target.
func (s *Store) ProposeMerge(targetRegionID, sourceRegionID uint64) error {
	plan, err := s.planMerge(targetRegionID, sourceRegionID)
	if err != nil {
		return err
	}
	return s.executeTransitionPlan(plan)
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
	transition, err := s.buildMergeTransition(merge.GetTargetRegionId(), merge.GetSourceRegionId())
	if err != nil {
		// Merge apply must be replay-safe across restart. Once the source region
		// has already been removed locally, replaying the committed merge is a
		// no-op instead of a fatal state divergence.
		if _, ok := s.RegionMetaByID(merge.GetSourceRegionId()); !ok {
			return nil
		}
		return err
	}
	sourceMeta := transition.source
	if sourceMeta.State == metaregion.ReplicaStateTombstone {
		return nil
	}
	updated := transition.target
	return s.applyTerminalTransition(terminalTransition{
		Event:  committedMergeEvent(transition),
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

func pbRegionMetaToManifest(pbMeta *metapb.RegionDescriptor) localmeta.RegionMeta {
	if pbMeta == nil {
		return localmeta.RegionMeta{}
	}
	meta, _ := metacodec.LocalRegionMetaFromDescriptorProto(pbMeta)
	return meta
}
