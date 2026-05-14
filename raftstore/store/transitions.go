// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	"github.com/feichai0017/NoKV/meta/topology"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

type splitPlan struct {
	originalParent localmeta.RegionMeta
	parent         localmeta.RegionMeta
	child          localmeta.RegionMeta
	parentDesc     topology.Descriptor
	childDesc      topology.Descriptor
}

type mergePlan struct {
	target     localmeta.RegionMeta
	source     localmeta.RegionMeta
	mergedDesc topology.Descriptor
	leftID     uint64
	rightID    uint64
}

func (s *Store) buildPeerChangeTarget(regionID uint64, cc raftpb.ConfChangeV2) (transitionTarget, error) {
	if s == nil {
		return transitionTarget{}, errNilStore
	}
	if regionID == 0 || len(cc.Changes) != 1 {
		return transitionTarget{}, errInvalidPeerChangeTarget
	}
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		return transitionTarget{}, errRegionMetadataNotFound(regionID)
	}
	next := localmeta.CloneRegionMeta(meta)
	changed, err := applyConfChangeToMeta(&next, cc)
	if err != nil || !changed {
		if err != nil {
			return transitionTarget{}, err
		}
		return transitionTarget{RegionID: regionID, Noop: true}, nil
	}
	next.Epoch.ConfVersion += uint64(len(cc.Changes))
	event, err := plannedPeerChangeEvent(next, cc)
	if err != nil {
		return transitionTarget{}, err
	}
	return transitionTarget{
		TransitionID: rootstate.TransitionIDFromEvent(event),
		RegionID:     regionID,
		Event:        event,
		Action:       "peer change",
		ConfChange:   &cc,
	}, nil
}

func (s *Store) buildSplitPlan(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) (splitPlan, error) {
	if s == nil {
		return splitPlan{}, errNilStore
	}
	if parentID == 0 || childMeta.ID == 0 {
		return splitPlan{}, errInvalidRegionIdentifiers
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return splitPlan{}, errParentRegionNotFound(parentID)
	}
	if len(parentMeta.EndKey) > 0 && bytes.Compare(splitKey, parentMeta.EndKey) >= 0 {
		return splitPlan{}, fmt.Errorf("raftstore: split key >= parent end key")
	}
	if bytes.Compare(splitKey, parentMeta.StartKey) <= 0 {
		return splitPlan{}, fmt.Errorf("raftstore: split key must be greater than parent start key")
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
	parentDesc := localmeta.Descriptor(newParent, 0)
	childDesc := localmeta.Descriptor(childMeta, 0)
	parentDesc.Lineage = append(parentDesc.Lineage, topology.LineageRef{
		RegionID: originalParent.ID,
		Epoch:    originalParent.Epoch,
		Kind:     topology.LineageKindSplitParent,
	})
	childDesc.Lineage = append(childDesc.Lineage, topology.LineageRef{
		RegionID: originalParent.ID,
		Epoch:    originalParent.Epoch,
		Kind:     topology.LineageKindSplitParent,
	})
	return splitPlan{
		originalParent: originalParent,
		parent:         newParent,
		child:          childMeta,
		parentDesc:     parentDesc,
		childDesc:      childDesc,
	}, nil
}

func (s *Store) buildMergePlan(targetRegionID, sourceRegionID uint64) (mergePlan, error) {
	if s == nil {
		return mergePlan{}, errNilStore
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return mergePlan{}, errInvalidRegionIdentifiers
	}
	parentMeta, ok := s.RegionMetaByID(targetRegionID)
	if !ok {
		return mergePlan{}, errTargetRegionNotFound(targetRegionID)
	}
	sourceMeta, ok := s.RegionMetaByID(sourceRegionID)
	if !ok {
		return mergePlan{}, errSourceRegionNotFound(sourceRegionID)
	}
	updated := parentMeta
	updated.Epoch.Version++
	if len(sourceMeta.EndKey) == 0 || bytes.Compare(sourceMeta.EndKey, updated.EndKey) > 0 {
		updated.EndKey = append([]byte(nil), sourceMeta.EndKey...)
	}
	mergedDesc := localmeta.Descriptor(updated, 0)
	mergedDesc.Lineage = append(mergedDesc.Lineage, topology.LineageRef{
		RegionID: sourceMeta.ID,
		Epoch:    sourceMeta.Epoch,
		Kind:     topology.LineageKindMergeSource,
	})
	leftID, rightID := mergedDesc.RegionID, sourceMeta.ID
	if bytes.Compare(sourceMeta.StartKey, mergedDesc.StartKey) < 0 {
		leftID, rightID = sourceMeta.ID, mergedDesc.RegionID
	}
	return mergePlan{
		target:     updated,
		source:     sourceMeta,
		mergedDesc: mergedDesc,
		leftID:     leftID,
		rightID:    rightID,
	}, nil
}

func (s *Store) buildSplitTarget(parentID uint64, childMeta localmeta.RegionMeta, splitKey []byte) (transitionTarget, error) {
	if s == nil {
		return transitionTarget{}, errNilStore
	}
	if parentID == 0 || childMeta.ID == 0 {
		return transitionTarget{}, errInvalidRegionIdentifiers
	}
	if splitAlreadyAppliedLocal(s, parentID, childMeta, splitKey) {
		return transitionTarget{RegionID: parentID, Noop: true}, nil
	}
	plan, err := s.buildSplitPlan(parentID, childMeta, splitKey)
	if err != nil {
		return transitionTarget{}, err
	}
	command := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_SPLIT,
		Split: &raftcmdpb.SplitCommand{
			ParentRegionId: parentID,
			SplitKey:       append([]byte(nil), splitKey...),
			Child:          localmeta.DescriptorToProto(plan.child),
		},
	}
	event := splitEvent(rootevent.KindRegionSplitPlanned, plan)
	return transitionTarget{
		TransitionID: rootstate.TransitionIDFromEvent(event),
		RegionID:     parentID,
		Event:        event,
		Action:       "split",
		Admin:        command,
	}, nil
}

func (s *Store) buildMergeTarget(targetRegionID, sourceRegionID uint64) (transitionTarget, error) {
	if s == nil {
		return transitionTarget{}, errNilStore
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return transitionTarget{}, errInvalidRegionIdentifiers
	}
	plan, err := s.buildMergePlan(targetRegionID, sourceRegionID)
	if err != nil {
		return transitionTarget{}, err
	}
	command := &raftcmdpb.AdminCommand{
		Type: raftcmdpb.AdminCommand_MERGE,
		Merge: &raftcmdpb.MergeCommand{
			TargetRegionId: targetRegionID,
			SourceRegionId: sourceRegionID,
		},
	}
	event := mergeEvent(rootevent.KindRegionMergePlanned, plan)
	return transitionTarget{
		TransitionID: rootstate.TransitionIDFromEvent(event),
		RegionID:     targetRegionID,
		Event:        event,
		Action:       "merge",
		Admin:        command,
	}, nil
}

func applyConfChangeToMeta(meta *localmeta.RegionMeta, cc raftpb.ConfChangeV2) (bool, error) {
	if meta == nil {
		return false, errRegionMetaNil
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
			return nil, errInvalidConfChangeContext
		}
		ctx = ctx[n:]
		peerID, m := binary.Uvarint(ctx)
		if m <= 0 {
			return nil, errInvalidConfChangeContext
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

func plannedPeerChangeEvent(meta localmeta.RegionMeta, cc raftpb.ConfChangeV2) (rootevent.Event, error) {
	if meta.ID == 0 || len(cc.Changes) != 1 {
		return rootevent.Event{}, fmt.Errorf("raftstore: invalid peer change event")
	}
	desc := localmeta.Descriptor(meta, 0)
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
	got := localmeta.Descriptor(currentChild, 0)
	want := localmeta.Descriptor(nextChild, 0)
	return got.Equal(want)
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

type transitionTarget struct {
	TransitionID string
	RegionID     uint64
	Event        rootevent.Event
	Action       string
	Noop         bool
	ConfChange   *raftpb.ConfChangeV2
	Admin        *raftcmdpb.AdminCommand
}

type terminalOutcome struct {
	TransitionID string
	RegionID     uint64
	Event        rootevent.Event
	Action       string
	Noop         bool
	Apply        func() error
}

func (s *Store) leaderPeer(regionID uint64) (*peer.Peer, error) {
	if s == nil {
		return nil, errNilStore
	}
	if regionID == 0 {
		return nil, errZeroRegionID
	}
	peerRef := s.regions.Peer(regionID)
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
	ctx, cancel := s.schedulerPublishContext()
	defer cancel()
	if err := s.schedulerClient().PublishRootEvent(ctx, event); err != nil {
		if action == "" {
			action = "transition"
		}
		return fmt.Errorf("raftstore: publish %s target: %w", action, err)
	}
	return nil
}

func (s *Store) proposeTransition(target transitionTarget) error {
	if s == nil {
		return errNilStore
	}
	if target.RegionID == 0 {
		return errTransitionRegionIDZero
	}
	if target.ConfChange == nil && target.Admin == nil {
		return errTransitionProposalEmpty
	}
	peerRef, err := s.leaderPeer(target.RegionID)
	if err != nil {
		return err
	}
	switch {
	case target.ConfChange != nil:
		return peerRef.ProposeConfChange(*target.ConfChange)
	case target.Admin != nil:
		payload, err := proto.Marshal(target.Admin)
		if err != nil {
			return fmt.Errorf("raftstore: marshal transition admin proposal: %w", err)
		}
		return peerRef.ProposeAdmin(payload)
	default:
		return errTransitionProposalEmpty
	}
}

func (s *Store) executeTransitionTarget(target transitionTarget) error {
	if s == nil {
		return errNilStore
	}
	if target.Noop {
		return nil
	}
	if target.RegionID == 0 {
		s.recordAdmission(Admission{Class: AdmissionClassTopology, Reason: AdmissionReasonInvalid, Detail: "transition region id is zero"})
		s.recordTopologyRejected(target, errTransitionRegionIDZero)
		return errTransitionRegionIDZero
	}
	if target.ConfChange == nil && target.Admin == nil {
		s.recordAdmission(Admission{Class: AdmissionClassTopology, Reason: AdmissionReasonInvalid, RegionID: target.RegionID, Detail: "transition proposal is empty"})
		s.recordTopologyRejected(target, errTransitionProposalEmpty)
		return errTransitionProposalEmpty
	}
	s.recordAdmission(Admission{
		Class:    AdmissionClassTopology,
		Reason:   AdmissionReasonAccepted,
		Accepted: true,
		RegionID: target.RegionID,
		Detail:   target.Action,
	})
	s.recordTopologyQueued(target)
	if err := s.publishPlannedRootEvent(target.RegionID, target.Event, target.Action); err != nil {
		s.recordTopologyFailed(target.TransitionID, target.RegionID, target.Action, PublishStateUnknown, err)
		return err
	}
	if err := s.proposeTransition(target); err != nil {
		s.recordTopologyFailed(target.TransitionID, target.RegionID, target.Action, PublishStatePlannedPublished, err)
		return err
	}
	s.recordTopologyProposed(target)
	return nil
}

func (s *Store) enqueueAppliedRootEvent(event rootevent.Event) {
	if s == nil || s.sched == nil || event.Kind == rootevent.KindUnknown {
		return
	}
	s.enqueueRegionEvent(regionEvent{
		root:         event,
		transitionID: rootstate.TransitionIDFromEvent(event),
	})
}

func (s *Store) applyTerminalOutcome(term terminalOutcome) error {
	if s == nil {
		return errNilStore
	}
	if term.Noop {
		return nil
	}
	if term.Apply != nil {
		if err := term.Apply(); err != nil {
			s.recordTopologyFailed(term.TransitionID, term.RegionID, term.Action, PublishStateUnknown, err)
			return err
		}
	}
	if term.TransitionID == "" && term.Event.Kind != rootevent.KindUnknown {
		term.TransitionID = rootstate.TransitionIDFromEvent(term.Event)
	}
	s.recordTopologyApplied(term)
	s.enqueueAppliedRootEvent(term.Event)
	return nil
}

func appliedPeerChangeEvent(meta localmeta.RegionMeta, cc raftpb.ConfChangeV2) (rootevent.Event, bool) {
	if meta.ID == 0 || len(cc.Changes) != 1 {
		return rootevent.Event{}, false
	}
	desc := localmeta.Descriptor(meta, 0)
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

func splitEvent(kind rootevent.Kind, plan splitPlan) rootevent.Event {
	switch kind {
	case rootevent.KindRegionSplitPlanned:
		return rootevent.RegionSplitPlanned(
			plan.originalParent.ID,
			plan.child.StartKey,
			plan.parentDesc,
			plan.childDesc,
		)
	case rootevent.KindRegionSplitCommitted:
		return rootevent.RegionSplitCommitted(
			plan.originalParent.ID,
			plan.child.StartKey,
			plan.parentDesc,
			plan.childDesc,
		)
	default:
		return rootevent.Event{}
	}
}

func mergeEvent(kind rootevent.Kind, plan mergePlan) rootevent.Event {
	switch kind {
	case rootevent.KindRegionMergePlanned:
		return rootevent.RegionMergePlanned(plan.leftID, plan.rightID, plan.mergedDesc)
	case rootevent.KindRegionMerged:
		return rootevent.RegionMerged(plan.leftID, plan.rightID, plan.mergedDesc)
	default:
		return rootevent.Event{}
	}
}
