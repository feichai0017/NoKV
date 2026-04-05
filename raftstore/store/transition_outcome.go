package store

import (
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

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

func committedSplitEvent(plan splitPlan) rootevent.Event {
	return rootevent.RegionSplitCommitted(
		plan.originalParent.ID,
		plan.child.StartKey,
		plan.parentDesc,
		plan.childDesc,
	)
}

func committedMergeEvent(plan mergePlan) rootevent.Event {
	return rootevent.RegionMerged(plan.leftID, plan.rightID, plan.mergedDesc)
}
