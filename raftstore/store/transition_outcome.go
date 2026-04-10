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
