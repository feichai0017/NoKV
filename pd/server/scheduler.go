package server

import (
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
)

// planStoreOperations builds lightweight scheduling hints for the heartbeat
// source store. This is a minimal heuristic to bootstrap PD-to-store
// downlinks; it can be replaced by a richer scheduler in follow-up work.
func (s *Service) planStoreOperations(heartbeatStoreID uint64) []*pb.SchedulerOperation {
	if s == nil || s.cluster == nil || heartbeatStoreID == 0 {
		return nil
	}
	stores := s.cluster.StoreSnapshot()
	if len(stores) < 2 {
		return nil
	}
	src, dst, ok := selectLeaderRebalancePair(stores, heartbeatStoreID)
	if !ok {
		return nil
	}
	regions := s.cluster.RegionSnapshot()
	op, ok := chooseLeaderTransferOperation(regions, src.StoreID, dst.StoreID)
	if !ok {
		return nil
	}
	return []*pb.SchedulerOperation{op}
}

func selectLeaderRebalancePair(stores []core.StoreStats, heartbeatStoreID uint64) (core.StoreStats, core.StoreStats, bool) {
	if len(stores) < 2 {
		return core.StoreStats{}, core.StoreStats{}, false
	}
	src := stores[0]
	dst := stores[0]
	for _, st := range stores[1:] {
		if st.LeaderNum > src.LeaderNum || (st.LeaderNum == src.LeaderNum && st.StoreID < src.StoreID) {
			src = st
		}
		if st.LeaderNum < dst.LeaderNum || (st.LeaderNum == dst.LeaderNum && st.StoreID < dst.StoreID) {
			dst = st
		}
	}
	if src.StoreID == 0 || dst.StoreID == 0 || src.StoreID == dst.StoreID {
		return core.StoreStats{}, core.StoreStats{}, false
	}
	if heartbeatStoreID != src.StoreID {
		return core.StoreStats{}, core.StoreStats{}, false
	}
	// Require at least two leaders skew before scheduling a transfer.
	if src.LeaderNum <= dst.LeaderNum+1 {
		return core.StoreStats{}, core.StoreStats{}, false
	}
	return src, dst, true
}

func chooseLeaderTransferOperation(regions []core.RegionInfo, srcStoreID, dstStoreID uint64) (*pb.SchedulerOperation, bool) {
	if srcStoreID == 0 || dstStoreID == 0 {
		return nil, false
	}
	// Prefer regions whose first peer belongs to the source store; this aligns
	// with common bootstrap ordering and increases chance of immediate success.
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Meta, srcStoreID, dstStoreID, true); ok {
			return op, true
		}
	}
	// Fallback to any region that has peers on both stores.
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Meta, srcStoreID, dstStoreID, false); ok {
			return op, true
		}
	}
	return nil, false
}

func buildLeaderTransfer(meta manifest.RegionMeta, srcStoreID, dstStoreID uint64, requireFirstPeerSrc bool) (*pb.SchedulerOperation, bool) {
	if meta.ID == 0 || len(meta.Peers) == 0 {
		return nil, false
	}
	if requireFirstPeerSrc && meta.Peers[0].StoreID != srcStoreID {
		return nil, false
	}
	srcPeerID, ok := peerIDOnStore(meta.Peers, srcStoreID)
	if !ok {
		return nil, false
	}
	dstPeerID, ok := peerIDOnStore(meta.Peers, dstStoreID)
	if !ok {
		return nil, false
	}
	if srcPeerID == dstPeerID {
		return nil, false
	}
	return &pb.SchedulerOperation{
		Type:         pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		RegionId:     meta.ID,
		SourcePeerId: srcPeerID,
		TargetPeerId: dstPeerID,
	}, true
}

func peerIDOnStore(peers []manifest.PeerMeta, storeID uint64) (uint64, bool) {
	for _, p := range peers {
		if p.StoreID == storeID && p.PeerID != 0 {
			return p.PeerID, true
		}
	}
	return 0, false
}
