// Package scheduling owns Coordinator-side cluster scheduling policy.
//
// The package consumes disposable Coordinator views and emits store-control
// operations. It must not import coordinator/server or raftstore packages:
// servers own RPC admission, stores own execution, and this package owns only
// policy selection.
package scheduling

import (
	"github.com/feichai0017/NoKV/coordinator/catalog"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/meta/topology"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// PlanStoreOperations builds lightweight control-plane operations for the
// heartbeat source store. The current policy only balances raft leaders; richer
// replica repair, drain, placement, and hotspot policies should plug in here
// instead of growing coordinator/server.
func PlanStoreOperations(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot) []*coordpb.SchedulerOperation {
	if heartbeatStoreID == 0 || len(snapshot.Stores) < 2 {
		return nil
	}
	src, dst, ok := selectLeaderRebalancePair(snapshot.Stores, heartbeatStoreID)
	if !ok {
		return nil
	}
	op, ok := chooseLeaderTransferOperation(snapshot.Regions, src.StoreID, dst.StoreID)
	if !ok {
		return nil
	}
	return []*coordpb.SchedulerOperation{op}
}

func selectLeaderRebalancePair(stores []catalog.StoreStats, heartbeatStoreID uint64) (catalog.StoreStats, catalog.StoreStats, bool) {
	if len(stores) < 2 {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
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
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	if heartbeatStoreID != src.StoreID {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	// Require at least two leaders of skew before scheduling. One-leader skew
	// is common during startup and would otherwise produce noisy transfers.
	if src.LeaderNum <= dst.LeaderNum+1 {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	return src, dst, true
}

func chooseLeaderTransferOperation(regions []catalog.RegionInfo, srcStoreID, dstStoreID uint64) (*coordpb.SchedulerOperation, bool) {
	if srcStoreID == 0 || dstStoreID == 0 {
		return nil, false
	}
	// Prefer regions whose first peer belongs to the source store; this aligns
	// with common bootstrap ordering and increases chance of immediate success.
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Descriptor, srcStoreID, dstStoreID, true); ok {
			return op, true
		}
	}
	// Fallback to any region that has peers on both stores.
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Descriptor, srcStoreID, dstStoreID, false); ok {
			return op, true
		}
	}
	return nil, false
}

func buildLeaderTransfer(desc topology.Descriptor, srcStoreID, dstStoreID uint64, requireFirstPeerSrc bool) (*coordpb.SchedulerOperation, bool) {
	if desc.RegionID == 0 || len(desc.Peers) == 0 {
		return nil, false
	}
	if requireFirstPeerSrc && desc.Peers[0].StoreID != srcStoreID {
		return nil, false
	}
	srcPeerID, ok := peerIDOnStore(desc.Peers, srcStoreID)
	if !ok {
		return nil, false
	}
	dstPeerID, ok := peerIDOnStore(desc.Peers, dstStoreID)
	if !ok {
		return nil, false
	}
	if srcPeerID == dstPeerID {
		return nil, false
	}
	return &coordpb.SchedulerOperation{
		Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		RegionId:     desc.RegionID,
		SourcePeerId: srcPeerID,
		TargetPeerId: dstPeerID,
	}, true
}

func peerIDOnStore(peers []metaregion.Peer, storeID uint64) (uint64, bool) {
	for _, p := range peers {
		if p.StoreID == storeID && p.PeerID != 0 {
			return p.PeerID, true
		}
	}
	return 0, false
}
