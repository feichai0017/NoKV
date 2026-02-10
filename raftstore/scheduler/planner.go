package scheduler

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

// Planner defines the interface for schedulers that consume cluster snapshots
// and produce scheduling operations (e.g. leader transfer, rebalance).
type Planner interface {
	Plan(snapshot Snapshot) []Operation
}

// Snapshot aggregates cluster state for planning. It captures regions, stores,
// and can be extended with additional metadata.
type Snapshot struct {
	Regions []RegionDescriptor
	Stores  []StoreStats
}

// RegionDescriptor is a lightweight view of region metadata required for
// scheduling decisions.
type RegionDescriptor struct {
	ID            uint64
	StartKey      []byte
	EndKey        []byte
	Peers         []PeerDescriptor
	Epoch         manifest.RegionEpoch
	LastHeartbeat time.Time
	Lag           time.Duration
}

// PeerDescriptor describes a raft peer in a region.
type PeerDescriptor struct {
	StoreID uint64
	PeerID  uint64
	Leader  bool
}

// Operation represents a scheduling decision.
type Operation struct {
	Type   OperationType
	Region uint64
	Source uint64
	Target uint64
}

// OperationType defines an exported API type.
type OperationType uint8

const (
	OperationNone OperationType = iota
	OperationLeaderTransfer
)

// NoopPlanner returns no scheduling operations.
type NoopPlanner struct{}

// Plan is part of the exported receiver API.
func (NoopPlanner) Plan(Snapshot) []Operation { return nil }

// LeaderBalancePlanner suggests leader transfers away from stores whose leader
// count exceeds MaxLeaders. This is a naive heuristic for demonstration.
type LeaderBalancePlanner struct {
	MaxLeaders     uint64
	StaleThreshold time.Duration
}

// Plan is part of the exported receiver API.
func (p LeaderBalancePlanner) Plan(snapshot Snapshot) []Operation {
	if p.MaxLeaders == 0 {
		return nil
	}
	leaderCount := make(map[uint64]uint64)
	regionLeader := make(map[uint64]PeerDescriptor)
	for _, region := range snapshot.Regions {
		if p.StaleThreshold > 0 && (region.LastHeartbeat.IsZero() || region.Lag > p.StaleThreshold) {
			continue
		}
		for _, peer := range region.Peers {
			if peer.Leader {
				leaderCount[peer.StoreID]++
				regionLeader[region.ID] = peer
			}
		}
	}
	var ops []Operation
	for storeID, count := range leaderCount {
		if count <= p.MaxLeaders {
			continue
		}
		for regionID, leader := range regionLeader {
			if leader.StoreID != storeID {
				continue
			}
			var target PeerDescriptor
			for _, region := range snapshot.Regions {
				if region.ID != regionID {
					continue
				}
				for _, peer := range region.Peers {
					if peer.StoreID != leader.StoreID {
						target = peer
						break
					}
				}
			}
			if target.StoreID == 0 {
				continue
			}
			op := Operation{
				Type:   OperationLeaderTransfer,
				Region: regionID,
				Source: leader.PeerID,
				Target: target.PeerID,
			}
			ops = append(ops, op)
		}
	}
	return ops
}
