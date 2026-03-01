package scheduler

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

// Planner consumes a scheduler Snapshot and produces scheduling operations.
//
// In NoKV cluster mode, scheduling decisions are expected to come from PD.
// Local Planner implementations are mainly for standalone/dev/test experiments.
type Planner interface {
	Plan(snapshot Snapshot) []Operation
}

// Snapshot aggregates scheduler-visible state for planning. In standalone mode
// it is typically built from Coordinator snapshots.
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

// Operation represents a scheduling decision to be executed by store runtime.
type Operation struct {
	Type   OperationType
	Region uint64
	Source uint64
	Target uint64
}

type OperationType uint8

const (
	OperationNone OperationType = iota
	OperationLeaderTransfer
)

// NoopPlanner disables local scheduling and always returns no operations.
// This is the default when no planner capability is provided by the sink.
type NoopPlanner struct{}

func (NoopPlanner) Plan(Snapshot) []Operation { return nil }

// LeaderBalancePlanner suggests leader transfers away from stores whose leader
// count exceeds MaxLeaders.
//
// This heuristic is intentionally simple and intended for local experiments and
// tests. It is not a replacement for PD's centralized scheduling policy.
type LeaderBalancePlanner struct {
	MaxLeaders     uint64
	StaleThreshold time.Duration
}

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
