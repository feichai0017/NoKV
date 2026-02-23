package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaderBalancePlannerCreatesOperations(t *testing.T) {
	planner := LeaderBalancePlanner{MaxLeaders: 1}
	snap := Snapshot{
		Regions: []RegionDescriptor{
			{
				ID: 1,
				Peers: []PeerDescriptor{
					{StoreID: 1, PeerID: 101, Leader: true},
					{StoreID: 2, PeerID: 202},
				},
			},
			{
				ID: 2,
				Peers: []PeerDescriptor{
					{StoreID: 1, PeerID: 103, Leader: true},
					{StoreID: 3, PeerID: 303},
				},
			},
		},
	}
	ops := planner.Plan(snap)
	require.NotEmpty(t, ops)
	var found bool
	for _, op := range ops {
		if op.Region == 1 {
			found = true
			require.Equal(t, OperationLeaderTransfer, op.Type)
			require.Equal(t, uint64(101), op.Source)
			require.Equal(t, uint64(202), op.Target)
			break
		}
	}
	require.True(t, found, "expected leader transfer for region 1")
}

func TestLeaderBalancePlannerNoOpsWhenBalanced(t *testing.T) {
	planner := LeaderBalancePlanner{MaxLeaders: 2}
	snap := Snapshot{
		Regions: []RegionDescriptor{
			{ID: 1, Peers: []PeerDescriptor{{StoreID: 1, PeerID: 101, Leader: true}}},
		},
	}
	require.Empty(t, planner.Plan(snap))
}

func TestLeaderBalancePlannerSkipsStaleRegions(t *testing.T) {
	planner := LeaderBalancePlanner{MaxLeaders: 1, StaleThreshold: 500 * time.Millisecond}
	snap := Snapshot{
		Regions: []RegionDescriptor{
			{
				ID:  1,
				Lag: time.Second,
				Peers: []PeerDescriptor{
					{StoreID: 1, PeerID: 101, Leader: true},
					{StoreID: 2, PeerID: 202},
				},
			},
		},
	}
	require.Empty(t, planner.Plan(snap))
}
