package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	roottestcluster "github.com/feichai0017/NoKV/meta/root/testcluster"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

func TestMetaRootNodeIsolationElectsNewLeaderAndHeals(t *testing.T) {
	cluster := roottestcluster.Open(t)
	leaderID := cluster.WaitLeader()

	initialCommit, err := cluster.Stores[leaderID].Append(context.Background(),
		rootevent.StoreJoined(1),
	)
	require.NoError(t, err)
	cluster.RefreshAll()

	followers := cluster.FollowerIDs(leaderID)
	candidateID := followers[0]
	voterID := followers[1]
	cluster.PauseTicks(voterID)
	defer cluster.ResumeTicks(voterID)

	cluster.IsolateNodeEgress(leaderID)
	// The old leader's egress is blocked, so Campaign may observe a transport
	// timeout while the live voter still receives enough messages to elect.
	_ = cluster.Drivers[candidateID].Campaign()
	newLeaderID := cluster.WaitLeader(leaderID)
	require.Equal(t, candidateID, newLeaderID)
	cluster.RefreshStore(newLeaderID)

	commit, err := cluster.Stores[newLeaderID].Append(context.Background(),
		rootevent.RegionDescriptorPublished(testDescriptor(71, []byte("a"), []byte("z"), 3)),
	)
	require.NoError(t, err)
	require.NotEqual(t, initialCommit.Cursor, commit.Cursor)

	cluster.RestoreNode(leaderID)
	require.Eventually(t, func() bool {
		if err := cluster.Stores[leaderID].Refresh(); err != nil {
			return false
		}
		current, err := cluster.Stores[leaderID].Current()
		return err == nil && reflect.DeepEqual(current, commit.State)
	}, 8*time.Second, 50*time.Millisecond)
}

func TestMetaRootLeaderChangePreservesClosureLineage(t *testing.T) {
	cluster := roottestcluster.Open(t)
	leaderID := cluster.WaitLeader()

	desc := testDescriptor(88, []byte("a"), []byte("z"), 56)
	_, err := cluster.Stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	grant, err := issueGrant(cluster.Stores[leaderID], "c1", 1_000, 100, 12, 34, 56)
	require.NoError(t, err)
	retirement, err := sealGrant(cluster.Stores[leaderID], "c1", grant.GrantID, 200, 12, 34, 56)
	require.NoError(t, err)

	followers := cluster.FollowerIDs(leaderID)
	newLeaderID := followers[0]
	passiveVoterID := followers[1]
	cluster.PauseTicks(leaderID)
	cluster.PauseTicks(passiveVoterID)
	defer cluster.ResumeTicks(passiveVoterID)
	defer cluster.ResumeTicks(leaderID)

	// This test is about rooted grant lineage across a leader handoff, not
	// randomized election scheduling. Keep the old leader and the spare voter
	// from starting competing elections while the selected follower campaigns;
	// message handling remains live, so the new leader still wins through Raft.
	cluster.Campaign(newLeaderID)
	require.Equal(t, newLeaderID, cluster.WaitLeader(leaderID))
	cluster.RefreshStore(newLeaderID)

	successor, err := issueGrant(cluster.Stores[newLeaderID], "c2", 1_400, 300, 12, 34, 56)
	require.NoError(t, err)
	require.Greater(t, successor.Era, retirement.Era)
	require.Equal(t, grant.Era, retirement.Era)

	require.Eventually(t, func() bool {
		for _, id := range []uint64{1, 2, 3} {
			if err := cluster.Stores[id].Refresh(); err != nil {
				return false
			}
			current, err := cluster.Stores[id].Current()
			if err != nil {
				return false
			}
			if current.ActiveGrant.HolderID != "c2" ||
				current.ActiveGrant.Era != successor.Era ||
				len(current.RetiredGrants) != 1 ||
				current.RetiredGrants[0].Era != retirement.Era ||
				current.RetiredGrants[0].HolderID != "c1" ||
				current.IDFence != 12 ||
				current.TSOFence != 34 {
				return false
			}
		}
		return true
	}, 8*time.Second, 50*time.Millisecond)
}

func TestMetaRootPartialSealRecoversFromCommittedLog(t *testing.T) {
	cluster := roottestcluster.Open(t)
	leaderID := cluster.WaitLeader()

	desc := testDescriptor(99, []byte("a"), []byte("z"), 64)
	_, err := cluster.Stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)
	grant, err := issueGrant(cluster.Stores[leaderID], "c1", 1_000, 100, 12, 34, 64)
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.AfterAppendCommittedBeforeCheckpoint)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	_, err = sealGrant(cluster.Stores[leaderID], "c1", grant.GrantID, 200, 12, 34, 64)
	require.ErrorIs(t, err, rootfailpoints.ErrAfterAppendCommittedBeforeCheckpoint)

	current, err := cluster.Stores[leaderID].Current()
	require.NoError(t, err)
	require.Empty(t, current.RetiredGrants)

	rootfailpoints.Set(rootfailpoints.None)
	reopened := cluster.ReopenStore(leaderID)
	current, err = reopened.Current()
	require.NoError(t, err)
	require.Len(t, current.RetiredGrants, 1)
	require.Equal(t, grant.Era, current.RetiredGrants[0].Era)
	require.Equal(t, "c1", current.RetiredGrants[0].HolderID)

	successor, err := issueGrant(reopened, "c2", 1_400, 300, 12, 34, 64)
	require.NoError(t, err)
	require.Equal(t, uint64(2), successor.Era)
}

func issueGrant(store interface {
	ApplyGrant(context.Context, rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
}, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64) (rootproto.AuthorityGrant, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, idFence),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, tsoFence),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, descriptorRevision, 0),
		},
	})
	return state.ActiveGrant, err
}

func sealGrant(store interface {
	ApplyGrant(context.Context, rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
}, holderID, grantID string, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64) (rootproto.GrantRetirement, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:        rootproto.GrantActSeal,
		HolderID:    holderID,
		GrantID:     grantID,
		NowUnixNano: nowUnixNano,
		ExactUsages: []rootproto.AuthorityUsage{
			{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: idFence}},
			{DutyID: rootproto.DutyTSO, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: tsoFence}},
			{DutyID: rootproto.DutyRegionLookup, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: descriptorRevision}},
		},
	})
	if err != nil || len(state.RetiredGrants) == 0 {
		return rootproto.GrantRetirement{}, err
	}
	return state.RetiredGrants[len(state.RetiredGrants)-1], nil
}

func testDescriptor(id uint64, start, end []byte, rootEpoch uint64) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: id*10 + 1}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: rootEpoch,
	}
	desc.EnsureHash()
	return desc
}
