package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	roottestcluster "github.com/feichai0017/NoKV/meta/root/testcluster"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestMetaRootNodeIsolationElectsNewLeaderAndHeals(t *testing.T) {
	cluster := roottestcluster.Open(t)
	leaderID := cluster.WaitLeader()

	initialCommit, err := cluster.Stores[leaderID].Append(context.Background(),
		rootevent.StoreJoined(1, "s1"),
	)
	require.NoError(t, err)
	cluster.RefreshAll()

	cluster.IsolateNode(leaderID)
	_ = cluster.Drivers[cluster.FollowerIDs(leaderID)[0]].Campaign()
	newLeaderID := cluster.WaitLeader(leaderID)
	require.NotEqual(t, leaderID, newLeaderID)
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

	lease, err := campaignLease(cluster.Stores[leaderID], "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(cluster.Stores[leaderID], "c1", 200, succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56))
	require.NoError(t, err)

	newLeaderID := cluster.FollowerIDs(leaderID)[0]
	cluster.Campaign(newLeaderID)
	newLeaderID = cluster.WaitLeader(leaderID)
	cluster.RefreshStore(newLeaderID)

	successor, err := campaignLease(cluster.Stores[newLeaderID], "c2", 1_400, 300, 12, 34, 56, rootstate.DigestOfLegacy(seal))
	require.NoError(t, err)
	require.Greater(t, successor.Epoch, seal.Epoch)
	require.Equal(t, lease.Epoch, seal.Epoch)

	require.Eventually(t, func() bool {
		for _, id := range []uint64{1, 2, 3} {
			if err := cluster.Stores[id].Refresh(); err != nil {
				return false
			}
			current, err := cluster.Stores[id].Current()
			if err != nil {
				return false
			}
			if current.Tenure.HolderID != "c2" ||
				current.Tenure.Epoch != successor.Epoch ||
				current.Legacy.Epoch != seal.Epoch ||
				current.Legacy.HolderID != "c1" ||
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
	lease, err := campaignLease(cluster.Stores[leaderID], "c1", 1_000, 100, 10, 20, 64, "")
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.AfterAppendCommittedBeforeCheckpoint)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	_, err = sealLease(cluster.Stores[leaderID], "c1", 200, succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 64))
	require.ErrorIs(t, err, rootfailpoints.ErrAfterAppendCommittedBeforeCheckpoint)

	current, err := cluster.Stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, uint64(0), current.Legacy.Epoch)

	rootfailpoints.Set(rootfailpoints.None)
	reopened := cluster.ReopenStore(leaderID)
	current, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, lease.Epoch, current.Legacy.Epoch)
	require.Equal(t, "c1", current.Legacy.HolderID)

	successor, err := campaignLease(reopened, "c2", 1_400, 300, 12, 34, 64, rootstate.DigestOfLegacy(current.Legacy))
	require.NoError(t, err)
	require.Equal(t, uint64(2), successor.Epoch)
}

func campaignLease(store interface {
	ApplyTenure(context.Context, rootproto.TenureCommand) (rootstate.SuccessionState, error)
}, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64, lineageDigest string) (rootstate.Tenure, error) {
	state, err := store.ApplyTenure(context.Background(), rootproto.TenureCommand{
		Kind:               rootproto.TenureActIssue,
		HolderID:           holderID,
		ExpiresUnixNano:    expiresUnixNano,
		NowUnixNano:        nowUnixNano,
		LineageDigest:      lineageDigest,
		InheritedFrontiers: succession.Frontiers(rootstate.State{IDFence: idFence, TSOFence: tsoFence}, descriptorRevision),
	})
	return state.Tenure, err
}

func sealLease(store interface {
	ApplyTransit(context.Context, rootproto.TransitCommand) (rootstate.SuccessionState, error)
}, holderID string, nowUnixNano int64, frontiers rootproto.MandateFrontiers) (rootstate.Legacy, error) {
	state, err := store.ApplyTransit(context.Background(), rootproto.TransitCommand{
		Kind:        rootproto.TransitActSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	return state.Legacy, err
}

func testDescriptor(id uint64, start, end []byte, rootEpoch uint64) descriptor.Descriptor {
	desc := descriptor.Descriptor{
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
