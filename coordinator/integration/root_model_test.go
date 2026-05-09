package integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestRootModelReplayAndWatchSchedule(t *testing.T) {
	seeds := rootModelEnvInt("NOKV_ROOT_MODEL_SEEDS", 2)
	steps := rootModelEnvInt("NOKV_ROOT_MODEL_STEPS", 24)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			cluster := pdtestcluster.OpenReplicated(t)
			leaderID, leader := cluster.LeaderService()
			followerID := cluster.FollowerIDs(leaderID)[0]
			subscription := cluster.SubscribeTail(followerID, rootstorage.TailToken{})
			require.NotNil(t, subscription)

			model := newRootScheduleModel(seed)
			for step := range steps {
				event := model.nextEvent()
				before, err := cluster.Roots[leaderID].Snapshot()
				require.NoError(t, err)

				resp, err := leader.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
					Event: metawire.RootEventToProto(event),
				})
				require.NoError(t, err, "step=%d event=%v", step, event.Kind)
				require.True(t, resp.GetAccepted(), "step=%d event=%v", step, event.Kind)

				after, err := cluster.Roots[leaderID].Snapshot()
				require.NoError(t, err)
				assertRootEpochDelta(t, before, after, event)
				assertRootReplayMatchesSnapshot(t, cluster.Roots[leaderID])
				waitFollowerRootMatchesLeader(t, cluster, followerID, leaderID, subscription)
			}
		})
	}
}

type rootScheduleModel struct {
	rng             *rand.Rand
	nextStoreID     uint64
	activeStores    []uint64
	nextRegionID    uint64
	activeRegions   []uint64
	nextMountID     uint64
	activeMounts    []string
	mountKeyIDs     map[string]uint64
	snapshotVersion map[string]uint64
	quotaEra        map[string]uint64
}

func newRootScheduleModel(seed int64) *rootScheduleModel {
	return &rootScheduleModel{
		rng:             rand.New(rand.NewSource(seed)),
		nextStoreID:     10,
		nextRegionID:    1000,
		nextMountID:     1,
		mountKeyIDs:     make(map[string]uint64),
		snapshotVersion: make(map[string]uint64),
		quotaEra:        make(map[string]uint64),
	}
}

func (m *rootScheduleModel) nextEvent() rootevent.Event {
	if len(m.activeStores) == 0 {
		return m.storeJoined()
	}
	if len(m.activeMounts) == 0 {
		return m.mountRegistered()
	}
	switch m.rng.Intn(100) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
		return m.storeJoined()
	case 10, 11, 12, 13:
		return m.storeRetired()
	case 14, 15, 16, 17, 18, 19:
		return m.mountRegistered()
	case 20, 21, 22:
		return m.mountRetired()
	case 23, 24, 25, 26, 27, 28, 29:
		return m.snapshotPublished()
	case 30, 31, 32, 33:
		return m.snapshotRetired()
	case 34, 35, 36, 37, 38:
		return m.quotaUpdated()
	case 39, 40, 41, 42, 43, 44, 45, 46:
		return m.regionTombstoned()
	default:
		return m.regionPublished()
	}
}

func (m *rootScheduleModel) storeJoined() rootevent.Event {
	storeID := m.nextStoreID
	m.nextStoreID++
	m.activeStores = append(m.activeStores, storeID)
	return rootevent.StoreJoined(storeID)
}

func (m *rootScheduleModel) storeRetired() rootevent.Event {
	if len(m.activeStores) == 0 {
		return m.storeJoined()
	}
	idx := m.rng.Intn(len(m.activeStores))
	storeID := m.activeStores[idx]
	m.activeStores = append(m.activeStores[:idx], m.activeStores[idx+1:]...)
	return rootevent.StoreRetired(storeID)
}

func (m *rootScheduleModel) mountRegistered() rootevent.Event {
	mount := fmt.Sprintf("vol-%03d", m.nextMountID)
	mountKeyID := m.nextMountID
	m.nextMountID++
	m.activeMounts = append(m.activeMounts, mount)
	m.mountKeyIDs[mount] = mountKeyID
	return rootevent.MountRegistered(mount, mountKeyID, 1, 1)
}

func (m *rootScheduleModel) mountRetired() rootevent.Event {
	if len(m.activeMounts) == 0 {
		return m.mountRegistered()
	}
	idx := m.rng.Intn(len(m.activeMounts))
	mount := m.activeMounts[idx]
	m.activeMounts = append(m.activeMounts[:idx], m.activeMounts[idx+1:]...)
	delete(m.mountKeyIDs, mount)
	return rootevent.MountRetired(mount)
}

func (m *rootScheduleModel) snapshotPublished() rootevent.Event {
	mount := m.randomMount()
	m.snapshotVersion[mount]++
	return rootevent.SnapshotEpochPublished(mount, m.mountKeyIDs[mount], 1, m.snapshotVersion[mount])
}

func (m *rootScheduleModel) snapshotRetired() rootevent.Event {
	mount := m.randomMount()
	if m.snapshotVersion[mount] == 0 {
		return m.snapshotPublished()
	}
	return rootevent.SnapshotEpochRetired(mount, m.mountKeyIDs[mount], 1, m.snapshotVersion[mount])
}

func (m *rootScheduleModel) quotaUpdated() rootevent.Event {
	mount := m.randomMount()
	m.quotaEra[mount]++
	era := m.quotaEra[mount]
	return rootevent.QuotaFenceUpdated(mount, 0, 1024*era, 100+era, era, era*10)
}

func (m *rootScheduleModel) regionPublished() rootevent.Event {
	regionID := m.nextRegionID
	m.nextRegionID++
	m.activeRegions = append(m.activeRegions, regionID)
	return rootevent.RegionDescriptorPublished(rootModelDescriptor(regionID))
}

func (m *rootScheduleModel) regionTombstoned() rootevent.Event {
	if len(m.activeRegions) == 0 {
		return m.regionPublished()
	}
	idx := m.rng.Intn(len(m.activeRegions))
	regionID := m.activeRegions[idx]
	m.activeRegions = append(m.activeRegions[:idx], m.activeRegions[idx+1:]...)
	return rootevent.RegionTombstoned(regionID)
}

func (m *rootScheduleModel) randomMount() string {
	if len(m.activeMounts) == 0 {
		return "vol-000"
	}
	return m.activeMounts[m.rng.Intn(len(m.activeMounts))]
}

func rootModelDescriptor(regionID uint64) topology.Descriptor {
	start := fmt.Appendf(nil, "rk%06d", regionID)
	end := fmt.Appendf(nil, "rk%06d", regionID+1)
	desc := topology.Descriptor{
		RegionID: regionID,
		StartKey: start,
		EndKey:   end,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}

func assertRootEpochDelta(t *testing.T, before, after rootstate.Snapshot, event rootevent.Event) {
	t.Helper()
	wantCluster := before.State.ClusterEpoch
	wantMembership := before.State.MembershipEpoch
	switch event.Kind {
	case rootevent.KindStoreJoined, rootevent.KindStoreRetired:
		wantMembership++
	case rootevent.KindRegionBootstrap,
		rootevent.KindRegionDescriptorPublished,
		rootevent.KindRegionTombstoned,
		rootevent.KindRegionSplitPlanned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionSplitCancelled,
		rootevent.KindRegionMergePlanned,
		rootevent.KindRegionMerged,
		rootevent.KindRegionMergeCancelled,
		rootevent.KindPeerAdditionPlanned,
		rootevent.KindPeerRemovalPlanned,
		rootevent.KindPeerAdded,
		rootevent.KindPeerRemoved,
		rootevent.KindPeerAdditionCancelled,
		rootevent.KindPeerRemovalCancelled:
		wantCluster++
	}
	require.Equal(t, wantCluster, after.State.ClusterEpoch, "event=%v", event.Kind)
	require.Equal(t, wantMembership, after.State.MembershipEpoch, "event=%v", event.Kind)
	require.True(t, rootstate.CursorAfter(after.State.LastCommitted, before.State.LastCommitted), "event=%v", event.Kind)
}

func assertRootReplayMatchesSnapshot(t *testing.T, root interface {
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
	Snapshot() (rootstate.Snapshot, error)
}) {
	t.Helper()
	observed, err := root.ObserveCommitted()
	require.NoError(t, err)
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	snapshot, err := root.Snapshot()
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(normalizeRootSnapshot(snapshot), normalizeRootSnapshot(bootstrap.Snapshot)), "root snapshot and replayed bootstrap diverged")
}

func waitFollowerRootMatchesLeader(t *testing.T, cluster *pdtestcluster.Cluster, followerID, leaderID uint64, sub *rootstorage.TailSubscription) {
	t.Helper()
	require.Eventually(t, func() bool {
		advance, err := sub.Next(context.Background(), 500*time.Millisecond)
		if err != nil {
			return false
		}
		switch advance.CatchUpAction() {
		case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap:
			if err := cluster.RootStores[followerID].Refresh(); err != nil {
				return false
			}
			sub.Acknowledge(advance)
		case rootstorage.TailCatchUpAcknowledgeWindow:
			sub.Acknowledge(advance)
		default:
			return false
		}
		leaderSnapshot, err := cluster.Roots[leaderID].Snapshot()
		if err != nil {
			return false
		}
		followerSnapshot, err := cluster.Roots[followerID].Snapshot()
		if err != nil {
			return false
		}
		return reflect.DeepEqual(normalizeRootSnapshot(leaderSnapshot), normalizeRootSnapshot(followerSnapshot))
	}, 8*time.Second, 50*time.Millisecond)
}

func normalizeRootSnapshot(snapshot rootstate.Snapshot) rootstate.Snapshot {
	out := rootstate.CloneSnapshot(snapshot)
	if out.Stores == nil {
		out.Stores = make(map[uint64]rootstate.StoreMembership)
	}
	if out.SnapshotEpochs == nil {
		out.SnapshotEpochs = make(map[string]rootstate.SnapshotEpoch)
	}
	if out.Mounts == nil {
		out.Mounts = make(map[string]rootstate.MountRecord)
	}
	if out.Subtrees == nil {
		out.Subtrees = make(map[string]rootstate.SubtreeAuthority)
	}
	if out.Quotas == nil {
		out.Quotas = make(map[string]rootstate.QuotaFence)
	}
	if out.Descriptors == nil {
		out.Descriptors = make(map[uint64]topology.Descriptor)
	}
	if out.PendingPeerChanges == nil {
		out.PendingPeerChanges = make(map[uint64]rootstate.PendingPeerChange)
	}
	if out.PendingRangeChanges == nil {
		out.PendingRangeChanges = make(map[uint64]rootstate.PendingRangeChange)
	}
	return out
}

func rootModelEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
