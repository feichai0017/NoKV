package store

import (
	"bytes"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"testing"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

func TestStoreSchedulerReceivesRegionHeartbeats(t *testing.T) {
	const eventuallyTimeout = 3 * time.Second

	sink := newTestSchedulerSink()
	rs := NewStore(Config{Scheduler: sink, StoreID: 1})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       42,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: 42, rootKind: rootevent.KindRegionBootstrap},
		)
	}, eventuallyTimeout, 10*time.Millisecond)
	sink.ResetHistory()

	var snapshot []regionHeartbeat
	require.Eventually(t, func() bool {
		snapshot = sink.RegionSnapshot()
		return len(snapshot) == 1
	}, eventuallyTimeout, 10*time.Millisecond)
	require.Equal(t, uint64(42), snapshot[0].Descriptor.RegionID)
	require.False(t, snapshot[0].LastHeartbeat.IsZero())

	require.NoError(t, rs.applyRegionState(42, metaregion.ReplicaStateRemoving))
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: 42, rootKind: rootevent.KindRegionDescriptorPublished},
		)
	}, eventuallyTimeout, 10*time.Millisecond)
	sink.ResetHistory()
	require.Eventually(t, func() bool {
		snapshot = sink.RegionSnapshot()
		return len(snapshot) == 1 && snapshot[0].Descriptor.State == metaregion.ReplicaStateRemoving
	}, eventuallyTimeout, 10*time.Millisecond)
	require.Equal(t, metaregion.ReplicaStateRemoving, snapshot[0].Descriptor.State)

	require.NoError(t, rs.applyRegionRemoval(42))
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: 42, rootKind: rootevent.KindRegionTombstoned},
		)
	}, eventuallyTimeout, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(sink.RegionSnapshot()) == 0
	}, eventuallyTimeout, 10*time.Millisecond)
}

func TestStoreRegionApplyDoesNotBlockOnSchedulerPublish(t *testing.T) {
	sink := &slowSchedulerSink{
		testSchedulerSink: *newTestSchedulerSink(),
		publishDelay:      200 * time.Millisecond,
	}
	rs := NewStore(Config{Scheduler: sink, StoreID: 1})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       88,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	start := time.Now()
	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.Less(t, elapsed, sink.publishDelay/2, "region apply path should not block on slow PD publish")
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		return len(snapshot) == 1 && snapshot[0].Descriptor.RegionID == 88
	}, time.Second, 10*time.Millisecond)
}

func TestStoreSchedulerPeriodicHeartbeats(t *testing.T) {
	coord := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         coord,
		StoreID:           9,
		PeerBuilder:       testPeerBuilder(9),
		HeartbeatInterval: 25 * time.Millisecond,
	})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       77,
			StartKey: []byte("alpha"),
			EndKey:   []byte("beta"),
		},
	}
	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 7}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.NoError(t, peer.Campaign())

	require.Eventually(t, func() bool {
		_, ok := coord.LastUpdate(77)
		return ok
	}, time.Second, 10*time.Millisecond)

	first, _ := coord.LastUpdate(77)
	time.Sleep(80 * time.Millisecond)
	second, ok := coord.LastUpdate(77)
	require.True(t, ok)
	require.True(t, second.After(first))

	storeSnap := coord.StoreSnapshot()
	require.Len(t, storeSnap, 1)
	require.Equal(t, uint64(1), storeSnap[0].LeaderNum)
	regionSnap := coord.RegionSnapshot()
	require.NotEmpty(t, regionSnap)
	require.Equal(t, uint64(77), regionSnap[0].Descriptor.RegionID)
	require.False(t, regionSnap[0].LastHeartbeat.IsZero())
}

func TestStoreProposeSplitApplies(t *testing.T) {
	storeID := uint64(11)
	sink := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         sink,
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       3000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 31}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 31}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())
	require.Eventually(t, func() bool { return len(sink.RegionSnapshot()) >= 1 }, time.Second, 10*time.Millisecond)
	sink.ResetHistory()

	childMeta := localmeta.RegionMeta{
		ID:       3001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 32}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))

	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)

	childUpdated, ok := rs.RegionMetaByID(childMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), childUpdated.StartKey)
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		if len(snapshot) < 2 {
			return false
		}
		for _, info := range snapshot {
			if info.Descriptor.RegionID == childMeta.ID {
				return len(info.Descriptor.Lineage) == 1 &&
					info.Descriptor.Lineage[0].RegionID == parentMeta.ID &&
					info.Descriptor.Lineage[0].Kind == descriptor.LineageKindSplitParent
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionSplitPlanned},
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionSplitCommitted},
		)
	}, time.Second, 10*time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionBootstrap, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionDescriptorPublished, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionTombstoned, ev.rootKind)
	}

	sink.ResetHistory()
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	time.Sleep(50 * time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionSplitPlanned, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionSplitCommitted, ev.rootKind)
	}
}

func TestStoreProposeMergeApplies(t *testing.T) {
	storeID := uint64(12)
	sink := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         sink,
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       4000,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 41}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 41}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	sourceMeta := localmeta.RegionMeta{
		ID:       4001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 42}},
	}
	sourceCfg, err := testPeerBuilder(storeID)(sourceMeta)
	require.NoError(t, err)
	sourcePeer, err := rs.StartPeer(sourceCfg, []myraft.Peer{{ID: 42}})
	require.NoError(t, err)
	defer rs.StopPeer(sourcePeer.ID())
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool { return len(sink.RegionSnapshot()) >= 2 }, time.Second, 10*time.Millisecond)
	sink.ResetHistory()

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, sourceMeta.ID))

	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(sourceMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.True(t, bytes.Equal(parentUpdated.EndKey, []byte("z")))

	_, ok = rs.RegionMetaByID(sourceMeta.ID)
	require.False(t, ok)
	if peer, exists := rs.Peer(sourceMeta.Peers[0].PeerID); exists {
		rs.StopPeer(peer.ID())
	}
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		for _, info := range snapshot {
			if info.Descriptor.RegionID == parentMeta.ID {
				return len(info.Descriptor.Lineage) == 1 &&
					info.Descriptor.Lineage[0].RegionID == sourceMeta.ID &&
					info.Descriptor.Lineage[0].Kind == descriptor.LineageKindMergeSource
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionMergePlanned},
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionMerged},
		)
	}, time.Second, 10*time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionBootstrap, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionDescriptorPublished, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionTombstoned, ev.rootKind)
	}
}

func hasSchedulerEventSubsequence(history []schedulerEvent, want ...schedulerEvent) bool {
	if len(want) == 0 {
		return true
	}
	idx := 0
	for _, got := range history {
		if schedulerEventMatches(got, want[idx]) {
			idx++
			if idx == len(want) {
				return true
			}
		}
	}
	return false
}

func schedulerEventMatches(got, want schedulerEvent) bool {
	if want.kind != "" && got.kind != want.kind {
		return false
	}
	if want.regionID != 0 && got.regionID != want.regionID {
		return false
	}
	if want.rootKind != 0 && got.rootKind != want.rootKind {
		return false
	}
	return true
}

func TestStoreSplitMergeLifecycle(t *testing.T) {
	storeID := uint64(13)
	rs := NewStore(Config{
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 15 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       5000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 51}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 51}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := localmeta.RegionMeta{
		ID:       5001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 52}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, childMeta.ID))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.True(t, bytes.Equal(parentUpdated.EndKey, []byte("z")))
	if peer, exists := rs.Peer(childMeta.Peers[0].PeerID); exists {
		rs.StopPeer(peer.ID())
	}
}

func TestStoreRestartPreservesSplitMergeLocalMeta(t *testing.T) {
	storeID := uint64(21)
	dir := t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()

	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
		LocalMeta:   localMeta,
	})

	parentMeta := localmeta.RegionMeta{
		ID:       7000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 71}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 71}})
	require.NoError(t, err)
	require.NoError(t, parentPeer.Campaign())

	childMeta := localmeta.RegionMeta{
		ID:       7001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 72}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, childMeta.ID))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	rs.Close()
	require.NoError(t, localMeta.Close())

	reopenedMeta, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = reopenedMeta.Close() }()

	reopened := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
		LocalMeta:   reopenedMeta,
	})
	defer reopened.Close()

	metas := reopened.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(7000), metas[0].ID)
	require.Equal(t, []byte("z"), metas[0].EndKey)
	_, ok := reopened.RegionMetaByID(childMeta.ID)
	require.False(t, ok)
}

func TestStoreHandleSplitCommandReplayIsIdempotent(t *testing.T) {
	storeID := uint64(31)
	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       8100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 81}},
	}
	require.NoError(t, rs.applyRegionMeta(parentMeta))

	childMeta := localmeta.RegionMeta{
		ID:       8101,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 82}},
	}
	_, err := rs.splitRegionLocal(parentMeta.ID, childMeta)
	require.NoError(t, err)

	cmd := &raftcmdpb.SplitCommand{
		ParentRegionId: parentMeta.ID,
		SplitKey:       []byte("m"),
		Child:          metacodec.LocalRegionMetaToDescriptorProto(childMeta),
	}
	require.NoError(t, rs.handleSplitCommand(cmd))

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)
	childUpdated, ok := rs.RegionMetaByID(childMeta.ID)
	require.True(t, ok)
	require.Equal(t, childMeta.Peers, childUpdated.Peers)
}

func TestStoreHandleMergeCommandReplayIsIdempotent(t *testing.T) {
	storeID := uint64(32)
	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       8200,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 91}},
	}
	sourceMeta := localmeta.RegionMeta{
		ID:       8201,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 92}},
	}
	require.NoError(t, rs.applyRegionMeta(parentMeta))
	require.NoError(t, rs.applyRegionMeta(sourceMeta))
	require.NoError(t, rs.applyRegionRemoval(sourceMeta.ID))

	require.NoError(t, rs.handleMergeCommand(&raftcmdpb.MergeCommand{
		TargetRegionId: parentMeta.ID,
		SourceRegionId: sourceMeta.ID,
	}))
}
