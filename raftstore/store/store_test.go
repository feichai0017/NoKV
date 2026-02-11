package store_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
	"github.com/feichai0017/NoKV/raftstore/store"
)

type noopTransport struct{}

func (noopTransport) Send(myraft.Message) {}

type repeatingPlanner struct {
	op scheduler.Operation
}

func (p *repeatingPlanner) Plan(s scheduler.Snapshot) []scheduler.Operation {
	return []scheduler.Operation{p.op}
}

func testPeerBuilder(storeID uint64) store.PeerBuilder {
	return func(meta manifest.RegionMeta) (*raftstore.Config, error) {
		var peerID uint64
		for _, peerMeta := range meta.Peers {
			if peerMeta.StoreID == storeID {
				peerID = peerMeta.PeerID
				break
			}
		}
		if peerID == 0 {
			return nil, fmt.Errorf("store %d missing peer in region %d", storeID, meta.ID)
		}
		cfg := &raftstore.Config{
			RaftConfig: myraft.Config{
				ID:              peerID,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport: noopTransport{},
			Apply:     func([]myraft.Entry) error { return nil },
			GroupID:   meta.ID,
			Region:    manifest.CloneRegionMetaPtr(&meta),
		}
		return cfg, nil
	}
}

func openStoreDB(t *testing.T) *NoKV.DB {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestStorePeerLifecycle(t *testing.T) {
	router := store.NewRouter()
	rs := store.NewStore(router)

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       100,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	require.NotNil(t, peer)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(100), metas[0].ID)

	require.NoError(t, router.SendTick(peer.ID()))
	require.NoError(t, router.BroadcastTick())
	require.NoError(t, router.BroadcastFlush())

	rs.StopPeer(peer.ID())
	_, ok := router.Peer(peer.ID())
	require.False(t, ok)
}

func TestStoreDuplicatePeer(t *testing.T) {
	rs := store.NewStore(nil)
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	defer rs.StopPeer(peer.ID())

	_, err = rs.StartPeer(cfg, nil)
	require.Error(t, err)
}

func TestStoreRejectsLegacyApplyPayload(t *testing.T) {
	var wrapped peer.ApplyFunc
	rs := store.NewStoreWithConfig(store.Config{
		PeerFactory: func(cfg *peer.Config) (*peer.Peer, error) {
			wrapped = cfg.Apply
			return peer.NewPeer(cfg)
		},
	})
	defer rs.Close()

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       700,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}

	p, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	defer rs.StopPeer(p.ID())
	require.NotNil(t, wrapped)

	err = wrapped([]myraft.Entry{{Type: myraft.EntryNormal, Data: []byte("legacy-payload")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported legacy raft payload")
}

func TestStoreCustomFactoryAndHooks(t *testing.T) {
	router := store.NewRouter()
	factoryCalls := 0
	startCalls := 0
	stopCalls := 0

	rs := store.NewStoreWithConfig(store.Config{
		Router: router,
		PeerFactory: func(cfg *peer.Config) (*peer.Peer, error) {
			factoryCalls++
			return peer.NewPeer(cfg)
		},
		Hooks: store.LifecycleHooks{
			OnPeerStart: func(*peer.Peer) { startCalls++ },
			OnPeerStop:  func(*peer.Peer) { stopCalls++ },
		},
	})

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              2,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       200,
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 1, startCalls)

	handles := rs.Peers()
	require.Len(t, handles, 1)
	require.NotNil(t, handles[0].Region)
	handles[0].Region.StartKey[0] = 'x'
	meta := peer.RegionMeta()
	require.Equal(t, byte('c'), meta.StartKey[0])

	rs.StopPeer(peer.ID())
	require.Equal(t, 1, stopCalls)
	require.Empty(t, rs.Peers())
}

func TestStorePersistsRegionMetadata(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	updateCalls := 0
	removeCalls := 0
	var states []manifest.RegionState

	rs := store.NewStoreWithConfig(store.Config{
		Manifest: mgr,
		RegionHooks: store.RegionHooks{
			OnRegionUpdate: func(meta manifest.RegionMeta) {
				updateCalls++
				states = append(states, meta.State)
			},
			OnRegionRemove: func(id uint64) {
				removeCalls++
			},
		},
	})

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              3,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       500,
			StartKey: []byte("k"),
			EndKey:   []byte("z"),
		},
	}

	p, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, p)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(500), metas[0].ID)
	require.Equal(t, manifest.RegionStateRunning, metas[0].State)
	require.Equal(t, 1, updateCalls)

	snapshot := mgr.RegionSnapshot()
	require.Len(t, snapshot, 1)
	meta, ok := snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRunning, meta.State)
	require.Zero(t, meta.Epoch.Version)

	updated := manifest.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     4,
			ConfVersion: 6,
		},
		State: manifest.RegionStateRunning,
		Peers: []manifest.PeerMeta{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
	}
	require.NoError(t, rs.UpdateRegion(updated))

	peerMeta := p.RegionMeta()
	require.NotNil(t, peerMeta)
	require.Equal(t, uint64(4), peerMeta.Epoch.Version)
	require.Len(t, peerMeta.Peers, 2)

	metas = rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(4), metas[0].Epoch.Version)

	metaByID, ok := rs.RegionMetaByID(500)
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRunning, metaByID.State)
	_, ok = rs.RegionMetaByID(999)
	require.False(t, ok)

	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, uint64(4), meta.Epoch.Version)
	require.Len(t, meta.Peers, 2)

	require.NoError(t, rs.UpdateRegionState(500, manifest.RegionStateRemoving))

	metas = rs.RegionMetas()
	require.Equal(t, manifest.RegionStateRemoving, metas[0].State)
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRemoving, meta.State)

	rs.StopPeer(p.ID())
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRemoving, meta.State)

	require.NoError(t, rs.UpdateRegionState(500, manifest.RegionStateTombstone))

	metas = rs.RegionMetas()
	require.Equal(t, manifest.RegionStateTombstone, metas[0].State)
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateTombstone, meta.State)

	err = rs.UpdateRegionState(500, manifest.RegionStateRunning)
	require.Error(t, err)

	require.NoError(t, rs.RemoveRegion(500))

	metas = rs.RegionMetas()
	require.Len(t, metas, 0)

	snapshot = mgr.RegionSnapshot()
	require.Len(t, snapshot, 0)
	require.Equal(t, 1, removeCalls)
	expectedStates := []manifest.RegionState{
		manifest.RegionStateRunning,
		manifest.RegionStateRunning,
		manifest.RegionStateRemoving,
		manifest.RegionStateRemoving,
		manifest.RegionStateTombstone,
	}
	require.Equal(t, expectedStates, states)

	child := manifest.RegionMeta{
		ID:       600,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    manifest.RegionStateRunning,
	}
	parent := manifest.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("m"),
		State:    manifest.RegionStateRunning,
	}
	require.NoError(t, rs.UpdateRegion(parent))
	require.NoError(t, rs.UpdateRegion(child))

	metas = rs.RegionMetas()
	require.Len(t, metas, 2)

	_ = len(states)
}

func TestStoreSplitRegionStartsChildPeer(t *testing.T) {
	storeID := uint64(1)
	peerBuilder := testPeerBuilder(storeID)
	rs := store.NewStoreWithConfig(store.Config{PeerBuilder: peerBuilder, StoreID: storeID})
	defer rs.Close()

	parentMeta := manifest.RegionMeta{
		ID:       1000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 1}},
	}
	parentCfg, err := peerBuilder(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())

	childMeta := manifest.RegionMeta{
		ID:       2000,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 2}},
	}
	childPeer, err := rs.SplitRegion(parentMeta.ID, childMeta)
	require.NoError(t, err)
	require.NotNil(t, childPeer)
	defer rs.StopPeer(childPeer.ID())

	parentUpdated, ok := rs.RegionMetaByID(1000)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)
	require.Equal(t, uint64(1), parentUpdated.Epoch.Version)

	childUpdated, ok := rs.RegionMetaByID(2000)
	require.True(t, ok)
	require.Equal(t, []byte("m"), childUpdated.StartKey)
	require.Equal(t, []byte("z"), childUpdated.EndKey)
	require.Len(t, childUpdated.Peers, 1)
	require.Equal(t, uint64(2), childUpdated.Peers[0].PeerID)

}

func TestStoreSchedulerReceivesRegionHeartbeats(t *testing.T) {
	coord := scheduler.NewCoordinator()
	rs := store.NewStoreWithConfig(store.Config{Scheduler: coord, StoreID: 1})
	defer rs.Close()

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       42,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())

	snapshot := coord.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, uint64(42), snapshot[0].Meta.ID)
	require.False(t, snapshot[0].LastHeartbeat.IsZero())

	require.NoError(t, rs.UpdateRegionState(42, manifest.RegionStateRemoving))
	snapshot = coord.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, manifest.RegionStateRemoving, snapshot[0].Meta.State)

	require.NoError(t, rs.RemoveRegion(42))
	snapshot = coord.RegionSnapshot()
	require.Empty(t, snapshot)

	storeSnap := rs.SchedulerSnapshot()
	require.Empty(t, storeSnap.Regions)
}

func TestStoreSchedulerPeriodicHeartbeats(t *testing.T) {
	coord := scheduler.NewCoordinator()
	rs := store.NewStoreWithConfig(store.Config{
		Scheduler:         coord,
		StoreID:           9,
		PeerBuilder:       testPeerBuilder(9),
		HeartbeatInterval: 25 * time.Millisecond,
	})
	defer rs.Close()

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
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

	snap := rs.SchedulerSnapshot()
	require.Len(t, snap.Stores, 1)
	require.Equal(t, uint64(1), snap.Stores[0].LeaderNum)
	require.NotEmpty(t, snap.Regions)
	require.Equal(t, uint64(77), snap.Regions[0].ID)
	require.False(t, snap.Regions[0].LastHeartbeat.IsZero())
}

func TestStorePlannerQueuesOperations(t *testing.T) {
	coord := scheduler.NewCoordinator()
	var mu sync.Mutex
	var applied []time.Time
	planner := &repeatingPlanner{op: scheduler.Operation{
		Type:   scheduler.OperationLeaderTransfer,
		Region: 200,
		Source: 701,
		Target: 702,
	}}
	rs := store.NewStoreWithConfig(store.Config{
		Scheduler:          coord,
		StoreID:            12,
		PeerBuilder:        testPeerBuilder(12),
		HeartbeatInterval:  25 * time.Millisecond,
		OperationQueueSize: 8,
		OperationCooldown:  80 * time.Millisecond,
		OperationInterval:  20 * time.Millisecond,
		OperationBurst:     1,
		Planner:            planner,
		OperationObserver: func(op scheduler.Operation) {
			mu.Lock()
			applied = append(applied, time.Now())
			mu.Unlock()
		},
	})
	defer rs.Close()

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              701,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       200,
			StartKey: []byte("k0"),
			EndKey:   []byte("k9"),
			Peers: []manifest.PeerMeta{
				{StoreID: 12, PeerID: 701},
				{StoreID: 99, PeerID: 702},
			},
		},
	}
	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 701}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.NoError(t, peer.Campaign())

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) >= 2
	}, time.Second, 20*time.Millisecond)

	mu.Lock()
	recorded := append([]time.Time(nil), applied...)
	mu.Unlock()
	require.GreaterOrEqual(t, len(recorded), 2)
	delta := recorded[1].Sub(recorded[0])
	require.GreaterOrEqual(t, delta, 60*time.Millisecond)
}

func TestStoreProposeCommandPrewriteCommit(t *testing.T) {
	db := openStoreDB(t)
	coord := scheduler.NewCoordinator()
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &manifest.RegionMeta{
		ID:       101,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []manifest.PeerMeta{{StoreID: 1, PeerID: 1}},
	}
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   101,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	epoch := &pb.RegionEpoch{Version: 1, ConfVer: 1}
	prewrite := &pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_PREWRITE,
			Cmd: &pb.Request_Prewrite{Prewrite: &pb.PrewriteRequest{
				Mutations: []*pb.Mutation{{
					Op:    pb.Mutation_Put,
					Key:   []byte("cmd-key"),
					Value: []byte("cmd-value"),
				}},
				PrimaryLock:  []byte("cmd-key"),
				StartVersion: 20,
				LockTtl:      3000,
			}},
		}},
	}
	resp, err := st.ProposeCommand(prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	require.Empty(t, resp.GetResponses()[0].GetPrewrite().GetErrors())
	require.NotZero(t, resp.GetHeader().GetRequestId())

	commit := &pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_COMMIT,
			Cmd: &pb.Request_Commit{Commit: &pb.CommitRequest{
				Keys:          [][]byte{[]byte("cmd-key")},
				StartVersion:  20,
				CommitVersion: 40,
			}},
		}},
	}
	resp, err = st.ProposeCommand(commit)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	require.Nil(t, resp.GetResponses()[0].GetCommit().GetError())

	reader := percolator.NewReader(db)
	val, err := reader.GetValue([]byte("cmd-key"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("cmd-value"), val)
}

func TestStoreProposeCommandNotLeader(t *testing.T) {
	db := openStoreDB(t)
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &manifest.RegionMeta{
		ID:       202,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 2, PeerID: 5}},
	}
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              5,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   202,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 5}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })

	req := &pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RegionId: region.ID, RegionEpoch: &pb.RegionEpoch{Version: 1, ConfVer: 1}},
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_PREWRITE,
			Cmd:     &pb.Request_Prewrite{Prewrite: &pb.PrewriteRequest{StartVersion: 1}},
		}},
	}
	resp, err := st.ProposeCommand(req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
}

func TestStoreProposeCommandEpochMismatch(t *testing.T) {
	db := openStoreDB(t)
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{StoreID: 3, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &manifest.RegionMeta{
		ID:       303,
		StartKey: []byte("a"),
		EndKey:   []byte("h"),
		Epoch:    manifest.RegionEpoch{Version: 2, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 3, PeerID: 7}},
	}
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   303,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 7}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	badReq := &pb.RaftCmdRequest{
		Header:   &pb.CmdHeader{RegionId: region.ID, RegionEpoch: &pb.RegionEpoch{Version: 1, ConfVer: 1}},
		Requests: []*pb.Request{{CmdType: pb.CmdType_CMD_PREWRITE}},
	}
	resp, err := st.ProposeCommand(badReq)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetEpochNotMatch())
}

func TestStoreProposeSplitApplies(t *testing.T) {
	storeID := uint64(11)
	rs := store.NewStoreWithConfig(store.Config{
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := manifest.RegionMeta{
		ID:       3000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 31}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 31}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := manifest.RegionMeta{
		ID:       3001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 32}},
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
}

func TestStoreProposeMergeApplies(t *testing.T) {
	storeID := uint64(12)
	rs := store.NewStoreWithConfig(store.Config{
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := manifest.RegionMeta{
		ID:       4000,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 41}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 41}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	sourceMeta := manifest.RegionMeta{
		ID:       4001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 42}},
	}
	sourceCfg, err := testPeerBuilder(storeID)(sourceMeta)
	require.NoError(t, err)
	sourcePeer, err := rs.StartPeer(sourceCfg, []myraft.Peer{{ID: 42}})
	require.NoError(t, err)
	defer rs.StopPeer(sourcePeer.ID())
	require.NoError(t, sourcePeer.Campaign())

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
}

func TestStoreSplitMergeLifecycle(t *testing.T) {
	storeID := uint64(13)
	rs := store.NewStoreWithConfig(store.Config{
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 15 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := manifest.RegionMeta{
		ID:       5000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 51}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 51}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := manifest.RegionMeta{
		ID:       5001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []manifest.PeerMeta{{StoreID: storeID, PeerID: 52}},
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
