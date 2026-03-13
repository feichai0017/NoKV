package store_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/store"
)

type noopTransport struct{}

func (noopTransport) Send(myraft.Message) {}

type testSchedulerSink struct {
	mu      sync.RWMutex
	regions map[uint64]regionHeartbeat
	stores  map[uint64]store.StoreStats
}

type regionHeartbeat struct {
	Meta          raftmeta.RegionMeta
	LastHeartbeat time.Time
}

func newTestSchedulerSink() *testSchedulerSink {
	return &testSchedulerSink{
		regions: make(map[uint64]regionHeartbeat),
		stores:  make(map[uint64]store.StoreStats),
	}
}

func (s *testSchedulerSink) PublishRegion(meta raftmeta.RegionMeta) {
	if s == nil || meta.ID == 0 {
		return
	}
	s.mu.Lock()
	s.regions[meta.ID] = regionHeartbeat{
		Meta:          raftmeta.CloneRegionMeta(meta),
		LastHeartbeat: time.Now(),
	}
	s.mu.Unlock()
}

func (s *testSchedulerSink) RemoveRegion(id uint64) {
	if s == nil || id == 0 {
		return
	}
	s.mu.Lock()
	delete(s.regions, id)
	s.mu.Unlock()
}

func (s *testSchedulerSink) StoreHeartbeat(stats store.StoreStats) []store.Operation {
	if s == nil || stats.StoreID == 0 {
		return nil
	}
	stats.UpdatedAt = time.Now()
	s.mu.Lock()
	s.stores[stats.StoreID] = stats
	s.mu.Unlock()
	return nil
}

func (s *testSchedulerSink) Status() store.SchedulerStatus {
	return store.SchedulerStatus{}
}

func (s *testSchedulerSink) RegionSnapshot() []regionHeartbeat {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]regionHeartbeat, 0, len(s.regions))
	for _, info := range s.regions {
		out = append(out, regionHeartbeat{
			Meta:          raftmeta.CloneRegionMeta(info.Meta),
			LastHeartbeat: info.LastHeartbeat,
		})
	}
	s.mu.RUnlock()
	return out
}

func (s *testSchedulerSink) StoreSnapshot() []store.StoreStats {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]store.StoreStats, 0, len(s.stores))
	for _, st := range s.stores {
		out = append(out, st)
	}
	s.mu.RUnlock()
	return out
}

func (s *testSchedulerSink) LastUpdate(regionID uint64) (time.Time, bool) {
	if s == nil || regionID == 0 {
		return time.Time{}, false
	}
	s.mu.RLock()
	info, ok := s.regions[regionID]
	s.mu.RUnlock()
	if !ok {
		return time.Time{}, false
	}
	return info.LastHeartbeat, true
}

func (s *testSchedulerSink) Close() error {
	return nil
}

func testPeerBuilder(storeID uint64) store.PeerBuilder {
	return func(meta raftmeta.RegionMeta) (*peer.Config, error) {
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
		cfg := &peer.Config{
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
			Region:    raftmeta.CloneRegionMetaPtr(&meta),
		}
		return cfg, nil
	}
}

func openStoreDB(t *testing.T) (*NoKV.DB, *raftmeta.Store) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	localMeta, err := raftmeta.OpenLocalStore(opt.WorkDir, nil)
	require.NoError(t, err)
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() { _ = localMeta.Close() })
	return db, localMeta
}

func TestStorePeerLifecycle(t *testing.T) {
	router := store.NewRouter()
	rs := store.NewStore(router)

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &raftmeta.RegionMeta{
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
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	defer rs.StopPeer(peer.ID())

	_, err = rs.StartPeer(cfg, nil)
	require.Error(t, err)
}

func TestStorePeersSnapshot(t *testing.T) {
	router := store.NewRouter()
	rs := store.NewStoreWithConfig(store.Config{
		Router: router,
	})

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              2,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &raftmeta.RegionMeta{
			ID:       200,
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	handles := rs.Peers()
	require.Len(t, handles, 1)
	require.NotNil(t, handles[0].Region)
	handles[0].Region.StartKey[0] = 'x'
	meta := peer.RegionMeta()
	require.Equal(t, byte('c'), meta.StartKey[0])

	rs.StopPeer(peer.ID())
	require.Empty(t, rs.Peers())
}

func TestStorePersistsRegionMetadata(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := raftmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = metaStore.Close() })

	rs := store.NewStoreWithConfig(store.Config{
		LocalMeta: metaStore,
	})

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              3,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &raftmeta.RegionMeta{
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
	require.Equal(t, raftmeta.RegionStateRunning, metas[0].State)
	metricsSnap := rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Running)

	snapshot := metaStore.Snapshot()
	require.Len(t, snapshot, 1)
	meta, ok := snapshot[500]
	require.True(t, ok)
	require.Equal(t, raftmeta.RegionStateRunning, meta.State)
	require.Zero(t, meta.Epoch.Version)

	updated := raftmeta.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch: raftmeta.RegionEpoch{
			Version:     4,
			ConfVersion: 6,
		},
		State: raftmeta.RegionStateRunning,
		Peers: []raftmeta.PeerMeta{
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
	require.Equal(t, raftmeta.RegionStateRunning, metaByID.State)
	_, ok = rs.RegionMetaByID(999)
	require.False(t, ok)

	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, uint64(4), meta.Epoch.Version)
	require.Len(t, meta.Peers, 2)

	require.NoError(t, rs.UpdateRegionState(500, raftmeta.RegionStateRemoving))

	metas = rs.RegionMetas()
	require.Equal(t, raftmeta.RegionStateRemoving, metas[0].State)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Removing)
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, raftmeta.RegionStateRemoving, meta.State)

	rs.StopPeer(p.ID())
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, raftmeta.RegionStateRemoving, meta.State)

	require.NoError(t, rs.UpdateRegionState(500, raftmeta.RegionStateTombstone))

	metas = rs.RegionMetas()
	require.Equal(t, raftmeta.RegionStateTombstone, metas[0].State)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Tombstone)
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, raftmeta.RegionStateTombstone, meta.State)

	err = rs.UpdateRegionState(500, raftmeta.RegionStateRunning)
	require.Error(t, err)

	require.NoError(t, rs.RemoveRegion(500))

	metas = rs.RegionMetas()
	require.Len(t, metas, 0)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Zero(t, metricsSnap.Total)

	snapshot = metaStore.Snapshot()
	require.Len(t, snapshot, 0)

	child := raftmeta.RegionMeta{
		ID:       600,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    raftmeta.RegionStateRunning,
	}
	parent := raftmeta.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("m"),
		State:    raftmeta.RegionStateRunning,
	}
	require.NoError(t, rs.UpdateRegion(parent))
	require.NoError(t, rs.UpdateRegion(child))

	metas = rs.RegionMetas()
	require.Len(t, metas, 2)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(2), metricsSnap.Total)
	require.Equal(t, uint64(2), metricsSnap.Running)
}

func TestStoreSchedulerReceivesRegionHeartbeats(t *testing.T) {
	sink := newTestSchedulerSink()
	rs := store.NewStoreWithConfig(store.Config{Scheduler: sink, StoreID: 1})
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
		Region: &raftmeta.RegionMeta{
			ID:       42,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())

	snapshot := sink.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, uint64(42), snapshot[0].Meta.ID)
	require.False(t, snapshot[0].LastHeartbeat.IsZero())

	require.NoError(t, rs.UpdateRegionState(42, raftmeta.RegionStateRemoving))
	snapshot = sink.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, raftmeta.RegionStateRemoving, snapshot[0].Meta.State)

	require.NoError(t, rs.RemoveRegion(42))
	snapshot = sink.RegionSnapshot()
	require.Empty(t, snapshot)
}

func TestStoreSchedulerPeriodicHeartbeats(t *testing.T) {
	coord := newTestSchedulerSink()
	rs := store.NewStoreWithConfig(store.Config{
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
		Region: &raftmeta.RegionMeta{
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
	require.Equal(t, uint64(77), regionSnap[0].Meta.ID)
	require.False(t, regionSnap[0].LastHeartbeat.IsZero())
}

func TestStoreProposeCommandPrewriteCommit(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	applier := kv.NewApplier(db, nil)
	st := store.NewStoreWithConfig(store.Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &raftmeta.RegionMeta{
		ID:       101,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: raftmeta.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []raftmeta.PeerMeta{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
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
		LocalMeta: localMeta,
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
	val, _, err := reader.GetValue([]byte("cmd-key"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("cmd-value"), val)
}

func TestStoreProposeCommandRejectsDuplicateRequestID(t *testing.T) {
	db, localMeta := openStoreDB(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	applier := func(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return &pb.RaftCmdResponse{
			Header: req.GetHeader(),
		}, nil
	}
	st := store.NewStoreWithConfig(store.Config{
		StoreID:        1,
		CommandApplier: applier,
		CommandTimeout: time.Second,
	})
	t.Cleanup(func() { st.Close() })

	region := &raftmeta.RegionMeta{
		ID:       777,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    raftmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []raftmeta.PeerMeta{{StoreID: 1, PeerID: 17}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              17,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		LocalMeta: localMeta,
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 17}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	req := func() *pb.RaftCmdRequest {
		return &pb.RaftCmdRequest{
			Header: &pb.CmdHeader{
				RegionId:    region.ID,
				RegionEpoch: &pb.RegionEpoch{Version: 1, ConfVer: 1},
				RequestId:   9001,
			},
			Requests: []*pb.Request{{
				CmdType: pb.CmdType_CMD_GET,
				Cmd: &pb.Request_Get{Get: &pb.GetRequest{
					Key: []byte("dup-key"),
				}},
			}},
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := st.ProposeCommand(req())
		firstDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first proposal did not enter apply path in time")
	}

	start := time.Now()
	_, err = st.ProposeCommand(req())
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate proposal id")
	require.Less(t, elapsed, 300*time.Millisecond)

	close(release)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first proposal did not finish in time")
	}
}

func TestStoreProposeCommandNotLeader(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := kv.NewApplier(db, nil)
	st := store.NewStoreWithConfig(store.Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &raftmeta.RegionMeta{
		ID:       202,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch:    raftmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []raftmeta.PeerMeta{{StoreID: 2, PeerID: 5}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              5,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		LocalMeta: localMeta,
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
	db, localMeta := openStoreDB(t)
	applier := kv.NewApplier(db, nil)
	st := store.NewStoreWithConfig(store.Config{StoreID: 3, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &raftmeta.RegionMeta{
		ID:       303,
		StartKey: []byte("a"),
		EndKey:   []byte("h"),
		Epoch:    raftmeta.RegionEpoch{Version: 2, ConfVersion: 1},
		Peers:    []raftmeta.PeerMeta{{StoreID: 3, PeerID: 7}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		LocalMeta: localMeta,
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

	parentMeta := raftmeta.RegionMeta{
		ID:       3000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 31}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 31}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := raftmeta.RegionMeta{
		ID:       3001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 32}},
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

	parentMeta := raftmeta.RegionMeta{
		ID:       4000,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 41}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 41}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	sourceMeta := raftmeta.RegionMeta{
		ID:       4001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 42}},
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

	parentMeta := raftmeta.RegionMeta{
		ID:       5000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 51}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 51}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := raftmeta.RegionMeta{
		ID:       5001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []raftmeta.PeerMeta{{StoreID: storeID, PeerID: 52}},
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
