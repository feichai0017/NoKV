package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/latch"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

type noopTransport struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

type testSchedulerSink struct {
	mu      sync.RWMutex
	regions map[uint64]regionHeartbeat
	stores  map[uint64]StoreStats
}

type slowSchedulerSink struct {
	testSchedulerSink
	publishDelay time.Duration
	removeDelay  time.Duration
}

type degradedSchedulerSink struct {
	testSchedulerSink
	status SchedulerStatus
}

type regionHeartbeat struct {
	Meta          raftmeta.RegionMeta
	LastHeartbeat time.Time
}

func newTestSchedulerSink() *testSchedulerSink {
	return &testSchedulerSink{
		regions: make(map[uint64]regionHeartbeat),
		stores:  make(map[uint64]StoreStats),
	}
}

func (s *testSchedulerSink) PublishRegion(_ context.Context, meta raftmeta.RegionMeta) {
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

func (s *testSchedulerSink) RemoveRegion(_ context.Context, id uint64) {
	if s == nil || id == 0 {
		return
	}
	s.mu.Lock()
	delete(s.regions, id)
	s.mu.Unlock()
}

func (s *testSchedulerSink) StoreHeartbeat(_ context.Context, stats StoreStats) []Operation {
	if s == nil || stats.StoreID == 0 {
		return nil
	}
	stats.UpdatedAt = time.Now()
	s.mu.Lock()
	s.stores[stats.StoreID] = stats
	s.mu.Unlock()
	return nil
}

func (s *testSchedulerSink) Status() SchedulerStatus {
	return SchedulerStatus{}
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

func (s *testSchedulerSink) StoreSnapshot() []StoreStats {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]StoreStats, 0, len(s.stores))
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

func (s *degradedSchedulerSink) Status() SchedulerStatus {
	return s.status
}

func (s *slowSchedulerSink) PublishRegion(ctx context.Context, meta raftmeta.RegionMeta) {
	if s.publishDelay > 0 {
		select {
		case <-time.After(s.publishDelay):
		case <-ctx.Done():
			return
		}
	}
	s.testSchedulerSink.PublishRegion(ctx, meta)
}

func (s *slowSchedulerSink) RemoveRegion(ctx context.Context, id uint64) {
	if s.removeDelay > 0 {
		select {
		case <-time.After(s.removeDelay):
		case <-ctx.Done():
			return
		}
	}
	s.testSchedulerSink.RemoveRegion(ctx, id)
}

func testPeerBuilder(storeID uint64) PeerBuilder {
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
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	localMeta, err := raftmeta.OpenLocalStore(opt.WorkDir, nil)
	require.NoError(t, err)
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() { _ = localMeta.Close() })
	return db, localMeta
}

func mustPeerStorage(t *testing.T, db *NoKV.DB, localMeta *raftmeta.Store, groupID uint64) engine.PeerStorage {
	t.Helper()
	storage, err := db.RaftLog().Open(groupID, localMeta)
	require.NoError(t, err)
	return storage
}

func newTestMVCCApplier(db NoKV.MVCCStore) func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	latches := latch.NewManager(512)
	return func(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
		resp := &pb.RaftCmdResponse{Header: req.GetHeader()}
		for _, r := range req.GetRequests() {
			if r == nil {
				continue
			}
			switch r.GetCmdType() {
			case pb.CmdType_CMD_PREWRITE:
				result := &pb.PrewriteResponse{Errors: percolator.Prewrite(db, latches, r.GetPrewrite())}
				resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Prewrite{Prewrite: result}})
			case pb.CmdType_CMD_COMMIT:
				err := percolator.Commit(db, latches, r.GetCommit())
				resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Commit{Commit: &pb.CommitResponse{Error: err}}})
			default:
				return nil, fmt.Errorf("unsupported test command %v", r.GetCmdType())
			}
		}
		return resp, nil
	}
}
