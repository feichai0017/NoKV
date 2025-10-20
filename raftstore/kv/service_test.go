package kv_test

import (
	"context"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"
)

type noopTransport struct{}

func (noopTransport) Send(myraft.Message) {}

func openTestDB(t *testing.T) *NoKV.DB {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyToDB(db *NoKV.DB) raftstore.ApplyFunc {
	return func(entries []myraft.Entry) error {
		for _, entry := range entries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			req, ok, err := command.Decode(entry.Data)
			if err != nil {
				return err
			}
			if ok {
				if _, err := kv.Apply(db, req); err != nil {
					return err
				}
				continue
			}
			var legacy pb.KV
			if err := proto.Unmarshal(entry.Data, &legacy); err != nil {
				return err
			}
			if len(legacy.GetValue()) == 0 {
				if err := db.DelCF(utils.CFDefault, legacy.GetKey()); err != nil {
					return err
				}
				continue
			}
			if err := db.SetCF(utils.CFDefault, legacy.GetKey(), legacy.GetValue()); err != nil {
				return err
			}
		}
		return nil
	}
}

type harnessConfig struct {
	storeID        uint64
	peerID         uint64
	regionID       uint64
	startKey       []byte
	endKey         []byte
	epochVersion   uint64
	epochConfVer   uint64
	campaignLeader bool
}

type serviceHarness struct {
	db      *NoKV.DB
	store   *store.Store
	service *kv.Service
	ctx     *pb.Context
	region  *manifest.RegionMeta
}

func newServiceHarness(t *testing.T, cfg harnessConfig) serviceHarness {
	t.Helper()
	if cfg.storeID == 0 {
		cfg.storeID = 1
	}
	if cfg.peerID == 0 {
		cfg.peerID = 1
	}
	if cfg.regionID == 0 {
		cfg.regionID = 100
	}
	if cfg.startKey == nil {
		cfg.startKey = []byte("a")
	}
	if cfg.endKey == nil {
		cfg.endKey = []byte("z")
	}
	if cfg.epochVersion == 0 {
		cfg.epochVersion = 1
	}
	if cfg.epochConfVer == 0 {
		cfg.epochConfVer = 1
	}

	db := openTestDB(t)
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{StoreID: cfg.storeID, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	meta := &manifest.RegionMeta{
		ID:       cfg.regionID,
		StartKey: append([]byte(nil), cfg.startKey...),
		EndKey:   append([]byte(nil), cfg.endKey...),
		Epoch:    manifest.RegionEpoch{Version: cfg.epochVersion, ConfVersion: cfg.epochConfVer},
		Peers:    []manifest.PeerMeta{{StoreID: cfg.storeID, PeerID: cfg.peerID}},
	}
	cfgPeer := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              cfg.peerID,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   cfg.regionID,
		Region:    meta,
		Apply:     applyToDB(db),
	}
	p, err := st.StartPeer(cfgPeer, []myraft.Peer{{ID: cfg.peerID}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	if cfg.campaignLeader {
		require.NoError(t, p.Campaign())
	}

	service := kv.NewService(st)
	ctx := &pb.Context{
		RegionId:    meta.ID,
		RegionEpoch: &pb.RegionEpoch{Version: cfg.epochVersion, ConfVer: cfg.epochConfVer},
	}
	return serviceHarness{
		db:      db,
		store:   st,
		service: service,
		ctx:     ctx,
		region:  meta,
	}
}

func prewriteKey(t *testing.T, service *kv.Service, ctx *pb.Context, key, value []byte, startVersion uint64) {
	t.Helper()
	req := &pb.KvPrewriteRequest{
		Context: ctx,
		Request: &pb.PrewriteRequest{
			Mutations: []*pb.Mutation{{
				Op:    pb.Mutation_Put,
				Key:   key,
				Value: value,
			}},
			PrimaryLock:  key,
			StartVersion: startVersion,
			LockTtl:      3000,
		},
	}
	resp, err := service.KvPrewrite(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	require.Empty(t, resp.GetResponse().GetErrors())
}

func commitKey(t *testing.T, service *kv.Service, ctx *pb.Context, key []byte, startVersion, commitVersion uint64) {
	t.Helper()
	req := &pb.KvCommitRequest{
		Context: ctx,
		Request: &pb.CommitRequest{
			Keys:          [][]byte{utils.SafeCopy(nil, key)},
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := service.KvCommit(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	require.Nil(t, resp.GetResponse().GetError())
}

func TestServicePrewriteCommit(t *testing.T) {
	db := openTestDB(t)
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &manifest.RegionMeta{
		ID:       501,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 11}},
	}
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   501,
		Region:    region,
		Apply:     applyToDB(db),
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 11}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	service := kv.NewService(st)
	ctx := &pb.Context{RegionId: region.ID, RegionEpoch: &pb.RegionEpoch{Version: 1, ConfVer: 1}}
	prewriteReq := &pb.KvPrewriteRequest{
		Context: ctx,
		Request: &pb.PrewriteRequest{
			Mutations: []*pb.Mutation{{
				Op:    pb.Mutation_Put,
				Key:   []byte("sk"),
				Value: []byte("sv"),
			}},
			PrimaryLock:  []byte("sk"),
			StartVersion: 7,
			LockTtl:      1000,
		},
	}
	preResp, err := service.KvPrewrite(context.Background(), prewriteReq)
	require.NoError(t, err)
	require.Nil(t, preResp.GetRegionError())
	require.Empty(t, preResp.GetResponse().GetErrors())

	commitReq := &pb.KvCommitRequest{Context: ctx, Request: &pb.CommitRequest{
		Keys:          [][]byte{[]byte("sk")},
		StartVersion:  7,
		CommitVersion: 9,
	}}
	commitResp, err := service.KvCommit(context.Background(), commitReq)
	require.NoError(t, err)
	require.Nil(t, commitResp.GetRegionError())
	require.Nil(t, commitResp.GetResponse().GetError())
}

func TestServiceRegionEpochMismatch(t *testing.T) {
	db := openTestDB(t)
	applier := kv.NewApplier(db)
	st := store.NewStoreWithConfig(store.Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &manifest.RegionMeta{
		ID:       601,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Epoch:    manifest.RegionEpoch{Version: 2, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 2, PeerID: 22}},
	}
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              22,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		WAL:       db.WAL(),
		Manifest:  db.Manifest(),
		GroupID:   601,
		Region:    region,
		Apply:     applyToDB(db),
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 22}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	service := kv.NewService(st)
	badCtx := &pb.Context{RegionId: region.ID, RegionEpoch: &pb.RegionEpoch{Version: 1, ConfVer: 1}}
	resp, err := service.KvCommit(context.Background(), &pb.KvCommitRequest{Context: badCtx, Request: &pb.CommitRequest{StartVersion: 1}})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetEpochNotMatch())
}

func TestServiceKvGetLeaderRead(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        3,
		peerID:         33,
		regionID:       701,
		campaignLeader: true,
	})

	key := []byte("alpha")
	value := []byte("value")
	prewriteReq := &pb.KvPrewriteRequest{
		Context: env.ctx,
		Request: &pb.PrewriteRequest{
			Mutations: []*pb.Mutation{{
				Op:    pb.Mutation_Put,
				Key:   key,
				Value: value,
			}},
			PrimaryLock:  key,
			StartVersion: 42,
			LockTtl:      3000,
		},
	}
	preResp, err := env.service.KvPrewrite(context.Background(), prewriteReq)
	require.NoError(t, err)
	require.NotNil(t, preResp)
	require.Nil(t, preResp.GetRegionError())
	require.NotNil(t, preResp.GetResponse())
	require.Empty(t, preResp.GetResponse().GetErrors())

	commitReq := &pb.KvCommitRequest{
		Context: env.ctx,
		Request: &pb.CommitRequest{
			Keys:          [][]byte{key},
			StartVersion:  42,
			CommitVersion: 46,
		},
	}
	commitResp, err := env.service.KvCommit(context.Background(), commitReq)
	require.NoError(t, err)
	require.NotNil(t, commitResp)
	require.Nil(t, commitResp.GetRegionError())
	require.NotNil(t, commitResp.GetResponse())
	require.Nil(t, commitResp.GetResponse().GetError())

	readReq := &pb.KvGetRequest{
		Context: env.ctx,
		Request: &pb.GetRequest{
			Key:     key,
			Version: 50,
		},
	}
	readResp, err := env.service.KvGet(context.Background(), readReq)
	require.NoError(t, err)
	require.NotNil(t, readResp)
	require.Nil(t, readResp.GetRegionError())
	require.NotNil(t, readResp.GetResponse())
	require.False(t, readResp.GetResponse().GetNotFound())
	require.Nil(t, readResp.GetResponse().GetError())
	require.Equal(t, value, readResp.GetResponse().GetValue())
}

func TestServiceKvGetNotLeader(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        4,
		peerID:         44,
		regionID:       801,
		campaignLeader: false,
	})

	req := &pb.KvGetRequest{
		Context: env.ctx,
		Request: &pb.GetRequest{
			Key:     []byte("missing"),
			Version: 1,
		},
	}
	resp, err := env.service.KvGet(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	notLeader := resp.GetRegionError().GetNotLeader()
	require.NotNil(t, notLeader)
	require.Equal(t, env.region.ID, notLeader.GetRegionId())
	require.Nil(t, resp.GetResponse())
}

func TestServiceKvScanLeaderRead(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        5,
		peerID:         55,
		regionID:       901,
		campaignLeader: true,
	})

	prewriteKey(t, env.service, env.ctx, []byte("ak"), []byte("value-a"), 100)
	commitKey(t, env.service, env.ctx, []byte("ak"), 100, 150)

	prewriteKey(t, env.service, env.ctx, []byte("bk"), []byte("value-b"), 200)
	commitKey(t, env.service, env.ctx, []byte("bk"), 200, 250)

	resp, err := env.service.KvScan(context.Background(), &pb.KvScanRequest{
		Context: env.ctx,
		Request: &pb.ScanRequest{
			StartKey:     []byte("a"),
			Limit:        4,
			Version:      500,
			IncludeStart: true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	kvs := resp.GetResponse().GetKvs()
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("ak"), kvs[0].GetKey())
	require.Equal(t, []byte("value-a"), kvs[0].GetValue())
	require.Equal(t, []byte("bk"), kvs[1].GetKey())
	require.Equal(t, []byte("value-b"), kvs[1].GetValue())

	next, err := env.service.KvScan(context.Background(), &pb.KvScanRequest{
		Context: env.ctx,
		Request: &pb.ScanRequest{
			StartKey:     []byte("ak"),
			Limit:        1,
			Version:      500,
			IncludeStart: false,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Nil(t, next.GetRegionError())
	require.NotNil(t, next.GetResponse())
	require.Len(t, next.GetResponse().GetKvs(), 1)
	require.Equal(t, []byte("bk"), next.GetResponse().GetKvs()[0].GetKey())
	require.Equal(t, []byte("value-b"), next.GetResponse().GetKvs()[0].GetValue())
}

func TestServiceKvScanNotLeader(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        6,
		peerID:         66,
		regionID:       1001,
		campaignLeader: false,
	})

	resp, err := env.service.KvScan(context.Background(), &pb.KvScanRequest{
		Context: env.ctx,
		Request: &pb.ScanRequest{
			StartKey: []byte("a"),
			Limit:    1,
			Version:  10,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
	require.Nil(t, resp.GetResponse())
}
