package server_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type testNode struct {
	storeID uint64
	peerID  uint64
	region  manifest.RegionMeta
	db      *NoKV.DB
	srv     *raftstore.Server
	addr    string
}

type staticRegionResolver struct {
	regions []*pb.RegionMeta
}

func (r *staticRegionResolver) GetRegionByKey(_ context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolver: nil request")
	}
	for _, region := range r.regions {
		if regionContainsKey(region, req.GetKey()) {
			return &pb.GetRegionByKeyResponse{
				Region: cloneRegionMetaPB(region),
			}, nil
		}
	}
	return &pb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) Close() error { return nil }

func TestServerWithClientTwoPhaseCommit(t *testing.T) {
	nodes := []testNode{
		{
			storeID: 1,
			peerID:  101,
			region: manifest.RegionMeta{
				ID:       1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 101}},
			},
		},
		{
			storeID: 2,
			peerID:  201,
			region: manifest.RegionMeta{
				ID:       2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []manifest.PeerMeta{{StoreID: 2, PeerID: 201}},
			},
		},
	}

	for i := range nodes {
		workDir := t.TempDir()
		opt := NoKV.NewDefaultOptions()
		opt.WorkDir = workDir
		db := NoKV.Open(opt)
		nodes[i].db = db

		srv, err := raftstore.NewServer(raftstore.ServerConfig{
			DB: db,
			Store: raftstore.StoreConfig{
				StoreID: nodes[i].storeID,
			},
			Raft: myraft.Config{
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			TransportAddr: "127.0.0.1:0",
		})
		require.NoError(t, err)
		nodes[i].srv = srv
		nodes[i].addr = srv.Addr()
	}

	defer func() {
		for i := range nodes {
			if nodes[i].srv != nil {
				_ = nodes[i].srv.Close()
			}
			if nodes[i].db != nil {
				_ = nodes[i].db.Close()
			}
		}
	}()

	for i := range nodes {
		startRegionPeer(t, nodes[i])
	}

	stores := make([]client.StoreEndpoint, 0, len(nodes))
	regions := make([]*pb.RegionMeta, 0, len(nodes))
	for _, n := range nodes {
		stores = append(stores, client.StoreEndpoint{StoreID: n.storeID, Addr: n.addr})
		regions = append(regions, regionMetaToPB(n.region))
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].GetId() < regions[j].GetId() })

	cli, err := client.New(client.Config{
		Stores:         stores,
		RegionResolver: &staticRegionResolver{regions: regions},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mutations := []*pb.Mutation{
		{Op: pb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
		{Op: pb.Mutation_Put, Key: []byte("zoo"), Value: []byte("value-z")},
	}
	err = cli.Mutate(ctx, []byte("alfa"), mutations, 100, 150, 3000)
	require.NoError(t, err)

	resp, err := cli.Get(context.Background(), []byte("alfa"), 200)
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resp2, err := cli.Get(context.Background(), []byte("zoo"), 200)
	require.NoError(t, err)
	require.False(t, resp2.GetNotFound())
	require.Equal(t, []byte("value-z"), resp2.GetValue())

	kvs, err := cli.Scan(context.Background(), []byte("a"), 4, 200)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	err = cli.Delete(context.Background(), []byte("alfa"), 300, 350, 3000)
	require.NoError(t, err)
	delResp, err := cli.Get(context.Background(), []byte("alfa"), 400)
	require.NoError(t, err)
	require.True(t, delResp.GetNotFound())
}

func startRegionPeer(t *testing.T, n testNode) {
	store := n.srv.Store()
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              n.peerID,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: n.srv.Transport(),
		Apply:     kv.NewEntryApplier(n.db),
		WAL:       n.db.WAL(),
		Manifest:  n.db.Manifest(),
		GroupID:   n.region.ID,
		Region:    manifest.CloneRegionMetaPtr(&n.region),
	}
	bootstrap := []myraft.Peer{{ID: n.peerID}}
	p, err := store.StartPeer(cfg, bootstrap)
	require.NoError(t, err)
	require.NoError(t, p.Campaign())
	require.Eventually(t, func() bool {
		status := p.Status()
		return status.RaftState == myraft.StateLeader
	}, time.Second, 10*time.Millisecond)
}

func regionMetaToPB(meta manifest.RegionMeta) *pb.RegionMeta {
	peers := make([]*pb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
		Peers:            peers,
	}
}

func cloneRegionMetaPB(meta *pb.RegionMeta) *pb.RegionMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*pb.RegionMeta)
}

func regionContainsKey(meta *pb.RegionMeta, key []byte) bool {
	if meta == nil {
		return false
	}
	start := meta.GetStartKey()
	end := meta.GetEndKey()
	if len(start) > 0 && bytesCompare(start, key) > 0 {
		return false
	}
	return len(end) == 0 || bytesCompare(end, key) > 0
}

func bytesCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
