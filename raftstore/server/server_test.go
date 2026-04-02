package server_test

import (
	"context"
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/legacy"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/kv"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
)

type fakeMVCCStore struct{}

func (fakeMVCCStore) ApplyInternalEntries(entries []*entrykv.Entry) error { return nil }
func (fakeMVCCStore) GetInternalEntry(cf entrykv.ColumnFamily, key []byte, version uint64) (*entrykv.Entry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (fakeMVCCStore) NewInternalIterator(opt *utils.Options) utils.Iterator { return nil }

type fakeRaftLog struct{}

func (fakeRaftLog) Open(groupID uint64, meta *localmeta.Store) (engine.PeerStorage, error) {
	return nil, nil
}

func openTestDB(t *testing.T) (*NoKV.DB, *localmeta.Store) {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(opt.WorkDir, nil)
	require.NoError(t, err)
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() { _ = localMeta.Close() })
	return db, localMeta
}

func TestServerStartsNoKVService(t *testing.T) {
	db, _ := openTestDB(t)
	srv, err := serverpkg.New(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: db.RaftLog(),
		},
		Store: storepkg.Config{
			StoreID: 1,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	addr := srv.Addr()
	require.NotEmpty(t, addr)
	require.NotNil(t, srv.Store())
	require.NotNil(t, srv.Transport())
	require.NotNil(t, srv.Service())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := kvrpcpb.NewNoKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.KvGet(ctx, &kvrpcpb.KvGetRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServerRequiresSnapshotBridge(t *testing.T) {
	srv, err := serverpkg.New(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: fakeMVCCStore{},
			Raft: fakeRaftLog{},
		},
		Store: storepkg.Config{
			StoreID: 1,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.Nil(t, srv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot bridge")
}

func TestServerStartsRaftAdminService(t *testing.T) {
	db, localMeta := openTestDB(t)
	srv, err := serverpkg.New(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: db.RaftLog(),
		},
		Store: storepkg.Config{
			StoreID:   1,
			LocalMeta: localMeta,
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
	t.Cleanup(func() { _ = srv.Close() })

	region := localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   nil,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	startRegionPeer(t, testNode{
		storeID:   1,
		peerID:    101,
		region:    region,
		db:        db,
		localMeta: localMeta,
		srv:       srv,
	})

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	adminClient := adminpb.NewRaftAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	statusResp, err := adminClient.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: region.ID})
	require.NoError(t, err)
	require.True(t, statusResp.GetKnown())
	require.True(t, statusResp.GetHosted())
	require.True(t, statusResp.GetLeader())
	require.Equal(t, uint64(101), statusResp.GetLocalPeerId())
	require.GreaterOrEqual(t, statusResp.GetAppliedIndex(), uint64(0))

	addResp, err := adminClient.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: region.ID,
		StoreId:  2,
		PeerId:   202,
	})
	require.NoError(t, err)
	require.Len(t, addResp.GetRegion().GetPeers(), 2)

	statusResp, err = adminClient.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: region.ID})
	require.NoError(t, err)
	require.True(t, statusResp.GetKnown())
	require.Len(t, statusResp.GetRegion().GetPeers(), 2)

	_, err = adminClient.TransferLeader(ctx, &adminpb.TransferLeaderRequest{
		RegionId: region.ID,
		PeerId:   101,
	})
	require.NoError(t, err)

	_, err = adminClient.RemovePeer(ctx, &adminpb.RemovePeerRequest{
		RegionId: region.ID,
		PeerId:   202,
	})
	require.NoError(t, err)
}

type testNode struct {
	storeID   uint64
	peerID    uint64
	region    localmeta.RegionMeta
	db        *NoKV.DB
	localMeta *localmeta.Store
	srv       *serverpkg.Server
	addr      string
}

type staticRegionResolver struct {
	regions []*metapb.RegionMeta
}

func (r *staticRegionResolver) GetRegionByKey(_ context.Context, req *pdpb.GetRegionByKeyRequest) (*pdpb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolver: nil request")
	}
	for _, region := range r.regions {
		if regionContainsKey(region, req.GetKey()) {
			return &pdpb.GetRegionByKeyResponse{
				RegionDescriptor: metacodec.DescriptorToProto(metacodec.DescriptorFromLegacyRegionMeta(cloneRegionMetaPB(region))),
			}, nil
		}
	}
	return &pdpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) Close() error { return nil }

func TestServerWithClientTwoPhaseCommit(t *testing.T) {
	nodes := []testNode{
		{
			storeID: 1,
			peerID:  101,
			region: localmeta.RegionMeta{
				ID:       1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
				Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
			},
		},
		{
			storeID: 2,
			peerID:  201,
			region: localmeta.RegionMeta{
				ID:       2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
				Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 201}},
			},
		},
	}

	for i := range nodes {
		workDir := t.TempDir()
		opt := NoKV.NewDefaultOptions()
		opt.WorkDir = workDir
		localMeta, err := localmeta.OpenLocalStore(workDir, nil)
		require.NoError(t, err)
		opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
		db, err := NoKV.Open(opt)
		require.NoError(t, err)
		nodes[i].db = db
		nodes[i].localMeta = localMeta

		srv, err := serverpkg.New(serverpkg.Config{
			Storage: serverpkg.Storage{
				MVCC: db,
				Raft: db.RaftLog(),
			},
			Store: storepkg.Config{
				StoreID:   nodes[i].storeID,
				LocalMeta: localMeta,
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
			if nodes[i].localMeta != nil {
				_ = nodes[i].localMeta.Close()
			}
		}
	}()

	for i := range nodes {
		startRegionPeer(t, nodes[i])
	}

	stores := make([]client.StoreEndpoint, 0, len(nodes))
	regions := make([]*metapb.RegionMeta, 0, len(nodes))
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
	mutations := []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("zoo"), Value: []byte("value-z")},
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
	peerStorage, err := n.db.RaftLog().Open(n.region.ID, n.localMeta)
	require.NoError(t, err)
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
		Storage:   peerStorage,
		GroupID:   n.region.ID,
		Region:    localmeta.CloneRegionMetaPtr(&n.region),
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

func regionMetaToPB(meta localmeta.RegionMeta) *metapb.RegionMeta {
	peers := make([]*metapb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &metapb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &metapb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
		Peers:            peers,
	}
}

func cloneRegionMetaPB(meta *metapb.RegionMeta) *metapb.RegionMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*metapb.RegionMeta)
}

func regionContainsKey(meta *metapb.RegionMeta, key []byte) bool {
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
	minLen := min(len(b), len(a))
	for i := range minLen {
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
