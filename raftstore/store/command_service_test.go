package store

import (
	"context"
	"errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

func TestStoreProposeCommandPrewriteCommit(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       101,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
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
		Storage:   mustPeerStorage(t, db, localMeta, 101),
		GroupID:   101,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	epoch := &metapb.RegionEpoch{Version: 1, ConfVersion: 1}
	prewrite := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
				Mutations: []*kvrpcpb.Mutation{{
					Op:    kvrpcpb.Mutation_Put,
					Key:   []byte("cmd-key"),
					Value: []byte("cmd-value"),
				}},
				PrimaryLock:  []byte("cmd-key"),
				StartVersion: 20,
				LockTtl:      3000,
			}},
		}},
	}
	resp, err := st.ProposeCommand(context.Background(), prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	require.Empty(t, resp.GetResponses()[0].GetPrewrite().GetErrors())
	require.NotZero(t, resp.GetHeader().GetRequestId())

	commit := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_COMMIT,
			Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
				Keys:          [][]byte{[]byte("cmd-key")},
				StartVersion:  20,
				CommitVersion: 40,
			}},
		}},
	}
	resp, err = st.ProposeCommand(context.Background(), commit)
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
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
		}, nil
	}
	st := NewStore(Config{
		StoreID:        1,
		CommandApplier: applier,
		CommandTimeout: time.Second,
	})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       777,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 17}},
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
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 17}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	req := func() *raftcmdpb.RaftCmdRequest {
		return &raftcmdpb.RaftCmdRequest{
			Header: &raftcmdpb.CmdHeader{
				RegionId:    region.ID,
				RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				RequestId:   9001,
			},
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_GET,
				Cmd: &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{
					Key: []byte("dup-key"),
				}},
			}},
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := st.ProposeCommand(context.Background(), req())
		firstDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first proposal did not enter apply path in time")
	}

	start := time.Now()
	_, err = st.ProposeCommand(context.Background(), req())
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
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &localmeta.RegionMeta{
		ID:       202,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 5}},
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
		Storage:   mustPeerStorage(t, db, localMeta, 202),
		GroupID:   202,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 5}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{StartVersion: 1}},
		}},
	}
	resp, err := st.ProposeCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
}

func TestStoreProposeCommandEpochMismatch(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{StoreID: 3, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &localmeta.RegionMeta{
		ID:       303,
		StartKey: []byte("a"),
		EndKey:   []byte("h"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 3, PeerID: 7}},
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
		Storage:   mustPeerStorage(t, db, localMeta, 303),
		GroupID:   303,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 7}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	badReq := &raftcmdpb.RaftCmdRequest{
		Header:   &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_PREWRITE}},
	}
	resp, err := st.ProposeCommand(context.Background(), badReq)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetEpochNotMatch())
}

func TestStoreProposeCommandSurvivesSchedulerUnavailable(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := &degradedSchedulerSink{
		testSchedulerSink: *newTestSchedulerSink(),
		status: SchedulerStatus{
			Mode:      SchedulerModeUnavailable,
			Degraded:  true,
			LastError: "pd unavailable",
		},
	}
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       909,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, PreVote: true},
		Transport:  noopTransport{},
		Storage:    mustPeerStorage(t, db, localMeta, 909),
		GroupID:    909,
		Region:     region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	epoch := &metapb.RegionEpoch{Version: 1, ConfVersion: 1}
	prewrite := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_PREWRITE, Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
			Mutations:   []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("sched-key"), Value: []byte("sched-value")}},
			PrimaryLock: []byte("sched-key"), StartVersion: 50, LockTtl: 3000,
		}}}},
	}
	resp, err := st.ProposeCommand(context.Background(), prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())

	commit := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_COMMIT, Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
			Keys: [][]byte{[]byte("sched-key")}, StartVersion: 50, CommitVersion: 80,
		}}}},
	}
	resp, err = st.ProposeCommand(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())

	status := st.SchedulerStatus()
	require.True(t, status.Degraded)
	require.Equal(t, SchedulerModeUnavailable, status.Mode)
	require.Contains(t, status.LastError, "pd unavailable")

	reader := percolator.NewReader(db)
	val, _, err := reader.GetValue([]byte("sched-key"), 90)
	require.NoError(t, err)
	require.Equal(t, []byte("sched-value"), val)
}

func TestStoreReadCommandValidation(t *testing.T) {
	store := NewStore(Config{})
	t.Cleanup(func() { store.Close() })

	if _, err := store.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{}); err == nil {
		t.Fatal("expected missing region id error")
	}

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}
	resp, err := store.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetRegionNotFound())
}

func TestStoreReadCommandStoreNotMatch(t *testing.T) {
	store := NewStore(Config{StoreID: 7})
	t.Cleanup(func() { store.Close() })

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1, StoreId: 9}}
	resp, err := store.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetStoreNotMatch())
	require.Equal(t, uint64(9), resp.GetRegionError().GetStoreNotMatch().GetRequestStoreId())
	require.Equal(t, uint64(7), resp.GetRegionError().GetStoreNotMatch().GetActualStoreId())
}

func TestReadOnlyRequestPredicate(t *testing.T) {
	require.False(t, isReadOnlyRequest(nil))

	readReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_GET},
		{CmdType: raftcmdpb.CmdType_CMD_SCAN},
	}}
	require.True(t, isReadOnlyRequest(readReq))

	writeReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_PREWRITE},
	}}
	require.False(t, isReadOnlyRequest(writeReq))
}

func TestStoreReadCommandExecutesAndTrimsScanResponse(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
			Responses: []*raftcmdpb.Response{{
				Cmd: &raftcmdpb.Response_Scan{Scan: &kvrpcpb.ScanResponse{
					Kvs: []*kvrpcpb.KV{
						{Key: []byte("a"), Value: []byte("drop-left")},
						{Key: []byte("b"), Value: []byte("keep-start")},
						{Key: []byte("l"), Value: []byte("keep-middle")},
						{Key: []byte("m"), Value: []byte("drop-right")},
						nil,
					},
				}},
			}},
		}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       404,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 11}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_SCAN,
			Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{
				StartKey: []byte("b"),
				Limit:    4,
			}},
		}},
	}
	resp, err := st.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	kvs := resp.GetResponses()[0].GetScan().GetKvs()
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("b"), kvs[0].GetKey())
	require.Equal(t, []byte("l"), kvs[1].GetKey())
}

func TestValidateRequestKeysAcrossCommandKinds(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       12,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
	}

	cases := []struct {
		name string
		req  *raftcmdpb.RaftCmdRequest
		kind func(*errorpb.RegionError) any
	}{
		{
			name: "get out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_GET,
				Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("a")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "scan out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_SCAN,
				Cmd:     &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("z")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "prewrite out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
				Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
					Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("a")}},
				}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "commit out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_COMMIT,
				Cmd:     &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "rollback out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
				Cmd:     &raftcmdpb.Request_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "resolve lock out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
				Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "check txn out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
				Cmd:     &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: &kvrpcpb.CheckTxnStatusRequest{PrimaryKey: []byte("a")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "unknown command",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType(255),
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetEpochNotMatch() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequestKeys(meta, tc.req)
			require.NotNil(t, err)
			require.NotNil(t, tc.kind(err))
		})
	}
}

func TestCommandServiceErrorHelpers(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       88,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 1},
			{StoreID: 2, PeerID: 2},
		},
	}

	notLeader := notLeaderError(meta, 2)
	require.NotNil(t, notLeader.GetNotLeader())
	require.Equal(t, uint64(88), notLeader.GetNotLeader().GetRegionId())
	require.Equal(t, uint64(2), notLeader.GetNotLeader().GetLeader().GetPeerId())
	require.Equal(t, uint64(2), notLeader.GetNotLeader().GetLeader().GetStoreId())

	key := []byte("a")
	out := keyNotInRegionError(meta, key)
	key[0] = 'z'
	meta.StartKey[0] = 'x'
	meta.EndKey[0] = 'y'
	require.Equal(t, []byte("a"), out.GetKeyNotInRegion().GetKey())
	require.Equal(t, []byte("b"), out.GetKeyNotInRegion().GetStartKey())
	require.Equal(t, []byte("m"), out.GetKeyNotInRegion().GetEndKey())
}

func TestStoreReadCommandRejectsMissingRequestsAndWriteCommands(t *testing.T) {
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
	})
	require.ErrorContains(t, err, "read command missing requests")

	_, err = st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{}},
		}},
	})
	require.ErrorContains(t, err, "read command must be read-only")
}

func TestStoreReadCommandRequiresApplyHandler(t *testing.T) {
	st, region := startReadLeaderStore(t, nil, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.ErrorContains(t, err, "command apply without handler")
}

func TestStoreReadCommandPropagatesApplyError(t *testing.T) {
	applyErr := errors.New("apply read boom")
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return nil, applyErr
	}, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.ErrorIs(t, err, applyErr)
}

func TestStoreReadCommandReturnsNilResponseWhenApplierDoes(t *testing.T) {
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return nil, nil
	}, 0)

	resp, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.NoError(t, err)
	require.Nil(t, resp)
}

func TestTrimScanResponseHandlesMismatchedResponses(t *testing.T) {
	meta := localmeta.RegionMeta{ID: 99, StartKey: []byte("b"), EndKey: []byte("m")}

	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_SCAN, Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("b")}}},
		{CmdType: raftcmdpb.CmdType_CMD_SCAN, Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("c")}}},
	}}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{
		{Cmd: &raftcmdpb.Response_Scan{Scan: &kvrpcpb.ScanResponse{
			Kvs: []*kvrpcpb.KV{
				{Key: []byte("a")},
				{Key: []byte("c")},
				nil,
			},
		}}},
	}}

	trimScanResponse(meta, req, resp)
	require.Len(t, resp.GetResponses(), 1)
	require.Len(t, resp.GetResponses()[0].GetScan().GetKvs(), 1)
	require.Equal(t, []byte("c"), resp.GetResponses()[0].GetScan().GetKvs()[0].GetKey())
}

func startReadLeaderStore(t *testing.T, applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error), timeout time.Duration) (*Store, *localmeta.RegionMeta) {
	t.Helper()

	db, localMeta := openStoreDB(t)
	st := NewStore(Config{StoreID: 1, CommandApplier: applier, CommandTimeout: timeout})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       451,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 17}},
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
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 17}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	return st, region
}
