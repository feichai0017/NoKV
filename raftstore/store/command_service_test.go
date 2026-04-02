package store

import (
	"context"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/legacy"
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

	epoch := &metapb.RegionEpoch{Version: 1, ConfVer: 1}
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
				RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
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
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}},
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
		Header:   &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}},
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

	epoch := &metapb.RegionEpoch{Version: 1, ConfVer: 1}
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
