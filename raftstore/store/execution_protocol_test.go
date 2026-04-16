package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestExecutionProtocolRecordsAdmissionReasons(t *testing.T) {
	db, localMeta := openStoreDB(t)
	st := NewStore(Config{StoreID: 9, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(st.Close)

	region := &localmeta.RegionMeta{
		ID:       901,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 9, PeerID: 19}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              19,
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
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 19}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		}},
	}

	resp, err := st.ProposeCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.Equal(t, AdmissionReasonNotLeader, st.LastAdmission().Reason)

	require.NoError(t, p.Campaign())
	keyReq := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd: &raftcmdpb.Request_Get{
				Get: &kvrpcpb.GetRequest{Key: []byte("z")},
			},
		}},
	}
	readResp, err := st.ReadCommand(context.Background(), keyReq)
	require.NoError(t, err)
	require.NotNil(t, readResp.GetRegionError())
	require.Equal(t, AdmissionReasonKeyNotInRegion, st.LastAdmission().Reason)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = st.ProposeCommand(ctx, &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		}},
	})
	require.Error(t, err)
	require.Equal(t, AdmissionReasonCanceled, st.LastAdmission().Reason)
}

func TestExecutionProtocolTracksTopologyLifecycle(t *testing.T) {
	sink := newTestSchedulerSink()
	rs, _, region := startTransitionExecutorStore(t, sink, true)

	target, err := rs.buildPeerChangeTarget(region.ID, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	})
	require.NoError(t, err)

	require.NoError(t, rs.executeTransitionTarget(target))
	require.Equal(t, AdmissionReasonAccepted, rs.LastAdmission().Reason)
	require.Eventually(t, func() bool {
		return historyContainsRootKind(sink.EventHistory(), rootevent.KindPeerAdditionPlanned)
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		status, ok := rs.TopologyExecution(target.TransitionID)
		return ok && status.Outcome == ExecutionOutcomeApplied && status.Publish == PublishStateTerminalPublished
	}, 3*time.Second, 10*time.Millisecond)
}

func TestExecutionProtocolRetainsTerminalPublishFailure(t *testing.T) {
	sink := newFailingKindSchedulerSink(rootevent.KindPeerAdded)
	rs := NewStore(Config{Scheduler: sink})
	t.Cleanup(rs.Close)

	event := rootevent.PeerAdded(401, 2, 201, descriptorForOutcome(401, []byte("a"), []byte("z")))
	require.NoError(t, rs.applyTerminalOutcome(terminalOutcome{
		TransitionID: rootstate.TransitionIDFromEvent(event),
		RegionID:     401,
		Event:        event,
		Action:       "peer change",
	}))

	require.Eventually(t, func() bool {
		status, ok := rs.TopologyExecution(rootstate.TransitionIDFromEvent(event))
		return ok && status.Publish == PublishStateTerminalFailed
	}, time.Second, 10*time.Millisecond)

	sink.Allow(rootevent.KindPeerAdded)
	rs.flushRegionUpdates()
	require.Eventually(t, func() bool {
		status, ok := rs.TopologyExecution(rootstate.TransitionIDFromEvent(event))
		return ok && status.Publish == PublishStateTerminalPublished
	}, time.Second, 10*time.Millisecond)
	require.False(t, rs.hasPendingRegionUpdate(401))
}

func TestExecutionProtocolReportsRestartStatus(t *testing.T) {
	_, localMeta := openStoreDB(t)
	require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
		ID:       77,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
		State:    metaregion.ReplicaStateRunning,
	}))

	rs := NewStore(Config{LocalMeta: localMeta})
	t.Cleanup(rs.Close)

	status := rs.RestartStatus()
	require.Equal(t, RestartStateDegraded, status.State)
	require.Equal(t, []uint64{77}, status.MissingRaftPointer)

	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{GroupID: 77, AppliedIndex: 1, AppliedTerm: 1}))
	status = rs.RestartStatus()
	require.Equal(t, RestartStateReady, status.State)
	require.Empty(t, status.MissingRaftPointer)
}

func TestExecutionProtocolReplaysPendingRootEventsFromLocalMeta(t *testing.T) {
	_, localMeta := openStoreDB(t)
	event := rootevent.RegionTombstoned(77)
	require.NoError(t, localMeta.SavePendingRootEvent(localmeta.PendingRootEvent{
		Sequence: 1,
		Event:    event,
	}))

	sink := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         sink,
		LocalMeta:         localMeta,
		StoreID:           9,
		HeartbeatInterval: 25 * time.Millisecond,
		PublishTimeout:    50 * time.Millisecond,
	})
	t.Cleanup(rs.Close)

	require.Eventually(t, func() bool {
		return historyContainsRootKind(sink.EventHistory(), event.Kind)
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(localMeta.PendingRootEvents()) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestExecutionProtocolRetainsPendingRootEventsUntilPublishAck(t *testing.T) {
	_, localMeta := openStoreDB(t)
	event := rootevent.RegionTombstoned(88)
	require.NoError(t, localMeta.SavePendingRootEvent(localmeta.PendingRootEvent{
		Sequence: 1,
		Event:    event,
	}))

	sink := &slowSchedulerSink{testSchedulerSink: *newTestSchedulerSink(), publishDelay: 80 * time.Millisecond}
	rs := NewStore(Config{
		Scheduler:         sink,
		LocalMeta:         localMeta,
		StoreID:           9,
		HeartbeatInterval: 25 * time.Millisecond,
		PublishTimeout:    10 * time.Millisecond,
	})
	t.Cleanup(rs.Close)

	require.Eventually(t, func() bool {
		return len(localMeta.PendingRootEvents()) == 1
	}, time.Second, 10*time.Millisecond)

	sink.publishDelay = 0
	rs.flushRegionUpdates()

	require.Eventually(t, func() bool {
		return len(localMeta.PendingRootEvents()) == 0
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return historyContainsRootKind(sink.EventHistory(), event.Kind)
	}, time.Second, 10*time.Millisecond)
}

func TestExecutionProtocolRetainsRecentTopologyExecutionsOnly(t *testing.T) {
	rs := NewStore(Config{})
	t.Cleanup(rs.Close)

	for i := range executionTransitionRetention + 8 {
		rs.recordTopologyQueued(transitionTarget{
			TransitionID: fmt.Sprintf("transition-%03d", i),
			RegionID:     uint64(i + 1),
			Action:       "peer change",
		})
	}

	all := rs.TopologyExecutions()
	require.Len(t, all, executionTransitionRetention)
	_, ok := rs.TopologyExecution("transition-000")
	require.False(t, ok)
	latest, ok := rs.TopologyExecution(fmt.Sprintf("transition-%03d", executionTransitionRetention+7))
	require.True(t, ok)
	require.Equal(t, ExecutionOutcomeQueued, latest.Outcome)
}

type failingKindSchedulerSink struct {
	*testSchedulerSink
	mu      sync.RWMutex
	blocked map[rootevent.Kind]bool
}

func newFailingKindSchedulerSink(kinds ...rootevent.Kind) *failingKindSchedulerSink {
	blocked := make(map[rootevent.Kind]bool, len(kinds))
	for _, kind := range kinds {
		blocked[kind] = true
	}
	return &failingKindSchedulerSink{
		testSchedulerSink: newTestSchedulerSink(),
		blocked:           blocked,
	}
}

func (s *failingKindSchedulerSink) PublishRootEvent(ctx context.Context, event rootevent.Event) error {
	s.mu.RLock()
	blocked := s.blocked[event.Kind]
	s.mu.RUnlock()
	if blocked {
		return context.DeadlineExceeded
	}
	return s.testSchedulerSink.PublishRootEvent(ctx, event)
}

func (s *failingKindSchedulerSink) Allow(kind rootevent.Kind) {
	s.mu.Lock()
	delete(s.blocked, kind)
	s.mu.Unlock()
}
