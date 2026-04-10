package store

import (
	"context"
	"errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestTransitionExecutorLeaderPeerAndPublishGuards(t *testing.T) {
	var nilStore *Store
	_, err := nilStore.leaderPeer(1)
	require.Error(t, err)
	require.NoError(t, nilStore.publishPlannedRootEvent(1, rootevent.Event{}, ""))

	rs := NewStore(Config{})
	require.NoError(t, rs.publishPlannedRootEvent(1, rootevent.RegionTombstoned(1), ""))

	_, err = rs.leaderPeer(0)
	require.Error(t, err)
	_, err = rs.leaderPeer(99)
	require.Error(t, err)

	nonLeaderStore, _, region := startTransitionExecutorStore(t, nil, false)
	_, err = nonLeaderStore.leaderPeer(region.ID)
	require.Error(t, err)

	errSink := &errorSchedulerSink{err: errors.New("publish failed")}
	leaderStore, _, leaderRegion := startTransitionExecutorStore(t, errSink, true)
	err = leaderStore.publishPlannedRootEvent(leaderRegion.ID, rootevent.RegionTombstoned(leaderRegion.ID), "split")
	require.ErrorContains(t, err, "publish split target")
}

func TestTransitionExecutorDispatchesConfChangeAndAdminTargets(t *testing.T) {
	sink := newTestSchedulerSink()
	rs, _, region := startTransitionExecutorStore(t, sink, true)

	confTarget, err := rs.buildPeerChangeTarget(region.ID, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	})
	require.NoError(t, err)
	require.NoError(t, rs.executeTransitionTarget(confTarget))
	require.Eventually(t, func() bool {
		meta, ok := rs.RegionMetaByID(region.ID)
		return ok && peerIndexByID(meta.Peers, 2) >= 0
	}, time.Second, 10*time.Millisecond)
	require.True(t, historyContainsRootKind(sink.EventHistory(), rootevent.KindPeerAdditionPlanned))

	sink.ResetHistory()
	child := localmeta.RegionMeta{
		ID:     402,
		EndKey: []byte("z"),
		Peers:  []metaregion.Peer{{StoreID: 1, PeerID: 2}},
	}
	splitTarget, err := rs.buildSplitTarget(region.ID, child, []byte("m"))
	require.NoError(t, err)
	require.NoError(t, rs.executeTransitionTarget(splitTarget))
	require.True(t, historyContainsRootKind(sink.EventHistory(), rootevent.KindRegionSplitPlanned))
}

func TestTransitionExecutorProposalAndTerminalOutcomeGuards(t *testing.T) {
	var nilStore *Store
	require.Error(t, nilStore.proposeTransition(transitionTarget{RegionID: 1}))
	require.Error(t, nilStore.executeTransitionTarget(transitionTarget{}))
	require.Error(t, nilStore.applyTerminalOutcome(terminalOutcome{}))

	rs := NewStore(Config{Scheduler: newTestSchedulerSink()})
	require.NoError(t, rs.executeTransitionTarget(transitionTarget{Noop: true}))
	require.Error(t, rs.executeTransitionTarget(transitionTarget{RegionID: 1}))
	require.Error(t, rs.proposeTransition(transitionTarget{ConfChange: &raftpb.ConfChangeV2{}}))
	require.Error(t, rs.proposeTransition(transitionTarget{RegionID: 1}))

	applyErr := errors.New("apply failed")
	err := rs.applyTerminalOutcome(terminalOutcome{
		Event: rootevent.RegionTombstoned(55),
		Apply: func() error { return applyErr },
	})
	require.ErrorIs(t, err, applyErr)

	require.NoError(t, rs.applyTerminalOutcome(terminalOutcome{
		Event:  rootevent.RegionTombstoned(55),
		Action: "remove",
	}))
	require.True(t, rs.hasPendingRegionUpdate(55))
}

type errorSchedulerSink struct {
	err error
}

func (*errorSchedulerSink) ReportRegionHeartbeat(_ context.Context, _ uint64) {}

func (s *errorSchedulerSink) PublishRootEvent(_ context.Context, _ rootevent.Event) error {
	return s.err
}

func (*errorSchedulerSink) StoreHeartbeat(context.Context, StoreStats) []Operation { return nil }
func (*errorSchedulerSink) Status() SchedulerStatus                                { return SchedulerStatus{} }
func (*errorSchedulerSink) Close() error                                           { return nil }

func startTransitionExecutorStore(t *testing.T, scheduler SchedulerClient, campaign bool) (*Store, *peer.Peer, localmeta.RegionMeta) {
	t.Helper()
	db, localMeta := openStoreDB(t)
	region := localmeta.RegionMeta{
		ID:       401,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
		State:    metaregion.ReplicaStateRunning,
	}
	rs := NewStore(Config{Scheduler: scheduler, StoreID: 1})
	t.Cleanup(rs.Close)
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
		Apply:     func([]myraft.Entry) error { return nil },
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    localmeta.CloneRegionMetaPtr(&region),
	}
	p, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { rs.StopPeer(p.ID()) })
	if campaign {
		require.NoError(t, p.Campaign())
		require.Eventually(t, func() bool {
			return p.Status().RaftState == myraft.StateLeader
		}, time.Second, 10*time.Millisecond)
	}
	return rs, p, region
}

func historyContainsRootKind(events []schedulerEvent, kind rootevent.Kind) bool {
	for _, ev := range events {
		if ev.rootKind == kind {
			return true
		}
	}
	return false
}
