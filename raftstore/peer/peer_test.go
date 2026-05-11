package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type noopPayloadTransport struct{}

func (noopPayloadTransport) Send(context.Context, myraft.Message) {}

type payloadTestStorage struct {
	*myraft.MemoryStorage
	onApply func()
}

func newPayloadTestStorage() *payloadTestStorage {
	return &payloadTestStorage{MemoryStorage: myraft.NewMemoryStorage()}
}

func (s *payloadTestStorage) ApplySnapshot(snap myraft.Snapshot) error {
	if s.onApply != nil {
		s.onApply()
	}
	return s.MemoryStorage.ApplySnapshot(snap)
}

func (s *payloadTestStorage) SetHardState(st myraft.HardState) error {
	return s.MemoryStorage.SetHardState(st)
}

func newTestPeer(t *testing.T, storage raftlog.PeerStorage, apply ApplyFunc) *Peer {
	t.Helper()
	if apply == nil {
		apply = func([]myraft.Entry) error { return nil }
	}
	p, err := NewPeer(&Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopPayloadTransport{},
		Apply:     apply,
		Storage:   storage,
		GroupID:   1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestEnableLeaseReadConfig(t *testing.T) {
	cfg := EnableLeaseRead(myraft.Config{ID: 7, ElectionTick: 5, HeartbeatTick: 1})
	require.True(t, cfg.CheckQuorum)
	require.Equal(t, myraft.ReadOnlyLeaseBased, cfg.ReadOnlyOption)
	require.Equal(t, uint64(7), cfg.ID)
}

func TestFastLeaseReadRequiresLeaseReadConfig(t *testing.T) {
	_, err := NewPeer(&Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:     noopPayloadTransport{},
		Apply:         func([]myraft.Entry) error { return nil },
		Storage:       newPayloadTestStorage(),
		FastLeaseRead: true,
		GroupID:       1,
	})
	require.ErrorIs(t, err, errFastLeaseReadRequiresLeaseRead)
}

func TestFastLeaseReadSingleNodeLinearizableReadCompletes(t *testing.T) {
	storage := newPayloadTestStorage()
	cfg := EnableLeaseRead(myraft.Config{
		ID:              11,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	})
	p, err := NewPeer(&Config{
		RaftConfig:    cfg,
		Transport:     noopPayloadTransport{},
		Apply:         func([]myraft.Entry) error { return nil },
		Storage:       storage,
		FastLeaseRead: true,
		GroupID:       1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	index, err := p.LinearizableRead(ctx)
	require.NoError(t, err)
	require.Greater(t, index, uint64(0))
	require.NoError(t, p.WaitApplied(ctx, index))
	p.readMu.Lock()
	defer p.readMu.Unlock()
	require.Empty(t, p.pendingReads)
}

func TestBoundedStaleReadIndexUsesAppliedProgress(t *testing.T) {
	storage := newPayloadTestStorage()
	cfg := EnableLeaseRead(myraft.Config{
		ID:              11,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	})
	p, err := NewPeer(&Config{
		RaftConfig:    cfg,
		Transport:     noopPayloadTransport{},
		Apply:         func([]myraft.Entry) error { return nil },
		Storage:       storage,
		FastLeaseRead: true,
		GroupID:       1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	index, ok := p.BoundedStaleReadIndex(0, 0)
	require.False(t, ok)
	require.Zero(t, index)

	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())

	index, ok = p.BoundedStaleReadIndex(0, 0)
	require.True(t, ok)
	require.Equal(t, p.AppliedIndex(), index)
}

func TestSnapshotExportsPayloadAndRefreshesMetadata(t *testing.T) {
	storage := newPayloadTestStorage()
	require.NoError(t, storage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 2,
			Term:  1,
		},
	}))
	require.NoError(t, storage.Append([]raftpb.Entry{
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}))

	p := newTestPeer(t, storage, nil)
	p.region = &localmeta.RegionMeta{
		ID: 7,
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
	}
	p.snapshotExport = func(region localmeta.RegionMeta) ([]byte, error) {
		require.Equal(t, uint64(7), region.ID)
		return []byte("logical-payload"), nil
	}

	snap, err := p.Snapshot()
	require.NoError(t, err)
	require.Equal(t, []byte("logical-payload"), snap.Data)
	require.Equal(t, uint64(4), snap.Metadata.Index)
	require.Equal(t, uint64(3), snap.Metadata.Term)
	require.ElementsMatch(t, []uint64{11, 22}, snap.Metadata.ConfState.Voters)
}

func TestLinearizableReadSingleNode(t *testing.T) {
	p := newTestPeer(t, newPayloadTestStorage(), nil)
	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	index, err := p.LinearizableRead(ctx)
	require.NoError(t, err)
	require.Greater(t, index, uint64(0))
	require.NoError(t, p.WaitApplied(ctx, index))
}

func TestLinearizableReadCanceledClearsPending(t *testing.T) {
	p := newTestPeer(t, newPayloadTestStorage(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	index, err := p.LinearizableRead(ctx)
	require.Zero(t, index)
	require.ErrorIs(t, err, context.Canceled)

	p.readMu.Lock()
	defer p.readMu.Unlock()
	require.Empty(t, p.pendingReads)
}

func TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks(t *testing.T) {
	var applied []string
	p := newTestPeer(t, newPayloadTestStorage(), func(entries []myraft.Entry) error {
		for _, entry := range entries {
			if len(entry.Data) > 0 {
				applied = append(applied, string(entry.Data))
			}
		}
		return nil
	})
	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())

	failpoints.Set(failpoints.AfterReadyAdvanceBeforeSend)
	t.Cleanup(func() { failpoints.Set(failpoints.None) })
	err := p.Propose([]byte("first-after-advance"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "after ready advance before send")

	failpoints.Set(failpoints.None)
	require.NoError(t, p.Flush())
	require.NoError(t, p.Propose([]byte("second-after-recovery")))
	require.NoError(t, p.Flush())
	require.Contains(t, applied, "second-after-recovery")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	index, err := p.LinearizableRead(ctx)
	require.NoError(t, err)
	require.NoError(t, p.WaitApplied(ctx, index))
}

func TestPeerApplyErrorDoesNotAdvanceAppliedWatermark(t *testing.T) {
	applyErr := errors.New("apply failed")
	p := newTestPeer(t, newPayloadTestStorage(), func(entries []myraft.Entry) error {
		require.NotEmpty(t, entries)
		return applyErr
	})
	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())
	appliedBefore := p.AppliedIndex()

	require.ErrorIs(t, p.Propose([]byte("bad-apply")), applyErr)
	require.Equal(t, appliedBefore, p.AppliedIndex())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, p.WaitApplied(ctx, appliedBefore+1))
}

func TestPeerAsyncApplyKeepsContiguousAppliedWatermark(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })

	storage := newPayloadTestStorage()
	cfg := myraft.Config{
		ID:              11,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	p, err := NewPeer(&Config{
		RaftConfig: cfg,
		Transport:  noopPayloadTransport{},
		Apply:      func([]myraft.Entry) error { return nil },
		ApplyAsync: func(entries []myraft.Entry, done func(error)) error {
			require.Len(t, entries, 1)
			entry := entries[0]
			started <- entry.Index
			go func() {
				if string(entry.Data) == "first" {
					<-releaseFirst
				}
				done(nil)
			}()
			return nil
		},
		Storage: storage,
		GroupID: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	require.NoError(t, p.Bootstrap([]myraft.Peer{{ID: 11}}))
	require.NoError(t, p.Campaign())
	require.NoError(t, p.Flush())
	appliedBefore := p.AppliedIndex()

	require.NoError(t, p.Propose([]byte("first")))
	firstIndex := <-started
	require.Greater(t, firstIndex, appliedBefore)
	require.Equal(t, appliedBefore, p.AppliedIndex())

	require.NoError(t, p.Propose([]byte("second")))
	secondIndex := <-started
	require.Greater(t, secondIndex, firstIndex)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, appliedBefore, p.AppliedIndex())

	releaseOnce.Do(func() { close(releaseFirst) })
	require.Eventually(t, func() bool {
		return p.AppliedIndex() >= secondIndex
	}, time.Second, time.Millisecond)
}

func TestReadIndexHelpersDeliverAndCancel(t *testing.T) {
	p := &Peer{
		id:           9,
		node:         mustNewRawNode(t, 9, myraft.NewMemoryStorage()),
		pendingReads: make(map[string]chan uint64),
	}

	key, ch := p.startReadIndex()
	require.Len(t, p.pendingReads, 1)

	p.handleReadStates([]myraft.ReadState{{
		Index:      42,
		RequestCtx: []byte(key),
	}})

	select {
	case idx, ok := <-ch:
		require.True(t, ok)
		require.Equal(t, uint64(42), idx)
	default:
		t.Fatal("expected read state to be delivered")
	}
	require.Empty(t, p.pendingReads)

	key2, ch2 := p.startReadIndex()
	p.cancelReadIndex(key2)
	_, ok := <-ch2
	require.False(t, ok)
	require.Empty(t, p.pendingReads)
}

func TestPendingSnapshotPeekAndPop(t *testing.T) {
	p := &Peer{snapshotQueue: newSnapshotResendQueue()}
	msg := myraft.Message{
		Type: myraft.MsgSnapshot,
		To:   23,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 3},
		},
	}
	p.snapshotQueue.record(msg)

	snap, ok := p.PendingSnapshot()
	require.True(t, ok)
	require.Equal(t, uint64(8), snap.Metadata.Index)

	popped, ok := p.PopPendingSnapshot()
	require.True(t, ok)
	require.Equal(t, uint64(8), popped.Metadata.Index)

	_, ok = p.PendingSnapshot()
	require.False(t, ok)
}

func TestEnsureEmptySnapshotPayloadTargetAllowsRetry(t *testing.T) {
	storage := newPayloadTestStorage()
	require.NoError(t, storage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 3,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{11},
			},
		},
	}))
	require.NoError(t, storage.SetHardState(myraft.HardState{Term: 2, Commit: 3}))

	p := &Peer{
		storage:                   storage,
		allowSnapshotInstallRetry: true,
	}
	require.NoError(t, p.ensureEmptySnapshotPayloadTarget())
}

func TestPrepareMessagesAttachesSnapshotPayload(t *testing.T) {
	p := &Peer{
		region: &localmeta.RegionMeta{ID: 7, State: metaregion.ReplicaStateRunning},
		snapshotExport: func(region localmeta.RegionMeta) ([]byte, error) {
			require.Equal(t, uint64(7), region.ID)
			return []byte("payload"), nil
		},
	}
	msgs := []myraft.Message{{
		Type: myraft.MsgSnapshot,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 12, Term: 3},
		},
	}}
	require.NoError(t, p.prepareMessages(msgs))
	require.Equal(t, []byte("payload"), msgs[0].Snapshot.Data)
}

func TestHandleReadyImportsSnapshotPayloadBeforeRaftSnapshot(t *testing.T) {
	storage := newPayloadTestStorage()
	imported := false
	storage.onApply = func() {
		require.True(t, imported)
	}
	p := &Peer{
		storage: storage,
		raftLog: newRaftLogTracker(1),
		snapshotApply: func(payload []byte) (localmeta.RegionMeta, error) {
			require.Equal(t, []byte("payload"), payload)
			imported = true
			return localmeta.RegionMeta{ID: 9, State: metaregion.ReplicaStateRunning}, nil
		},
	}
	rd := myraft.Ready{
		Snapshot: myraft.Snapshot{
			Data: []byte("payload"),
			Metadata: raftpb.SnapshotMetadata{
				Index: 1,
				Term:  1,
			},
		},
	}
	require.NoError(t, p.handleReady(rd))
	require.True(t, imported)
	require.NotNil(t, p.RegionMeta())
	require.Equal(t, uint64(9), p.RegionMeta().ID)
}

func TestStepRejectsSnapshotPayloadOnNonEmptyPeer(t *testing.T) {
	storage := newPayloadTestStorage()
	p, err := NewPeer(&Config{
		RaftConfig: myraft.Config{
			ID:              5,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopPayloadTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Storage:   storage,
		SnapshotApply: func(payload []byte) (localmeta.RegionMeta, error) {
			t.Fatalf("snapshot apply hook should not run")
			return localmeta.RegionMeta{}, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, storage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{5},
			},
		},
	}))
	require.NoError(t, storage.SetHardState(myraft.HardState{Term: 1, Commit: 1}))
	msg := myraft.Message{
		Type: myraft.MsgSnapshot,
		From: 1,
		To:   5,
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("payload"),
			Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1},
		},
	}
	err = p.Step(msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot payload install requires empty peer state")
}

func mustNewRawNode(t *testing.T, id uint64, storage myraft.Storage) *myraft.RawNode {
	t.Helper()
	node, err := myraft.NewRawNode(&myraft.Config{
		ID:              id,
		Storage:         storage,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	})
	require.NoError(t, err)
	return node
}
