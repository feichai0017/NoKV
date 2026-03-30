package peer

import (
	"context"
	"testing"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func newTestPeer(t *testing.T, storage engine.PeerStorage, apply ApplyFunc) *Peer {
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
	p.region = &raftmeta.RegionMeta{
		ID: 7,
		Peers: []raftmeta.PeerMeta{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
	}
	p.snapshotExport = func(region raftmeta.RegionMeta) ([]byte, error) {
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

func TestEnsureEmptyLogicalSnapshotTargetAllowsRetry(t *testing.T) {
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
	require.NoError(t, p.ensureEmptyLogicalSnapshotTarget())
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
