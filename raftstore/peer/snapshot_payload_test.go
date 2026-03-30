package peer

import (
	"context"
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

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

func TestPrepareMessagesAttachesSnapshotPayload(t *testing.T) {
	p := &Peer{
		region: &raftmeta.RegionMeta{ID: 7, State: raftmeta.RegionStateRunning},
		snapshotExport: func(region raftmeta.RegionMeta) ([]byte, error) {
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

func TestHandleReadyImportsLogicalSnapshotBeforeRaftSnapshot(t *testing.T) {
	storage := newPayloadTestStorage()
	imported := false
	storage.onApply = func() {
		require.True(t, imported)
	}
	p := &Peer{
		storage: storage,
		raftLog: newRaftLogTracker(1),
		snapshotApply: func(payload []byte) (raftmeta.RegionMeta, error) {
			require.Equal(t, []byte("payload"), payload)
			imported = true
			return raftmeta.RegionMeta{ID: 9, State: raftmeta.RegionStateRunning}, nil
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

func TestStepRejectsLogicalSnapshotOnNonEmptyPeer(t *testing.T) {
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
		SnapshotApply: func(payload []byte) (raftmeta.RegionMeta, error) {
			t.Fatalf("snapshot apply hook should not run")
			return raftmeta.RegionMeta{}, nil
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
	require.Contains(t, err.Error(), "logical snapshot install requires empty peer state")
}

type noopPayloadTransport struct{}

func (noopPayloadTransport) Send(context.Context, myraft.Message) {}
