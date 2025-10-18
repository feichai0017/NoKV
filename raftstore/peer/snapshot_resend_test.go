package peer

import (
	"testing"

	"github.com/stretchr/testify/require"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestSnapshotResendQueueRecordAndDrop(t *testing.T) {
	q := newSnapshotResendQueue()
	msg1 := myraft.Message{
		Type: myraft.MsgSnapshot,
		To:   2,
		From: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 12, Term: 3},
			Data:     []byte("snap-1"),
		},
	}
	msg2 := myraft.Message{
		Type: myraft.MsgSnapshot,
		To:   3,
		From: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 42, Term: 4},
			Data:     []byte("snap-2"),
		},
	}
	q.record(msg1)
	q.record(msg2)

	got, ok := q.pendingFor(2)
	require.True(t, ok, "expected snapshot for peer 2")
	require.Equal(t, msg1.Snapshot.Metadata, got.Snapshot.Metadata)

	q.drop(2)
	_, ok = q.pendingFor(2)
	require.False(t, ok, "expected peer 2 entry to be cleared")

	got2, ok := q.pendingFor(3)
	require.True(t, ok, "expected snapshot for peer 3")
	require.Equal(t, msg2.Snapshot.Metadata, got2.Snapshot.Metadata)
}

type recordingTransport struct {
	messages []myraft.Message
}

func (rt *recordingTransport) Send(msg myraft.Message) {
	rt.messages = append(rt.messages, msg)
}

func TestPeerResendSnapshot(t *testing.T) {
	transport := &recordingTransport{}
	peer, err := NewPeer(&Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 16,
			PreVote:         true,
		},
		Transport: transport,
		Apply: func(entries []myraft.Entry) error {
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, peer.Bootstrap([]myraft.Peer{{ID: 1}}))
	require.NoError(t, peer.Campaign())
	require.NoError(t, peer.Flush())

	msg := myraft.Message{
		Type: myraft.MsgSnapshot,
		To:   2,
		From: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 100, Term: 7},
			Data:     []byte("snap-data"),
		},
	}
	peer.snapshotQueue.record(msg)

	require.True(t, peer.ResendSnapshot(2))
	require.Len(t, transport.messages, 1)
	require.Equal(t, msg.Snapshot.Metadata, transport.messages[0].Snapshot.Metadata)

	transport.messages = nil
	require.NoError(t, peer.Tick())
	require.Len(t, transport.messages, 1, "tick should resend snapshot")
	require.Equal(t, msg.Snapshot.Metadata, transport.messages[0].Snapshot.Metadata)

	transport.messages = nil
	require.NoError(t, peer.Tick())
	require.Len(t, transport.messages, 1, "subsequent ticks should continue resending until ack")

	peer.snapshotQueue.drop(2)
	transport.messages = nil
	require.NoError(t, peer.Tick())
	require.Empty(t, transport.messages, "drop should stop resends")
	require.False(t, peer.ResendSnapshot(2), "snapshot should be cleared after drop")
}
