package replicated

import (
	"sync"
	"testing"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/stretchr/testify/require"
)

func TestGRPCTransportSendsToPeer(t *testing.T) {
	t1, err := NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t1.Close() })

	t2, err := NewGRPCTransport(2, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t2.Close() })

	t1.SetPeer(2, t2.Addr())

	var (
		mu   sync.Mutex
		got  []myraft.Message
		done = make(chan struct{}, 1)
	)
	t2.SetHandler(func(msg myraft.Message) error {
		mu.Lock()
		got = append(got, msg)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	err = t1.Send(myraft.Message{From: 1, To: 2, Type: myraft.MsgHeartbeat, Term: 3, Index: 9})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for grpc transport delivery")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	require.Equal(t, uint64(1), got[0].From)
	require.Equal(t, uint64(2), got[0].To)
	require.Equal(t, myraft.MsgHeartbeat, got[0].Type)
	require.Equal(t, uint64(3), got[0].Term)
	require.Equal(t, uint64(9), got[0].Index)
}

func TestGRPCTransportRejectsUnknownPeer(t *testing.T) {
	t1, err := NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t1.Close() })

	err = t1.Send(myraft.Message{From: 1, To: 2, Type: myraft.MsgHeartbeat})
	require.Error(t, err)
}
