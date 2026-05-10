package router

import (
	"errors"
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

// stubPeer implements just enough of the *peer.Peer surface for router tests.
// We avoid using a real *peer.Peer because Construction needs a transport,
// raft RawNode, etc.
//
// Since router calls real peer.Peer methods (Step/Propose/ProposeCommand/Tick/Flush),
// these unit tests focus on registration/lookup behavior. Methods that forward
// to peer.Peer are exercised by integration tests in raftstore/store.

func TestNewRouterEmpty(t *testing.T) {
	r := New()
	require.NotNil(t, r)
	require.Empty(t, r.List())

	_, ok := r.Peer(1)
	require.False(t, ok)

	require.Nil(t, r.Deregister(1))
	require.Nil(t, r.Deregister(0))
}

func TestRegisterRejectsNil(t *testing.T) {
	r := New()
	err := r.Register(nil)
	require.ErrorIs(t, err, ErrRegisterNilPeer)
}

func TestRegisterNilRouterReturnsError(t *testing.T) {
	var r *Router
	err := r.Register(nil)
	require.ErrorIs(t, err, ErrRegisterNilPeer)
}

func TestSendRaftMissingPeer(t *testing.T) {
	r := New()
	err := r.SendRaft(42, myraft.Message{To: 42})
	var notFound *ErrPeerNotFound
	require.True(t, errors.As(err, &notFound))
	require.Equal(t, uint64(42), notFound.PeerID)
}

func TestSendProposeMissingPeer(t *testing.T) {
	r := New()
	err := r.SendPropose(7, []byte("x"))
	var notFound *ErrPeerNotFound
	require.True(t, errors.As(err, &notFound))
}

func TestSendCommandRejectsNilRequest(t *testing.T) {
	r := New()
	err := r.SendCommand(1, nil)
	require.ErrorIs(t, err, ErrNilCommandRequest)
}

func TestSendCommandMissingPeer(t *testing.T) {
	r := New()
	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}
	err := r.SendCommand(99, req)
	var notFound *ErrPeerNotFound
	require.True(t, errors.As(err, &notFound))
}

func TestSendTickMissingPeer(t *testing.T) {
	r := New()
	err := r.SendTick(99)
	var notFound *ErrPeerNotFound
	require.True(t, errors.As(err, &notFound))
}

func TestBroadcastTickEmpty(t *testing.T) {
	r := New()
	require.NoError(t, r.BroadcastTick())
}

func TestBroadcastFlushEmpty(t *testing.T) {
	r := New()
	require.NoError(t, r.BroadcastFlush())
}

func TestVisitNilFnIsNoop(t *testing.T) {
	r := New()
	r.Visit(nil)
}

func TestVisitNilRouterIsNoop(t *testing.T) {
	var r *Router
	r.Visit(func(*peer.Peer) { t.Fatalf("should not be called") })
}

func TestPeerNotFoundErrorMessage(t *testing.T) {
	err := &ErrPeerNotFound{PeerID: 7}
	require.Contains(t, err.Error(), "peer 7")
}

func TestPeerAlreadyRegisteredErrorMessage(t *testing.T) {
	err := &ErrPeerAlreadyRegistered{PeerID: 11}
	require.Contains(t, err.Error(), "peer 11")
}
