package store

import (
	"context"
	"errors"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
)

func mustCommandEntry(t *testing.T, regionID, peerID, requestID uint64) myraft.Entry {
	t.Helper()
	payload, err := command.Encode(&raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: regionID, PeerId: peerID, RequestId: requestID},
	})
	require.NoError(t, err)
	return myraft.Entry{Type: myraft.EntryNormal, Data: payload}
}

func testProposalKey(regionID, peerID, requestID uint64) commandProposalKey {
	return commandProposalKey{regionID: regionID, peerID: peerID, requestID: requestID}
}

func TestCommandPipelineApplyEntriesReturnsApplyError(t *testing.T) {
	applyErr := errors.New("apply boom")
	var applied []uint64
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		applied = append(applied, req.GetHeader().GetRequestId())
		return nil, applyErr
	})

	prop1, err := cp.registerProposal(testProposalKey(1, 101, 11))
	require.NoError(t, err)
	prop2, err := cp.registerProposal(testProposalKey(1, 101, 22))
	require.NoError(t, err)
	require.NotNil(t, prop1)
	require.NotNil(t, prop2)

	err = cp.applyEntries([]myraft.Entry{
		mustCommandEntry(t, 1, 101, 11),
		mustCommandEntry(t, 1, 101, 22),
	})
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, []uint64{11}, applied)

	result := <-prop1.ch
	require.ErrorIs(t, result.err, applyErr)
	require.Nil(t, result.resp)

	select {
	case <-prop2.ch:
		t.Fatal("second proposal should not complete after apply failure")
	default:
	}
}

func TestCommandPipelineRegisterProposalRejectsDuplicateID(t *testing.T) {
	cp := newCommandPipeline(nil)

	key := testProposalKey(2, 202, 7)
	first, err := cp.registerProposal(key)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := cp.registerProposal(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate proposal id")
	require.Nil(t, second)

	cp.completeProposal(key, &raftcmdpb.RaftCmdResponse{}, nil)
	result := <-first.ch
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
}

func TestCommandPipelineRemoveProposalDropsPendingResult(t *testing.T) {
	cp := newCommandPipeline(nil)

	key := testProposalKey(3, 303, 9)
	prop, err := cp.registerProposal(key)
	require.NoError(t, err)
	require.NotNil(t, prop)

	cp.removeProposal(key)
	cp.completeProposal(key, &raftcmdpb.RaftCmdResponse{}, nil)

	select {
	case <-prop.ch:
		t.Fatal("removed proposal should not receive a completion result")
	default:
	}
}

func TestCommandPipelineIgnoresForeignPeerRequestIDCollision(t *testing.T) {
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	})

	localKey := testProposalKey(7, 701, 1)
	prop, err := cp.registerProposal(localKey)
	require.NoError(t, err)
	require.NotNil(t, prop)

	require.NoError(t, cp.applyEntries([]myraft.Entry{
		mustCommandEntry(t, 7, 702, 1),
	}))

	select {
	case <-prop.ch:
		t.Fatal("foreign peer entry with colliding request id completed local proposal")
	default:
	}

	cp.completeProposal(localKey, &raftcmdpb.RaftCmdResponse{}, nil)
	result := <-prop.ch
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
}

func TestCommandPipelineRejectsUnframedPayload(t *testing.T) {
	cp := newCommandPipeline(func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		t.Fatal("unframed payload must not reach applier")
		return nil, nil
	})

	err := cp.applyEntries([]myraft.Entry{
		{Type: myraft.EntryNormal, Data: []byte("legacy-payload")},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported unframed raft payload")
}

func TestCommandRuntimeHelpers(t *testing.T) {
	var nilStore *Store
	require.NotNil(t, nilStore.runtimeContext())
	require.Error(t, nilStore.applyEntries(nil))

	applier := func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}
	st := NewStore(Config{CommandApplier: applier, CommandTimeout: 2 * time.Second})
	t.Cleanup(func() { st.Close() })

	require.NotNil(t, st.cmds)
	require.Equal(t, 2*time.Second, st.cmds.timeout)
	require.Equal(t, context.Background().Err(), st.runtimeContext().Err())
	require.NoError(t, st.applyEntries([]myraft.Entry{}))

	empty := NewStore(Config{})
	t.Cleanup(func() { empty.Close() })
	require.NoError(t, empty.applyEntries([]myraft.Entry{}))
}
