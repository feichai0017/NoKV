package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
)

func mustCommandEntry(t *testing.T, requestID uint64) myraft.Entry {
	t.Helper()
	payload, err := command.Encode(&pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RequestId: requestID},
	})
	require.NoError(t, err)
	return myraft.Entry{Type: myraft.EntryNormal, Data: payload}
}

func TestCommandPipelineApplyEntriesReturnsApplyError(t *testing.T) {
	applyErr := errors.New("apply boom")
	var applied []uint64
	cp := newCommandPipeline(func(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
		applied = append(applied, req.GetHeader().GetRequestId())
		return nil, applyErr
	})

	prop1, err := cp.registerProposal(11)
	require.NoError(t, err)
	prop2, err := cp.registerProposal(22)
	require.NoError(t, err)
	require.NotNil(t, prop1)
	require.NotNil(t, prop2)

	err = cp.applyEntries([]myraft.Entry{
		mustCommandEntry(t, 11),
		mustCommandEntry(t, 22),
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

	first, err := cp.registerProposal(7)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := cp.registerProposal(7)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate proposal id")
	require.Nil(t, second)

	cp.completeProposal(7, &pb.RaftCmdResponse{}, nil)
	result := <-first.ch
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
}
