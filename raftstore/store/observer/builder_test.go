package observer

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/stretchr/testify/require"
)

func TestEventsFromCommandExtractsVisibleCommitSources(t *testing.T) {
	commitKeys := [][]byte{[]byte("a"), []byte("b")}
	resolveKeys := [][]byte{[]byte("c")}
	createMutations := []*kvrpcpb.Mutation{
		{Key: []byte("dentry")},
		{Key: []byte("inode")},
	}
	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 7},
		Requests: []*raftcmdpb.Request{
			{
				CmdType: raftcmdpb.CmdType_CMD_COMMIT,
				Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
					Keys: commitKeys, CommitVersion: 20,
				}},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
				Cmd: &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{
					Keys: resolveKeys, CommitVersion: 21,
				}},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
				Cmd: &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{
					Keys: [][]byte{[]byte("rollback")}, CommitVersion: 0,
				}},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
				Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{}},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
					Mutations:     createMutations,
					CommitVersion: 22,
				}},
			},
		},
	}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{
		{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{}}},
		{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{}}},
		{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{}}},
		{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: &kvrpcpb.PrewriteResponse{}}},
		{Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{AppliedKeys: 2}}},
	}}

	events := EventsFromCommand(myraft.Entry{Term: 3, Index: 11}, req, resp)
	require.Len(t, events, 3)
	require.Equal(t, Event{
		RegionID:      7,
		Term:          3,
		Index:         11,
		Source:        SourceCommit,
		CommitVersion: 20,
		Keys:          [][]byte{[]byte("a"), []byte("b")},
	}, events[0])
	require.Equal(t, Event{
		RegionID:      7,
		Term:          3,
		Index:         11,
		Source:        SourceResolveLock,
		CommitVersion: 21,
		Keys:          [][]byte{[]byte("c")},
	}, events[1])
	require.Equal(t, Event{
		RegionID:      7,
		Term:          3,
		Index:         11,
		Source:        SourceCommit,
		CommitVersion: 22,
		Keys:          [][]byte{[]byte("dentry"), []byte("inode")},
		AtomicMutate:  true,
	}, events[2])

	commitKeys[0][0] = 'z'
	require.Equal(t, []byte("a"), events[0].Keys[0])
	createMutations[0].Key[0] = 'x'
	require.Equal(t, []byte("dentry"), events[2].Keys[0])
}

func TestEventsFromCommandSkipsErroredResponses(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 7},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_COMMIT,
			Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
				Keys: [][]byte{[]byte("a")}, CommitVersion: 20,
			}},
		}},
	}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{{
		Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}}},
	}}}

	require.Empty(t, EventsFromCommand(myraft.Entry{Term: 3, Index: 11}, req, resp))
}

func TestEventsFromCommandSkipsAtomicMutateFallback(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 7},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
				Mutations: []*kvrpcpb.Mutation{
					{Key: []byte("dentry")},
					{Key: []byte("inode")},
				},
				CommitVersion: 22,
			}},
		}},
	}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{{
		Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{FallbackToTwoPhaseCommit: true}},
	}}}

	require.Empty(t, EventsFromCommand(myraft.Entry{Term: 3, Index: 11}, req, resp))
}

func TestEventsFromCommandExtractsPerasSegmentInstallKeys(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 7,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentryKey, Value: []byte("dentry-value")},
				{Key: inodeKey, Value: []byte("inode-value")},
			},
		}},
	})
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 7},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT,
			Cmd: &raftcmdpb.Request_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentRequest{
				SegmentRoot:          append([]byte(nil), segment.Root[:]...),
				SegmentPayloadDigest: append([]byte(nil), digest[:]...),
				SegmentPayload:       payload,
				InstallVersion:       44,
			}},
		}},
	}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{{
		Cmd: &raftcmdpb.Response_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentResponse{}},
	}}}

	events := EventsFromCommand(myraft.Entry{Term: 3, Index: 11}, req, resp)
	require.Len(t, events, 1)
	require.Equal(t, Event{
		RegionID:      7,
		Term:          3,
		Index:         11,
		Source:        SourceCommit,
		CommitVersion: 44,
		Keys:          [][]byte{dentryKey},
	}, events[0])
}

func TestAttachCommandCursorAnnotatesPerasInstallResponse(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 9},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT,
			Cmd: &raftcmdpb.Request_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentRequest{
				InstallVersion: 55,
			}},
		}},
	}
	perasResp := &kvrpcpb.PerasInstallSegmentResponse{}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{{
		Cmd: &raftcmdpb.Response_PerasInstallSegment{PerasInstallSegment: perasResp},
	}}}

	AttachCommandCursor(myraft.Entry{Term: 4, Index: 12}, req, resp)

	require.Equal(t, uint64(9), perasResp.GetRegionId())
	require.Equal(t, uint64(4), perasResp.GetTerm())
	require.Equal(t, uint64(12), perasResp.GetIndex())
	require.Equal(t, uint64(55), perasResp.GetCommitVersion())
}
