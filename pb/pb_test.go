package pb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestKVMajorFieldsRoundTrip(t *testing.T) {
	orig := &KV{
		Key:       []byte("k"),
		Value:     []byte("v"),
		UserMeta:  []byte{0x01, 0x02},
		Version:   42,
		ExpiresAt: 123,
		Meta:      []byte{0x0a},
		StreamId:  7,
	}

	data, err := proto.Marshal(orig)
	require.NoError(t, err)

	var decoded KV
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.Equal(t, orig.GetKey(), decoded.GetKey())
	require.Equal(t, orig.GetValue(), decoded.GetValue())
	require.Equal(t, orig.GetUserMeta(), decoded.GetUserMeta())
	require.Equal(t, orig.GetVersion(), decoded.GetVersion())
	require.Equal(t, orig.GetExpiresAt(), decoded.GetExpiresAt())
	require.Equal(t, orig.GetMeta(), decoded.GetMeta())
	require.Equal(t, orig.GetStreamId(), decoded.GetStreamId())
}

func TestRequestOneofRoundTrip(t *testing.T) {
	req := &Request{
		CmdType: CmdType_CMD_GET,
		Cmd: &Request_Get{
			Get: &GetRequest{Key: []byte("abc"), Version: 99},
		},
	}

	data, err := proto.Marshal(req)
	require.NoError(t, err)

	var decoded Request
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.Equal(t, req.CmdType, decoded.CmdType)

	origGet := req.GetGet()
	getReq := decoded.GetGet()
	require.NotNil(t, getReq, "expected Get oneof")
	require.Equal(t, origGet.GetKey(), getReq.GetKey())
	require.Equal(t, origGet.GetVersion(), getReq.GetVersion())
}

func TestResponseOneofRoundTrip(t *testing.T) {
	resp := &Response{
		Cmd: &Response_Scan{
			Scan: &ScanResponse{
				Kvs: []*KV{
					{Key: []byte("k1"), Value: []byte("v1")},
					{Key: []byte("k2"), Value: []byte("v2")},
				},
			},
		},
	}
	data, err := proto.Marshal(resp)
	require.NoError(t, err)

	var decoded Response
	require.NoError(t, proto.Unmarshal(data, &decoded))

	scan := decoded.GetScan()
	require.Len(t, scan.GetKvs(), 2)
	require.Equal(t, []byte("k1"), scan.GetKvs()[0].GetKey())
	require.Equal(t, []byte("v1"), scan.GetKvs()[0].GetValue())
}

func TestAdminCommandRoundTrip(t *testing.T) {
	cmd := &AdminCommand{
		Type: AdminCommand_SPLIT,
		Split: &SplitCommand{
			ParentRegionId: 1,
			SplitKey:       []byte("m"),
			Child: &RegionMeta{
				Id:               2,
				StartKey:         []byte("m"),
				EndKey:           []byte("z"),
				EpochVersion:     3,
				EpochConfVersion: 4,
				Peers: []*RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 21},
				},
			},
		},
	}
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	var decoded AdminCommand
	require.NoError(t, proto.Unmarshal(data, &decoded))

	require.Equal(t, cmd.GetType(), decoded.GetType())
	require.Equal(t, cmd.GetSplit().GetParentRegionId(), decoded.GetSplit().GetParentRegionId())
	require.Equal(t, cmd.GetSplit().GetChild().GetId(), decoded.GetSplit().GetChild().GetId())
	require.Len(t, decoded.GetSplit().GetChild().GetPeers(), 2)
}

func TestCommitAndResolveMessages(t *testing.T) {
	req := &Request{
		CmdType: CmdType_CMD_COMMIT,
		Cmd: &Request_Commit{
			Commit: &CommitRequest{
				StartVersion:  10,
				Keys:          [][]byte{[]byte("a"), []byte("b")},
				CommitVersion: 11,
			},
		},
	}
	resp := &Response{
		Cmd: &Response_Commit{
			Commit: &CommitResponse{
				Error: &KeyError{Retryable: "not leader"},
			},
		},
	}

	dataReq, err := proto.Marshal(req)
	require.NoError(t, err)
	var decodedReq Request
	require.NoError(t, proto.Unmarshal(dataReq, &decodedReq))
	commit := decodedReq.GetCommit()
	require.NotNil(t, commit)
	require.Equal(t, uint64(10), commit.GetStartVersion())
	require.Equal(t, []byte("b"), commit.GetKeys()[1])

	dataResp, err := proto.Marshal(resp)
	require.NoError(t, err)
	var decodedResp Response
	require.NoError(t, proto.Unmarshal(dataResp, &decodedResp))
	commitResp := decodedResp.GetCommit()
	require.NotNil(t, commitResp)
	require.Equal(t, "not leader", commitResp.GetError().GetRetryable())
}
