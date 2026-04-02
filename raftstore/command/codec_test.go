package command

import (
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{
		Header:   &raftcmdpb.CmdHeader{RegionId: 42},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_GET, Cmd: &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("a"), Version: 5}}}},
	}
	payload, err := Encode(req)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, PayloadPrefix, payload[0])

	decoded, ok, err := Decode(payload)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, req.GetHeader().GetRegionId(), decoded.GetHeader().GetRegionId())
	require.Equal(t, req.GetRequests()[0].GetCmdType(), decoded.GetRequests()[0].GetCmdType())
}

func TestDecodeNonCommand(t *testing.T) {
	payload := []byte("legacy")
	decoded, ok, err := Decode(payload)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, decoded)
}

func TestDecodeCorruptedPayload(t *testing.T) {
	bad := []byte{PayloadPrefix, 0x01, 0x02}
	decoded, ok, err := Decode(bad)
	require.Error(t, err)
	require.True(t, ok)
	require.Nil(t, decoded)
}
