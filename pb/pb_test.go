package pb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
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

type fakeConn struct {
	methods []string
}

func (c *fakeConn) Invoke(_ context.Context, method string, _ any, _ any, _ ...grpc.CallOption) error {
	c.methods = append(c.methods, method)
	return nil
}

func (c *fakeConn) NewStream(_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "no stream")
}

type fakeRegistrar struct {
	desc *grpc.ServiceDesc
	srv  any
}

func (r *fakeRegistrar) RegisterService(desc *grpc.ServiceDesc, srv any) {
	r.desc = desc
	r.srv = srv
}

func TestAllMessagesRoundTrip(t *testing.T) {
	total := 0
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if string(fd.Package()) != "pb" {
			return true
		}
		total += roundTripMessages(t, fd.Messages())
		return true
	})
	require.Greater(t, total, 0, "expected to discover pb messages for round-trip checks")
}

func roundTripMessages(t *testing.T, messages protoreflect.MessageDescriptors) int {
	t.Helper()
	count := 0
	for i := 0; i < messages.Len(); i++ {
		md := messages.Get(i)
		msg := dynamicpb.NewMessage(md)
		data, err := proto.Marshal(msg)
		require.NoError(t, err)

		clone := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(data, clone))
		require.True(t, proto.Equal(msg, clone))
		count++

		count += roundTripMessages(t, md.Messages())
	}
	return count
}

func TestEnumHelpers(t *testing.T) {
	tests := []struct {
		desc protoreflectEnum
	}{
		{desc: CmdType_CMD_GET},
		{desc: CheckTxnStatusAction_CheckTxnStatusNoAction},
		{desc: ManifestChange_CREATE},
		{desc: AdminCommand_SPLIT},
		{desc: Mutation_Put},
	}
	for _, tt := range tests {
		_ = tt.desc.Descriptor()
		_ = tt.desc.Number()
		_, _ = tt.desc.EnumDescriptor()
	}
}

type protoreflectEnum interface {
	Descriptor() protoreflect.EnumDescriptor
	Number() protoreflect.EnumNumber
	EnumDescriptor() ([]byte, []int)
}

func TestTinyKvClientAndServerHelpers(t *testing.T) {
	conn := &fakeConn{}
	client := NewTinyKvClient(conn)

	_, err := client.KvGet(context.Background(), &KvGetRequest{})
	require.NoError(t, err)
	_, err = client.KvBatchGet(context.Background(), &KvBatchGetRequest{})
	require.NoError(t, err)
	_, err = client.KvScan(context.Background(), &KvScanRequest{})
	require.NoError(t, err)
	_, err = client.KvPrewrite(context.Background(), &KvPrewriteRequest{})
	require.NoError(t, err)
	_, err = client.KvCommit(context.Background(), &KvCommitRequest{})
	require.NoError(t, err)
	_, err = client.KvBatchRollback(context.Background(), &KvBatchRollbackRequest{})
	require.NoError(t, err)
	_, err = client.KvResolveLock(context.Background(), &KvResolveLockRequest{})
	require.NoError(t, err)
	_, err = client.KvCheckTxnStatus(context.Background(), &KvCheckTxnStatusRequest{})
	require.NoError(t, err)

	require.Len(t, conn.methods, 8)
	require.Equal(t, TinyKv_KvGet_FullMethodName, conn.methods[0])

	reg := &fakeRegistrar{}
	RegisterTinyKvServer(reg, UnimplementedTinyKvServer{})
	require.NotNil(t, reg.desc)
	require.Equal(t, "pb.TinyKv", reg.desc.ServiceName)
	require.NotNil(t, reg.srv)

	srv := UnimplementedTinyKvServer{}
	_, err = srv.KvGet(context.Background(), &KvGetRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvBatchGet(context.Background(), &KvBatchGetRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvScan(context.Background(), &KvScanRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvPrewrite(context.Background(), &KvPrewriteRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvCommit(context.Background(), &KvCommitRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvBatchRollback(context.Background(), &KvBatchRollbackRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvResolveLock(context.Background(), &KvResolveLockRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.KvCheckTxnStatus(context.Background(), &KvCheckTxnStatusRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestPDClientAndServerHelpers(t *testing.T) {
	conn := &fakeConn{}
	client := NewPDClient(conn)

	_, err := client.StoreHeartbeat(context.Background(), &StoreHeartbeatRequest{})
	require.NoError(t, err)
	_, err = client.RegionHeartbeat(context.Background(), &RegionHeartbeatRequest{})
	require.NoError(t, err)
	_, err = client.GetRegionByKey(context.Background(), &GetRegionByKeyRequest{})
	require.NoError(t, err)
	_, err = client.AllocID(context.Background(), &AllocIDRequest{})
	require.NoError(t, err)
	_, err = client.Tso(context.Background(), &TsoRequest{})
	require.NoError(t, err)

	require.Len(t, conn.methods, 5)
	require.Equal(t, PD_StoreHeartbeat_FullMethodName, conn.methods[0])
	require.Equal(t, PD_Tso_FullMethodName, conn.methods[4])

	reg := &fakeRegistrar{}
	RegisterPDServer(reg, UnimplementedPDServer{})
	require.NotNil(t, reg.desc)
	require.Equal(t, "pb.PD", reg.desc.ServiceName)
	require.NotNil(t, reg.srv)

	srv := UnimplementedPDServer{}
	_, err = srv.StoreHeartbeat(context.Background(), &StoreHeartbeatRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.RegionHeartbeat(context.Background(), &RegionHeartbeatRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.GetRegionByKey(context.Background(), &GetRegionByKeyRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.AllocID(context.Background(), &AllocIDRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = srv.Tso(context.Background(), &TsoRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
