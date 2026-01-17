package pb

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func TestAllMessageMethods(t *testing.T) {
	for i := range file_pb_proto_msgTypes {
		msg := file_pb_proto_msgTypes[i].New().Interface()
		rv := reflect.ValueOf(msg)
		for m := 0; m < rv.NumMethod(); m++ {
			method := rv.Type().Method(m)
			if method.Type.NumIn() != 1 {
				continue
			}
			if strings.HasPrefix(method.Name, "XXX_") {
				continue
			}
			method.Func.Call([]reflect.Value{rv})
		}

		data, err := proto.Marshal(msg)
		require.NoError(t, err)
		clone := file_pb_proto_msgTypes[i].New().Interface()
		require.NoError(t, proto.Unmarshal(data, clone))
		require.True(t, proto.Equal(msg, clone))
	}
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
