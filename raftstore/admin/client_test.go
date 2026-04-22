package admin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type fakeClientStream struct{}

func (fakeClientStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (fakeClientStream) Trailer() metadata.MD         { return metadata.MD{} }
func (fakeClientStream) CloseSend() error             { return nil }
func (fakeClientStream) Context() context.Context     { return context.Background() }
func (fakeClientStream) SendMsg(any) error            { return nil }
func (fakeClientStream) RecvMsg(any) error            { return nil }

type fakeExportStream struct {
	fakeClientStream
	responses []*adminpb.ExportRegionSnapshotStreamResponse
	idx       int
	err       error
}

func (s *fakeExportStream) Recv() (*adminpb.ExportRegionSnapshotStreamResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.idx >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.idx]
	s.idx++
	return resp, nil
}

type fakeImportStream struct {
	fakeClientStream
	reqs     []*adminpb.ImportRegionSnapshotStreamRequest
	sendErr  error
	closeErr error
	resp     *adminpb.ImportRegionSnapshotResponse
}

func (s *fakeImportStream) Send(req *adminpb.ImportRegionSnapshotStreamRequest) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.reqs = append(s.reqs, req)
	return nil
}

func (s *fakeImportStream) CloseAndRecv() (*adminpb.ImportRegionSnapshotResponse, error) {
	if s.closeErr != nil {
		return nil, s.closeErr
	}
	if s.resp == nil {
		return &adminpb.ImportRegionSnapshotResponse{}, nil
	}
	return s.resp, nil
}

type fakeRaftAdminClient struct {
	addResp        *adminpb.AddPeerResponse
	removeResp     *adminpb.RemovePeerResponse
	transferResp   *adminpb.TransferLeaderResponse
	runtimeResp    *adminpb.RegionRuntimeStatusResponse
	executionResp  *adminpb.ExecutionStatusResponse
	exportStream   adminpb.RaftAdmin_ExportRegionSnapshotStreamClient
	importStream   adminpb.RaftAdmin_ImportRegionSnapshotStreamClient
	exportErr      error
	importErr      error
	addReq         *adminpb.AddPeerRequest
	removeReq      *adminpb.RemovePeerRequest
	transferReq    *adminpb.TransferLeaderRequest
	runtimeReq     *adminpb.RegionRuntimeStatusRequest
	executionReq   *adminpb.ExecutionStatusRequest
	importOpenCall int
}

func (f *fakeRaftAdminClient) AddPeer(_ context.Context, req *adminpb.AddPeerRequest, _ ...grpc.CallOption) (*adminpb.AddPeerResponse, error) {
	f.addReq = req
	if f.addResp == nil {
		return &adminpb.AddPeerResponse{}, nil
	}
	return f.addResp, nil
}

func (f *fakeRaftAdminClient) RemovePeer(_ context.Context, req *adminpb.RemovePeerRequest, _ ...grpc.CallOption) (*adminpb.RemovePeerResponse, error) {
	f.removeReq = req
	if f.removeResp == nil {
		return &adminpb.RemovePeerResponse{}, nil
	}
	return f.removeResp, nil
}

func (f *fakeRaftAdminClient) TransferLeader(_ context.Context, req *adminpb.TransferLeaderRequest, _ ...grpc.CallOption) (*adminpb.TransferLeaderResponse, error) {
	f.transferReq = req
	if f.transferResp == nil {
		return &adminpb.TransferLeaderResponse{}, nil
	}
	return f.transferResp, nil
}

func (f *fakeRaftAdminClient) ExportRegionSnapshot(_ context.Context, _ *adminpb.ExportRegionSnapshotRequest, _ ...grpc.CallOption) (*adminpb.ExportRegionSnapshotResponse, error) {
	return nil, errors.New("unused")
}

func (f *fakeRaftAdminClient) ExportRegionSnapshotStream(_ context.Context, _ *adminpb.ExportRegionSnapshotStreamRequest, _ ...grpc.CallOption) (adminpb.RaftAdmin_ExportRegionSnapshotStreamClient, error) {
	if f.exportErr != nil {
		return nil, f.exportErr
	}
	return f.exportStream, nil
}

func (f *fakeRaftAdminClient) ImportRegionSnapshot(_ context.Context, _ *adminpb.ImportRegionSnapshotRequest, _ ...grpc.CallOption) (*adminpb.ImportRegionSnapshotResponse, error) {
	return nil, errors.New("unused")
}

func (f *fakeRaftAdminClient) ImportRegionSnapshotStream(_ context.Context, _ ...grpc.CallOption) (adminpb.RaftAdmin_ImportRegionSnapshotStreamClient, error) {
	f.importOpenCall++
	if f.importErr != nil {
		return nil, f.importErr
	}
	return f.importStream, nil
}

func (f *fakeRaftAdminClient) RegionRuntimeStatus(_ context.Context, req *adminpb.RegionRuntimeStatusRequest, _ ...grpc.CallOption) (*adminpb.RegionRuntimeStatusResponse, error) {
	f.runtimeReq = req
	if f.runtimeResp == nil {
		return &adminpb.RegionRuntimeStatusResponse{}, nil
	}
	return f.runtimeResp, nil
}

func (f *fakeRaftAdminClient) ExecutionStatus(_ context.Context, req *adminpb.ExecutionStatusRequest, _ ...grpc.CallOption) (*adminpb.ExecutionStatusResponse, error) {
	f.executionReq = req
	if f.executionResp == nil {
		return &adminpb.ExecutionStatusResponse{}, nil
	}
	return f.executionResp, nil
}

func TestGRPCClientUnaryWrappersAndDial(t *testing.T) {
	fake := &fakeRaftAdminClient{
		addResp:       &adminpb.AddPeerResponse{},
		removeResp:    &adminpb.RemovePeerResponse{},
		transferResp:  &adminpb.TransferLeaderResponse{},
		runtimeResp:   &adminpb.RegionRuntimeStatusResponse{},
		executionResp: &adminpb.ExecutionStatusResponse{},
	}
	client := &grpcClient{client: fake}

	_, err := client.AddPeer(context.Background(), &adminpb.AddPeerRequest{RegionId: 1, StoreId: 2, PeerId: 3})
	require.NoError(t, err)
	_, err = client.RemovePeer(context.Background(), &adminpb.RemovePeerRequest{RegionId: 1, PeerId: 3})
	require.NoError(t, err)
	_, err = client.TransferLeader(context.Background(), &adminpb.TransferLeaderRequest{RegionId: 1, PeerId: 3})
	require.NoError(t, err)
	_, err = client.RegionRuntimeStatus(context.Background(), &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	_, err = client.ExecutionStatus(context.Background(), &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)

	require.NotNil(t, fake.addReq)
	require.NotNil(t, fake.removeReq)
	require.NotNil(t, fake.transferReq)
	require.NotNil(t, fake.runtimeReq)
	require.NotNil(t, fake.executionReq)

	dialed, closeFn, err := Dial(context.Background(), "127.0.0.1:1")
	require.NoError(t, err)
	require.NotNil(t, dialed)
	require.NotNil(t, closeFn)
	require.NoError(t, closeFn())
}

func TestGRPCClientExportRegionSnapshotStream(t *testing.T) {
	region := &metapb.RegionDescriptor{RegionId: 8}
	fake := &fakeRaftAdminClient{
		exportStream: &fakeExportStream{
			responses: []*adminpb.ExportRegionSnapshotStreamResponse{
				{
					SnapshotHeader: []byte("header"),
					Region:         region,
					Chunk:          []byte("hello "),
				},
				{Chunk: []byte("world")},
			},
		},
	}
	client := &grpcClient{client: fake}

	out, err := client.ExportRegionSnapshotStream(context.Background(), &adminpb.ExportRegionSnapshotStreamRequest{RegionId: 8})
	require.NoError(t, err)
	require.Equal(t, []byte("header"), out.Header)
	require.Equal(t, region, out.Region)

	payload, err := io.ReadAll(out.Reader)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), payload)
	require.NoError(t, out.Reader.Close())
}

func TestGRPCClientExportRegionSnapshotStreamRejectsBadHeaders(t *testing.T) {
	client := &grpcClient{client: &fakeRaftAdminClient{
		exportStream: &fakeExportStream{
			responses: []*adminpb.ExportRegionSnapshotStreamResponse{{Region: &metapb.RegionDescriptor{RegionId: 1}}},
		},
	}}
	_, err := client.ExportRegionSnapshotStream(context.Background(), &adminpb.ExportRegionSnapshotStreamRequest{RegionId: 1})
	require.ErrorContains(t, err, "missing snapshot header")

	client = &grpcClient{client: &fakeRaftAdminClient{
		exportStream: &fakeExportStream{
			responses: []*adminpb.ExportRegionSnapshotStreamResponse{{SnapshotHeader: []byte("header")}},
		},
	}}
	_, err = client.ExportRegionSnapshotStream(context.Background(), &adminpb.ExportRegionSnapshotStreamRequest{RegionId: 1})
	require.ErrorContains(t, err, "missing region metadata")

	client = &grpcClient{client: &fakeRaftAdminClient{exportErr: errors.New("open stream failed")}}
	_, err = client.ExportRegionSnapshotStream(context.Background(), &adminpb.ExportRegionSnapshotStreamRequest{RegionId: 1})
	require.EqualError(t, err, "open stream failed")
}

func TestSnapshotChunkReaderRejectsRepeatedHeader(t *testing.T) {
	reader := &snapshotChunkReader{
		stream: &fakeExportStream{
			responses: []*adminpb.ExportRegionSnapshotStreamResponse{
				{SnapshotHeader: []byte("again")},
			},
		},
	}
	buf := make([]byte, 16)
	_, err := reader.Read(buf)
	require.ErrorContains(t, err, "repeated header")
	require.NoError(t, reader.Close())
}

func TestGRPCClientImportRegionSnapshotStream(t *testing.T) {
	importStream := &fakeImportStream{
		resp: &adminpb.ImportRegionSnapshotResponse{Region: &metapb.RegionDescriptor{RegionId: 9}},
	}
	client := &grpcClient{client: &fakeRaftAdminClient{importStream: importStream}}
	region := &metapb.RegionDescriptor{RegionId: 9}

	resp, err := client.ImportRegionSnapshotStream(context.Background(), []byte("header"), region, io.NopCloser(io.MultiReader(
		bytes.NewReader([]byte("abc")),
		bytes.NewReader([]byte("def")),
	)))
	require.NoError(t, err)
	require.Equal(t, uint64(9), resp.GetRegion().GetRegionId())
	require.Len(t, importStream.reqs, 2)
	require.Equal(t, []byte("header"), importStream.reqs[0].GetSnapshotHeader())
	require.Equal(t, region, importStream.reqs[0].GetRegion())
	require.Nil(t, importStream.reqs[1].GetSnapshotHeader())
	require.Nil(t, importStream.reqs[1].GetRegion())
	require.Equal(t, []byte("abc"), importStream.reqs[0].GetChunk())
	require.Equal(t, []byte("def"), importStream.reqs[1].GetChunk())
}

func TestGRPCClientImportRegionSnapshotStreamValidationAndErrors(t *testing.T) {
	client := &grpcClient{client: &fakeRaftAdminClient{}}
	region := &metapb.RegionDescriptor{RegionId: 1}

	_, err := client.ImportRegionSnapshotStream(context.Background(), nil, region, io.NopCloser(bytes.NewReader(nil)))
	require.ErrorContains(t, err, "requires snapshot header")
	_, err = client.ImportRegionSnapshotStream(context.Background(), []byte("header"), nil, io.NopCloser(bytes.NewReader(nil)))
	require.ErrorContains(t, err, "requires region metadata")
	_, err = client.ImportRegionSnapshotStream(context.Background(), []byte("header"), region, nil)
	require.ErrorContains(t, err, "requires reader")

	client = &grpcClient{client: &fakeRaftAdminClient{importErr: errors.New("open import failed")}}
	_, err = client.ImportRegionSnapshotStream(context.Background(), []byte("header"), region, io.NopCloser(bytes.NewReader(nil)))
	require.EqualError(t, err, "open import failed")

	importStream := &fakeImportStream{sendErr: errors.New("send failed")}
	client = &grpcClient{client: &fakeRaftAdminClient{importStream: importStream}}
	_, err = client.ImportRegionSnapshotStream(context.Background(), []byte("header"), region, io.NopCloser(bytes.NewReader([]byte("x"))))
	require.EqualError(t, err, "send failed")

	importStream = &fakeImportStream{closeErr: errors.New("close failed")}
	client = &grpcClient{client: &fakeRaftAdminClient{importStream: importStream}}
	_, err = client.ImportRegionSnapshotStream(context.Background(), []byte("header"), region, io.NopCloser(bytes.NewReader(nil)))
	require.EqualError(t, err, "close failed")
}
