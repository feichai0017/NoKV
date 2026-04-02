package migrate

import (
	"context"
	"fmt"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/legacy"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const adminSnapshotChunkSize = 64 << 10

// SnapshotExportStream carries one exported region snapshot stream.
type SnapshotExportStream struct {
	Header []byte
	Region *metapb.RegionMeta
	Reader io.ReadCloser
}

// AdminClient captures the admin control-plane calls used by migration.
type AdminClient interface {
	AddPeer(ctx context.Context, req *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error)
	RemovePeer(ctx context.Context, req *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error)
	TransferLeader(ctx context.Context, req *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error)
	ExportRegionSnapshotStream(ctx context.Context, req *adminpb.ExportRegionSnapshotStreamRequest) (*SnapshotExportStream, error)
	ImportRegionSnapshotStream(ctx context.Context, header []byte, region *metapb.RegionMeta, r io.Reader) (*adminpb.ImportRegionSnapshotResponse, error)
	RegionRuntimeStatus(ctx context.Context, req *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error)
}

// DialFunc connects one admin client to one store address.
type DialFunc func(ctx context.Context, addr string) (AdminClient, func() error, error)

type grpcAdminClient struct {
	client adminpb.RaftAdminClient
}

func (c *grpcAdminClient) AddPeer(ctx context.Context, req *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error) {
	return c.client.AddPeer(ctx, req)
}

func (c *grpcAdminClient) RemovePeer(ctx context.Context, req *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error) {
	return c.client.RemovePeer(ctx, req)
}

func (c *grpcAdminClient) TransferLeader(ctx context.Context, req *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error) {
	return c.client.TransferLeader(ctx, req)
}

func (c *grpcAdminClient) ExportRegionSnapshotStream(ctx context.Context, req *adminpb.ExportRegionSnapshotStreamRequest) (*SnapshotExportStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.client.ExportRegionSnapshotStream(streamCtx, req)
	if err != nil {
		cancel()
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	if len(first.GetSnapshotHeader()) == 0 {
		cancel()
		return nil, fmt.Errorf("migrate: export region snapshot stream missing snapshot header")
	}
	if first.GetRegion() == nil {
		cancel()
		return nil, fmt.Errorf("migrate: export region snapshot stream missing region metadata")
	}
	return &SnapshotExportStream{
		Header: first.GetSnapshotHeader(),
		Region: first.GetRegion(),
		Reader: &snapshotChunkReader{
			cancel: cancel,
			stream: stream,
			buf:    append([]byte(nil), first.GetChunk()...),
		},
	}, nil
}

func (c *grpcAdminClient) ImportRegionSnapshotStream(ctx context.Context, header []byte, region *metapb.RegionMeta, r io.Reader) (*adminpb.ImportRegionSnapshotResponse, error) {
	if len(header) == 0 {
		return nil, fmt.Errorf("migrate: import region snapshot stream requires snapshot header")
	}
	if region == nil {
		return nil, fmt.Errorf("migrate: import region snapshot stream requires region metadata")
	}
	if r == nil {
		return nil, fmt.Errorf("migrate: import region snapshot stream requires reader")
	}
	stream, err := c.client.ImportRegionSnapshotStream(ctx)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, adminSnapshotChunkSize)
	first := true
	sendChunk := func(chunk []byte) error {
		req := &adminpb.ImportRegionSnapshotStreamRequest{Chunk: append([]byte(nil), chunk...)}
		if first {
			req.SnapshotHeader = header
			req.Region = region
			first = false
		}
		return stream.Send(req)
	}
	for {
		n, readErr := r.Read(buf)
		if n > 0 || first {
			if err := sendChunk(buf[:n]); err != nil {
				return nil, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return stream.CloseAndRecv()
}

func (c *grpcAdminClient) RegionRuntimeStatus(ctx context.Context, req *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error) {
	return c.client.RegionRuntimeStatus(ctx, req)
}

func defaultDial(ctx context.Context, addr string) (AdminClient, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() error { return conn.Close() }
	return &grpcAdminClient{client: adminpb.NewRaftAdminClient(conn)}, closeFn, nil
}

type snapshotChunkReader struct {
	cancel func()
	stream adminpb.RaftAdmin_ExportRegionSnapshotStreamClient
	buf    []byte
	done   bool
}

func (r *snapshotChunkReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 && !r.done {
		resp, err := r.stream.Recv()
		if err == io.EOF {
			r.done = true
		} else if err != nil {
			return 0, err
		} else {
			if len(resp.GetSnapshotHeader()) != 0 || resp.GetRegion() != nil {
				return 0, fmt.Errorf("migrate: export region snapshot stream repeated header")
			}
			r.buf = append(r.buf[:0], resp.GetChunk()...)
		}
	}
	if len(r.buf) == 0 && r.done {
		return 0, io.EOF
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *snapshotChunkReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}
