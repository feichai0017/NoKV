package migrate

import (
	"context"

	"github.com/feichai0017/NoKV/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AdminClient captures the admin control-plane calls used by migration.
type AdminClient interface {
	AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error)
	RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error)
	TransferLeader(ctx context.Context, req *pb.TransferLeaderRequest) (*pb.TransferLeaderResponse, error)
	RegionRuntimeStatus(ctx context.Context, req *pb.RegionRuntimeStatusRequest) (*pb.RegionRuntimeStatusResponse, error)
}

// DialFunc connects one admin client to one store address.
type DialFunc func(ctx context.Context, addr string) (AdminClient, func() error, error)

type grpcAdminClient struct {
	client pb.RaftAdminClient
}

func (c *grpcAdminClient) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	return c.client.AddPeer(ctx, req)
}

func (c *grpcAdminClient) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	return c.client.RemovePeer(ctx, req)
}

func (c *grpcAdminClient) TransferLeader(ctx context.Context, req *pb.TransferLeaderRequest) (*pb.TransferLeaderResponse, error) {
	return c.client.TransferLeader(ctx, req)
}

func (c *grpcAdminClient) RegionRuntimeStatus(ctx context.Context, req *pb.RegionRuntimeStatusRequest) (*pb.RegionRuntimeStatusResponse, error) {
	return c.client.RegionRuntimeStatus(ctx, req)
}

func defaultDial(ctx context.Context, addr string) (AdminClient, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() error { return conn.Close() }
	return &grpcAdminClient{client: pb.NewRaftAdminClient(conn)}, closeFn, nil
}
