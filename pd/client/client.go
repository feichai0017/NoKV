package client

import (
	"context"
	"errors"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var errEmptyAddress = errors.New("pd client: empty address")

// Client defines the PD-lite control-plane RPC contract consumed by stores.
type Client interface {
	StoreHeartbeat(ctx context.Context, req *pb.StoreHeartbeatRequest) (*pb.StoreHeartbeatResponse, error)
	RegionHeartbeat(ctx context.Context, req *pb.RegionHeartbeatRequest) (*pb.RegionHeartbeatResponse, error)
	RemoveRegion(ctx context.Context, req *pb.RemoveRegionRequest) (*pb.RemoveRegionResponse, error)
	GetRegionByKey(ctx context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error)
	AllocID(ctx context.Context, req *pb.AllocIDRequest) (*pb.AllocIDResponse, error)
	Tso(ctx context.Context, req *pb.TsoRequest) (*pb.TsoResponse, error)
	Close() error
}

// GRPCClient is a thin wrapper around generated pb.PDClient.
type GRPCClient struct {
	conn *grpc.ClientConn
	pd   pb.PDClient
}

// NewGRPCClient dials a PD-lite endpoint and returns a ready client.
func NewGRPCClient(ctx context.Context, addr string, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	if addr == "" {
		return nil, errEmptyAddress
	}
	opts := normalizeDialOptions(dialOpts)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	if err := waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &GRPCClient{
		conn: conn,
		pd:   pb.NewPDClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// StoreHeartbeat forwards store heartbeat RPC.
func (c *GRPCClient) StoreHeartbeat(ctx context.Context, req *pb.StoreHeartbeatRequest) (*pb.StoreHeartbeatResponse, error) {
	return c.pd.StoreHeartbeat(ctx, req)
}

// RegionHeartbeat forwards region heartbeat RPC.
func (c *GRPCClient) RegionHeartbeat(ctx context.Context, req *pb.RegionHeartbeatRequest) (*pb.RegionHeartbeatResponse, error) {
	return c.pd.RegionHeartbeat(ctx, req)
}

// RemoveRegion forwards region removal RPC.
func (c *GRPCClient) RemoveRegion(ctx context.Context, req *pb.RemoveRegionRequest) (*pb.RemoveRegionResponse, error) {
	return c.pd.RemoveRegion(ctx, req)
}

// GetRegionByKey forwards region lookup RPC.
func (c *GRPCClient) GetRegionByKey(ctx context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	return c.pd.GetRegionByKey(ctx, req)
}

// AllocID forwards ID allocation RPC.
func (c *GRPCClient) AllocID(ctx context.Context, req *pb.AllocIDRequest) (*pb.AllocIDResponse, error) {
	return c.pd.AllocID(ctx, req)
}

// Tso forwards TSO allocation RPC.
func (c *GRPCClient) Tso(ctx context.Context, req *pb.TsoRequest) (*pb.TsoResponse, error) {
	return c.pd.Tso(ctx, req)
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	if len(opts) == 0 {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: 2 * time.Second,
			}),
		}
	}
	return opts
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	if ctx == nil {
		return nil
	}
	conn.Connect()
	for {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return errors.New("pd client: grpc connection shutdown")
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}
