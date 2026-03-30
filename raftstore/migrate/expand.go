package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultExpandPollInterval = 200 * time.Millisecond

// ExpandConfig defines one seed-region expansion request.
type ExpandConfig struct {
	Addr         string
	TargetAddr   string
	RegionID     uint64
	StoreID      uint64
	PeerID       uint64
	WaitTimeout  time.Duration
	PollInterval time.Duration

	Dial DialFunc
}

// ExpandResult reports the observed state after one add-peer request.
type ExpandResult struct {
	Addr              string         `json:"addr"`
	TargetAddr        string         `json:"target_addr,omitempty"`
	RegionID          uint64         `json:"region_id"`
	StoreID           uint64         `json:"store_id"`
	PeerID            uint64         `json:"peer_id"`
	LeaderKnown       bool           `json:"leader_known"`
	LeaderRegion      *pb.RegionMeta `json:"leader_region,omitempty"`
	TargetKnown       bool           `json:"target_known"`
	TargetHosted      bool           `json:"target_hosted"`
	TargetLocalPeerID uint64         `json:"target_local_peer_id,omitempty"`
	Waited            bool           `json:"waited"`
}

// AdminClient captures the admin control-plane calls used by migration.
type AdminClient interface {
	AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error)
	RegionStatus(ctx context.Context, req *pb.RegionStatusRequest) (*pb.RegionStatusResponse, error)
}

// DialFunc connects one admin client to one store address.
type DialFunc func(ctx context.Context, addr string) (AdminClient, func() error, error)

type grpcAdminClient struct {
	client pb.RaftAdminClient
}

func (c *grpcAdminClient) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	return c.client.AddPeer(ctx, req)
}

func (c *grpcAdminClient) RegionStatus(ctx context.Context, req *pb.RegionStatusRequest) (*pb.RegionStatusResponse, error) {
	return c.client.RegionStatus(ctx, req)
}

func defaultDial(ctx context.Context, addr string) (AdminClient, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() error { return conn.Close() }
	return &grpcAdminClient{client: pb.NewRaftAdminClient(conn)}, closeFn, nil
}

// Expand adds one peer to one seeded region and, when requested, waits until
// the target store reports the new peer as hosted.
func Expand(ctx context.Context, cfg ExpandConfig) (ExpandResult, error) {
	if cfg.Addr == "" {
		return ExpandResult{}, fmt.Errorf("migrate: leader addr is required")
	}
	if cfg.RegionID == 0 || cfg.StoreID == 0 || cfg.PeerID == 0 {
		return ExpandResult{}, fmt.Errorf("migrate: region, store, and peer ids are required")
	}
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultExpandPollInterval
	}

	leaderClient, closeLeader, err := cfg.Dial(ctx, cfg.Addr)
	if err != nil {
		return ExpandResult{}, fmt.Errorf("migrate: dial leader admin %s: %w", cfg.Addr, err)
	}
	defer func() {
		if closeLeader != nil {
			_ = closeLeader()
		}
	}()

	addResp, err := leaderClient.AddPeer(ctx, &pb.AddPeerRequest{
		RegionId: cfg.RegionID,
		StoreId:  cfg.StoreID,
		PeerId:   cfg.PeerID,
	})
	if err != nil {
		return ExpandResult{}, err
	}

	result := ExpandResult{
		Addr:         cfg.Addr,
		TargetAddr:   cfg.TargetAddr,
		RegionID:     cfg.RegionID,
		StoreID:      cfg.StoreID,
		PeerID:       cfg.PeerID,
		LeaderKnown:  addResp.GetRegion() != nil,
		LeaderRegion: addResp.GetRegion(),
	}
	if cfg.WaitTimeout <= 0 {
		return result, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.WaitTimeout)
	defer cancel()
	result.Waited = true

	if err := waitForLeaderPeer(waitCtx, leaderClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	if cfg.TargetAddr == "" {
		return result, nil
	}

	targetClient, closeTarget, err := cfg.Dial(waitCtx, cfg.TargetAddr)
	if err != nil {
		return result, fmt.Errorf("migrate: dial target admin %s: %w", cfg.TargetAddr, err)
	}
	defer func() {
		if closeTarget != nil {
			_ = closeTarget()
		}
	}()
	if err := waitForTargetHosted(waitCtx, targetClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	return result, nil
}

func waitForLeaderPeer(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *ExpandResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionStatus(ctx, &pb.RegionStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll leader region status: %w", err)
		}
		if result != nil {
			result.LeaderKnown = status.GetKnown()
			if status.GetRegion() != nil {
				result.LeaderRegion = status.GetRegion()
			}
		}
		if status.GetKnown() && regionContainsPeer(status.GetRegion(), peerID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for leader region %d to publish peer %d: %w", regionID, peerID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForTargetHosted(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *ExpandResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionStatus(ctx, &pb.RegionStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll target region status: %w", err)
		}
		if result != nil {
			result.TargetKnown = status.GetKnown()
			result.TargetHosted = status.GetHosted()
			result.TargetLocalPeerID = status.GetLocalPeerId()
		}
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for target store to host peer %d for region %d: %w", peerID, regionID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func regionContainsPeer(meta *pb.RegionMeta, peerID uint64) bool {
	if meta == nil || peerID == 0 {
		return false
	}
	for _, peer := range meta.GetPeers() {
		if peer.GetPeerId() == peerID {
			return true
		}
	}
	return false
}
