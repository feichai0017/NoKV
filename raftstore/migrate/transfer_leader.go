package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/pb"
)

// TransferLeaderConfig defines one leader-transfer request.
type TransferLeaderConfig struct {
	Addr         string
	TargetAddr   string
	RegionID     uint64
	PeerID       uint64
	WaitTimeout  time.Duration
	PollInterval time.Duration

	Dial DialFunc
}

// TransferLeaderResult reports the observed state after one leader transfer.
type TransferLeaderResult struct {
	Addr          string         `json:"addr"`
	TargetAddr    string         `json:"target_addr,omitempty"`
	RegionID      uint64         `json:"region_id"`
	PeerID        uint64         `json:"peer_id"`
	LeaderKnown   bool           `json:"leader_known"`
	LeaderRegion  *pb.RegionMeta `json:"leader_region,omitempty"`
	LeaderPeerID  uint64         `json:"leader_peer_id,omitempty"`
	TargetKnown   bool           `json:"target_known"`
	TargetHosted  bool           `json:"target_hosted"`
	TargetLeader  bool           `json:"target_leader"`
	TargetLocalID uint64         `json:"target_local_peer_id,omitempty"`
	TargetApplied uint64         `json:"target_applied_index,omitempty"`
	Waited        bool           `json:"waited"`
}

// TransferLeader requests leadership transfer and optionally waits until the
// target peer becomes leader.
func TransferLeader(ctx context.Context, cfg TransferLeaderConfig) (TransferLeaderResult, error) {
	if cfg.Addr == "" {
		return TransferLeaderResult{}, fmt.Errorf("migrate: leader addr is required")
	}
	if cfg.RegionID == 0 || cfg.PeerID == 0 {
		return TransferLeaderResult{}, fmt.Errorf("migrate: region and peer ids are required")
	}
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultExpandPollInterval
	}

	leaderClient, closeLeader, err := cfg.Dial(ctx, cfg.Addr)
	if err != nil {
		return TransferLeaderResult{}, fmt.Errorf("migrate: dial leader admin %s: %w", cfg.Addr, err)
	}
	defer func() {
		if closeLeader != nil {
			_ = closeLeader()
		}
	}()

	resp, err := leaderClient.TransferLeader(ctx, &pb.TransferLeaderRequest{RegionId: cfg.RegionID, PeerId: cfg.PeerID})
	if err != nil {
		return TransferLeaderResult{}, err
	}
	result := TransferLeaderResult{
		Addr:         cfg.Addr,
		TargetAddr:   cfg.TargetAddr,
		RegionID:     cfg.RegionID,
		PeerID:       cfg.PeerID,
		LeaderKnown:  resp.GetRegion() != nil,
		LeaderRegion: resp.GetRegion(),
	}
	if cfg.WaitTimeout <= 0 {
		return result, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.WaitTimeout)
	defer cancel()
	result.Waited = true
	if err := waitForLeaderTransfer(waitCtx, leaderClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
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
	if err := waitForTargetLeader(waitCtx, targetClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	return result, nil
}

func waitForLeaderTransfer(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *TransferLeaderResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionStatus(ctx, &pb.RegionStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll leader region status after transfer: %w", err)
		}
		if result != nil {
			result.LeaderKnown = status.GetKnown()
			result.LeaderRegion = status.GetRegion()
			result.LeaderPeerID = status.GetLeaderPeerId()
		}
		if status.GetKnown() && status.GetLeaderPeerId() == peerID {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for region %d to elect peer %d as leader: %w", regionID, peerID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForTargetLeader(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *TransferLeaderResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionStatus(ctx, &pb.RegionStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll target region status after transfer: %w", err)
		}
		if result != nil {
			result.TargetKnown = status.GetKnown()
			result.TargetHosted = status.GetHosted()
			result.TargetLeader = status.GetLeader()
			result.TargetLocalID = status.GetLocalPeerId()
			result.TargetApplied = status.GetAppliedIndex()
		}
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetLeader() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for target peer %d to become leader for region %d: %w", peerID, regionID, ctx.Err())
		case <-ticker.C:
		}
	}
}
