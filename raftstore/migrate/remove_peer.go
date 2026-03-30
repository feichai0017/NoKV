package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/pb"
)

// RemovePeerConfig defines one explicit peer-removal request.
type RemovePeerConfig struct {
	Addr            string
	TargetAdminAddr string
	RegionID        uint64
	PeerID          uint64
	WaitTimeout     time.Duration
	PollInterval    time.Duration

	Dial DialFunc
}

// RemovePeerResult reports the observed state after one remove-peer request.
type RemovePeerResult struct {
	Addr             string         `json:"addr"`
	TargetAdminAddr  string         `json:"target_admin_addr,omitempty"`
	RegionID         uint64         `json:"region_id"`
	PeerID           uint64         `json:"peer_id"`
	LeaderKnown      bool           `json:"leader_known"`
	LeaderRegion     *pb.RegionMeta `json:"leader_region,omitempty"`
	TargetKnown      bool           `json:"target_known"`
	TargetHosted     bool           `json:"target_hosted"`
	TargetLocalPeer  uint64         `json:"target_local_peer_id,omitempty"`
	TargetAppliedIdx uint64         `json:"target_applied_index,omitempty"`
	Waited           bool           `json:"waited"`
}

// RemovePeer removes one peer from a region and optionally waits until the
// leader metadata and target store both reflect the removal.
func RemovePeer(ctx context.Context, cfg RemovePeerConfig) (RemovePeerResult, error) {
	if cfg.Addr == "" {
		return RemovePeerResult{}, fmt.Errorf("migrate: leader addr is required")
	}
	if cfg.RegionID == 0 || cfg.PeerID == 0 {
		return RemovePeerResult{}, fmt.Errorf("migrate: region and peer ids are required")
	}
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultExpandPollInterval
	}

	leaderClient, closeLeader, err := cfg.Dial(ctx, cfg.Addr)
	if err != nil {
		return RemovePeerResult{}, fmt.Errorf("migrate: dial leader admin %s: %w", cfg.Addr, err)
	}
	defer func() {
		if closeLeader != nil {
			_ = closeLeader()
		}
	}()

	resp, err := leaderClient.RemovePeer(ctx, &pb.RemovePeerRequest{RegionId: cfg.RegionID, PeerId: cfg.PeerID})
	if err != nil {
		return RemovePeerResult{}, err
	}
	result := RemovePeerResult{
		Addr:            cfg.Addr,
		TargetAdminAddr: cfg.TargetAdminAddr,
		RegionID:        cfg.RegionID,
		PeerID:          cfg.PeerID,
		LeaderKnown:     resp.GetRegion() != nil,
		LeaderRegion:    resp.GetRegion(),
	}
	if cfg.WaitTimeout <= 0 {
		return result, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.WaitTimeout)
	defer cancel()
	result.Waited = true
	if err := waitForLeaderPeerRemoval(waitCtx, leaderClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	if cfg.TargetAdminAddr == "" {
		return result, nil
	}

	targetClient, closeTarget, err := cfg.Dial(waitCtx, cfg.TargetAdminAddr)
	if err != nil {
		return result, fmt.Errorf("migrate: dial target admin %s: %w", cfg.TargetAdminAddr, err)
	}
	defer func() {
		if closeTarget != nil {
			_ = closeTarget()
		}
	}()
	if err := waitForTargetRemoval(waitCtx, targetClient, cfg.RegionID, cfg.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	return result, nil
}

func waitForLeaderPeerRemoval(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *RemovePeerResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll leader region status after remove: %w", err)
		}
		if result != nil {
			result.LeaderKnown = status.GetKnown()
			result.LeaderRegion = status.GetRegion()
		}
		if status.GetKnown() && !regionContainsPeer(status.GetRegion(), peerID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for leader region %d to remove peer %d: %w", regionID, peerID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForTargetRemoval(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *RemovePeerResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll target region status after remove: %w", err)
		}
		if result != nil {
			result.TargetKnown = status.GetKnown()
			result.TargetHosted = status.GetHosted()
			result.TargetLocalPeer = status.GetLocalPeerId()
			result.TargetAppliedIdx = status.GetAppliedIndex()
		}
		if !status.GetKnown() || !status.GetHosted() || status.GetLocalPeerId() != peerID {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: timed out waiting for target store to drop peer %d for region %d: %w", peerID, regionID, ctx.Err())
		case <-ticker.C:
		}
	}
}
