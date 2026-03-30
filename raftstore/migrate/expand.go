package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/pb"
)

const defaultExpandPollInterval = 200 * time.Millisecond

// PeerTarget describes one peer to add during migration rollout.
type PeerTarget struct {
	StoreID    uint64 `json:"store_id"`
	PeerID     uint64 `json:"peer_id"`
	TargetAddr string `json:"target_addr,omitempty"`
}

// ExpandConfig defines one seed-region expansion request.
type ExpandConfig struct {
	Addr         string
	TargetAddr   string
	RegionID     uint64
	StoreID      uint64
	PeerID       uint64
	WaitTimeout  time.Duration
	PollInterval time.Duration
	Targets      []PeerTarget

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
	TargetAppliedIdx  uint64         `json:"target_applied_index,omitempty"`
	TargetAppliedTerm uint64         `json:"target_applied_term,omitempty"`
	Waited            bool           `json:"waited"`
}

// ExpandManyResult reports one sequential multi-peer rollout.
type ExpandManyResult struct {
	Addr     string         `json:"addr"`
	RegionID uint64         `json:"region_id"`
	Results  []ExpandResult `json:"results"`
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
	return expandWithLeaderClient(ctx, leaderClient, cfg)
}

// ExpandMany performs one sequential add-peer rollout against a single region.
func ExpandMany(ctx context.Context, cfg ExpandConfig) (ExpandManyResult, error) {
	if cfg.Addr == "" {
		return ExpandManyResult{}, fmt.Errorf("migrate: leader addr is required")
	}
	if cfg.RegionID == 0 {
		return ExpandManyResult{}, fmt.Errorf("migrate: region id is required")
	}
	if len(cfg.Targets) == 0 {
		if cfg.StoreID == 0 || cfg.PeerID == 0 {
			return ExpandManyResult{}, fmt.Errorf("migrate: at least one peer target is required")
		}
		cfg.Targets = []PeerTarget{{StoreID: cfg.StoreID, PeerID: cfg.PeerID, TargetAddr: cfg.TargetAddr}}
	}
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultExpandPollInterval
	}

	leaderClient, closeLeader, err := cfg.Dial(ctx, cfg.Addr)
	if err != nil {
		return ExpandManyResult{}, fmt.Errorf("migrate: dial leader admin %s: %w", cfg.Addr, err)
	}
	defer func() {
		if closeLeader != nil {
			_ = closeLeader()
		}
	}()

	result := ExpandManyResult{Addr: cfg.Addr, RegionID: cfg.RegionID, Results: make([]ExpandResult, 0, len(cfg.Targets))}
	for _, target := range cfg.Targets {
		stepCfg := cfg
		stepCfg.StoreID = target.StoreID
		stepCfg.PeerID = target.PeerID
		stepCfg.TargetAddr = target.TargetAddr
		stepCfg.Targets = nil
		step, err := expandWithLeaderClient(ctx, leaderClient, stepCfg)
		result.Results = append(result.Results, step)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func expandWithLeaderClient(ctx context.Context, leaderClient AdminClient, cfg ExpandConfig) (ExpandResult, error) {
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
			result.TargetAppliedIdx = status.GetAppliedIndex()
			result.TargetAppliedTerm = status.GetAppliedTerm()
		}
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetAppliedIndex() > 0 {
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
