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
	StoreID         uint64 `json:"store_id"`
	PeerID          uint64 `json:"peer_id"`
	TargetAdminAddr string `json:"target_admin_addr,omitempty"`
}

// ExpandConfig defines one seed-region expansion request.
type ExpandConfig struct {
	WorkDir      string
	Addr         string
	RegionID     uint64
	WaitTimeout  time.Duration
	PollInterval time.Duration
	Targets      []PeerTarget

	Dial DialFunc
}

// ExpandResult reports the observed state after one add-peer request.
type ExpandResult struct {
	Addr              string         `json:"addr"`
	TargetAdminAddr   string         `json:"target_admin_addr,omitempty"`
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

// ExpandResultSet reports one sequential multi-peer rollout.
type ExpandResultSet struct {
	Addr     string         `json:"addr"`
	RegionID uint64         `json:"region_id"`
	Results  []ExpandResult `json:"results"`
}

// Expand performs one sequential add-peer rollout against a single region.
func Expand(ctx context.Context, cfg ExpandConfig) (ExpandResultSet, error) {
	if cfg.Addr == "" {
		return ExpandResultSet{}, fmt.Errorf("migrate: leader addr is required")
	}
	if cfg.RegionID == 0 {
		return ExpandResultSet{}, fmt.Errorf("migrate: region id is required")
	}
	if len(cfg.Targets) == 0 {
		return ExpandResultSet{}, fmt.Errorf("migrate: at least one peer target is required")
	}
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultExpandPollInterval
	}

	leaderClient, closeLeader, err := cfg.Dial(ctx, cfg.Addr)
	if err != nil {
		return ExpandResultSet{}, fmt.Errorf("migrate: dial leader admin %s: %w", cfg.Addr, err)
	}
	defer func() {
		if closeLeader != nil {
			_ = closeLeader()
		}
	}()

	result := ExpandResultSet{Addr: cfg.Addr, RegionID: cfg.RegionID, Results: make([]ExpandResult, 0, len(cfg.Targets))}
	if cfg.WorkDir != "" {
		if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
			Stage:            CheckpointExpandStarted,
			RegionID:         cfg.RegionID,
			CompletedTargets: 0,
			TotalTargets:     len(cfg.Targets),
		}); err != nil {
			return result, err
		}
	}
	for i, target := range cfg.Targets {
		if cfg.WorkDir != "" {
			if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
				Stage:            CheckpointExpandTarget,
				RegionID:         cfg.RegionID,
				TargetStoreID:    target.StoreID,
				TargetPeerID:     target.PeerID,
				CompletedTargets: i,
				TotalTargets:     len(cfg.Targets),
			}); err != nil {
				return result, err
			}
		}
		step, err := expandTargetWithLeaderClient(ctx, leaderClient, cfg, target)
		result.Results = append(result.Results, step)
		if err != nil {
			return result, err
		}
		if err := validateExpandResult(step); err != nil {
			return result, err
		}
		if cfg.WorkDir != "" {
			if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
				Stage:            CheckpointExpandHosted,
				RegionID:         cfg.RegionID,
				TargetStoreID:    target.StoreID,
				TargetPeerID:     target.PeerID,
				CompletedTargets: i + 1,
				TotalTargets:     len(cfg.Targets),
			}); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func expandTargetWithLeaderClient(ctx context.Context, leaderClient AdminClient, cfg ExpandConfig, target PeerTarget) (ExpandResult, error) {
	addResp, err := leaderClient.AddPeer(ctx, &pb.AddPeerRequest{
		RegionId: cfg.RegionID,
		StoreId:  target.StoreID,
		PeerId:   target.PeerID,
	})
	if err != nil {
		return ExpandResult{}, err
	}

	result := ExpandResult{
		Addr:            cfg.Addr,
		TargetAdminAddr: target.TargetAdminAddr,
		RegionID:        cfg.RegionID,
		StoreID:         target.StoreID,
		PeerID:          target.PeerID,
		LeaderKnown:     addResp.GetRegion() != nil,
		LeaderRegion:    addResp.GetRegion(),
	}
	if cfg.WaitTimeout <= 0 {
		return result, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.WaitTimeout)
	defer cancel()
	result.Waited = true

	leaderRegion, err := waitForLeaderPeer(waitCtx, leaderClient, cfg.RegionID, target.PeerID, cfg.PollInterval, &result)
	if err != nil {
		return result, err
	}
	if target.TargetAdminAddr == "" {
		return result, nil
	}

	targetClient, closeTarget, err := cfg.Dial(waitCtx, target.TargetAdminAddr)
	if err != nil {
		return result, fmt.Errorf("migrate: dial target admin %s: %w", target.TargetAdminAddr, err)
	}
	defer func() {
		if closeTarget != nil {
			_ = closeTarget()
		}
	}()
	if leaderRegion == nil {
		return result, fmt.Errorf("migrate: leader region %d missing published metadata for peer %d", cfg.RegionID, target.PeerID)
	}
	snapshotStream, err := leaderClient.ExportRegionSnapshotStream(waitCtx, &pb.ExportRegionSnapshotStreamRequest{
		RegionId: cfg.RegionID,
	})
	if err != nil {
		return result, fmt.Errorf("migrate: export region %d snapshot from %s: %w", cfg.RegionID, cfg.Addr, err)
	}
	defer func() {
		if snapshotStream != nil && snapshotStream.Reader != nil {
			_ = snapshotStream.Reader.Close()
		}
	}()
	if len(snapshotStream.Header) == 0 {
		return result, fmt.Errorf("migrate: exported region %d snapshot header from %s is empty", cfg.RegionID, cfg.Addr)
	}
	if snapshotStream.Region != nil {
		result.LeaderRegion = snapshotStream.Region
	}
	importRegion := leaderRegion
	if snapshotStream.Region != nil {
		importRegion = snapshotStream.Region
	}
	if _, err := targetClient.ImportRegionSnapshotStream(waitCtx, snapshotStream.Header, importRegion, snapshotStream.Reader); err != nil {
		return result, fmt.Errorf("migrate: import region %d snapshot on %s: %w", cfg.RegionID, target.TargetAdminAddr, err)
	}
	if err := waitForTargetHosted(waitCtx, targetClient, cfg.RegionID, target.PeerID, cfg.PollInterval, &result); err != nil {
		return result, err
	}
	return result, nil
}

func waitForLeaderPeer(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *ExpandResult) (*pb.RegionMeta, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
		if err != nil {
			return nil, fmt.Errorf("migrate: poll leader region status: %w", err)
		}
		if result != nil {
			result.LeaderKnown = status.GetKnown()
			if status.GetRegion() != nil {
				result.LeaderRegion = status.GetRegion()
			}
		}
		if status.GetKnown() && regionContainsPeer(status.GetRegion(), peerID) {
			return status.GetRegion(), nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("migrate: timed out waiting for leader region %d to publish peer %d: %w", regionID, peerID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForTargetHosted(ctx context.Context, client AdminClient, regionID, peerID uint64, interval time.Duration, result *ExpandResult) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastStatus *pb.RegionRuntimeStatusResponse
	for {
		status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
		if err != nil {
			return fmt.Errorf("migrate: poll target region status: %w", err)
		}
		lastStatus = status
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
			if lastStatus != nil {
				return fmt.Errorf(
					"migrate: timed out waiting for target store to host peer %d for region %d: known=%t hosted=%t local_peer=%d applied=%d term=%d err=%w",
					peerID,
					regionID,
					lastStatus.GetKnown(),
					lastStatus.GetHosted(),
					lastStatus.GetLocalPeerId(),
					lastStatus.GetAppliedIndex(),
					lastStatus.GetAppliedTerm(),
					ctx.Err(),
				)
			}
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
