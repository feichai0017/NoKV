package migrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/pb"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/vfs"
)

const defaultStatusDialTimeout = 3 * time.Second

type StatusConfig struct {
	WorkDir   string
	AdminAddr string
	RegionID  uint64
	Timeout   time.Duration
	Dial      DialFunc
}

type RuntimeStatus struct {
	Addr            string         `json:"addr"`
	RegionID        uint64         `json:"region_id"`
	Known           bool           `json:"known"`
	Hosted          bool           `json:"hosted"`
	LocalPeerID     uint64         `json:"local_peer_id,omitempty"`
	LeaderPeerID    uint64         `json:"leader_peer_id,omitempty"`
	Leader          bool           `json:"leader"`
	MembershipPeers int            `json:"membership_peers,omitempty"`
	Region          *pb.RegionMeta `json:"region,omitempty"`
	AppliedIndex    uint64         `json:"applied_index,omitempty"`
	AppliedTerm     uint64         `json:"applied_term,omitempty"`
}

// StatusResult describes the current migration mode of one workdir.
type StatusResult struct {
	WorkDir             string         `json:"workdir"`
	Mode                Mode           `json:"mode"`
	StoreID             uint64         `json:"store_id,omitempty"`
	RegionID            uint64         `json:"region_id,omitempty"`
	PeerID              uint64         `json:"peer_id,omitempty"`
	LocalCatalogRegions int            `json:"local_catalog_regions,omitempty"`
	SeedSnapshotDir     string         `json:"seed_snapshot_dir,omitempty"`
	SeedSnapshotPresent bool           `json:"seed_snapshot_present,omitempty"`
	Next                string         `json:"next,omitempty"`
	Warnings            []string       `json:"warnings,omitempty"`
	Runtime             *RuntimeStatus `json:"runtime,omitempty"`
	RuntimeError        string         `json:"runtime_error,omitempty"`
}

// ReadStatus returns the current migration mode and seed identifiers for one
// workdir.
func ReadStatus(workDir string) (StatusResult, error) {
	return ReadStatusWithConfig(StatusConfig{WorkDir: workDir})
}

// ReadStatusWithConfig returns local migration state and, when configured,
// a best-effort remote runtime view from one admin endpoint.
func ReadStatusWithConfig(cfg StatusConfig) (StatusResult, error) {
	workDir := filepath.Clean(cfg.WorkDir)
	if workDir == "" || workDir == "." {
		return StatusResult{}, fmt.Errorf("migrate: workdir is required")
	}
	state, err := readState(workDir)
	if err != nil {
		return StatusResult{}, err
	}
	result := StatusResult{
		WorkDir:  workDir,
		Mode:     state.Mode,
		StoreID:  state.StoreID,
		RegionID: state.RegionID,
		PeerID:   state.PeerID,
	}

	localMeta, err := raftmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("local catalog open failed: %v", err))
	} else {
		result.LocalCatalogRegions = len(localMeta.Snapshot())
		_ = localMeta.Close()
	}

	if result.RegionID != 0 {
		result.SeedSnapshotDir = SeedSnapshotDir(workDir, result.RegionID)
		fs := vfs.Ensure(nil)
		if _, err := fs.Stat(result.SeedSnapshotDir); err == nil {
			result.SeedSnapshotPresent = true
		} else if !os.IsNotExist(err) {
			result.Warnings = append(result.Warnings, fmt.Sprintf("seed snapshot stat failed: %v", err))
		}
	}

	switch result.Mode {
	case ModeStandalone:
		result.Next = "nokv migrate plan"
	case ModePreparing:
		result.Next = "retry nokv migrate init after inspecting partial migration state"
	case ModeSeeded:
		result.Next = fmt.Sprintf("nokv serve --workdir %s --store-id %d --pd-addr <pd>", result.WorkDir, result.StoreID)
	case ModeCluster:
		result.Next = "nokv migrate expand | nokv migrate transfer-leader | nokv migrate remove-peer"
	}

	if cfg.AdminAddr != "" {
		regionID := cfg.RegionID
		if regionID == 0 {
			regionID = result.RegionID
		}
		if regionID == 0 {
			result.RuntimeError = "region id is required to query remote runtime status"
			return result, nil
		}
		runtime, err := queryRuntimeStatus(StatusConfig{
			AdminAddr: cfg.AdminAddr,
			RegionID:  regionID,
			Timeout:   cfg.Timeout,
			Dial:      cfg.Dial,
		})
		if err != nil {
			result.RuntimeError = err.Error()
		} else {
			result.Runtime = runtime
		}
	}

	return result, nil
}

func queryRuntimeStatus(cfg StatusConfig) (*RuntimeStatus, error) {
	if cfg.AdminAddr == "" {
		return nil, fmt.Errorf("migrate: admin addr is required")
	}
	if cfg.RegionID == 0 {
		return nil, fmt.Errorf("migrate: region id is required")
	}
	dial := cfg.Dial
	if dial == nil {
		dial = defaultDial
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultStatusDialTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, closeFn, err := dial(ctx, cfg.AdminAddr)
	if err != nil {
		return nil, fmt.Errorf("migrate: dial admin %s: %w", cfg.AdminAddr, err)
	}
	defer func() {
		if closeFn != nil {
			_ = closeFn()
		}
	}()

	resp, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: cfg.RegionID})
	if err != nil {
		return nil, fmt.Errorf("migrate: query runtime status from %s for region %d: %w", cfg.AdminAddr, cfg.RegionID, err)
	}
	runtime := &RuntimeStatus{
		Addr:         cfg.AdminAddr,
		RegionID:     cfg.RegionID,
		Known:        resp.GetKnown(),
		Hosted:       resp.GetHosted(),
		LocalPeerID:  resp.GetLocalPeerId(),
		LeaderPeerID: resp.GetLeaderPeerId(),
		Leader:       resp.GetLeader(),
		Region:       resp.GetRegion(),
		AppliedIndex: resp.GetAppliedIndex(),
		AppliedTerm:  resp.GetAppliedTerm(),
	}
	if resp.GetRegion() != nil {
		runtime.MembershipPeers = len(resp.GetRegion().GetPeers())
	}
	return runtime, nil
}
