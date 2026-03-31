package migrate

import "fmt"

type MembershipPeerSummary struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}

type ClusterSummary struct {
	Source          string                  `json:"source"`
	AdminAddr       string                  `json:"admin_addr"`
	RegionID        uint64                  `json:"region_id"`
	Known           bool                    `json:"known"`
	Hosted          bool                    `json:"hosted"`
	Leader          bool                    `json:"leader"`
	LocalPeerID     uint64                  `json:"local_peer_id,omitempty"`
	LeaderPeerID    uint64                  `json:"leader_peer_id,omitempty"`
	LeaderStoreID   uint64                  `json:"leader_store_id,omitempty"`
	MembershipPeers int                     `json:"membership_peers,omitempty"`
	Membership      []MembershipPeerSummary `json:"membership,omitempty"`
	AppliedIndex    uint64                  `json:"applied_index,omitempty"`
	AppliedTerm     uint64                  `json:"applied_term,omitempty"`
}

// ReportResult combines migration preflight and local state into one operator
// facing report for a single workdir.
type ReportResult struct {
	WorkDir       string          `json:"workdir"`
	Mode          Mode            `json:"mode"`
	Stage         string          `json:"stage"`
	Summary       string          `json:"summary"`
	ReadyForInit  bool            `json:"ready_for_init"`
	ReadyForServe bool            `json:"ready_for_serve"`
	NextSteps     []string        `json:"next_steps,omitempty"`
	ResumeHint    string          `json:"resume_hint,omitempty"`
	Plan          PlanResult      `json:"plan"`
	Status        StatusResult    `json:"status"`
	Cluster       *ClusterSummary `json:"cluster,omitempty"`
}

// BuildReport returns one consolidated migration report for a local workdir.
func BuildReport(workDir string) (ReportResult, error) {
	return BuildReportWithConfig(StatusConfig{WorkDir: workDir})
}

// BuildReportWithConfig returns one consolidated migration report and may
// include a best-effort remote runtime view when an admin address is provided.
func BuildReportWithConfig(cfg StatusConfig) (ReportResult, error) {
	plan, err := BuildPlan(cfg.WorkDir)
	if err != nil {
		return ReportResult{}, err
	}
	status, err := ReadStatusWithConfig(cfg)
	if err != nil {
		return ReportResult{}, err
	}

	result := ReportResult{
		WorkDir:       status.WorkDir,
		Mode:          status.Mode,
		Plan:          plan,
		Status:        status,
		ReadyForInit:  status.Mode == ModeStandalone && plan.Eligible,
		ReadyForServe: status.Mode == ModeSeeded && status.StoreID != 0 && status.SeedSnapshotPresent && status.LocalCatalogRegions > 0,
		ResumeHint:    status.ResumeHint,
	}
	if status.Runtime != nil {
		result.Cluster = buildClusterSummary(status.Runtime)
	}

	switch status.Mode {
	case ModeStandalone:
		if plan.Eligible {
			result.Stage = "standalone-ready"
			result.Summary = "workdir is still standalone and eligible to be promoted into a seed"
			result.NextSteps = []string{
				"nokv migrate init --workdir " + status.WorkDir + " --store <store> --region <region> --peer <peer>",
			}
		} else {
			result.Stage = "standalone-blocked"
			result.Summary = "workdir is standalone, but migration preflight still has blockers"
			result.NextSteps = []string{"fix blockers reported by nokv migrate plan before running migrate init"}
		}
	case ModePreparing:
		result.Stage = "seed-preparing"
		result.Summary = "workdir is mid-promotion; inspect local catalog and seed snapshot before retrying init"
		result.NextSteps = []string{"nokv migrate status --workdir " + status.WorkDir, "retry nokv migrate init once partial state looks consistent"}
	case ModeSeeded:
		result.Stage = "seed-ready"
		result.Summary = "workdir has been promoted into a seed and is ready to boot in distributed mode"
		result.NextSteps = []string{
			fmt.Sprintf("nokv serve --workdir %s --store-id %d --pd-addr <pd>", status.WorkDir, status.StoreID),
			"after boot, use nokv migrate expand to add more peers",
		}
	case ModeCluster:
		result.Stage = "cluster-active"
		result.Summary = "workdir is already running in cluster mode; operate on membership and leadership instead of reinitializing"
		result.NextSteps = []string{
			"nokv migrate expand --addr <leader-admin> --region <region> --target <store>:<peer>[@addr]",
			"nokv migrate transfer-leader --addr <leader-admin> --region <region> --peer <peer>",
			"nokv migrate remove-peer --addr <leader-admin> --region <region> --peer <peer>",
		}
	default:
		result.Stage = "unknown"
		result.Summary = fmt.Sprintf("unrecognized migration mode %q", status.Mode)
	}

	return result, nil
}

func buildClusterSummary(runtime *RuntimeStatus) *ClusterSummary {
	if runtime == nil {
		return nil
	}
	summary := &ClusterSummary{
		Source:          "single-admin-endpoint",
		AdminAddr:       runtime.Addr,
		RegionID:        runtime.RegionID,
		Known:           runtime.Known,
		Hosted:          runtime.Hosted,
		Leader:          runtime.Leader,
		LocalPeerID:     runtime.LocalPeerID,
		LeaderPeerID:    runtime.LeaderPeerID,
		MembershipPeers: runtime.MembershipPeers,
		AppliedIndex:    runtime.AppliedIndex,
		AppliedTerm:     runtime.AppliedTerm,
	}
	if runtime.Region != nil {
		summary.Membership = make([]MembershipPeerSummary, 0, len(runtime.Region.GetPeers()))
		for _, peer := range runtime.Region.GetPeers() {
			if peer == nil {
				continue
			}
			summary.Membership = append(summary.Membership, MembershipPeerSummary{
				StoreID: peer.GetStoreId(),
				PeerID:  peer.GetPeerId(),
			})
			if summary.LeaderPeerID != 0 && summary.LeaderPeerID == peer.GetPeerId() {
				summary.LeaderStoreID = peer.GetStoreId()
			}
		}
	}
	return summary
}
