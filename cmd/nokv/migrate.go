package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/pb"
	migratepkg "github.com/feichai0017/NoKV/raftstore/migrate"
)

var runExpand = migratepkg.Expand
var runRemovePeer = migratepkg.RemovePeer
var runTransferLeader = migratepkg.TransferLeader
var runReadStatus = migratepkg.ReadStatusWithConfig
var runBuildReport = migratepkg.BuildReportWithConfig

type migrateRuntimeFlags struct {
	adminAddr *string
	regionID  *uint64
	timeout   *time.Duration
}

func runMigrateCmd(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("migrate subcommand required")
	}

	subcmd := args[0]
	subargs := args[1:]

	switch subcmd {
	case "plan":
		return runMigratePlanCmd(w, subargs)
	case "init":
		return runMigrateInitCmd(w, subargs)
	case "status":
		return runMigrateStatusCmd(w, subargs)
	case "report":
		return runMigrateReportCmd(w, subargs)
	case "expand":
		return runMigrateExpandCmd(w, subargs)
	case "remove-peer":
		return runMigrateRemovePeerCmd(w, subargs)
	case "transfer-leader":
		return runMigrateTransferLeaderCmd(w, subargs)
	case "help", "-h", "--help":
		printMigrateUsage(w)
		return nil
	default:
		return fmt.Errorf("unknown migrate subcommand %q", subcmd)
	}
}

func printMigrateUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, `Usage: nokv migrate <subcommand> [flags]

	Subcommands:
	  plan     Inspect whether a standalone workdir can be seeded for cluster mode
	  init     Convert a standalone workdir into a single-store cluster seed
	  status   Show migration mode for one workdir
	  report   Combine preflight and local state into one migration report
	  expand   Expand a single-store seed into a replicated region
	  remove-peer      Remove one peer from a replicated region
	  transfer-leader  Transfer region leadership to a specific peer`)
}

func bindMigrateRuntimeFlags(fs *flag.FlagSet) migrateRuntimeFlags {
	return migrateRuntimeFlags{
		adminAddr: fs.String("addr", "", "optional admin address to query remote region runtime status"),
		regionID:  fs.Uint64("region", 0, "optional region id override for remote region runtime status"),
		timeout:   fs.Duration("timeout", 3*time.Second, "timeout for remote runtime status queries"),
	}
}

func (f migrateRuntimeFlags) config(workDir string) migratepkg.StatusConfig {
	return migratepkg.StatusConfig{
		WorkDir:   workDir,
		AdminAddr: strings.TrimSpace(valueString(f.adminAddr)),
		RegionID:  valueUint64(f.regionID),
		Timeout:   valueDuration(f.timeout),
	}
}

func valueString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func valueUint64(v *uint64) uint64 {
	if v == nil {
		return 0
	}
	return *v
}

func valueDuration(v *time.Duration) time.Duration {
	if v == nil {
		return 0
	}
	return *v
}

func writeIndentedJSON(w io.Writer, payload any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

type peerTargetsFlag []migratepkg.PeerTarget

func (f *peerTargetsFlag) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*f))
	for _, target := range *f {
		part := fmt.Sprintf("%d:%d", target.StoreID, target.PeerID)
		if target.TargetAdminAddr != "" {
			part += "@" + target.TargetAdminAddr
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ",")
}

func (f *peerTargetsFlag) Set(value string) error {
	target, err := parsePeerTarget(value)
	if err != nil {
		return err
	}
	*f = append(*f, target)
	return nil
}

func parsePeerTarget(value string) (migratepkg.PeerTarget, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return migratepkg.PeerTarget{}, fmt.Errorf("empty peer target")
	}
	addr := ""
	if at := strings.IndexByte(value, '@'); at >= 0 {
		addr = strings.TrimSpace(value[at+1:])
		value = strings.TrimSpace(value[:at])
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return migratepkg.PeerTarget{}, fmt.Errorf("invalid peer target %q, want <store>:<peer>[@addr]", value)
	}
	storeID, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || storeID == 0 {
		return migratepkg.PeerTarget{}, fmt.Errorf("invalid store id in %q", value)
	}
	peerID, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || peerID == 0 {
		return migratepkg.PeerTarget{}, fmt.Errorf("invalid peer id in %q", value)
	}
	return migratepkg.PeerTarget{StoreID: storeID, PeerID: peerID, TargetAdminAddr: addr}, nil
}

func runMigratePlanCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate plan", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	result, err := migratepkg.BuildPlan(*workDir)
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}

	_, _ = fmt.Fprintf(w, "Workdir              %s\n", result.WorkDir)
	_, _ = fmt.Fprintf(w, "Mode                 %s\n", result.Mode)
	_, _ = fmt.Fprintf(w, "Eligible             %t\n", result.Eligible)
	_, _ = fmt.Fprintf(w, "LocalCatalogRegions  %d\n", result.LocalCatalogRegions)
	if len(result.Blockers) > 0 {
		_, _ = fmt.Fprintf(w, "Blockers             %s\n", strings.Join(result.Blockers, "; "))
	}
	if len(result.Warnings) > 0 {
		_, _ = fmt.Fprintf(w, "Warnings             %s\n", strings.Join(result.Warnings, "; "))
	}
	if result.Next != "" {
		_, _ = fmt.Fprintf(w, "Next                 %s\n", result.Next)
	}
	return nil
}

func runMigrateStatusCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate status", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	runtimeFlags := bindMigrateRuntimeFlags(fs)
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	result, err := runReadStatus(runtimeFlags.config(*workDir))
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}

	_, _ = fmt.Fprintf(w, "Workdir  %s\n", result.WorkDir)
	_, _ = fmt.Fprintf(w, "Mode     %s\n", result.Mode)
	if result.StoreID != 0 {
		_, _ = fmt.Fprintf(w, "Store    %d\n", result.StoreID)
	}
	if result.RegionID != 0 {
		_, _ = fmt.Fprintf(w, "Region   %d\n", result.RegionID)
	}
	if result.PeerID != 0 {
		_, _ = fmt.Fprintf(w, "Peer     %d\n", result.PeerID)
	}
	if result.LocalCatalogRegions != 0 {
		_, _ = fmt.Fprintf(w, "Catalog  %d region(s)\n", result.LocalCatalogRegions)
	}
	if result.SeedSnapshotDir != "" {
		_, _ = fmt.Fprintf(w, "SeedDir  %s\n", result.SeedSnapshotDir)
		_, _ = fmt.Fprintf(w, "Seeded   %t\n", result.SeedSnapshotPresent)
	}
	if len(result.Warnings) > 0 {
		_, _ = fmt.Fprintf(w, "Warnings %s\n", strings.Join(result.Warnings, "; "))
	}
	if result.Next != "" {
		_, _ = fmt.Fprintf(w, "Next     %s\n", result.Next)
	}
	if result.ResumeHint != "" {
		_, _ = fmt.Fprintf(w, "Resume   %s\n", result.ResumeHint)
	}
	if result.Checkpoint != nil {
		_, _ = fmt.Fprintf(w, "Phase    %s\n", result.Checkpoint.Stage)
		_, _ = fmt.Fprintf(w, "Updated  %s\n", result.Checkpoint.UpdatedAt)
	}
	if result.Runtime != nil {
		_, _ = fmt.Fprintf(w, "Runtime  %s region=%d known=%t hosted=%t leader=%t\n", result.Runtime.Addr, result.Runtime.RegionID, result.Runtime.Known, result.Runtime.Hosted, result.Runtime.Leader)
		if result.Runtime.MembershipPeers != 0 {
			_, _ = fmt.Fprintf(w, "Peers    %d\n", result.Runtime.MembershipPeers)
		}
		if result.Runtime.LeaderPeerID != 0 {
			_, _ = fmt.Fprintf(w, "Leader   %d\n", result.Runtime.LeaderPeerID)
		}
		if result.Runtime.LocalPeerID != 0 {
			_, _ = fmt.Fprintf(w, "Local    %d\n", result.Runtime.LocalPeerID)
		}
		if result.Runtime.AppliedIndex != 0 || result.Runtime.AppliedTerm != 0 {
			_, _ = fmt.Fprintf(w, "Applied  index=%d term=%d\n", result.Runtime.AppliedIndex, result.Runtime.AppliedTerm)
		}
	}
	if result.RuntimeError != "" {
		_, _ = fmt.Fprintf(w, "Remote   %s\n", result.RuntimeError)
	}
	return nil
}

func runMigrateReportCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate report", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	runtimeFlags := bindMigrateRuntimeFlags(fs)
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	result, err := runBuildReport(runtimeFlags.config(*workDir))
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}

	_, _ = fmt.Fprintf(w, "Workdir        %s\n", result.WorkDir)
	_, _ = fmt.Fprintf(w, "Mode           %s\n", result.Mode)
	_, _ = fmt.Fprintf(w, "Stage          %s\n", result.Stage)
	_, _ = fmt.Fprintf(w, "Summary        %s\n", result.Summary)
	_, _ = fmt.Fprintf(w, "ReadyForInit   %t\n", result.ReadyForInit)
	_, _ = fmt.Fprintf(w, "ReadyForServe  %t\n", result.ReadyForServe)
	if result.ResumeHint != "" {
		_, _ = fmt.Fprintf(w, "ResumeHint     %s\n", result.ResumeHint)
	}
	if len(result.NextSteps) > 0 {
		_, _ = fmt.Fprintln(w, "NextSteps")
		for _, step := range result.NextSteps {
			_, _ = fmt.Fprintf(w, "  - %s\n", step)
		}
	}
	if result.Cluster != nil {
		_, _ = fmt.Fprintln(w, "Cluster")
		_, _ = fmt.Fprintf(w, "  source=%s addr=%s region=%d known=%t hosted=%t leader=%t leader_store=%d leader_peer=%d local_peer=%d peers=%d applied_index=%d applied_term=%d\n",
			result.Cluster.Source,
			result.Cluster.AdminAddr,
			result.Cluster.RegionID,
			result.Cluster.Known,
			result.Cluster.Hosted,
			result.Cluster.Leader,
			result.Cluster.LeaderStoreID,
			result.Cluster.LeaderPeerID,
			result.Cluster.LocalPeerID,
			result.Cluster.MembershipPeers,
			result.Cluster.AppliedIndex,
			result.Cluster.AppliedTerm,
		)
		if len(result.Cluster.Membership) > 0 {
			_, _ = fmt.Fprintln(w, "  membership")
			for _, peer := range result.Cluster.Membership {
				_, _ = fmt.Fprintf(w, "    - store=%d peer=%d\n", peer.StoreID, peer.PeerID)
			}
		}
	}
	if result.Status.Runtime != nil {
		_, _ = fmt.Fprintln(w, "Runtime")
		_, _ = fmt.Fprintf(w, "  addr=%s region=%d known=%t hosted=%t leader=%t leader_peer=%d local_peer=%d peers=%d applied_index=%d applied_term=%d\n",
			result.Status.Runtime.Addr,
			result.Status.Runtime.RegionID,
			result.Status.Runtime.Known,
			result.Status.Runtime.Hosted,
			result.Status.Runtime.Leader,
			result.Status.Runtime.LeaderPeerID,
			result.Status.Runtime.LocalPeerID,
			result.Status.Runtime.MembershipPeers,
			result.Status.Runtime.AppliedIndex,
			result.Status.Runtime.AppliedTerm,
		)
	}
	if result.Status.RuntimeError != "" {
		_, _ = fmt.Fprintf(w, "RuntimeError   %s\n", result.Status.RuntimeError)
	}
	return nil
}

func runMigrateInitCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate init", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	storeID := fs.Uint64("store", 0, "seed store id")
	regionID := fs.Uint64("region", 0, "seed region id")
	peerID := fs.Uint64("peer", 0, "seed peer id")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := migratepkg.Init(migratepkg.InitConfig{
		WorkDir:  *workDir,
		StoreID:  *storeID,
		RegionID: *regionID,
		PeerID:   *peerID,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}
	_, _ = fmt.Fprintf(w, "Workdir      %s\n", result.WorkDir)
	_, _ = fmt.Fprintf(w, "Mode         %s\n", result.Mode)
	_, _ = fmt.Fprintf(w, "Store        %d\n", result.StoreID)
	_, _ = fmt.Fprintf(w, "Region       %d\n", result.RegionID)
	_, _ = fmt.Fprintf(w, "Peer         %d\n", result.PeerID)
	_, _ = fmt.Fprintf(w, "SnapshotDir  %s\n", result.SnapshotDir)
	return nil
}

func runMigrateExpandCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate expand", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "optional seed workdir used to persist migration rollout checkpoints")
	addr := fs.String("addr", "", "leader store admin address")
	regionID := fs.Uint64("region", 0, "region id")
	snapshotFormat := fs.String("snapshot-format", "sst", "snapshot transport format: sst or logical")
	var targets peerTargetsFlag
	fs.Var(&targets, "target", "peer rollout target in <store>:<peer>[@addr] form; may be repeated")
	waitTimeout := fs.Duration("wait", 30*time.Second, "how long to wait for peer publication/hosting; 0 disables waiting")
	pollInterval := fs.Duration("poll-interval", 200*time.Millisecond, "poll interval while waiting")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	cfg := migratepkg.ExpandConfig{
		WorkDir:           strings.TrimSpace(*workDir),
		Addr:              strings.TrimSpace(*addr),
		RegionID:          *regionID,
		SnapshotFormat:    parseRegionSnapshotFormat(strings.TrimSpace(*snapshotFormat)),
		SnapshotFormatSet: true,
		WaitTimeout:       *waitTimeout,
		PollInterval:      *pollInterval,
		Targets:           targets,
	}
	if cfg.SnapshotFormat == pb.RegionSnapshotFormat(-1) {
		return fmt.Errorf("unsupported --snapshot-format %q", *snapshotFormat)
	}
	if len(targets) == 0 {
		return fmt.Errorf("at least one --target <store>:<peer>[@addr] is required")
	}
	result, err := runExpand(ctx, cfg)
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}
	_, _ = fmt.Fprintf(w, "LeaderAddr        %s\n", result.Addr)
	_, _ = fmt.Fprintf(w, "Region            %d\n", result.RegionID)
	_, _ = fmt.Fprintf(w, "SnapshotFormat    %s\n", strings.ToLower(cfg.SnapshotFormat.String()))
	for i, step := range result.Results {
		_, _ = fmt.Fprintf(w, "Step[%d]           store=%d peer=%d hosted=%t applied=%d\n",
			i, step.StoreID, step.PeerID, step.TargetHosted, step.TargetAppliedIdx)
	}
	return nil
}

func parseRegionSnapshotFormat(raw string) pb.RegionSnapshotFormat {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "logical":
		return pb.RegionSnapshotFormat_REGION_SNAPSHOT_FORMAT_LOGICAL
	case "sst":
		return pb.RegionSnapshotFormat_REGION_SNAPSHOT_FORMAT_SST
	default:
		return pb.RegionSnapshotFormat(-1)
	}
}

func runMigrateRemovePeerCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate remove-peer", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "optional seed workdir used to persist migration rollout checkpoints")
	addr := fs.String("addr", "", "leader store admin address")
	targetAddr := fs.String("target-addr", "", "target store admin address for removal wait checks")
	regionID := fs.Uint64("region", 0, "region id")
	peerID := fs.Uint64("peer", 0, "peer id to remove")
	waitTimeout := fs.Duration("wait", 30*time.Second, "how long to wait for peer removal; 0 disables waiting")
	pollInterval := fs.Duration("poll-interval", 200*time.Millisecond, "poll interval while waiting")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := runRemovePeer(context.Background(), migratepkg.RemovePeerConfig{
		WorkDir:         strings.TrimSpace(*workDir),
		Addr:            strings.TrimSpace(*addr),
		TargetAdminAddr: strings.TrimSpace(*targetAddr),
		RegionID:        *regionID,
		PeerID:          *peerID,
		WaitTimeout:     *waitTimeout,
		PollInterval:    *pollInterval,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}
	_, _ = fmt.Fprintf(w, "LeaderAddr        %s\n", result.Addr)
	if result.TargetAdminAddr != "" {
		_, _ = fmt.Fprintf(w, "TargetAdminAddr   %s\n", result.TargetAdminAddr)
	}
	_, _ = fmt.Fprintf(w, "Region            %d\n", result.RegionID)
	_, _ = fmt.Fprintf(w, "Peer              %d\n", result.PeerID)
	_, _ = fmt.Fprintf(w, "LeaderKnown       %t\n", result.LeaderKnown)
	_, _ = fmt.Fprintf(w, "TargetKnown       %t\n", result.TargetKnown)
	_, _ = fmt.Fprintf(w, "TargetHosted      %t\n", result.TargetHosted)
	return nil
}

func runMigrateTransferLeaderCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate transfer-leader", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "optional seed workdir used to persist migration rollout checkpoints")
	addr := fs.String("addr", "", "leader store admin address")
	targetAddr := fs.String("target-addr", "", "target store admin address for leader wait checks")
	regionID := fs.Uint64("region", 0, "region id")
	peerID := fs.Uint64("peer", 0, "target peer id")
	waitTimeout := fs.Duration("wait", 30*time.Second, "how long to wait for leader transfer; 0 disables waiting")
	pollInterval := fs.Duration("poll-interval", 200*time.Millisecond, "poll interval while waiting")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := runTransferLeader(context.Background(), migratepkg.TransferLeaderConfig{
		WorkDir:         strings.TrimSpace(*workDir),
		Addr:            strings.TrimSpace(*addr),
		TargetAdminAddr: strings.TrimSpace(*targetAddr),
		RegionID:        *regionID,
		PeerID:          *peerID,
		WaitTimeout:     *waitTimeout,
		PollInterval:    *pollInterval,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return writeIndentedJSON(w, result)
	}
	_, _ = fmt.Fprintf(w, "LeaderAddr        %s\n", result.Addr)
	if result.TargetAdminAddr != "" {
		_, _ = fmt.Fprintf(w, "TargetAdminAddr   %s\n", result.TargetAdminAddr)
	}
	_, _ = fmt.Fprintf(w, "Region            %d\n", result.RegionID)
	_, _ = fmt.Fprintf(w, "TargetPeer        %d\n", result.PeerID)
	_, _ = fmt.Fprintf(w, "LeaderKnown       %t\n", result.LeaderKnown)
	_, _ = fmt.Fprintf(w, "LeaderPeer        %d\n", result.LeaderPeerID)
	_, _ = fmt.Fprintf(w, "TargetLeader      %t\n", result.TargetLeader)
	return nil
}
