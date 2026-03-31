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

	migratepkg "github.com/feichai0017/NoKV/raftstore/migrate"
)

var runExpand = migratepkg.Expand
var runRemovePeer = migratepkg.RemovePeer
var runTransferLeader = migratepkg.TransferLeader

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
	  expand   Expand a single-store seed into a replicated region
	  remove-peer      Remove one peer from a replicated region
	  transfer-leader  Transfer region leadership to a specific peer`)
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
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
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
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	result, err := migratepkg.ReadStatus(*workDir)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
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
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
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
	addr := fs.String("addr", "", "leader store admin address")
	regionID := fs.Uint64("region", 0, "region id")
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
		Addr:         strings.TrimSpace(*addr),
		RegionID:     *regionID,
		WaitTimeout:  *waitTimeout,
		PollInterval: *pollInterval,
		Targets:      targets,
	}
	if len(targets) == 0 {
		return fmt.Errorf("at least one --target <store>:<peer>[@addr] is required")
	}
	result, err := runExpand(ctx, cfg)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	_, _ = fmt.Fprintf(w, "LeaderAddr        %s\n", result.Addr)
	_, _ = fmt.Fprintf(w, "Region            %d\n", result.RegionID)
	for i, step := range result.Results {
		_, _ = fmt.Fprintf(w, "Step[%d]           store=%d peer=%d hosted=%t applied=%d\n",
			i, step.StoreID, step.PeerID, step.TargetHosted, step.TargetAppliedIdx)
	}
	return nil
}

func runMigrateRemovePeerCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("migrate remove-peer", flag.ContinueOnError)
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
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
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
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
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
