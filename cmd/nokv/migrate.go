package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	migratepkg "github.com/feichai0017/NoKV/raftstore/migrate"
)

var runExpand = migratepkg.Expand

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
	  expand   Expand a single-store seed into a replicated region`)
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
	targetAddr := fs.String("target-addr", "", "target store admin address for hosted-peer wait checks")
	regionID := fs.Uint64("region", 0, "region id")
	storeID := fs.Uint64("store", 0, "target store id")
	peerID := fs.Uint64("peer", 0, "target peer id")
	waitTimeout := fs.Duration("wait", 30*time.Second, "how long to wait for peer publication/hosting; 0 disables waiting")
	pollInterval := fs.Duration("poll-interval", 200*time.Millisecond, "poll interval while waiting")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	result, err := runExpand(ctx, migratepkg.ExpandConfig{
		Addr:         strings.TrimSpace(*addr),
		TargetAddr:   strings.TrimSpace(*targetAddr),
		RegionID:     *regionID,
		StoreID:      *storeID,
		PeerID:       *peerID,
		WaitTimeout:  *waitTimeout,
		PollInterval: *pollInterval,
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
	if result.TargetAddr != "" {
		_, _ = fmt.Fprintf(w, "TargetAddr        %s\n", result.TargetAddr)
	}
	_, _ = fmt.Fprintf(w, "Region            %d\n", result.RegionID)
	_, _ = fmt.Fprintf(w, "TargetStore       %d\n", result.StoreID)
	_, _ = fmt.Fprintf(w, "TargetPeer        %d\n", result.PeerID)
	_, _ = fmt.Fprintf(w, "LeaderKnown       %t\n", result.LeaderKnown)
	_, _ = fmt.Fprintf(w, "TargetKnown       %t\n", result.TargetKnown)
	_, _ = fmt.Fprintf(w, "TargetHosted      %t\n", result.TargetHosted)
	if result.TargetLocalPeerID != 0 {
		_, _ = fmt.Fprintf(w, "TargetLocalPeer   %d\n", result.TargetLocalPeerID)
	}
	return nil
}
