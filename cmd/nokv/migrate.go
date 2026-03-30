package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	migratepkg "github.com/feichai0017/NoKV/raftstore/migrate"
)

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
		return fmt.Errorf("migrate init: not implemented")
	case "status":
		return runMigrateStatusCmd(w, subargs)
	case "expand":
		return fmt.Errorf("migrate expand: not implemented")
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
