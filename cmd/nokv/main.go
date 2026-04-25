package main

import (
	"fmt"
	"io"
	"os"
)

var exit = os.Exit

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "stats":
		err = runStatsCmd(os.Stdout, args)
	case "execution":
		err = runExecutionCmd(os.Stdout, args)
	case "manifest":
		err = runManifestCmd(os.Stdout, args)
	case "vlog":
		err = runVlogCmd(os.Stdout, args)
	case "regions":
		err = runRegionsCmd(os.Stdout, args)
	case "migrate":
		err = runMigrateCmd(os.Stdout, args)
	case "serve":
		err = runServeCmd(os.Stdout, args)
	case "coordinator":
		err = runCoordinatorCmd(os.Stdout, args)
	case "meta-root":
		err = runMetaRootCmd(os.Stdout, args)
	case "mount":
		err = runMountCmd(os.Stdout, args)
	case "quota":
		err = runQuotaCmd(os.Stdout, args)
	case "eunomia-audit":
		err = runEunomiaAuditCmd(os.Stdout, args)
	case "help", "-h", "--help":
		printUsage(os.Stdout)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exit(1)
	}
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, `Usage: nokv <command> [flags]

	Commands:
	  stats     Dump runtime backlog metrics (requires working directory or expvar endpoint)
	  execution Query raftstore execution-plane diagnostics from admin RPC
	  manifest  Inspect manifest state, levels, and value log metadata
	  vlog      List value log segments and active head
	  regions   Show the local peer catalog used for store recovery
	  migrate   Inspect or convert a standalone workdir for distributed mode
	  serve     Start NoKV gRPC service backed by a local raftstore
	  coordinator Start coordinator gRPC service (control plane)
	  meta-root Start metadata root gRPC service
	  mount     Register, retire, or list rooted fsmeta mounts
	  quota     Set, clear, or list rooted fsmeta quota fences
	  eunomia-audit Read-only eunomia audit against a live meta-root cluster

Run "nokv <command> -h" for command-specific flags.`)
}
