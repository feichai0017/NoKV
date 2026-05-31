// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
	case "coordinator":
		err = runCoordinatorCmd(os.Stdout, args)
	case "meta-root":
		err = runMetaRootCmd(os.Stdout, args)
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
	  coordinator Start coordinator gRPC service (control plane)
	  meta-root Start metadata root gRPC service

	Other binaries:
	  nokv-fsmeta                         Start the local Pebble fsmeta gateway
	  cargo run -p nokv-raftstore-server  Start the Rust distributed data plane

Run "nokv <command> -h" for command-specific flags.`)
}
