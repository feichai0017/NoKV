// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/config"
	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/feichai0017/NoKV/coordinator/rootview"
)

// runAuditCmd implements the `nokv audit` subcommand: dial a remote
// metadata-root cluster, materialize one snapshot, and surface
// authority/finality anomalies through the coordinator/audit module.
//
// The command exists so the audit checker has a real production entry point;
// `coordinator/audit` was previously only reachable from its own unit tests.
func runAuditCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	holderID := fs.String("holder-id", "", "stable holder id recorded in the audit report (optional)")
	asJSON := fs.Bool("json", false, "emit the audit report as JSON (default: short human-readable text)")
	tracesPath := fs.String("traces", "", "path to a reply-trace file evaluated against the snapshot (optional)")
	traceFormat := fs.String("trace-format", string(coordaudit.ReplyTraceFormatNoKV), "reply-trace format: nokv|etcd-read-index|etcd-lease-renew|crdb-lease-start")
	dialTimeout := fs.Duration("dial-timeout", 5*time.Second, "metadata-root gRPC dial timeout")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve --root-peer when omitted")
	scope := fs.String("scope", "host", "scope for config-resolved root peer addresses: host|docker")
	var rootPeerFlags []string
	fs.Func("root-peer", "remote metadata root gRPC peer mapping nodeID=address (repeatable, exactly 3)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("root-peer value cannot be empty")
		}
		rootPeerFlags = append(rootPeerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*configPath) != "" && len(rootPeerFlags) == 0 {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid audit scope %q (expected host|docker)", *scope)
		}
		cfg, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("audit load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("audit validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if cfg.MetaRoot != nil {
			for id, paddr := range cfg.MetaRootServicePeers(scopeNorm) {
				rootPeerFlags = append(rootPeerFlags, fmt.Sprintf("%d=%s", id, paddr))
			}
		}
	}

	rootPeers, err := parseReplicatedRootPeers(rootPeerFlags)
	if err != nil {
		return err
	}
	if len(rootPeers) != 3 {
		return fmt.Errorf("audit requires exactly 3 --root-peer values (got %d)", len(rootPeers))
	}

	store, err := rootview.OpenRootRemoteStore(rootview.RemoteRootConfig{
		Targets:     rootPeers,
		DialTimeout: *dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("audit open remote metadata root: %w", err)
	}
	defer func() { _ = store.Close() }()

	snapshot, err := store.Load()
	if err != nil {
		return fmt.Errorf("audit load metadata root snapshot: %w", err)
	}

	report := coordaudit.BuildReport(snapshot, strings.TrimSpace(*holderID), time.Now().UnixNano())

	var traceAnomalies []coordaudit.ReplyTraceAnomaly
	if path := strings.TrimSpace(*tracesPath); path != "" {
		records, err := loadReplyTraces(path, *traceFormat)
		if err != nil {
			return err
		}
		traceAnomalies = coordaudit.EvaluateReplyTrace(report, records)
	}

	if *asJSON {
		return writeAuditJSON(w, report, traceAnomalies)
	}
	return writeAuditText(w, report, traceAnomalies)
}

func loadReplyTraces(path, format string) ([]coordaudit.ReplyTraceRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("audit read traces %q: %w", path, err)
	}
	records, err := coordaudit.DecodeReplyTrace(raw, coordaudit.ReplyTraceFormat(format))
	if err != nil {
		return nil, fmt.Errorf("audit decode traces %q (format=%s): %w", path, format, err)
	}
	return records, nil
}

func writeAuditText(w io.Writer, report coordaudit.Report, anomalies []coordaudit.ReplyTraceAnomaly) error {
	_, _ = fmt.Fprintf(w, "holder_id=%q now=%d root_revision=%d catch_up=%s\n",
		report.HolderID, report.NowUnixNano, report.RootDescriptorRevision, report.CatchUpState)
	_, _ = fmt.Fprintf(w, "active_grant: holder=%q era=%d grant_id=%q\n",
		report.CurrentHolderID, report.CurrentEra, report.ActiveGrant.GrantID)
	_, _ = fmt.Fprintf(w, "retired_grants=%d grant_inheritances=%d retired_era_floors=%d\n",
		len(report.RetiredGrants), len(report.GrantInheritances), len(report.RetiredEraFloors))
	_, _ = fmt.Fprintf(w, "authority_completion=%s\n", report.AuthorityCompletion)
	_, _ = fmt.Fprintf(w, "anomalies: retired_not_inherited=%t invalid_successor_bound=%t orphan_inheritance=%t finality_defect=%q\n",
		report.Anomalies.RetiredGrantNotInherited, report.Anomalies.InvalidSuccessorBound,
		report.Anomalies.OrphanInheritance, report.Anomalies.FinalityDefect)
	if len(anomalies) == 0 {
		return nil
	}
	_, _ = fmt.Fprintf(w, "reply_trace_anomalies=%d:\n", len(anomalies))
	for _, a := range anomalies {
		_, _ = fmt.Fprintf(w, "  - index=%d kind=%q duty=%q era=%d reason=%q\n",
			a.Index, a.Kind, a.Duty, a.Era, a.Reason)
	}
	return nil
}

func writeAuditJSON(w io.Writer, report coordaudit.Report, anomalies []coordaudit.ReplyTraceAnomaly) error {
	payload := map[string]any{
		"report":                report,
		"reply_trace_anomalies": anomalies,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}
